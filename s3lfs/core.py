import contextlib
import fnmatch
import glob
import gzip
import hashlib
import json
import mmap
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import CancelledError, ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Optional, Union
from uuid import uuid4

import boto3
import portalocker
import yaml
from boto3.s3.transfer import TransferConfig
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    NoCredentialsError,
    PartialCredentialsError,
)
from tqdm import tqdm
from urllib3.exceptions import SSLError

from s3lfs import metrics
from s3lfs.path_resolver import PathResolver
from s3lfs.utils import find_git_root

# Constants
DEFAULT_CHUNK_SIZE = 5 * 1024 * 1024 * 1024  # 5 GB
DEFAULT_BUFFER_SIZE = 1024 * 1024  # 1 MB
DEFAULT_THREAD_POOL_SIZE = 8  # Optimal for bandwidth-limited scenarios
DEFAULT_MULTIPART_THRESHOLD = 5 * 1024 * 1024 * 1024  # 5 GB
DEFAULT_MAX_CONCURRENCY = 15  # Balanced for bandwidth-limited downloads

# Common error messages
ERROR_MESSAGES = {
    "no_credentials": "AWS credentials are missing. Please configure them or use --no-sign-request.",
    "partial_credentials": "Incomplete AWS credentials. Check your AWS configuration.",
    "invalid_credentials": "Invalid AWS credentials. Please verify your access key and secret key.",
    "s3_access_denied": "Invalid or insufficient AWS credentials for bucket '{bucket_name}'.",
    "acceleration_not_supported": "Transfer acceleration is not supported for unsigned requests.",
}


def retry(times, exceptions):
    """
    Retry Decorator
    Retries the wrapped function/method `times` times if the exceptions listed
    in ``exceptions`` are thrown
    :param times: The number of times to repeat the wrapped function/method
    :type times: Int
    :param Exceptions: Lists of exceptions that trigger a retry attempt
    :type Exceptions: Tuple of Exceptions
    """

    def decorator(func):
        def newfn(*args, **kwargs):
            attempt = 0
            while attempt < times:
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    print(
                        f"Exception thrown when attempting to run {func}, attempt "
                        f"{attempt} of {times}: {exc}"
                    )
                    attempt += 1
            return func(*args, **kwargs)

        return newfn

    return decorator


class S3LFS:
    def __init__(
        self,
        bucket_name=None,
        manifest_file=".s3_manifest.yaml",
        repo_prefix=None,
        encryption=True,
        no_sign_request=False,
        temp_dir=None,
        chunk_size=DEFAULT_CHUNK_SIZE,
        s3_factory=None,
        use_acceleration=False,
    ):
        """
        :param bucket_name: Name of the S3 bucket (can be stored in manifest)
        :param manifest_file: Path to the local manifest file (YAML or JSON)
        :param repo_prefix: A unique prefix to isolate this repository's files
        :param encryption: If True, use AES256 server-side encryption
        :param no_sign_request: If True, use unsigned requests
        :param temp_dir: Path to the temporary directory for compression/decompression
        :param chunk_size: Size of chunks for multipart uploads (default: 5 GB)
        :param s3_factory: Custom S3 client factory function (for testing)
        :param use_acceleration: If True, enable S3 Transfer Acceleration
        """
        self.chunk_size = chunk_size
        self.use_acceleration = use_acceleration

        def default_s3_factory(no_sign_request):
            """Default S3 client factory with proper boto3 usage."""
            if no_sign_request:
                if self.use_acceleration:
                    raise RuntimeError(ERROR_MESSAGES["acceleration_not_supported"])
                config = Config(signature_version=UNSIGNED)
                return boto3.client("s3", config=config)
            else:
                if self.use_acceleration:
                    # Use transfer acceleration endpoint
                    return boto3.client(
                        "s3", config=Config(s3={"use_accelerate_endpoint": True})
                    )
                else:
                    return boto3.client("s3")

        self.s3_factory = s3_factory if s3_factory is not None else default_s3_factory

        # Set the temporary directory to the base of the repository if not provided
        self.temp_dir = Path(temp_dir or ".s3lfs_temp")
        self.temp_dir.mkdir(parents=True, exist_ok=True)  # Ensure the directory exists

        # Use a file-based lock for cross-process synchronization
        self._lock_file = self.temp_dir / ".s3lfs.lock"

        if no_sign_request:
            # If we're not signing, we can't use multipart. Set the threshold to the max.
            self.config = TransferConfig(
                multipart_threshold=DEFAULT_MULTIPART_THRESHOLD,
                max_concurrency=DEFAULT_MAX_CONCURRENCY,
            )
        else:
            self.config = TransferConfig(max_concurrency=DEFAULT_MAX_CONCURRENCY)
        self.thread_local = threading.local()
        self.manifest_file = Path(manifest_file)

        # Separate cache file - should NOT be version controlled
        # Use same format as manifest (YAML or JSON)
        cache_suffix = (
            ".yaml" if self.manifest_file.suffix in [".yaml", ".yml"] else ".json"
        )
        cache_file_name = self.manifest_file.stem + "_cache" + cache_suffix
        self.cache_file = self.manifest_file.parent / cache_file_name

        self.no_sign_request = no_sign_request
        self.load_manifest()
        self.load_cache()

        # Use the stored bucket name if none is provided
        with self._lock_context():
            if bucket_name:
                self.bucket_name = bucket_name
                self.manifest["bucket_name"] = bucket_name
            else:
                self.bucket_name = self.manifest.get("bucket_name")

        if not self.bucket_name:
            raise ValueError(
                "Bucket name must be provided either as a parameter or stored in the manifest. "
                "Use 'initialize_repo()' to set up the repository configuration."
            )

        with self._lock_context():
            if repo_prefix:
                self.repo_prefix = repo_prefix
                self.manifest["repo_prefix"] = repo_prefix
            else:
                self.repo_prefix = self.manifest.get("repo_prefix", "s3lfs")
            self.save_manifest()

        self.encryption = encryption

        # Initialize PathResolver for consistent path handling
        # Find git root for path resolution
        manifest_dir = Path(self.manifest_file).parent.resolve()
        git_root = find_git_root(start_path=manifest_dir)

        # Determine the base directory for PathResolver
        if git_root:
            # Check if manifest is within git repo
            try:
                manifest_dir.relative_to(git_root)
                # Manifest is within git repo, use git root
                self.path_resolver = PathResolver(git_root)
            except ValueError:
                # Manifest is outside git repo, use manifest directory
                self.path_resolver = PathResolver(manifest_dir)
        else:
            # If not in a git repo, use manifest directory
            self.path_resolver = PathResolver(manifest_dir)

        self._shutdown_requested = False  # Flag to track shutdown requests
        signal.signal(signal.SIGINT, self._handle_sigint)  # Register signal handler

    def _handle_sigint(self, signum, frame):
        """
        Handle SIGINT (Ctrl+C) to gracefully shut down parallel operations.
        """
        print("\n⚠️ Interrupt received. Shutting down...")
        self._shutdown_requested = True
        sys.exit(1)  # Exit the program

    @contextmanager
    def _lock_context(self):
        """
        Context manager for acquiring and releasing the file-based lock using portalocker.
        """
        lock = open(self._lock_file, "w")  # Open the lock file in write mode
        try:
            portalocker.lock(lock, portalocker.LOCK_EX)  # Acquire an exclusive lock
            yield lock  # Provide the lock to the context
        finally:
            portalocker.unlock(lock)  # Release the lock
            lock.close()  # Close the file handle

    def _get_s3_client(self):
        """Ensures each thread gets its own instance of the S3 client with appropriate authentication handling."""
        if not hasattr(self.thread_local, "s3"):
            try:
                self.thread_local.s3 = self.s3_factory(self.no_sign_request)
            except NoCredentialsError:
                raise RuntimeError(ERROR_MESSAGES["no_credentials"])
            except PartialCredentialsError:
                raise RuntimeError(ERROR_MESSAGES["partial_credentials"])
            except ClientError as e:
                if e.response["Error"]["Code"] in [
                    "InvalidAccessKeyId",
                    "SignatureDoesNotMatch",
                ]:
                    raise RuntimeError(ERROR_MESSAGES["invalid_credentials"])
                raise RuntimeError(f"Error initializing S3 client: {e}")

        return self.thread_local.s3

    def initialize_repo(self):
        """
        Initialize the repository with a bucket name and a repo-specific prefix.
        Also updates .gitignore to exclude S3LFS cache files.

        :param bucket_name: Name of the S3 bucket to use
        :param repo_prefix: A unique prefix for this repository in the bucket
        """
        with self._lock_context():
            # Store configuration in manifest
            if self.bucket_name is not None:
                self.manifest["bucket_name"] = str(self.bucket_name)  # type: ignore
            if self.repo_prefix is not None:
                self.manifest["repo_prefix"] = str(self.repo_prefix)  # type: ignore
            self.save_manifest()

        # Update .gitignore to exclude cache files
        self._update_gitignore()

        print("✅ Successfully initialized S3LFS with:")
        print(f"   Bucket Name: {self.bucket_name}")
        print(f"   Repo Prefix: {self.repo_prefix}")
        print(f"Manifest file saved as {self.manifest_file.name}")

    def _update_gitignore(self):
        """
        Update .gitignore to exclude S3LFS cache files and temporary directories.
        Creates .gitignore if it doesn't exist, or appends to existing one.
        """
        gitignore_path = Path(".gitignore")

        # S3LFS patterns to add
        s3lfs_patterns = [
            "",  # Empty line for separation
            "# S3LFS cache and temporary files - should not be version controlled",
            "*_cache.json",
            "*_cache.yaml",
            ".s3lfs_temp/",
            "*.s3lfs.lock",
        ]

        # Check if .gitignore exists and read current content
        existing_content = []
        if gitignore_path.exists():
            with open(gitignore_path, "r") as f:
                existing_content = [line.rstrip() for line in f.readlines()]

        # Check which patterns are already present
        patterns_to_add = []
        for pattern in s3lfs_patterns:
            if pattern.startswith("#") or pattern == "":
                # Always add comments and empty lines for structure
                patterns_to_add.append(pattern)
            elif pattern not in existing_content:
                patterns_to_add.append(pattern)

        # Only update if we have patterns to add
        if any(p for p in patterns_to_add if not p.startswith("#") and p != ""):
            # Check if we already have S3LFS section
            has_s3lfs_section = any("S3LFS" in line for line in existing_content)

            if not has_s3lfs_section:
                # Add all patterns including header
                with open(gitignore_path, "a") as f:
                    for pattern in patterns_to_add:
                        f.write(f"{pattern}\n")
                print("📝 Updated .gitignore to exclude S3LFS cache files")
            else:
                # Only add missing patterns (without header)
                missing_patterns = [
                    p for p in patterns_to_add if not p.startswith("#") and p != ""
                ]
                if missing_patterns:
                    with open(gitignore_path, "a") as f:
                        for pattern in missing_patterns:
                            f.write(f"{pattern}\n")
                    print(
                        f"📝 Added {len(missing_patterns)} missing S3LFS patterns to .gitignore"
                    )
        else:
            print("✅ .gitignore already contains S3LFS cache exclusions")

    def load_manifest(self):
        """Load the local manifest (YAML or JSON format)."""
        if self.manifest_file.exists():
            with open(self.manifest_file, "r") as f:
                # Detect format based on extension
                if self.manifest_file.suffix in [".yaml", ".yml"]:
                    self.manifest = yaml.safe_load(f) or {"files": {}}
                else:
                    self.manifest = json.load(f)
        else:
            self.manifest = {"files": {}}  # Use file paths as keys

    def save_manifest(self):
        """Save the manifest back to disk atomically (YAML or JSON format)."""
        temp_file = self.manifest_file.with_suffix(
            ".tmp"
        )  # Temporary file in the same directory
        try:
            # Write the manifest to a temporary file
            with open(temp_file, "w") as f:
                # Detect format based on extension
                if self.manifest_file.suffix in [".yaml", ".yml"]:
                    yaml.safe_dump(
                        self.manifest, f, default_flow_style=False, sort_keys=True
                    )
                else:
                    json.dump(self.manifest, f, indent=4, sort_keys=True)

            # Atomically move the temporary file to the target location
            temp_file.replace(self.manifest_file)
        except Exception as e:
            print(f"❌ Failed to save manifest: {e}")
            if temp_file.exists():
                temp_file.unlink()  # Clean up the temporary file

    def load_cache(self):
        """Load the hash cache from a separate cache file (YAML or JSON format)."""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, "r") as f:
                    # Detect format based on extension
                    if self.cache_file.suffix in [".yaml", ".yml"]:
                        self.hash_cache = yaml.safe_load(f) or {}
                    else:
                        self.hash_cache = json.load(f)
            except (json.JSONDecodeError, yaml.YAMLError, IOError) as e:
                print(
                    f"⚠️ Warning: Failed to load cache file, starting with empty cache: {e}"
                )
                self.hash_cache = {}
        else:
            self.hash_cache = {}

    def save_cache(self):
        """Save the hash cache back to disk atomically (YAML or JSON format)."""
        temp_file = self.cache_file.with_suffix(".tmp")
        try:
            # Write the cache to a temporary file
            with open(temp_file, "w") as f:
                # Detect format based on extension
                if self.cache_file.suffix in [".yaml", ".yml"]:
                    yaml.safe_dump(
                        self.hash_cache, f, default_flow_style=False, sort_keys=True
                    )
                else:
                    json.dump(self.hash_cache, f, indent=4, sort_keys=True)

            # Atomically move the temporary file to the target location
            temp_file.replace(self.cache_file)
        except Exception as e:
            print(f"❌ Failed to save cache: {e}")
            if temp_file.exists():
                temp_file.unlink()  # Clean up the temporary file

    def hash_file(self, file_path: Union[str, Path], method: str = "auto") -> str:
        """
        Compute a unique SHA-256 hash of the file using its content and relative path.
        Supports multiple hashing methods for performance optimization.

        :param file_path: Path to the file to hash.
        :param method: Hashing method to use. Options are:
                    - "auto": Automatically select the best method.
                    - "mmap": Use memory-mapped files (default for non-empty files).
                    - "iter": Use an iterative read approach (fallback for empty files).
                    - "cli": Use the `sha256sum` CLI utility (POSIX only).
        :return: The computed SHA-256 hash as a hexadecimal string.
        """
        file_path = Path(file_path)

        # Ensure the file exists
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Automatically select the best method if "auto" is specified
        if method == "auto":
            if file_path.stat().st_size == 0:  # Empty file
                method = "iter"
            elif shutil.which("sha256sum"):
                # Prefer CLI - no GIL contention, better parallelism
                method = "cli"
            else:
                method = "mmap"

        # Use the selected hashing method
        if method == "mmap":
            return self._hash_file_mmap(file_path)
        elif method == "iter":
            return self._hash_file_iter(file_path)
        elif method == "cli":
            return self._hash_file_cli(file_path)
        else:
            raise ValueError(f"Unsupported hashing method: {method}")

    def hash_file_cached(
        self, file_path: Union[str, Path], method: str = "auto"
    ) -> str:
        """
        Compute SHA-256 hash with caching based on file metadata (mtime, size, inode).
        Returns cached hash if file hasn't changed, otherwise computes and caches new hash.
        This method is multi-process safe using file-based locking.

        :param file_path: Path to the file to hash.
        :param method: Hashing method to use if computation is needed.
        :return: The computed SHA-256 hash as a hexadecimal string.
        """
        file_path = Path(file_path)
        file_path_str = str(file_path.as_posix())

        # Ensure the file exists
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Get current file metadata
        stat = file_path.stat()
        current_metadata = {
            "size": stat.st_size,
            "mtime": stat.st_mtime,
            "inode": getattr(
                stat, "st_ino", None
            ),  # inode may not exist on all platforms
        }

        # Use file lock for multi-process safety
        with self._lock_context():
            # Reload cache to get latest state from other processes
            self.load_cache()

            # Check if we have cached data for this file
            cached_data = self.hash_cache.get(file_path_str)
            if cached_data:
                cached_metadata = cached_data.get("metadata", {})

                # Compare metadata to see if file has changed
                if (
                    cached_metadata.get("size") == current_metadata["size"]
                    and cached_metadata.get("mtime") == current_metadata["mtime"]
                    and cached_metadata.get("inode") == current_metadata["inode"]
                ):
                    # File hasn't changed, return cached hash
                    return cached_data["hash"]

            # File has changed or no cache exists, compute new hash
            # Release lock while computing hash (can be expensive)
            pass

        # Compute hash outside of lock to avoid blocking other processes
        new_hash = self.hash_file(file_path, method)

        # Acquire lock again to update cache
        with self._lock_context():
            # Reload cache again in case it changed while we were computing hash
            self.load_cache()

            # Double-check if another process already computed this hash
            cached_data = self.hash_cache.get(file_path_str)
            if cached_data:
                cached_metadata = cached_data.get("metadata", {})
                if (
                    cached_metadata.get("size") == current_metadata["size"]
                    and cached_metadata.get("mtime") == current_metadata["mtime"]
                    and cached_metadata.get("inode") == current_metadata["inode"]
                ):
                    # Another process computed it while we were working
                    return cached_data["hash"]

            # Cache the new hash with metadata
            self.hash_cache[file_path_str] = {
                "hash": new_hash,
                "metadata": current_metadata,
                "timestamp": time.time(),  # When hash was computed
            }

            # Save cache with updated data
            self.save_cache()

        return new_hash

    def get_file_status(self, file_path: Union[str, Path]) -> dict:
        """
        Get comprehensive status information about a file including cache status.

        :param file_path: Path to the file to check.
        :return: Dictionary with file status information.
        """
        file_path = Path(file_path)
        file_path_str = str(file_path.as_posix())

        if not file_path.exists():
            return {"exists": False, "cached": False}

        stat = file_path.stat()
        current_metadata = {
            "size": stat.st_size,
            "mtime": stat.st_mtime,
            "inode": getattr(stat, "st_ino", None),
        }

        # Check cache status - reload cache to get latest state
        with self._lock_context():
            self.load_cache()
            cached_data = self.hash_cache.get(file_path_str)

        is_cached = False
        cache_valid = False

        if cached_data:
            is_cached = True
            cached_metadata = cached_data.get("metadata", {})
            cache_valid = (
                cached_metadata.get("size") == current_metadata["size"]
                and cached_metadata.get("mtime") == current_metadata["mtime"]
                and cached_metadata.get("inode") == current_metadata["inode"]
            )

        return {
            "exists": True,
            "size": current_metadata["size"],
            "mtime": current_metadata["mtime"],
            "cached": is_cached,
            "cache_valid": cache_valid,
            "cached_hash": cached_data.get("hash") if cached_data else None,
            "cache_timestamp": cached_data.get("timestamp") if cached_data else None,
        }

    def clear_hash_cache(self, file_path: Union[str, Path, None] = None):
        """
        Clear hash cache for a specific file or all files.
        This method is multi-process safe using file-based locking.

        :param file_path: If provided, clear cache only for this file. If None, clear all cache.
        """
        with self._lock_context():
            self.load_cache()  # Get latest state

            if file_path is None:
                # Clear all cache
                self.hash_cache = {}
                print("🗑 Cleared all hash cache entries.")
            else:
                # Clear cache for specific file
                file_path_str = str(Path(file_path).as_posix())
                if file_path_str in self.hash_cache:
                    del self.hash_cache[file_path_str]
                    print(f"🗑 Cleared hash cache for '{file_path}'.")

            self.save_cache()

    def cleanup_stale_cache(self, max_age_days: int = 30):
        """
        Remove cache entries for files that no longer exist or are very old.
        This method is multi-process safe using file-based locking.

        :param max_age_days: Remove cache entries older than this many days.
        """
        with self._lock_context():
            self.load_cache()  # Get latest state

            current_time = time.time()
            max_age_seconds = max_age_days * 24 * 60 * 60

            stale_entries = []

            for file_path_str, cached_data in self.hash_cache.items():
                # Check if file still exists
                if not Path(file_path_str).exists():
                    stale_entries.append(file_path_str)
                    continue

                # Check if cache entry is too old
                cache_timestamp = cached_data.get("timestamp", 0)
                if current_time - cache_timestamp > max_age_seconds:
                    stale_entries.append(file_path_str)

            # Remove stale entries
            for file_path_str in stale_entries:
                del self.hash_cache[file_path_str]

            if stale_entries:
                print(f"🗑 Cleaned up {len(stale_entries)} stale cache entries.")
                self.save_cache()  # Only save if changes were made

    def track_modified_files_cached(self, silence=True):
        """
        Check manifest for outdated hashes using cached hashing and upload changed files in parallel.
        This is an optimized version of track_modified_files that uses hash caching.
        """
        files_to_upload = []
        cache_hits = 0
        cache_misses = 0

        with self._lock_context():
            files_to_check = list(
                self.manifest["files"].keys()
            )  # Files listed in the manifest

        if not files_to_check:
            print(
                "⚠️ No files found in manifest. Use 's3lfs track <path>' to track files first."
            )
            return

        print(f"🔍 Checking {len(files_to_check)} tracked files for modifications...")

        # Use cached hashing for better performance with progress indication
        with tqdm(
            total=len(files_to_check), desc="Checking files", unit="file"
        ) as pbar:
            for file_path in files_to_check:
                try:
                    # Get file status to check cache validity
                    status = self.get_file_status(file_path)

                    if not status["exists"]:
                        print(f"⚠️ Warning: File {file_path} is missing. Skipping.")
                        pbar.update(1)
                        continue

                    # Use cached hash if available and valid
                    if status["cache_valid"]:
                        current_hash = status["cached_hash"]
                        cache_hits += 1
                    else:
                        current_hash = self.hash_file_cached(file_path)
                        cache_misses += 1

                    with self._lock_context():
                        stored_hash = self.manifest["files"].get(file_path)

                    if current_hash != stored_hash:
                        print(f"📝 File {file_path} has changed. Marking for upload.")
                        files_to_upload.append(file_path)

                    # Update progress bar with current status
                    pbar.set_postfix(
                        {
                            "changed": len(files_to_upload),
                            "cache_hits": cache_hits,
                            "cache_misses": cache_misses,
                        }
                    )
                    pbar.update(1)

                except Exception as e:
                    print(f"❌ Error processing {file_path}: {e}")
                    pbar.update(1)
                    continue

        if not silence:
            print(f"📊 Hash cache performance: {cache_hits} hits, {cache_misses} misses")

        # Upload files in parallel if needed
        if files_to_upload:
            print(f"📤 Uploading {len(files_to_upload)} modified file(s) in parallel...")
            self.parallel_upload(files_to_upload, silence=silence)

            # Save updated manifest (including cache)
            with self._lock_context():
                self.save_manifest()
        else:
            print("✅ No modified files needing upload.")

    def _hash_file_mmap(self, file_path):
        """
        Compute the SHA-256 hash using memory-mapped files.
        """
        if metrics.is_enabled():
            tracker = metrics.get_tracker()
            with tracker.track_task("hashing", str(file_path)):
                hasher = hashlib.sha256()
                with open(file_path, "rb") as f:
                    with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                        hasher.update(mm)
                return hasher.hexdigest()
        else:
            hasher = hashlib.sha256()
            with open(file_path, "rb") as f:
                with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                    hasher.update(mm)
            return hasher.hexdigest()

    def _hash_file_iter(self, file_path, chunk_size=DEFAULT_BUFFER_SIZE):
        """
        Compute the SHA-256 hash by iteratively reading the file in chunks.
        """
        if metrics.is_enabled():
            tracker = metrics.get_tracker()
            with tracker.track_task("hashing", str(file_path)):
                hasher = hashlib.sha256()
                with open(file_path, "rb") as f:
                    while chunk := f.read(chunk_size):
                        hasher.update(chunk)
                return hasher.hexdigest()
        else:
            hasher = hashlib.sha256()
            with open(file_path, "rb") as f:
                while chunk := f.read(chunk_size):
                    hasher.update(chunk)
            return hasher.hexdigest()

    def _hash_file_cli(self, file_path):
        """
        Compute the SHA-256 hash using the `sha256sum` CLI utility (POSIX only).
        """
        result = subprocess.run(
            ["sha256sum", str(file_path)],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.split()[0]  # Extract the hash from the output

    def md5_file(self, file_path: Union[str, Path], method: str = "auto") -> str:
        """
        Compute an MD5 hash of the file using its content.
        Supports multiple hashing methods for performance optimization.

        :param file_path: Path to the file to hash.
        :param method: Hashing method to use. Options are:
                    - "auto": Automatically select the best method.
                    - "mmap": Use memory-mapped files (default for non-empty files).
                    - "iter": Use an iterative read approach (fallback for empty files).
                    - "cli": Use the `md5sum` CLI utility (POSIX only).
        :return: The computed MD5 hash as a hexadecimal string.
        """
        file_path = Path(file_path)

        # Ensure the file exists
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Automatically select the best method if "auto" is specified
        if method == "auto":
            if file_path.stat().st_size == 0:  # Empty file
                method = "iter"
            elif sys.platform.startswith("linux") and shutil.which("md5sum"):
                method = "cli"
            elif sys.platform.startswith("darwin") and shutil.which("md5"):
                method = "cli"
            else:
                method = "mmap"

        # Use the selected hashing method
        if method == "mmap":
            return self._md5_file_mmap(file_path)
        elif method == "iter":
            return self._md5_file_iter(file_path)
        elif method == "cli":
            return self._md5_file_cli(file_path)
        else:
            raise ValueError(f"Unsupported MD5 hashing method: {method}")

    def _md5_file_mmap(self, file_path):
        """
        Compute the MD5 hash using memory-mapped files.
        """
        hasher = hashlib.md5()
        with open(file_path, "rb") as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                hasher.update(mm)
        return hasher.hexdigest()

    def _md5_file_iter(self, file_path, chunk_size=DEFAULT_BUFFER_SIZE):
        """
        Compute the MD5 hash by iteratively reading the file in chunks.
        """
        hasher = hashlib.md5()
        with open(file_path, "rb") as f:
            while chunk := f.read(chunk_size):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _md5_file_cli(self, file_path):
        """
        Compute the MD5 hash using the appropriate CLI utility (md5sum on Linux, md5 on macOS).
        """
        if sys.platform.startswith("linux") and shutil.which("md5sum"):
            # Linux: use md5sum
            result = subprocess.run(
                ["md5sum", str(file_path)],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.split()[0]  # Extract the hash from the output
        elif sys.platform.startswith("darwin") and shutil.which("md5"):
            # macOS: use md5 -r (for raw output similar to md5sum)
            result = subprocess.run(
                ["md5", "-r", str(file_path)],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.split()[0]  # Extract the hash from the output
        else:
            raise RuntimeError("No suitable MD5 CLI utility found (md5sum or md5)")

    def compress_file(self, file_path, method="auto"):
        """
        Compress the file using gzip and return the path of the compressed file in the temp directory.

        :param file_path: Path to the file to compress.
        :param method: Compression method to use. Options are:
                    - "auto": Automatically select the best method.
                    - "python": Use Python's gzip module (default).
                    - "cli": Use the `gzip` CLI utility (POSIX only).
        :return: The path to the compressed file.
        """
        file_path = Path(file_path)

        # Ensure the file exists
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Automatically select the best method if "auto" is specified
        if method == "auto":
            if shutil.which("gzip"):
                # Prefer CLI - no GIL contention, better parallelism
                method = "cli"
            else:
                method = "python"

        # Use the selected compression method
        if method == "python":
            return self._compress_file_python(file_path)
        elif method == "cli":
            return self._compress_file_cli(file_path)
        else:
            raise ValueError(f"Unsupported compression method: {method}")

    def _compress_file_python(self, file_path):
        """
        Compress the file deterministically using Python's gzip module.
        """
        if metrics.is_enabled():
            tracker = metrics.get_tracker()
            with tracker.track_task("compression", str(file_path)):
                compressed_path = self.temp_dir / f"{uuid4()}.gz"
                buffer_size = DEFAULT_BUFFER_SIZE

                with open(file_path, "rb") as f_in, open(
                    compressed_path, "wb"
                ) as f_out:
                    with gzip.GzipFile(
                        filename="",  # avoid embedding filename
                        mode="wb",
                        fileobj=f_out,
                        compresslevel=5,
                        mtime=0,  # fixed mtime for determinism
                    ) as gz_out:
                        shutil.copyfileobj(f_in, gz_out, length=buffer_size)

                return compressed_path
        else:
            compressed_path = self.temp_dir / f"{uuid4()}.gz"
            buffer_size = DEFAULT_BUFFER_SIZE

            with open(file_path, "rb") as f_in, open(compressed_path, "wb") as f_out:
                with gzip.GzipFile(
                    filename="",  # avoid embedding filename
                    mode="wb",
                    fileobj=f_out,
                    compresslevel=5,
                    mtime=0,  # fixed mtime for determinism
                ) as gz_out:
                    shutil.copyfileobj(f_in, gz_out, length=buffer_size)

            return compressed_path

    def _compress_file_cli(self, file_path):
        """
        Compress the file deterministically using the `gzip` CLI utility.
        """
        compressed_path = self.temp_dir / f"{uuid4()}.gz"

        with open(compressed_path, "wb") as f_out:
            subprocess.run(
                ["gzip", "-n", "-c", "-5", str(file_path)],  # -n = no name/timestamp
                stdout=f_out,
                check=True,
            )

        return compressed_path

    def decompress_file(self, compressed_path, output_path=None, method="auto"):
        """
        Decompress a file using gzip and return the path of the decompressed file.

        :param compressed_path: Path to the compressed file.
        :param output_path: Path to save the decompressed file. If None, use the same name without the `.gz` extension.
        :param method: Decompression method to use. Options are:
                    - "auto": Automatically select the best method.
                    - "python": Use Python's gzip module (default).
                    - "cli": Use the `gzip` CLI utility (POSIX only).
        :return: The path to the decompressed file.
        """
        compressed_path = Path(compressed_path)

        # Ensure the compressed file exists
        if not compressed_path.exists():
            raise FileNotFoundError(f"Compressed file not found: {compressed_path}")

        # Determine the output path
        if output_path is None:
            output_path = compressed_path.with_suffix("")  # Remove the `.gz` extension
        output_path = Path(output_path)

        # Automatically select the best method if "auto" is specified
        if method == "auto":
            if shutil.which("gzip"):
                # Prefer CLI - no GIL contention, better parallelism
                method = "cli"
            else:
                method = "python"

        # Use the selected decompression method
        if method == "python":
            return self._decompress_file_python(compressed_path, output_path)
        elif method == "cli":
            return self._decompress_file_cli(compressed_path, output_path)
        else:
            raise ValueError(f"Unsupported decompression method: {method}")

    def _decompress_file_python(self, compressed_path, output_path):
        """
        Decompress the file using Python's gzip module and save it to the output path.
        """
        if metrics.is_enabled():
            tracker = metrics.get_tracker()
            with tracker.track_task("decompression", str(output_path)):
                with gzip.open(compressed_path, "rb") as f_in:
                    with open(output_path, "wb") as f_out:
                        # Use manual chunked copy to avoid type issues
                        while True:
                            chunk = f_in.read(DEFAULT_BUFFER_SIZE)  # 1MB chunks
                            if not chunk:
                                break
                            # Ensure we have bytes for writing
                            if isinstance(chunk, str):
                                chunk = chunk.encode("utf-8")
                            f_out.write(chunk)

                return output_path
        else:
            with gzip.open(compressed_path, "rb") as f_in:
                with open(output_path, "wb") as f_out:
                    # Use manual chunked copy to avoid type issues
                    while True:
                        chunk = f_in.read(DEFAULT_BUFFER_SIZE)  # 1MB chunks
                        if not chunk:
                            break
                        # Ensure we have bytes for writing
                        if isinstance(chunk, str):
                            chunk = chunk.encode("utf-8")
                        f_out.write(chunk)

            return output_path

    def _decompress_file_cli(self, compressed_path, output_path):
        """
        Decompress the file using the `gzip` CLI utility and save it to the output path.
        """
        result = subprocess.run(
            ["gzip", "-d", "-c", str(compressed_path)],
            stdout=open(output_path, "wb"),
            check=True,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to decompress file using gzip CLI: {compressed_path}"
            )

        return output_path

    @retry(3, (BotoCoreError, ClientError, SSLError))
    def upload(
        self,
        file_path: Union[str, Path],
        silence: bool = False,
        needs_immediate_update: bool = True,
        progress_callback: Optional[Callable[[int], None]] = None,
    ) -> None:
        """
        Upload a file to S3 and update the manifest using the file path as the key.
        """
        file_path = Path(file_path)
        if not file_path.exists():
            print(f"Error: {file_path} does not exist.")
            return

        file_hash = self.hash_file(file_path)
        # Use manifest key (relative to git root) for S3 key
        manifest_key = self._get_manifest_key(file_path)
        s3_key = f"{self.repo_prefix}/assets/{file_hash}/{manifest_key}.gz"

        extra_args = {"ServerSideEncryption": "AES256"} if self.encryption else {}
        compressed_path = self.compress_file(file_path)

        chunked = False
        if compressed_path.stat().st_size > self.chunk_size:
            paths = self.split_file(compressed_path)
            chunked = True
        else:
            paths = [compressed_path]

        for chunk_idx, path in enumerate(paths):
            try:
                if not silence:
                    print(f"Uploading {path}")
                file_size = path.stat().st_size
                # Set up progress callback and context manager
                if progress_callback:
                    # Use the provided callback for progress updates
                    def upload_callback(bytes_transferred):
                        progress_callback(bytes_transferred)

                    context_manager = contextlib.nullcontext()
                elif not silence:
                    # Create individual progress bar only if not silenced
                    progress_bar = tqdm(
                        total=file_size,
                        unit="B",
                        unit_scale=True,
                        desc=f"Uploading {path.name}",
                        leave=False,
                    )

                    def upload_callback(bytes_transferred):
                        progress_bar.update(bytes_transferred)

                    context_manager = progress_bar
                else:
                    # No progress display
                    def upload_callback(bytes_transferred):
                        pass

                    context_manager = contextlib.nullcontext()

                with context_manager:
                    # Compute the local MD5 checksum
                    with open(path, "rb") as f:
                        local_md5 = hashlib.md5(f.read()).hexdigest()

                    # Check if the file already exists in S3 with the same MD5
                    try:
                        s3_object = self._get_s3_client().head_object(
                            Bucket=self.bucket_name,
                            Key=s3_key if not chunked else f"{s3_key}.chunk{chunk_idx}",
                        )
                        s3_etag = s3_object["ETag"].strip(
                            '"'
                        )  # Remove quotes from ETag
                        if local_md5 == s3_etag:
                            if not silence:
                                print(
                                    f"Skipping upload for {path}, already exists in S3 with matching MD5."
                                )
                            # Update progress for skipped file
                            if progress_callback:
                                progress_callback(file_size)
                            continue
                        else:
                            if not silence:
                                print(
                                    f"MD5 mismatch for {path}: {local_md5}/{s3_etag}, uploading new version."
                                )
                    except ClientError as e:
                        if e.response["Error"]["Code"] != "404":
                            raise  # Re-raise if it's not a "Not Found" error

                    # Proceed with the upload if MD5 does not match or file does not exist
                    if metrics.is_enabled():
                        tracker = metrics.get_tracker()
                        with tracker.track_task("s3_upload", str(path)):
                            with open(path, "rb") as f:
                                self._get_s3_client().upload_fileobj(
                                    f,
                                    self.bucket_name,
                                    s3_key
                                    if not chunked
                                    else f"{s3_key}.chunk{chunk_idx}",
                                    ExtraArgs=extra_args,
                                    Config=self.config,
                                    Callback=upload_callback,
                                )
                    else:
                        with open(path, "rb") as f:
                            self._get_s3_client().upload_fileobj(
                                f,
                                self.bucket_name,
                                s3_key if not chunked else f"{s3_key}.chunk{chunk_idx}",
                                ExtraArgs=extra_args,
                                Config=self.config,
                                Callback=upload_callback,
                            )
                if not silence:
                    print(f"{path} uploaded")
            finally:
                try:
                    os.remove(path)
                except OSError:
                    pass

        if not silence:
            print(f"Compressed file removed: {compressed_path}")
        try:
            os.remove(compressed_path)  # Ensure temp file is deleted
        except OSError:
            pass

        # Store file path as key, hash as value
        if needs_immediate_update:
            with self._lock_context():
                self.load_manifest()
                manifest_key = self._get_manifest_key(file_path)
                self.manifest["files"][manifest_key] = file_hash
                self.save_manifest()
        if not silence:
            print(f"Uploaded {file_path} -> s3://{self.bucket_name}/{s3_key}")

    def remove_file(self, file_path, keep_in_s3=True):
        """
        Remove a file from tracking.
        If `keep_in_s3` is True, the file will remain in S3 to support previous git commits.
        Otherwise, it will be scheduled for garbage collection.

        :param file_path: The local file path to remove from tracking.
        :param keep_in_s3: If False, schedule the file for deletion in future GC.
        """
        file_path = Path(file_path)
        file_path_str = str(file_path.as_posix())

        with self._lock_context():
            if file_path_str not in self.manifest["files"]:
                print(f"⚠️ File '{file_path}' is not currently tracked.")
                return

            # Retrieve the file hash before removal
            file_hash = self.manifest["files"].pop(file_path_str, None)
            self.save_manifest()

        print(f"🗑 Removed tracking for '{file_path}'.")

        if not keep_in_s3:
            s3_key = f"{self.repo_prefix}/assets/{file_hash}/{file_path.as_posix()}.gz"
            self._get_s3_client().delete_object(Bucket=self.bucket_name, Key=s3_key)
            print(f"🗑 File removed from S3: s3://{self.bucket_name}/{s3_key}")
        else:
            print(
                f"⚠️ File remains in S3: s3://{self.bucket_name}/{file_hash}/{file_path.as_posix()}"
            )

    def cleanup_s3(self, force=False):
        """
        Remove unreferenced assets from S3 that are not in the current manifest.

        :param force: If True, bypass confirmation (for automated tests).
        """
        with self._lock_context():
            current_hashes = set(self.manifest["files"].values())

        paginator = self._get_s3_client().get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=self.bucket_name, Prefix=f"{self.repo_prefix}/assets/"
        )

        unreferenced_files = []

        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    key = obj["Key"]
                    parts = key.replace(f"{self.repo_prefix}/", "").split("/")
                    if len(parts) < 3:
                        continue

                    file_hash = parts[1]  # Extract the hash from the S3 key

                    # Collect unreferenced files
                    if file_hash not in current_hashes:
                        unreferenced_files.append(key)

        if not unreferenced_files:
            print("✅ No unreferenced files found in S3.")
            return

        print(f"⚠️ Found {len(unreferenced_files)} unreferenced files in S3.")

        # If not in test mode, ask for confirmation
        if not force:
            confirm = input("Do you want to delete them? (yes/no): ").strip().lower()
            if confirm != "yes":
                print("❌ Cleanup aborted. No files were deleted.")
                return

        # Proceed with deletion
        for key in unreferenced_files:
            self._get_s3_client().delete_object(Bucket=self.bucket_name, Key=key)
            print(f"🗑 Deleted {key}")

        print("✅ S3 cleanup completed.")

    def track_modified_files(self, silence=True):
        """Check manifest for outdated hashes and upload changed files in parallel."""

        files_to_upload = []
        with self._lock_context():
            files_to_check = list(
                self.manifest["files"].keys()
            )  # Files listed in the manifest

        # Compute hashes in parallel
        with ThreadPoolExecutor(max_workers=DEFAULT_THREAD_POOL_SIZE) as executor:
            results = zip(files_to_check, executor.map(self.hash_file, files_to_check))

        # Process results
        for file, current_hash in results:
            with self._lock_context():
                stored_hash = self.manifest["files"].get(file)

            if current_hash is None:
                print(f"Warning: File {file} is missing. Skipping.")
                continue

            if current_hash != stored_hash:
                print(f"File {file} has changed. Marking for upload.")
                files_to_upload.append(file)

        # Upload files in parallel if needed
        if files_to_upload:
            print(f"Uploading {len(files_to_upload)} modified file(s) in parallel...")
            self.parallel_upload(files_to_upload, silence=silence)

            # Save updated manifest
            with self._lock_context():
                self.save_manifest()
        else:
            print("No modified files needing upload.")

    def parallel_upload(self, files, silence=True):
        """Parallel upload of multiple files using ThreadPoolExecutor."""
        # Test S3 credentials once before starting parallel operations
        if not silence:
            print("🔐 Testing S3 credentials...")
        self.test_s3_credentials(silence=silence)

        with ThreadPoolExecutor(max_workers=DEFAULT_THREAD_POOL_SIZE) as executor:
            # Submit each download task; unpack key from matching_files.items()
            futures = [
                executor.submit(
                    self.upload, f, silence=silence, needs_immediate_update=False
                )
                for f in files
            ]

            for future in tqdm(
                as_completed(futures), total=len(futures), desc="Uploading files"
            ):
                try:
                    # This will raise the exception if the download failed
                    future.result()
                except Exception as e:
                    # Handle any other exceptions that may occur
                    print(f"An error occurred: {e}")

    def parallel_download_all(self, silence=True):
        """Download all files listed in the manifest in parallel."""
        with self._lock_context():
            items = list(
                self.manifest["files"].items()
            )  # File paths as keys, hashes as values

        if not items:
            print("⚠️ Manifest is empty. Nothing to download.")
            return

        print("📥 Starting parallel download of all tracked files...")

        # Test S3 credentials once before starting the parallel download
        self.test_s3_credentials()

        try:
            with ThreadPoolExecutor(max_workers=DEFAULT_THREAD_POOL_SIZE) as executor:
                # Submit all tasks and collect futures
                futures = [
                    executor.submit(self.download, kv[0], silence=silence)
                    for kv in items
                ]

                # Iterate over futures as they complete
                for future in tqdm(
                    as_completed(futures), total=len(futures), desc="Downloading files"
                ):
                    if self._shutdown_requested:
                        print(
                            "⚠️ Shutdown requested. Cancelling remaining downloads..."
                        )
                        break

                    try:
                        future.result()  # This will re-raise any exceptions from the thread.
                    except CancelledError:
                        print("⚠️ Task was cancelled.")
                    except Exception as e:
                        print(f"An unexpected error occurred: {e}")

        except KeyboardInterrupt:
            print("\n⚠️ Download interrupted by user.")
        finally:
            print("✅ All files downloaded.")

    def remove_subtree(self, directory, keep_in_s3=True):
        """
        Remove files matching a pattern from tracking.
        Handles single files, directories, and glob patterns uniformly.
        Optionally keep the files in S3 for historical reference.

        :param directory: The path, directory, or glob pattern to remove from tracking.
        :param keep_in_s3: If False, delete the files from S3 as well.
        """
        directory = Path(directory)
        pattern = str(directory.as_posix())

        with self._lock_context():
            # Try matching with the pattern as-is (handles files and glob patterns)
            files_to_remove = [
                path
                for path in self.manifest["files"]
                if fnmatch.fnmatch(path, pattern)
            ]

            # If no matches, try as directory by appending /*
            # This handles cases like "dir" -> "dir/*" or "dir*" -> "dir*/*"
            if not files_to_remove:
                dir_pattern = pattern.rstrip("/") + "/*"
                files_to_remove = [
                    path
                    for path in self.manifest["files"]
                    if fnmatch.fnmatch(path, dir_pattern)
                ]

        if not files_to_remove:
            print(f"⚠️ No tracked files found matching '{directory}'.")
            return

        for file_path in files_to_remove:
            file_hash = self.manifest["files"].pop(file_path, None)
            if not keep_in_s3 and file_hash:
                s3_key = f"{self.repo_prefix}/assets/{file_hash}/{file_path}.gz"
                self._get_s3_client().delete_object(Bucket=self.bucket_name, Key=s3_key)
                print(f"🗑 File removed from S3: s3://{self.bucket_name}/{s3_key}")

        with self._lock_context():
            self.save_manifest()

        count = len(files_to_remove)
        print(
            f"🗑 Removed tracking for {count} file{'s' if count != 1 else ''} matching '{directory}'."
        )

    def test_s3_credentials(self, silence=False):
        """
        Test the S3 credentials to ensure they are valid for the target bucket.
        This prevents repeated failures during bulk operations.

        :param silence: If True, suppress success messages.
        """
        try:
            # Attempt to list objects in the target bucket with a minimal prefix
            self._get_s3_client().list_objects_v2(
                Bucket=self.bucket_name, MaxKeys=1, Prefix=""
            )
            if not silence:
                print(f"✅ S3 credentials are valid for bucket '{self.bucket_name}'.")
        except NoCredentialsError:
            raise RuntimeError(ERROR_MESSAGES["no_credentials"])
        except PartialCredentialsError:
            raise RuntimeError(ERROR_MESSAGES["partial_credentials"])
        except ClientError as e:
            if e.response["Error"]["Code"] in [
                "InvalidAccessKeyId",
                "SignatureDoesNotMatch",
                "AccessDenied",
            ]:
                raise RuntimeError(
                    ERROR_MESSAGES["s3_access_denied"].format(
                        bucket_name=self.bucket_name
                    )
                )
            raise RuntimeError(f"Error testing S3 credentials: {e}")

    def _get_manifest_key(self, file_path: Union[str, Path]) -> str:
        """
        Convert a file path to a manifest key (relative to git root).

        :param file_path: Absolute or relative file path
        :return: Path relative to git root as string (POSIX format)
        """
        # Use PathResolver for consistent path handling
        return self.path_resolver.to_manifest_key(file_path)

    def _resolve_filesystem_paths(self, path):
        """
        FILESYSTEM GLOB: Find files on disk matching a pattern.

        This is used for TRACKING operations where we need to find actual files
        on the filesystem (which may not be in the manifest yet).

        The glob pattern is applied against the filesystem, not the manifest.

        :param path: Either a manifest key (relative to git root) or an absolute path.
                     Could be:
                     - A file: "subdir/file.txt" or "/repo/subdir/file.txt"
                     - A directory: "subdir/" or "/repo/subdir/"
                     - A glob pattern: "subdir/*.txt" or "/repo/subdir/*.txt"
        :return: List of Path objects for files found on disk (as absolute paths)

        Example:
            User in /repo/subdir types: "*.txt"
            CLI converts to manifest key: "subdir/*.txt"
            This method converts to filesystem path: "/repo/subdir/*.txt"
            Glob finds actual files: ["/repo/subdir/a.txt", "/repo/subdir/b.txt"]
        """
        # Handle both manifest keys and absolute paths
        path_obj = Path(path)
        if path_obj.is_absolute():
            # Already an absolute path, use as-is
            filesystem_path = path_obj
        else:
            # Convert manifest key to filesystem path (prepends git_root)
            # For example: "subdir/file.txt" -> "/repo/subdir/file.txt"
            # For globs: "subdir/*.txt" -> "/repo/subdir/*.txt"
            filesystem_path = self.path_resolver.to_filesystem_path(path)

        # If it's an existing file, return it directly
        if filesystem_path.is_file():
            resolved_files = [filesystem_path]
        # If it's an existing directory, get all files recursively
        elif filesystem_path.is_dir():
            resolved_files = [f for f in filesystem_path.rglob("*") if f.is_file()]
        else:
            # Otherwise treat as a glob pattern against the filesystem
            matched_paths = glob.glob(str(filesystem_path), recursive=True)

            # Handle both files and directories that match the pattern
            resolved_files = []
            for p in matched_paths:
                path_obj = Path(p)
                if path_obj.is_file():
                    resolved_files.append(path_obj)
                elif path_obj.is_dir():
                    # For directories, find all files recursively
                    resolved_files.extend(
                        [f for f in path_obj.rglob("*") if f.is_file()]
                    )

        # Return absolute paths
        return [p.resolve() for p in resolved_files]

    def _resolve_manifest_paths(self, path):
        """
        MANIFEST GLOB: Find files in the manifest matching a pattern.

        This is used for CHECKOUT, REMOVE, and LS operations where we need to find
        files that are already tracked in the manifest.

        The glob pattern is applied against manifest keys, not the filesystem.

        :param path: Manifest key (relative to git root) that could be:
                     - A file: "subdir/file.txt"
                     - A directory: "subdir/"
                     - A glob pattern: "subdir/*.txt" or "dir*/file*"
        :return: Dictionary of manifest entries {manifest_key: hash}

        Example:
            User in /repo/subdir types: "*.txt"
            CLI converts to manifest key: "subdir/*.txt"
            This method matches against manifest keys: {"subdir/a.txt": "hash1", "subdir/b.txt": "hash2"}
            Files may or may not exist on disk - we're just finding tracked files.
        """
        # Convert absolute paths to manifest keys (relative to git root)
        path_obj = Path(path)
        if path_obj.is_absolute():
            path_str = self.path_resolver.to_manifest_key(path_obj)
        else:
            path_str = str(path_obj.as_posix())

        with self._lock_context():
            manifest_files = self.manifest["files"]

            # Try matching with the pattern as-is (handles files and glob patterns)
            matched_files = {}
            for file_path, file_hash in manifest_files.items():
                if self._glob_match(file_path, path_str):
                    matched_files[file_path] = file_hash

            # If no matches, try as directory by appending /**
            # This handles cases like "dir" -> "dir/**" (recursive)
            # This matches filesystem behavior where specifying a directory
            # returns all files recursively within it
            if not matched_files:
                dir_pattern = path_str.rstrip("/") + "/**"
                for file_path, file_hash in manifest_files.items():
                    if self._glob_match(file_path, dir_pattern):
                        matched_files[file_path] = file_hash

            return matched_files

    def _glob_match(self, file_path, pattern):
        """
        Glob matching that behaves like filesystem glob (glob.glob semantics).

        Follows glob.glob rules:
        - * matches within a directory level (doesn't cross /)
        - ** matches recursively across directories (zero or more levels)
        - ? matches a single character (not /)

        This ensures MANIFEST GLOB and FILESYSTEM GLOB are consistent.

        :param file_path: The file path to test (manifest key)
        :param pattern: The glob pattern
        :return: True if the file path matches the pattern
        """
        # Handle ** recursive patterns
        if "**" in pattern:
            # Convert pattern to regex for matching
            # ** can match zero or more directory levels
            # Examples:
            #   "**/file.txt" -> matches "file.txt" and "a/b/file.txt"
            #   "a/**" -> matches "a/b" and "a/b/c"
            #   "a/**/file.txt" -> matches "a/file.txt" and "a/b/c/file.txt"

            regex_pattern = pattern

            # Replace **/ with marker (zero or more directories with trailing /)
            regex_pattern = regex_pattern.replace("**/", "\x00DOUBLESTAR_SLASH\x00")

            # Replace /** with marker (/ followed by zero or more directories)
            regex_pattern = regex_pattern.replace("/**", "\x00SLASH_DOUBLESTAR\x00")

            # Replace remaining ** (standalone) with marker
            regex_pattern = regex_pattern.replace("**", "\x00DOUBLESTAR\x00")

            # Escape regex special chars
            regex_pattern = re.escape(regex_pattern)

            # Replace * with [^/]* (match anything except /)
            regex_pattern = regex_pattern.replace(r"\*", "[^/]*")

            # Replace ? with [^/] (match single char except /)
            regex_pattern = regex_pattern.replace(r"\?", "[^/]")

            # Replace markers with appropriate regex
            # **/ -> (?:.*/)?  (zero or more dirs with trailing /, optional)
            regex_pattern = regex_pattern.replace(
                "\x00DOUBLESTAR_SLASH\x00", "(?:.*/)?"
            )

            # /** -> (?:/.*)?  (optional / with zero or more dirs)
            regex_pattern = regex_pattern.replace(
                "\x00SLASH_DOUBLESTAR\x00", "(?:/.*)?"
            )

            # ** standalone -> .*  (match anything)
            regex_pattern = regex_pattern.replace("\x00DOUBLESTAR\x00", ".*")

            # Anchor the pattern
            regex_pattern = f"^{regex_pattern}$"

            return bool(re.match(regex_pattern, file_path))
        else:
            # For non-** patterns, match segment by segment
            # This ensures * doesn't cross directory boundaries
            pattern_parts = pattern.split("/")
            file_parts = file_path.split("/")

            # Pattern and file must have the same number of segments for exact match
            # (No prefix matching - that's handled by the caller appending /*)
            if len(pattern_parts) != len(file_parts):
                return False

            # Match each pattern segment against corresponding file segment
            for pattern_part, file_part in zip(pattern_parts, file_parts):
                if not fnmatch.fnmatch(file_part, pattern_part):
                    return False

            # All segments matched
            return True

    def track(self, path, silence=True, interleaved=True, use_cache=True):
        """
        Track and upload files, directories, or globs.

        :param path: A file, directory, or glob pattern to track.
        :param silence: Silences verbose logging.
        :param interleaved: If True, use interleaved hashing and uploading for better performance.
        :param use_cache: If True, use cached hashing for better performance on repeated operations.
        """
        if interleaved:
            return self.track_interleaved(path, silence=silence, use_cache=use_cache)

        # Original two-stage implementation
        # Phase 1: Resolve filesystem paths and compute hashes
        print("🔍 Resolving filesystem paths and computing hashes...")
        files_to_track = self._resolve_filesystem_paths(path)

        if not files_to_track:
            print(f"⚠️ No files found to track for '{path}'.")
            return

        # Compute hashes in parallel with a progress bar
        with tqdm(total=len(files_to_track), desc="Hashing files", unit="file") as pbar:
            with ThreadPoolExecutor(max_workers=DEFAULT_THREAD_POOL_SIZE) as executor:
                if use_cache:

                    def hash_func(f):
                        return self._hash_with_progress_cached(f, pbar)

                else:

                    def hash_func(f):
                        return self._hash_with_progress(f, pbar)

                file_hashes = {
                    str(file.as_posix()): hash_result
                    for file, hash_result in zip(
                        files_to_track,
                        executor.map(hash_func, files_to_track),
                    )
                }

        # Phase 2: Lock the manifest and determine which files need updates
        print("🔒 Locking manifest to determine files needing updates...")
        with self._lock_context():
            files_to_upload = []
            for file_path, current_hash in file_hashes.items():
                stored_hash = self.manifest["files"].get(file_path)
                if current_hash != stored_hash:
                    files_to_upload.append((file_path, current_hash))

        if not files_to_upload:
            print("✅ All files are up-to-date. No uploads needed.")
            return

        print(f"📤 {len(files_to_upload)} files need to be uploaded.")

        # Test S3 credentials once before starting parallel operations
        if not silence:
            print("🔐 Testing S3 credentials...")
        self.test_s3_credentials(silence=silence)

        # Phase 3: Upload files needing updates
        print("🚀 Uploading files...")
        try:
            with ThreadPoolExecutor(max_workers=DEFAULT_THREAD_POOL_SIZE) as executor:
                futures = [
                    executor.submit(
                        self.upload,
                        file_path,
                        silence=silence,
                        needs_immediate_update=False,
                    )
                    for file_path, _ in files_to_upload
                ]

                for future in tqdm(
                    as_completed(futures), total=len(futures), desc="Uploading files"
                ):
                    if self._shutdown_requested:
                        print("⚠️ Shutdown requested. Cancelling remaining uploads...")
                        return

                    try:
                        future.result()  # Will re-raise exceptions from the worker thread
                    except Exception as e:
                        print(f"An error occurred during upload: {e}")
                        raise

        except KeyboardInterrupt:
            print("\n⚠️ Upload interrupted by user.")
            return

        with self._lock_context():
            self.load_manifest()
            # Phase 4: Lock the manifest and update it
            for file_path, file_hash in files_to_upload:
                manifest_key = self._get_manifest_key(file_path)
                self.manifest["files"][manifest_key] = file_hash
            self.save_manifest()

        print(f"✅ Successfully tracked and uploaded files for '{path}'.")

    def _hash_with_progress_cached(self, file_path, progress_bar):
        """
        Helper function to compute the cached hash of a file and update the progress bar.
        """
        result = self.hash_file_cached(file_path)
        progress_bar.update(1)
        return result

    def _hash_with_progress(self, file_path, progress_bar):
        """
        Helper function to compute the hash of a file and update the progress bar.
        """
        result = self.hash_file(file_path)
        progress_bar.update(1)
        return result

    def checkout(self, path, silence=True, interleaved=True, use_cache=True):
        """
        Checkout files, directories, or globs from the manifest.

        :param path: A file, directory, or glob pattern to checkout.
        :param silence: Silences verbose logging.
        :param interleaved: If True, use interleaved hashing and downloading for better performance.
        :param use_cache: If True, use cached hashing for better performance on repeated operations.
        """
        if interleaved:
            return self.checkout_interleaved(path, silence=silence, use_cache=use_cache)

        # Original two-stage implementation
        # Phase 1: Resolve manifest paths using improved globbing
        print("🔒 Resolving paths from manifest...")
        files_to_checkout = self._resolve_manifest_paths(path)

        if not files_to_checkout:
            print(f"⚠️ No files found in the manifest for '{path}'.")
            return

        print(f"🔍 Found {len(files_to_checkout)} files to check out.")

        # Phase 2: Hash files to determine which need to be downloaded
        print("🔍 Hashing files to determine which need to be downloaded...")
        files_to_download = []
        file_hashes = {}

        with tqdm(
            total=len(files_to_checkout), desc="Hashing files", unit="file"
        ) as pbar:
            with ThreadPoolExecutor(max_workers=DEFAULT_THREAD_POOL_SIZE) as executor:
                if use_cache:

                    def hash_func(f):
                        return self._hash_with_progress_cached(f, pbar)

                else:

                    def hash_func(f):
                        return self._hash_with_progress(f, pbar)

                future_to_file = {
                    executor.submit(
                        hash_func, self.path_resolver.to_filesystem_path(file)
                    ): file
                    for file in files_to_checkout.keys()
                    if self.path_resolver.to_filesystem_path(
                        file
                    ).exists()  # Only hash files that exist on disk
                }

                for future in as_completed(future_to_file):
                    file = future_to_file[future]
                    try:
                        file_hashes[file] = future.result()
                    except Exception as exc:
                        print(f"Error hashing file {file}: {exc}")

        # Add files that don't exist on disk to the download list
        for file in files_to_checkout.keys():
            if not self.path_resolver.to_filesystem_path(file).exists():
                files_to_download.append(file)
            elif file_hashes.get(file) != files_to_checkout[file]:
                files_to_download.append(file)

        if not files_to_download:
            print("✅ All files are up-to-date. No downloads needed.")
            return

        print(f"📥 {len(files_to_download)} files need to be downloaded.")

        # Phase 3: Download files that need updates
        print("🚀 Downloading files...")
        try:
            with ThreadPoolExecutor(max_workers=DEFAULT_THREAD_POOL_SIZE) as executor:
                futures = [
                    executor.submit(self.download, file, silence=silence)
                    for file in files_to_download
                ]

                for future in tqdm(
                    as_completed(futures), total=len(futures), desc="Downloading files"
                ):
                    if self._shutdown_requested:
                        print(
                            "⚠️ Shutdown requested. Cancelling remaining downloads..."
                        )
                        break

                    try:
                        future.result()  # Will re-raise exceptions from the worker thread
                    except Exception as e:
                        print(f"An error occurred during download: {e}")
                        raise

        except KeyboardInterrupt:
            print("\n⚠️ Download interrupted by user.")
        finally:
            print(f"✅ Successfully checked out files for '{path}'.")

    def merge_files(self, output_path, chunk_paths):
        """
        Merge multiple chunk files into a single file.

        :param output_path: Path to the output file.
        :param chunk_paths: List of chunk file paths to merge.
        :return: Path to the merged file.
        """
        with open(output_path, "wb") as output_file:
            for chunk_path in chunk_paths:
                with open(chunk_path, "rb") as chunk_file:
                    shutil.copyfileobj(chunk_file, output_file)

        return output_path

    def split_file(self, file_path):
        """
        Split a file into smaller chunks.

        :param file_path: Path to the file to split.
        :param chunk_size: Size of each chunk in bytes (default: 5 GB).
        :return: List of chunk file paths.
        """
        file_path = Path(file_path)
        chunk_paths = []

        with open(file_path, "rb") as f:
            chunk_index = 0
            while True:
                chunk_data = f.read(self.chunk_size - 1)
                if not chunk_data:
                    break

                chunk_path = Path(f"{file_path}.chunk{chunk_index}")
                with open(chunk_path, "wb") as chunk_file:
                    chunk_file.write(chunk_data)

                chunk_paths.append(chunk_path)
                chunk_index += 1

        return chunk_paths

    def _hash_and_upload_worker(
        self, file_path, silence=True, progress_callback=None, use_cache=True
    ):
        """
        Worker function that hashes a file and uploads it if needed.
        Returns (file_path, hash, uploaded, bytes_transferred) tuple.

        :param file_path: Path to the file to process
        :param silence: Whether to suppress individual file progress bars
        :param progress_callback: Optional callback function for progress updates
        :param use_cache: Whether to use cached hashing for performance
        """
        try:
            if use_cache:
                current_hash = self.hash_file_cached(file_path)
            else:
                current_hash = self.hash_file(file_path)

            # Check if upload is needed
            manifest_key = self._get_manifest_key(file_path)
            with self._lock_context():
                stored_hash = self.manifest["files"].get(manifest_key)

            if current_hash == stored_hash:
                return (file_path, current_hash, False, 0)  # No upload needed

            # Get file size for progress tracking
            file_size = Path(file_path).stat().st_size

            # Upload the file with progress callback
            self.upload(
                file_path,
                silence=True,
                needs_immediate_update=False,
                progress_callback=progress_callback,
            )
            return (file_path, current_hash, True, file_size)  # Upload completed

        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            raise

    def _hash_and_download_worker(
        self, file_info, silence=True, progress_callback=None, use_cache=True
    ):
        """
        Worker function that checks if a file needs download and downloads it if needed.
        file_info is (file_path, expected_hash) tuple where file_path is a manifest key.
        Returns (file_path, downloaded, bytes_transferred) tuple.

        :param file_info: Tuple of (manifest_key, expected_hash)
        :param silence: Whether to suppress individual file progress bars
        :param progress_callback: Optional callback function for progress updates
        :param use_cache: Whether to use cached hashing for performance
        """
        file_path, expected_hash = file_info
        try:
            # Convert manifest key to filesystem path for checking existence
            filesystem_path = self.path_resolver.to_filesystem_path(file_path)

            # Check if file exists and has correct hash
            if filesystem_path.exists():
                # Track hashing even when cached (for metrics visibility)
                if metrics.is_enabled():
                    tracker = metrics.get_tracker()
                    with tracker.track_task("hashing", str(filesystem_path)):
                        if use_cache:
                            current_hash = self.hash_file_cached(filesystem_path)
                        else:
                            current_hash = self.hash_file(filesystem_path)
                else:
                    if use_cache:
                        current_hash = self.hash_file_cached(filesystem_path)
                    else:
                        current_hash = self.hash_file(filesystem_path)

                if current_hash == expected_hash:
                    # File is up-to-date, don't add to download total since no download is needed
                    return (file_path, False, 0)  # No download needed

            # Download the file with progress callback that supports size discovery
            # Pass expected_hash to avoid lock contention
            bytes_transferred = self.download(
                file_path,
                silence=True,
                progress_callback=progress_callback,
                expected_hash=expected_hash,
            )
            return (file_path, True, bytes_transferred or 0)  # Download completed

        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            raise

    def track_interleaved(self, path, silence=True, use_cache=True):
        """
        Track and upload files with interleaved hashing and uploading for better performance.

        :param path: A file, directory, or glob pattern to track.
        :param silence: Silences verbose logging.
        :param use_cache: If True, use cached hashing for better performance on repeated operations.
        """
        # Start pipeline metrics if enabled
        if metrics.is_enabled():
            tracker = metrics.get_tracker()
            tracker.start_pipeline()

        # Phase 1: Resolve filesystem paths
        print("🔍 Resolving filesystem paths...")
        files_to_track = self._resolve_filesystem_paths(path)

        if not files_to_track:
            print(f"⚠️ No files found to track for '{path}'.")
            if metrics.is_enabled():
                tracker.end_pipeline()
            return

        # Test S3 credentials once before starting parallel operations
        if not silence:
            print("🔐 Testing S3 credentials...")
        self.test_s3_credentials(silence=silence)

        print(
            f"🚀 Processing {len(files_to_track)} files with interleaved hashing and uploading..."
        )

        # Start tracking stages
        if metrics.is_enabled():
            tracker.start_stage("hashing", max_workers=DEFAULT_THREAD_POOL_SIZE)
            tracker.start_stage("compression", max_workers=DEFAULT_THREAD_POOL_SIZE)
            tracker.start_stage("s3_upload", max_workers=DEFAULT_THREAD_POOL_SIZE)

        # Phase 2: Process files with interleaved hashing and uploading
        files_uploaded = []
        files_processed = 0
        total_bytes_transferred = 0

        try:
            # Create unified progress bars
            with tqdm(
                total=len(files_to_track),
                desc="Files processed",
                unit="file",
                position=0,
            ) as file_pbar, tqdm(
                total=0, desc="Data transferred", unit="B", unit_scale=True, position=1
            ) as bytes_pbar:

                def progress_callback(bytes_chunk):
                    """Callback to update the bytes progress bar"""
                    bytes_pbar.update(bytes_chunk)

                with ThreadPoolExecutor(
                    max_workers=DEFAULT_THREAD_POOL_SIZE
                ) as executor:
                    # Submit all hash-and-upload tasks
                    future_to_file = {
                        executor.submit(
                            self._hash_and_upload_worker,
                            str(file.as_posix()),
                            True,
                            progress_callback,
                            use_cache,
                        ): file
                        for file in files_to_track
                    }

                    # Process results as they complete
                    for future in as_completed(future_to_file):
                        if self._shutdown_requested:
                            print(
                                "⚠️ Shutdown requested. Cancelling remaining operations..."
                            )
                            return

                        try:
                            (
                                file_path,
                                file_hash,
                                uploaded,
                                bytes_transferred,
                            ) = future.result()
                            files_processed += 1
                            total_bytes_transferred += bytes_transferred

                            if uploaded:
                                files_uploaded.append((file_path, file_hash))
                                # Update the bytes progress bar total for uploaded files
                                bytes_pbar.total = (
                                    bytes_pbar.total or 0
                                ) + bytes_transferred
                                bytes_pbar.refresh()

                            file_pbar.update(1)
                            file_pbar.set_postfix(
                                {
                                    "uploaded": len(files_uploaded),
                                    "skipped": files_processed - len(files_uploaded),
                                }
                            )

                        except Exception as e:
                            print(f"An error occurred during processing: {e}")
                            raise

        except KeyboardInterrupt:
            print("\n⚠️ Processing interrupted by user.")
            return

        # Phase 3: Update manifest with all changes
        if files_uploaded:
            print(f"📝 Updating manifest with {len(files_uploaded)} uploaded files...")
            with self._lock_context():
                self.load_manifest()
                for file_path, file_hash in files_uploaded:
                    manifest_key = self._get_manifest_key(file_path)
                    self.manifest["files"][manifest_key] = file_hash
                self.save_manifest()

        print(
            f"✅ Successfully processed {files_processed} files ({len(files_uploaded)} uploaded) for '{path}'."
        )

        # End metrics tracking
        if metrics.is_enabled():
            tracker.end_stage("hashing")
            tracker.end_stage("compression")
            tracker.end_stage("s3_upload")
            tracker.end_pipeline()
            tracker.print_summary(verbose=not silence)

    def checkout_interleaved(self, path, silence=True, use_cache=True):
        """
        Checkout files with interleaved hashing and downloading for better performance.

        :param path: A file, directory, or glob pattern to checkout.
        :param silence: Silences verbose logging.
        :param use_cache: If True, use cached hashing for better performance on repeated operations.
        """
        # Start pipeline metrics if enabled
        if metrics.is_enabled():
            tracker = metrics.get_tracker()
            tracker.start_pipeline()

        # Phase 1: Resolve manifest paths
        print("🔒 Resolving paths from manifest...")
        files_to_checkout = self._resolve_manifest_paths(path)

        if not files_to_checkout:
            print(f"⚠️ No files found in the manifest for '{path}'.")
            if metrics.is_enabled():
                tracker.end_pipeline()
            return

        # Test S3 credentials once before starting parallel operations
        if not silence:
            print("🔐 Testing S3 credentials...")
        self.test_s3_credentials(silence=silence)

        print(
            f"🚀 Processing {len(files_to_checkout)} files with interleaved hashing and downloading..."
        )

        # Start tracking stages
        if metrics.is_enabled():
            tracker.start_stage("hashing", max_workers=DEFAULT_THREAD_POOL_SIZE)
            tracker.start_stage("s3_download", max_workers=DEFAULT_THREAD_POOL_SIZE)
            tracker.start_stage("decompression", max_workers=DEFAULT_THREAD_POOL_SIZE)

        # Phase 2: Start processing immediately - discover sizes during download
        # We'll process ALL files to ensure proper progress tracking, even for up-to-date ones
        files_to_process = files_to_checkout

        if not files_to_process:
            if not silence:
                print("✅ No files to process.")
            return

        if not silence:
            print(
                f"📥 Processing {len(files_to_process)} files (calculating sizes during processing...)",
                flush=True,
            )

        # Phase 3: Process files with interleaved hashing and downloading
        files_downloaded = 0
        files_processed = 0
        total_bytes_transferred = 0

        try:
            # Create unified progress bars with dynamic total for bytes
            with tqdm(
                total=len(files_to_process),
                desc="Files processed",
                unit="file",
                position=0,
            ) as file_pbar, tqdm(
                total=0, desc="Data downloaded", unit="B", unit_scale=True, position=1
            ) as bytes_pbar:

                def progress_callback(bytes_chunk, file_size=None):
                    """Callback to update the bytes progress bar and optionally set total"""
                    if file_size is not None:
                        # Update total when we discover a new file size
                        bytes_pbar.total = (bytes_pbar.total or 0) + file_size
                        bytes_pbar.refresh()
                    bytes_pbar.update(bytes_chunk)

                with ThreadPoolExecutor(
                    max_workers=DEFAULT_THREAD_POOL_SIZE
                ) as executor:
                    # Submit hash-and-download tasks for all files (including up-to-date ones for progress tracking)
                    future_to_file = {
                        executor.submit(
                            self._hash_and_download_worker,
                            (file_path, expected_hash),
                            True,
                            progress_callback,
                            use_cache,
                        ): file_path
                        for file_path, expected_hash in files_to_process.items()
                    }

                    # Process results as they complete
                    for future in as_completed(future_to_file):
                        if self._shutdown_requested:
                            print(
                                "⚠️ Shutdown requested. Cancelling remaining operations..."
                            )
                            break

                        try:
                            file_path, downloaded, bytes_transferred = future.result()
                            files_processed += 1
                            total_bytes_transferred += bytes_transferred

                            if downloaded:
                                files_downloaded += 1

                            file_pbar.update(1)
                            file_pbar.set_postfix(
                                {
                                    "downloaded": files_downloaded,
                                    "skipped": files_processed - files_downloaded,
                                }
                            )

                        except Exception as e:
                            print(f"An error occurred during processing: {e}")
                            raise

        except KeyboardInterrupt:
            print("\n⚠️ Processing interrupted by user.")
        finally:
            print(
                f"✅ Successfully processed {files_processed} files ({files_downloaded} downloaded) for '{path}'."
            )

            # End metrics tracking
            if metrics.is_enabled():
                tracker.end_stage("hashing")
                tracker.end_stage("s3_download")
                tracker.end_stage("decompression")
                tracker.end_pipeline()
                tracker.print_summary(verbose=not silence)

    @retry(3, (BotoCoreError, ClientError, SSLError))
    def download(
        self,
        file_path: Union[str, Path],
        silence: bool = False,
        progress_callback: Optional[Callable[[int], None]] = None,
        expected_hash: Optional[str] = None,
    ) -> Optional[int]:
        """
        Download a file from S3 by its recorded hash, but skip if it already exists and matches.

        :param file_path: Manifest key (relative to git root)
        :param expected_hash: Optional pre-fetched hash to avoid lock contention in parallel downloads
        """
        # file_path is always a manifest key from _resolve_manifest_paths()
        manifest_key = str(file_path)

        # Convert manifest key to absolute filesystem path for operations
        filesystem_path = self.path_resolver.to_filesystem_path(manifest_key)

        # Get the expected hash for the file (use provided hash if available to avoid lock)
        if expected_hash is None:
            with self._lock_context():
                expected_hash = self.manifest["files"].get(manifest_key)
        if not expected_hash:
            print(f"⚠️ File '{file_path}' is not in the manifest.")
            return None

        # If the file exists, check its hash
        if not silence:
            print(f"file_path exists?: {filesystem_path.exists()}")
        if filesystem_path.exists():
            current_hash = self.hash_file(filesystem_path)
            if not silence:
                print(f"current_hash: {current_hash}")
                print(f"expected_hash: {expected_hash}")
            if current_hash == expected_hash:
                if not silence:
                    print(
                        f"✅ Skipping download: '{filesystem_path}' is already up-to-date."
                    )
                return 0  # Skip download if hashes match

        # Proceed with download if file is missing or different
        s3_key = f"{self.repo_prefix}/assets/{expected_hash}/{manifest_key}.gz"

        compressed_path = self.temp_dir / f"{uuid4()}.gz"

        chunk_keys = self._get_s3_client().list_objects_v2(
            Bucket=self.bucket_name, Prefix=f"{s3_key}.chunk"
        )
        chunk_keys = [ck["Key"] for ck in chunk_keys.get("Contents", [])]
        chunk_keys_sorted = []
        for i in range(len(chunk_keys)):
            chunk_keys_sorted.append(f"{s3_key}.chunk{i}")
        chunk_keys = chunk_keys_sorted

        if chunk_keys:
            keys = chunk_keys
        else:
            keys = [s3_key]

        base_directrory = os.path.dirname(compressed_path)
        os.makedirs(base_directrory, exist_ok=True)

        target_paths = []
        total_file_size = 0

        # First pass: discover total size and notify progress callback
        for idx, key in enumerate(keys):
            obj = self._get_s3_client().head_object(Bucket=self.bucket_name, Key=key)
            total_file_size += obj["ContentLength"]

        # Notify progress callback of total size discovery (for dynamic progress bar)
        if progress_callback:
            try:
                # Try to call with file_size parameter for dynamic progress bar
                progress_callback(0, **{"file_size": total_file_size})
            except TypeError:
                # Fallback: callback doesn't support file_size parameter
                pass

        for idx, key in enumerate(keys):
            try:
                target_path = self.temp_dir / f"{uuid4()}.gz"
                target_paths.append(target_path)
                obj = self._get_s3_client().head_object(
                    Bucket=self.bucket_name, Key=key
                )
                file_size = obj["ContentLength"]

                # Set up progress callback and context manager
                if progress_callback:
                    # Use the provided callback for unified progress tracking
                    def download_callback(bytes_transferred):
                        progress_callback(bytes_transferred)

                    context_manager = contextlib.nullcontext()
                elif not silence:
                    # Create individual progress bar only if not silenced and no unified callback
                    progress_bar = tqdm(
                        total=file_size,
                        unit="B",
                        unit_scale=True,
                        desc=f"Downloading {os.path.basename(key)}",
                        leave=False,
                    )

                    def download_callback(bytes_transferred):
                        progress_bar.update(bytes_transferred)

                    context_manager = progress_bar
                else:
                    # No progress display
                    def download_callback(bytes_transferred):
                        pass

                    context_manager = contextlib.nullcontext()

                with context_manager:
                    if not silence:
                        print(f"Downloading {key} to {target_path}")
                    if metrics.is_enabled():
                        tracker = metrics.get_tracker()
                        with tracker.track_task("s3_download", key):
                            with open(target_path, "wb") as f:
                                self._get_s3_client().download_fileobj(
                                    Bucket=self.bucket_name,
                                    Key=key,
                                    Fileobj=f,
                                    Callback=download_callback,
                                )
                    else:
                        with open(target_path, "wb") as f:
                            self._get_s3_client().download_fileobj(
                                Bucket=self.bucket_name,
                                Key=key,
                                Fileobj=f,
                                Callback=download_callback,
                            )
            except Exception as e:
                print(f"❌ Error downloading {key}: {e}")

        if chunk_keys:
            compressed_path = self.merge_files(compressed_path, target_paths)
            for path in target_paths:
                os.remove(path)
        else:
            compressed_path = target_paths[0]

        if os.path.dirname(filesystem_path):
            os.makedirs(os.path.dirname(filesystem_path), exist_ok=True)
        try:
            # Track decompression at the call site for better metrics visibility
            if metrics.is_enabled():
                tracker = metrics.get_tracker()
                with tracker.track_task("decompression", str(filesystem_path)):
                    self.decompress_file(compressed_path, filesystem_path)
            else:
                self.decompress_file(compressed_path, filesystem_path)
        except Exception as e:
            print(f"❌ Error decompressing {compressed_path} for key {keys}: {e}")
            raise
        os.remove(compressed_path)  # Ensure temp file is deleted
        if not silence:
            print(
                f"📥 Downloaded {filesystem_path} from s3://{self.bucket_name}/{s3_key}"
            )

        # Return bytes transferred for progress tracking
        return filesystem_path.stat().st_size if filesystem_path.exists() else 0

    def list_files(self, path, verbose=False, strip_prefix=None):
        """
        List tracked files matching a path pattern.

        :param path: A file, directory, or glob pattern to list.
        :param verbose: If True, show detailed information including file sizes and hashes.
        :param strip_prefix: If provided, strip this prefix from displayed paths.
        """
        # Resolve manifest paths using the same logic as checkout
        files_to_list = self._resolve_manifest_paths(path)

        if not files_to_list:
            if verbose:
                print(f"⚠️ No tracked files found for '{path}'.")
            return

        if verbose:
            print(f"📋 Found {len(files_to_list)} tracked file(s) for '{path}':")
            print()

        # Sort files for consistent output
        sorted_files = sorted(files_to_list.items())

        for file_path, file_hash in sorted_files:
            # Strip prefix if provided
            display_path = file_path
            if strip_prefix and file_path.startswith(strip_prefix + "/"):
                display_path = file_path[len(strip_prefix + "/") :]

            if verbose:
                # Get file status if it exists locally
                file_status = self.get_file_status(file_path)
                if file_status["exists"]:
                    size_str = f"{file_status['size']:,} bytes"
                    status = "✅" if file_status["cache_valid"] else "⚠️"
                else:
                    size_str = "missing"
                    status = "❌"

                print(f"{status} {display_path}")
                print(f"    Hash: {file_hash}")
                print(f"    Size: {size_str}")
                print()
            else:
                print(display_path)

    def list_all_files(self, verbose=False, strip_prefix=None):
        """
        List all tracked files from the manifest.

        :param verbose: If True, show detailed information including file sizes and hashes.
        :param strip_prefix: If provided, strip this prefix from displayed paths.
        """
        with self._lock_context():
            all_files = dict(self.manifest["files"])

        if not all_files:
            if verbose:
                print("⚠️ No files are currently tracked.")
            return

        if verbose:
            print(f"📋 All tracked files ({len(all_files)} total):")
            print()

        # Sort files for consistent output
        sorted_files = sorted(all_files.items())

        for file_path, file_hash in sorted_files:
            # Strip prefix if provided
            display_path = file_path
            if strip_prefix and file_path.startswith(strip_prefix + "/"):
                display_path = file_path[len(strip_prefix + "/") :]

            if verbose:
                # Get file status if it exists locally
                file_status = self.get_file_status(file_path)
                if file_status["exists"]:
                    size_str = f"{file_status['size']:,} bytes"
                    status = "✅" if file_status["cache_valid"] else "⚠️"
                else:
                    size_str = "missing"
                    status = "❌"

                print(f"{status} {display_path}")
                print(f"    Hash: {file_hash}")
                print(f"    Size: {size_str}")
                print()
            else:
                print(display_path)
