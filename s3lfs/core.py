import contextlib
import fnmatch
import functools
import glob
import gzip
import hashlib
import json
import mmap
import os
import random
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
DEFAULT_THREAD_POOL_SIZE = 8  # Fallback when os.cpu_count() is unavailable
DEFAULT_MULTIPART_THRESHOLD = 5 * 1024 * 1024 * 1024  # 5 GB
DEFAULT_MAX_CONCURRENCY = 15  # Balanced for bandwidth-limited downloads


def _default_workers():
    """Compute a sensible default worker count based on available CPUs.

    Uses the same heuristic as Python's ThreadPoolExecutor default:
    min(32, cpu_count + 4).  Falls back to DEFAULT_THREAD_POOL_SIZE if
    cpu_count is unavailable.
    """
    cpu = os.cpu_count()
    if cpu is None:
        return DEFAULT_THREAD_POOL_SIZE
    return min(32, cpu + 4)


# Common error messages
ERROR_MESSAGES = {
    "no_credentials": "AWS credentials are missing. Please configure them or use --no-sign-request.",
    "partial_credentials": "Incomplete AWS credentials. Check your AWS configuration.",
    "invalid_credentials": "Invalid AWS credentials. Please verify your access key and secret key.",
    "s3_access_denied": "Invalid or insufficient AWS credentials for bucket '{bucket_name}'.",
    "acceleration_not_supported": "Transfer acceleration is not supported for unsigned requests.",
}


# Errors that will not succeed on a second attempt. Retrying them wastes the
# caller's time and buries the real cause behind a delay.
NON_RETRYABLE_S3_CODES = frozenset(
    {
        "AccessDenied",
        "AllAccessDisabled",
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
        "NoSuchBucket",
        "InvalidBucketName",
        "AccountProblem",
        "InvalidObjectState",
        "EntityTooLarge",
    }
)


def _is_retryable(exc):
    """Is this exception worth another attempt?"""
    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code", "")
        if code in NON_RETRYABLE_S3_CODES:
            return False
        # 4xx other than throttling and request timeout are client errors.
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if isinstance(status, int) and 400 <= status < 500:
            return code in {
                "RequestTimeout",
                "SlowDown",
                "Throttling",
                "ThrottlingException",
            }
    return True


def retry(times, exceptions, max_delay=30):
    """Retry decorator with exponential backoff and full jitter.

    :param times: Maximum number of retry attempts.
    :param exceptions: Tuple of exception types that trigger a retry.
    :param max_delay: Cap on the backoff delay in seconds.
    """

    def decorator(func):
        @functools.wraps(func)
        def newfn(*args, **kwargs):
            for attempt in range(times):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    if attempt >= times - 1 or not _is_retryable(exc):
                        raise
                    # Full jitter. Without it, every worker that failed at the
                    # same moment retries at the same moment, so a transient
                    # blip becomes a synchronised stampede against the
                    # endpoint that just failed.
                    ceiling = min(2 ** (attempt + 1), max_delay)
                    delay = random.uniform(0, ceiling)
                    print(
                        f"Retry {attempt + 1}/{times} for {func.__name__} "
                        f"in {delay:.1f}s: {exc}"
                    )
                    time.sleep(delay)

        return newfn

    return decorator


class ShutdownRequested(Exception):
    """Raised by a worker that declined to start because of an interrupt.

    Distinguishes cancelled work from work that genuinely failed, so the
    drain loops can stay quiet about it rather than printing an error per
    queued task.
    """


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
        endpoint_url=None,
        workers=None,
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
        :param endpoint_url: Custom S3 endpoint URL for S3-compatible storage (e.g. MinIO, R2, Wasabi)
        :param workers: Number of parallel workers for uploads/downloads (default: auto-detected)
        """
        self.chunk_size = chunk_size
        self.use_acceleration = use_acceleration
        self.endpoint_url = endpoint_url
        self.workers = workers if workers is not None else _default_workers()

        def default_s3_factory(no_sign_request):
            """Default S3 client factory with proper boto3 usage."""
            kwargs = {}
            if self.endpoint_url:
                kwargs["endpoint_url"] = self.endpoint_url
            if no_sign_request:
                if self.use_acceleration:
                    raise RuntimeError(ERROR_MESSAGES["acceleration_not_supported"])
                config = Config(signature_version=UNSIGNED)
                return boto3.client("s3", config=config, **kwargs)
            else:
                if self.use_acceleration:
                    # Use transfer acceleration endpoint
                    return boto3.client(
                        "s3",
                        config=Config(s3={"use_accelerate_endpoint": True}),
                        **kwargs,
                    )
                else:
                    return boto3.client("s3", **kwargs)

        self.s3_factory = s3_factory if s3_factory is not None else default_s3_factory

        # Set the temporary directory to the base of the repository if not provided
        self.temp_dir = Path(temp_dir or ".s3lfs_temp")
        self.temp_dir.mkdir(parents=True, exist_ok=True)  # Ensure the directory exists

        # Note: boto3 spawns max_concurrency threads per transfer and s3lfs
        # runs self.workers transfers at once, so in the worst case these
        # multiply. That is deliberate: a single large asset is one transfer,
        # and dividing the budget across the pool would leave it with no
        # multipart parallelism at all, which is the case s3lfs exists for.
        max_concurrency = max(self.workers, DEFAULT_MAX_CONCURRENCY)
        if no_sign_request:
            # If we're not signing, we can't use multipart. Set the threshold to the max.
            self.config = TransferConfig(
                multipart_threshold=DEFAULT_MULTIPART_THRESHOLD,
                max_concurrency=max_concurrency,
            )
        else:
            self.config = TransferConfig(max_concurrency=max_concurrency)
        self.thread_local = threading.local()
        self.manifest_file = Path(manifest_file)

        # Separate cache file - should NOT be version controlled
        # Use same format as manifest (YAML or JSON)
        cache_suffix = (
            ".yaml" if self.manifest_file.suffix in [".yaml", ".yml"] else ".json"
        )
        cache_file_name = self.manifest_file.stem + "_cache" + cache_suffix
        self.cache_file = self.manifest_file.parent / cache_file_name

        # Use a file-based lock for cross-process synchronization. The lock is
        # anchored to the manifest it guards, not to the current working
        # directory: a CWD-relative path gives processes started from different
        # directories different lock files for the same manifest, silently
        # removing mutual exclusion between them. It is kept inside
        # .s3lfs_temp/ rather than beside the manifest so that file enumeration
        # (rglob) does not pick it up as a trackable file.
        lock_dir = self.manifest_file.parent.resolve() / ".s3lfs_temp"
        lock_dir.mkdir(parents=True, exist_ok=True)
        self._lock_file = lock_dir / ".s3lfs.lock"

        # Registry of hashes an uploader has claimed but not yet published in
        # the manifest. Garbage collection treats these as live; see
        # _inflight_claim.
        self._inflight_file = lock_dir / ".s3lfs_inflight.yaml"

        self.no_sign_request = no_sign_request
        self._cache_mtime: Optional[float] = None
        self._cache_dirty = False
        self.hash_cache: dict = {}
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

            # Load endpoint_url from manifest if not provided as parameter
            if self.endpoint_url:
                self.manifest["endpoint_url"] = self.endpoint_url
            else:
                self.endpoint_url = self.manifest.get("endpoint_url")

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

        self._shutdown_requested = False
        # signal.signal only works on the main thread of the main interpreter,
        # and raises ValueError elsewhere. Constructing an S3LFS from a worker
        # thread is legitimate for library callers, so treat the handler as
        # best-effort rather than a hard requirement.
        try:
            signal.signal(signal.SIGINT, self._handle_sigint)
        except ValueError:
            pass

    def _handle_sigint(self, signum, frame):
        """
        Handle SIGINT (Ctrl+C) to gracefully shut down parallel operations.
        """
        print("\nInterrupt received. Shutting down...")
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

    # A claim older than this is treated as abandoned by a crashed process.
    # This bounds the leak from a crash; it plays no part in closing the race
    # itself, which the registry handles outright.
    INFLIGHT_TTL_SECONDS = 24 * 60 * 60

    def _load_inflight(self):
        """Read the in-flight claim registry. Caller must hold the lock."""
        if not self._inflight_file.exists():
            return {}
        try:
            with open(self._inflight_file, "r") as f:
                data = yaml.safe_load(f) or {}
            claims = data.get("claims", {})
            return claims if isinstance(claims, dict) else {}
        except Exception:
            # A corrupt registry must not block uploads. Treating it as empty
            # is the conservative direction: GC may delete an object an
            # in-flight upload is about to reference, which is the behaviour
            # without a registry at all, rather than leaking forever.
            return {}

    def _save_inflight(self, claims):
        """Write the in-flight claim registry. Caller must hold the lock."""
        # Unique temp name: a shared one lets concurrent writers interleave
        # into a single file before the rename.
        temp_file = self._inflight_file.with_name(
            f"{self._inflight_file.name}.{uuid4().hex}.tmp"
        )
        with open(temp_file, "w") as f:
            yaml.safe_dump({"claims": claims}, f)
        temp_file.replace(self._inflight_file)

    def _live_inflight_keys(self):
        """Claims that have not aged out. Caller must hold the lock."""
        cutoff = time.time() - self.INFLIGHT_TTL_SECONDS
        return {k for k, ts in self._load_inflight().items() if ts >= cutoff}

    def _claim_inflight(self, base_key):
        """Register an asset key as in-flight, before any bytes reach S3."""
        with self._lock_context():
            claims = self._load_inflight()
            claims[base_key] = time.time()
            self._save_inflight(claims)

    def _release_inflight(self, base_keys):
        """Drop claims. Must run only after the manifest entry is published."""
        if not base_keys:
            return
        with self._lock_context():
            claims = self._load_inflight()
            cutoff = time.time() - self.INFLIGHT_TTL_SECONDS
            claims = {
                k: ts
                for k, ts in claims.items()
                if k not in base_keys and ts >= cutoff  # also prune aged-out claims
            }
            self._save_inflight(claims)

    def _asset_base_key(self, manifest_key, file_hash):
        """The S3 key a file's content is stored under."""
        return f"{self.repo_prefix}/assets/{file_hash}/{manifest_key}.gz"

    def _delete_asset(self, base_key):
        """Delete an asset and every chunk belonging to it.

        A large file is stored as base_key.chunk0..N rather than at base_key
        itself, so deleting only the base key leaves the whole file behind.
        """
        client = self._get_s3_client()
        resp = client.list_objects_v2(Bucket=self.bucket_name, Prefix=base_key)
        keys = [obj["Key"] for obj in resp.get("Contents", [])]
        # Guard against a prefix match on an unrelated, longer key.
        keys = [k for k in keys if self._key_covered_by(k, {base_key})]
        if not keys:
            keys = [base_key]

        for key in keys:
            client.delete_object(Bucket=self.bucket_name, Key=key)
            print(f"File removed from S3: s3://{self.bucket_name}/{key}")

    def _live_asset_keys(self):
        """Base keys reachable from the manifest. Caller must hold the lock."""
        return {
            self._asset_base_key(manifest_key, file_hash)
            for manifest_key, file_hash in self.manifest.get("files", {}).items()
        }

    def _is_asset_key(self, key):
        """Does this key have the shape of a stored asset?

        Anything else under the prefix is left alone: s3lfs did not put it
        there in a form it recognises, so it is not ours to delete.
        """
        prefix = f"{self.repo_prefix}/assets/"
        if not key.startswith(prefix):
            return False
        rest = key[len(prefix) :]
        head, sep, tail = rest.partition("/")
        return bool(head and sep and tail)

    @staticmethod
    def _key_covered_by(key, base_keys):
        """Is this key one of these assets, or a chunk belonging to one?

        Reachability has to be judged on hash *and* path, because that is what
        the storage layout keys on. Judging on the hash alone means an object
        stays reachable as long as any path shares its content, so a removed
        or renamed path leaks its object permanently.
        """
        if key in base_keys:
            return True
        head, sep, tail = key.rpartition(".chunk")
        return bool(sep) and tail.isdigit() and head in base_keys

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
                self.manifest["bucket_name"] = str(self.bucket_name)
            if self.repo_prefix is not None:
                self.manifest["repo_prefix"] = str(self.repo_prefix)
            if self.endpoint_url is not None:
                self.manifest["endpoint_url"] = str(self.endpoint_url)
            self.save_manifest()

        # Update .gitignore to exclude cache files
        self._update_gitignore()

        print("Successfully initialized S3LFS with:")
        print(f"   Bucket Name: {self.bucket_name}")
        print(f"   Repo Prefix: {self.repo_prefix}")
        if self.endpoint_url:
            print(f"   Endpoint URL: {self.endpoint_url}")
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
                print("Updated .gitignore to exclude S3LFS cache files")
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
                        f"Added {len(missing_patterns)} missing S3LFS patterns to .gitignore"
                    )
        else:
            print(".gitignore already contains S3LFS cache exclusions")

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
        # Unique temp name. A shared one lets two writers interleave into the
        # same file before either renames: the rename is atomic, the content
        # is not.
        temp_file = self.manifest_file.with_name(
            f"{self.manifest_file.name}.{uuid4().hex}.tmp"
        )
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
            print(f"Failed to save manifest: {e}")
            if temp_file.exists():
                temp_file.unlink()  # Clean up the temporary file

    def load_cache(self, force=False):
        """Load the hash cache from a separate cache file.

        Skips the disk read when the file's mtime has not changed since
        the last load, unless *force* is True.
        """
        if not self.cache_file.exists():
            # Only reset if we haven't already established that the
            # file is absent (avoids clearing in-memory mutations
            # between load-save cycles when the file doesn't exist yet).
            if self._cache_mtime is not None or not hasattr(self, "hash_cache"):
                self.hash_cache = {}
                self._cache_mtime = None
                self._cache_dirty = False
            return

        try:
            current_mtime = self.cache_file.stat().st_mtime
        except OSError:
            current_mtime = None

        # Skip re-read if on-disk file hasn't changed
        if (
            not force
            and self._cache_mtime is not None
            and current_mtime == self._cache_mtime
        ):
            return

        try:
            with open(self.cache_file, "r") as f:
                if self.cache_file.suffix in [".yaml", ".yml"]:
                    self.hash_cache = yaml.safe_load(f) or {}
                else:
                    self.hash_cache = json.load(f)
        except (json.JSONDecodeError, yaml.YAMLError, IOError) as e:
            print(f"Warning: Failed to load cache file, starting with empty cache: {e}")
            self.hash_cache = {}

        self._cache_mtime = current_mtime
        self._cache_dirty = False

    def save_cache(self):
        """Save the hash cache back to disk atomically.

        Skips the write when nothing has changed since the last
        load or save.
        """
        if hasattr(self, "_cache_dirty") and not self._cache_dirty:
            return

        # Unique temp name; see save_manifest.
        temp_file = self.cache_file.with_name(
            f"{self.cache_file.name}.{uuid4().hex}.tmp"
        )
        try:
            with open(temp_file, "w") as f:
                if self.cache_file.suffix in [".yaml", ".yml"]:
                    yaml.safe_dump(
                        self.hash_cache, f, default_flow_style=False, sort_keys=True
                    )
                else:
                    json.dump(self.hash_cache, f, indent=4, sort_keys=True)

            temp_file.replace(self.cache_file)

            # Update mtime so subsequent load_cache() calls are no-ops
            try:
                self._cache_mtime = self.cache_file.stat().st_mtime
            except OSError:
                self._cache_mtime = None
            self._cache_dirty = False
        except Exception as e:
            print(f"Failed to save cache: {e}")
            if temp_file.exists():
                temp_file.unlink()

    def hash_file(self, file_path: Union[str, Path], method: str = "auto") -> str:
        """
        Compute a SHA-256 hash of the file's contents.

        The hash covers content only: the same bytes at two different paths
        produce the same digest. Supports multiple hashing methods for
        performance optimization.

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

    # Filesystem mtime is frequently stored at 1-second resolution. A file
    # modified within this window of being hashed cannot be distinguished from
    # one that was not, so such entries are not cached at all.
    MTIME_GRANULARITY_SECONDS = 1.0

    def _changed_during_hashing(self, file_path, metadata):
        """Did the file change between the pre-hash stat and now?"""
        try:
            stat = Path(file_path).stat()
        except OSError:
            return True

        return (
            stat.st_size != metadata["size"]
            or stat.st_mtime != metadata["mtime"]
            or getattr(stat, "st_ino", None) != metadata["inode"]
        )

    def _entry_is_racy(self, cached_data):
        """Was this entry written too soon after the file was modified?

        If the gap between the file's mtime and the moment we recorded the
        hash is below mtime granularity, a further modification in that same
        tick would leave (size, mtime, inode) unchanged and go unnoticed. Such
        an entry cannot be trusted on the strength of its metadata alone.

        The entry is still kept: recomputing once refreshes it with a
        timestamp comfortably after the mtime, and it is trusted from then on.
        This mirrors git's "racily clean" handling.
        """
        written_at = cached_data.get("timestamp")
        mtime = cached_data.get("metadata", {}).get("mtime")
        if written_at is None or mtime is None:
            return True
        return (written_at - mtime) < self.MTIME_GRANULARITY_SECONDS

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
                    and not self._entry_is_racy(cached_data)
                ):
                    # File hasn't changed, return cached hash
                    return cached_data["hash"]

            # File has changed or no cache exists, compute new hash
            # Release lock while computing hash (can be expensive)
            pass

        # Compute hash outside of lock to avoid blocking other processes
        new_hash = self.hash_file(file_path, method)

        # The metadata above was read before hashing. If the file changed while
        # we were reading it, that hash belongs to no single version of the
        # file, and storing it against the pre-hash metadata would leave an
        # entry that is wrong whenever those metadata recur.
        if self._changed_during_hashing(file_path, current_metadata):
            return new_hash

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
                    and not self._entry_is_racy(cached_data)
                ):
                    # Another process computed it while we were working
                    return cached_data["hash"]

            # Cache the new hash with metadata
            self.hash_cache[file_path_str] = {
                "hash": new_hash,
                "metadata": current_metadata,
                "timestamp": time.time(),
            }
            self._cache_dirty = True

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
                self._cache_dirty = True
                print("Cleared all hash cache entries.")
            else:
                # Clear cache for specific file
                file_path_str = str(Path(file_path).as_posix())
                if file_path_str in self.hash_cache:
                    del self.hash_cache[file_path_str]
                    self._cache_dirty = True
                    print(f"Cleared hash cache for '{file_path}'.")

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
                self._cache_dirty = True

            if stale_entries:
                print(f"Cleaned up {len(stale_entries)} stale cache entries.")
                self.save_cache()

    def _check_cache_hit(self, file_path_str, current_metadata):
        """Check if the in-memory cache has a valid entry for this file.

        Does NOT acquire the lock or reload cache from disk.
        Returns the cached hash if valid, or None.
        """
        cached_data = self.hash_cache.get(file_path_str)
        if not cached_data:
            return None
        cached_meta = cached_data.get("metadata", {})
        if (
            cached_meta.get("size") == current_metadata["size"]
            and cached_meta.get("mtime") == current_metadata["mtime"]
            and cached_meta.get("inode") == current_metadata["inode"]
        ):
            return cached_data["hash"]
        return None

    def track_modified_files_cached(self, silence=True):
        """
        Check manifest for outdated hashes using cached hashing and upload
        changed files in parallel.

        Loads the manifest and cache once at the start, checks all files
        against the in-memory snapshot without holding the lock, and
        batch-writes cache updates at the end.
        """
        files_to_upload = []
        cache_hits = 0
        cache_misses = 0

        # Load manifest and cache once, snapshot stored hashes
        with self._lock_context():
            files_to_check = list(self.manifest["files"].keys())
            stored_hashes = dict(self.manifest["files"])
            self.load_cache()

        if not files_to_check:
            print(
                "No files found in manifest. "
                "Use 's3lfs track <path>' to track files first."
            )
            return

        print(f"Checking {len(files_to_check)} tracked files for modifications...")

        cache_updates = {}

        with tqdm(
            total=len(files_to_check), desc="Checking files", unit="file"
        ) as pbar:
            for file_path in files_to_check:
                try:
                    fp = Path(file_path)
                    filesystem_path = self.path_resolver.to_filesystem_path(file_path)

                    if not filesystem_path.exists():
                        print(f"Warning: File {file_path} is missing. Skipping.")
                        pbar.update(1)
                        continue

                    stat = filesystem_path.stat()
                    metadata = {
                        "size": stat.st_size,
                        "mtime": stat.st_mtime,
                        "inode": getattr(stat, "st_ino", None),
                    }
                    file_path_str = str(fp.as_posix())

                    cached_hash = self._check_cache_hit(file_path_str, metadata)
                    if cached_hash is not None:
                        current_hash = cached_hash
                        cache_hits += 1
                    else:
                        current_hash = self.hash_file(filesystem_path)
                        cache_misses += 1
                        cache_updates[file_path_str] = {
                            "hash": current_hash,
                            "metadata": metadata,
                            "timestamp": time.time(),
                        }

                    if current_hash != stored_hashes.get(file_path):
                        print(f"File {file_path} has changed. " f"Marking for upload.")
                        files_to_upload.append(file_path)

                    pbar.set_postfix(
                        {
                            "changed": len(files_to_upload),
                            "cache_hits": cache_hits,
                            "cache_misses": cache_misses,
                        }
                    )
                    pbar.update(1)

                except Exception as e:
                    print(f"Error processing {file_path}: {e}")
                    pbar.update(1)
                    continue

        # Batch-write cache updates in a single lock acquisition
        if cache_updates:
            with self._lock_context():
                self.hash_cache.update(cache_updates)
                self._cache_dirty = True
                self.save_cache()

        if not silence:
            print(
                f"Hash cache performance: " f"{cache_hits} hits, {cache_misses} misses"
            )

        # Upload files in parallel if needed
        if files_to_upload:
            print(
                f"Uploading {len(files_to_upload)} modified file(s) " f"in parallel..."
            )
            # parallel_upload_chunked commits the manifest itself, reloading
            # under the lock and merging only the keys it uploaded. Saving
            # again here would write this process's older in-memory copy back
            # over anything another process committed in between.
            self.parallel_upload(files_to_upload, silence=silence)
        else:
            print("No modified files needing upload.")

    def _hash_file_mmap(self, file_path):
        """
        Compute the SHA-256 hash using memory-mapped files.
        """
        with metrics.track("hashing", str(file_path)):
            hasher = hashlib.sha256()
            with open(file_path, "rb") as f:
                with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                    hasher.update(mm)
            return hasher.hexdigest()

    def _hash_file_iter(self, file_path, chunk_size=DEFAULT_BUFFER_SIZE):
        """
        Compute the SHA-256 hash by iteratively reading the file in chunks.
        """
        with metrics.track("hashing", str(file_path)):
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
            if shutil.which("pigz"):
                method = "pigz"
            elif shutil.which("gzip"):
                method = "cli"
            else:
                method = "python"

        # Use the selected compression method
        if method == "python":
            return self._compress_file_python(file_path)
        elif method == "pigz":
            return self._compress_file_pigz(file_path)
        elif method == "cli":
            return self._compress_file_cli(file_path)
        else:
            raise ValueError(f"Unsupported compression method: {method}")

    def _compress_file_python(self, file_path):
        """
        Compress the file deterministically using Python's gzip module.
        """
        with metrics.track("compression", str(file_path)):
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

    def _compress_file_pigz(self, file_path):
        """
        Compress the file deterministically using pigz (parallel gzip).

        pigz uses all available CPU cores and produces gzip-compatible
        output, so existing stored files remain readable.
        """
        compressed_path = self.temp_dir / f"{uuid4()}.gz"

        with open(compressed_path, "wb") as f_out:
            subprocess.run(
                ["pigz", "-n", "-c", "-5", str(file_path)],
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
            if shutil.which("pigz"):
                method = "pigz"
            elif shutil.which("gzip"):
                method = "cli"
            else:
                method = "python"

        # Use the selected decompression method
        if method == "python":
            return self._decompress_file_python(compressed_path, output_path)
        elif method == "pigz":
            return self._decompress_file_pigz(compressed_path, output_path)
        elif method == "cli":
            return self._decompress_file_cli(compressed_path, output_path)
        else:
            raise ValueError(f"Unsupported decompression method: {method}")

    def _decompress_file_python(self, compressed_path, output_path):
        """
        Decompress the file using Python's gzip module and save it to the output path.
        """
        with metrics.track("decompression", str(output_path)):
            with gzip.open(compressed_path, "rb") as f_in:
                with open(output_path, "wb") as f_out:
                    while True:
                        chunk = f_in.read(DEFAULT_BUFFER_SIZE)
                        if not chunk:
                            break
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

    def _decompress_file_pigz(self, compressed_path, output_path):
        """
        Decompress the file using pigz (parallel gzip) and save it to the output path.
        """
        result = subprocess.run(
            ["pigz", "-d", "-c", str(compressed_path)],
            stdout=open(output_path, "wb"),
            check=True,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to decompress file using pigz: {compressed_path}"
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
                    upload_key = s3_key if not chunked else f"{s3_key}.chunk{chunk_idx}"

                    # Compute the local MD5 checksum (streaming)
                    md5_hash = hashlib.md5()
                    with open(path, "rb") as f:
                        while True:
                            block = f.read(DEFAULT_BUFFER_SIZE)
                            if not block:
                                break
                            md5_hash.update(block)
                    local_md5 = md5_hash.hexdigest()

                    # Check if the file already exists in S3
                    try:
                        s3_object = self._get_s3_client().head_object(
                            Bucket=self.bucket_name,
                            Key=upload_key,
                        )
                        s3_etag = s3_object["ETag"].strip('"')
                        if local_md5 == s3_etag:
                            if not silence:
                                print(
                                    f"Skipping upload for {path}, "
                                    f"already exists in S3."
                                )
                            if progress_callback:
                                progress_callback(file_size)
                            continue
                    except ClientError as e:
                        if e.response["Error"]["Code"] != "404":
                            raise
                    with metrics.track("s3_upload", str(path)):
                        with open(path, "rb") as f:
                            self._get_s3_client().upload_fileobj(
                                f,
                                self.bucket_name,
                                upload_key,
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
        # Normalise the same way every other call site does. Using the raw
        # argument meant "./data/x.bin" or an absolute path reported the file
        # as untracked when it was tracked under "data/x.bin".
        file_path_str = self._get_manifest_key(file_path)

        with self._lock_context():
            # Re-read under the lock: this process's copy may predate another
            # process's commit, and saving without reloading would erase it.
            self.load_manifest()

            if file_path_str not in self.manifest["files"]:
                print(f"File '{file_path}' is not currently tracked.")
                return

            # Retrieve the file hash before removal
            file_hash = self.manifest["files"].pop(file_path_str, None)
            self.save_manifest()

        print(f"Removed tracking for '{file_path}'.")

        if not keep_in_s3:
            # Objects are stored under the manifest key, so deletion must use
            # it too; the raw argument would miss the object entirely.
            self._delete_asset(self._asset_base_key(file_path_str, file_hash))
        else:
            print(
                f"File remains in S3: s3://{self.bucket_name}/{file_hash}/{file_path_str}"
            )

    def cleanup_s3(self, force=False):
        """
        Remove unreferenced assets from S3 that are not in the current manifest.

        :param force: If True, bypass confirmation (for automated tests).
        """
        with self._lock_context():
            self.load_manifest()
            live_keys = self._live_asset_keys() | self._live_inflight_keys()

        paginator = self._get_s3_client().get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=self.bucket_name, Prefix=f"{self.repo_prefix}/assets/"
        )

        unreferenced_files = []

        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    key = obj["Key"]
                    if not self._is_asset_key(key):
                        continue

                    # Collect unreferenced files
                    if not self._key_covered_by(key, live_keys):
                        unreferenced_files.append(key)

        if not unreferenced_files:
            print("No unreferenced files found in S3.")
            return

        print(f"Found {len(unreferenced_files)} unreferenced files in S3.")

        # If not in test mode, ask for confirmation
        if not force:
            confirm = input("Do you want to delete them? (yes/no): ").strip().lower()
            if confirm != "yes":
                print("Cleanup aborted. No files were deleted.")
                return

        # Re-check under the lock immediately before deleting. Listing S3 and
        # waiting for confirmation both take unbounded time, during which an
        # upload may have claimed or published any of these keys.
        with self._lock_context():
            self.load_manifest()
            live_now = self._live_asset_keys() | self._live_inflight_keys()

        to_delete = []
        for key in unreferenced_files:
            if self._key_covered_by(key, live_now):
                print(f"Skipping {key} (became referenced during cleanup)")
                continue
            to_delete.append(key)

        # Proceed with deletion
        for key in to_delete:
            self._get_s3_client().delete_object(Bucket=self.bucket_name, Key=key)
            print(f"Deleted {key}")

        print("S3 cleanup completed.")

    def track_modified_files(self, silence=True):
        """Check manifest for outdated hashes and upload changed files in parallel."""

        files_to_upload = []
        with self._lock_context():
            files_to_check = list(
                self.manifest["files"].keys()
            )  # Files listed in the manifest

        # Compute hashes in parallel
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
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
            # parallel_upload_chunked commits the manifest itself; see the
            # note in track_modified_files_cached.
            self.parallel_upload(files_to_upload, silence=silence)
        else:
            print("No modified files needing upload.")

    def parallel_upload(self, files, silence=True):
        """Upload multiple files with block-level parallelism."""
        self.parallel_upload_chunked(files, silence=silence)

    def _prepare_file_for_upload(self, file_path):
        """Hash, compress, and split a file into uploadable chunks."""
        file_path = Path(file_path)
        file_hash = self.hash_file(file_path)
        manifest_key = self._get_manifest_key(file_path)

        with self._lock_context():
            stored_hash = self.manifest["files"].get(manifest_key)
        if file_hash == stored_hash:
            return None

        s3_key = f"{self.repo_prefix}/assets/{file_hash}/{manifest_key}.gz"
        extra_args = {"ServerSideEncryption": "AES256"} if self.encryption else {}

        compressed_path = self.compress_file(file_path)

        if compressed_path.stat().st_size > self.chunk_size:
            chunk_paths = self.split_file(compressed_path)
            try:
                os.remove(compressed_path)
            except OSError:
                pass
            chunks = [
                {
                    "path": p,
                    "s3_key": f"{s3_key}.chunk{i}",
                    "chunk_index": i,
                    "extra_args": extra_args,
                }
                for i, p in enumerate(chunk_paths)
            ]
        else:
            chunks = [
                {
                    "path": compressed_path,
                    "s3_key": s3_key,
                    "chunk_index": 0,
                    "extra_args": extra_args,
                }
            ]

        return (manifest_key, file_hash, chunks)

    @retry(3, (BotoCoreError, ClientError, SSLError))
    def _put_chunk(self, path, s3_key, extra_args):
        """PUT one chunk. Retried, so it must not consume its input."""
        with open(path, "rb") as f:
            self._get_s3_client().upload_fileobj(
                f,
                self.bucket_name,
                s3_key,
                ExtraArgs=extra_args,
                Config=self.config,
            )
        return path.stat().st_size

    def _upload_chunk(self, chunk_info):
        """Upload a single compressed chunk to S3.

        The chunk file is removed once, after all retry attempts. Deleting it
        inside the retried call would leave nothing for the next attempt to
        read, so the retry could only ever fail.
        """
        path = chunk_info["path"]
        s3_key = chunk_info["s3_key"]
        extra_args = chunk_info["extra_args"]

        # Queued work should not start after an interrupt. Only the drain
        # loops checked this, so every task already submitted to the pool ran
        # to completion and Ctrl-C appeared to hang on large transfers.
        if self._shutdown_requested:
            try:
                os.remove(path)
            except OSError:
                pass
            raise ShutdownRequested(f"Upload cancelled: {s3_key}")

        try:
            bytes_uploaded = self._put_chunk(path, s3_key, extra_args)
        finally:
            try:
                os.remove(path)
            except OSError:
                pass

        return (s3_key, bytes_uploaded)

    def parallel_upload_chunked(self, files, silence=True):
        """Upload files with block-level parallelism.

        Preparation futures (hash + compress + split) are collected via
        as_completed; as each resolves, chunk upload futures are
        submitted into the same pool.
        """
        if not files:
            print("Nothing to upload.")
            return

        self.test_s3_credentials(silence=silence)

        manifest_updates = {}
        # manifest_key -> {hash, expected, done}. A file earns its manifest
        # entry only once every one of its chunks has landed in S3.
        pending = {}
        # Hashes claimed in the in-flight registry, released once the manifest
        # has been written.
        claimed = set()
        total_bytes = 0
        total_chunks = 0
        chunks_done = 0

        try:
            with tqdm(desc="Uploading", unit="chunk") as pbar:
                with ThreadPoolExecutor(max_workers=self.workers) as executor:
                    # Phase 1: submit all prep tasks
                    prep_futures = {
                        executor.submit(self._prepare_file_for_upload, f): f
                        for f in files
                    }

                    # Phase 2: as preps complete, submit uploads
                    ul_futures = {}
                    for prep_future in as_completed(prep_futures):
                        if self._shutdown_requested:
                            break
                        try:
                            result = prep_future.result()
                        except Exception as e:
                            print(f"Error preparing file: {e}")
                            continue

                        if result is None:
                            continue

                        manifest_key, file_hash, chunks = result

                        # Claim the asset before submitting any chunk upload.
                        # Between a chunk landing in S3 and the manifest entry
                        # being published, the object is unreferenced and a
                        # concurrent cleanup_s3 would otherwise delete it.
                        base_key = self._asset_base_key(manifest_key, file_hash)
                        self._claim_inflight(base_key)
                        claimed.add(base_key)

                        pending[manifest_key] = {
                            "hash": file_hash,
                            "expected": len(chunks),
                            "done": 0,
                        }
                        total_chunks += len(chunks)
                        pbar.total = total_chunks
                        pbar.refresh()

                        for chunk in chunks:
                            f = executor.submit(self._upload_chunk, chunk)
                            ul_futures[f] = (manifest_key, chunk)

                    # Phase 3: collect upload results
                    for ul_future in as_completed(ul_futures):
                        if self._shutdown_requested:
                            break
                        manifest_key, chunk = ul_futures[ul_future]
                        try:
                            _, bytes_uploaded = ul_future.result()
                        except Exception as e:
                            if not isinstance(e, ShutdownRequested):
                                print(f"Error uploading " f"{chunk['s3_key']}: {e}")
                            continue

                        entry = pending[manifest_key]
                        entry["done"] += 1
                        if entry["done"] == entry["expected"]:
                            manifest_updates[manifest_key] = entry["hash"]

                        total_bytes += bytes_uploaded
                        chunks_done += 1
                        pbar.update(1)

        except KeyboardInterrupt:
            print("\nUpload interrupted by user.")
        finally:
            if manifest_updates:
                with self._lock_context():
                    self.load_manifest()
                    self.manifest["files"].update(manifest_updates)
                    self.save_manifest()

            # Strictly after the manifest is published. Releasing first would
            # reopen the window the registry exists to close: the hash would be
            # in neither the manifest nor the registry, and a sweep running in
            # between would delete the objects.
            self._release_inflight(claimed)

            # Files whose chunks did not all land are deliberately absent from
            # the manifest. Recording them would leave an entry pointing at an
            # incomplete object, and checkout infers the chunk count from the
            # objects that exist -- so a missing tail reassembles into a
            # truncated file with no error raised anywhere.
            incomplete = sorted(
                key for key, v in pending.items() if v["done"] != v["expected"]
            )
            if incomplete:
                print(
                    f"WARNING: {len(incomplete)} file(s) did not upload "
                    f"completely and were NOT added to the manifest:"
                )
                for key in incomplete[:10]:
                    v = pending[key]
                    print(f"  {key} ({v['done']}/{v['expected']} chunks)")
                if len(incomplete) > 10:
                    print(f"  ... and {len(incomplete) - 10} more")

            print(
                f"Uploaded {len(manifest_updates)} file(s) "
                f"({chunks_done} chunks, "
                f"{total_bytes / (1024 * 1024):.1f} MB compressed)."
            )

    def parallel_download_all(self, silence=True):
        """Download all files using block-level parallelism."""
        with self._lock_context():
            items = list(self.manifest["files"].items())

        if not items:
            print("Manifest is empty. Nothing to download.")
            return

        print("Starting block-level parallel download of all tracked files...")

        files_to_download = []
        for manifest_key, file_hash in items:
            filesystem_path = self.path_resolver.to_filesystem_path(manifest_key)
            if filesystem_path.exists():
                current_hash = self.hash_file(filesystem_path)
                if current_hash == file_hash:
                    if not silence:
                        print(f"  Skipping {manifest_key} (up-to-date)")
                    continue
            files_to_download.append((manifest_key, file_hash))

        if not files_to_download:
            print("All files are up-to-date.")
            return

        self.parallel_download_chunked(files_to_download, silence=silence)

    @retry(3, (BotoCoreError, ClientError, SSLError))
    def _discover_chunks_for_file(self, manifest_key, file_hash):
        """Discover S3 chunks for a single file."""
        s3_key = f"{self.repo_prefix}/assets/{file_hash}/{manifest_key}.gz"

        resp = self._get_s3_client().list_objects_v2(
            Bucket=self.bucket_name, Prefix=f"{s3_key}.chunk"
        )
        chunk_keys = [ck["Key"] for ck in resp.get("Contents", [])]

        if chunk_keys:
            return [
                {
                    "manifest_key": manifest_key,
                    "file_hash": file_hash,
                    "s3_key": f"{s3_key}.chunk{i}",
                    "chunk_index": i,
                    "is_chunked": True,
                    "num_chunks": len(chunk_keys),
                }
                for i in range(len(chunk_keys))
            ]
        else:
            return [
                {
                    "manifest_key": manifest_key,
                    "file_hash": file_hash,
                    "s3_key": s3_key,
                    "chunk_index": 0,
                    "is_chunked": False,
                    "num_chunks": 1,
                }
            ]

    @retry(3, (BotoCoreError, ClientError, SSLError))
    def _download_chunk(self, chunk_info, target_path):
        """Download a single S3 chunk to a target path."""
        # See _upload_chunk: queued work must not start after an interrupt.
        if self._shutdown_requested:
            raise ShutdownRequested(f"Download cancelled: {chunk_info['s3_key']}")

        with open(target_path, "wb") as f:
            self._get_s3_client().download_fileobj(
                Bucket=self.bucket_name,
                Key=chunk_info["s3_key"],
                Fileobj=f,
            )
        return (
            chunk_info["manifest_key"],
            chunk_info["chunk_index"],
            target_path,
            target_path.stat().st_size,
            chunk_info["is_chunked"],
            chunk_info["num_chunks"],
        )

    def _finalize_file(
        self, manifest_key, chunk_paths, is_chunked, expected_hash=None, silence=True
    ):
        """Merge chunks (if needed) and decompress to final location.

        If expected_hash is given, the reassembled file is hashed and compared
        against it. Chunk discovery infers the chunk count from the objects
        that happen to exist, so a partial upload whose tail is missing
        reassembles into a shorter file that is otherwise well-formed; without
        this check nothing downstream would notice.
        """
        filesystem_path = self.path_resolver.to_filesystem_path(manifest_key)

        if is_chunked and len(chunk_paths) > 1:
            compressed_path = self.temp_dir / f"{uuid4()}.gz"
            self.merge_files(compressed_path, chunk_paths)
            for p in chunk_paths:
                try:
                    os.remove(p)
                except OSError:
                    pass
        else:
            compressed_path = chunk_paths[0]

        os.makedirs(os.path.dirname(filesystem_path), exist_ok=True)
        try:
            self.decompress_file(compressed_path, filesystem_path)
        finally:
            try:
                os.remove(compressed_path)
            except OSError:
                pass

        if expected_hash is not None:
            actual_hash = self.hash_file(filesystem_path)
            if actual_hash != expected_hash:
                # Remove the bad output rather than leave it looking valid.
                try:
                    os.remove(filesystem_path)
                except OSError:
                    pass
                raise RuntimeError(
                    f"Checksum mismatch for {manifest_key}: "
                    f"expected {expected_hash}, got {actual_hash}. "
                    f"The stored object is incomplete or corrupt."
                )

    def parallel_download_chunked(self, file_items, silence=True):
        """Download files with block-level parallelism.

        Discovery and download share a single thread pool.  Discovery
        futures are collected via as_completed; as each resolves, its
        chunk download futures are submitted into the same pool.  A
        second as_completed pass collects download results and triggers
        per-file finalization when all chunks land.
        """
        if not file_items:
            print("Nothing to download.")
            return

        self.test_s3_credentials(silence=silence)

        file_tracker = {}
        files_finalized = 0
        files_failed = 0
        total_bytes = 0
        total_chunks = 0
        chunks_done = 0

        try:
            with tqdm(desc="Downloading", unit="chunk") as pbar:
                with ThreadPoolExecutor(max_workers=self.workers) as executor:
                    # Phase 1: submit all discovery tasks
                    disc_futures = {
                        executor.submit(self._discover_chunks_for_file, mk, fh): mk
                        for mk, fh in file_items
                    }

                    # Phase 2: as discoveries complete, submit downloads
                    dl_futures = {}
                    for disc_future in as_completed(disc_futures):
                        if self._shutdown_requested:
                            break
                        try:
                            chunks = disc_future.result()
                        except Exception as e:
                            print(f"Error discovering chunks: {e}")
                            continue

                        mk = chunks[0]["manifest_key"]
                        file_tracker[mk] = {
                            "expected": len(chunks),
                            "received": [],
                            "is_chunked": chunks[0]["is_chunked"],
                            "file_hash": chunks[0]["file_hash"],
                        }
                        total_chunks += len(chunks)
                        pbar.total = total_chunks
                        pbar.refresh()

                        for chunk in chunks:
                            target = self.temp_dir / f"{uuid4()}.gz"
                            f = executor.submit(self._download_chunk, chunk, target)
                            dl_futures[f] = chunk

                    # Phase 3: collect download results, finalize files
                    for dl_future in as_completed(dl_futures):
                        if self._shutdown_requested:
                            break
                        try:
                            (
                                manifest_key,
                                chunk_index,
                                target_path,
                                bytes_downloaded,
                                is_chunked,
                                num_chunks,
                            ) = dl_future.result()
                        except Exception as e:
                            chunk = dl_futures[dl_future]
                            if not isinstance(e, ShutdownRequested):
                                print(f"Error downloading " f"{chunk['s3_key']}: {e}")
                            continue

                        total_bytes += bytes_downloaded
                        chunks_done += 1
                        pbar.update(1)

                        entry = file_tracker.get(manifest_key)
                        if entry is None:
                            continue
                        entry["received"].append((chunk_index, target_path))

                        if len(entry["received"]) == entry["expected"]:
                            entry["received"].sort(key=lambda x: x[0])
                            chunk_paths = [p for _, p in entry["received"]]
                            try:
                                self._finalize_file(
                                    manifest_key,
                                    chunk_paths,
                                    entry["is_chunked"],
                                    expected_hash=entry["file_hash"],
                                    silence=silence,
                                )
                                files_finalized += 1
                            except Exception as e:
                                # One corrupt file must not abandon the rest.
                                print(f"ERROR: {manifest_key}: {e}")
                                files_failed += 1

        except KeyboardInterrupt:
            print("\nDownload interrupted by user.")
        finally:
            print(
                f"Downloaded {files_finalized} file(s) "
                f"({chunks_done} chunks, "
                f"{total_bytes / (1024 * 1024):.1f} MB compressed)."
            )

            # A file whose chunks did not all arrive is never finalized. Say so
            # and drop its partial chunks, rather than leaving the caller to
            # infer it from the count and the temp files behind.
            incomplete = sorted(
                mk
                for mk, e in file_tracker.items()
                if len(e["received"]) != e["expected"]
            )
            for mk in incomplete:
                for _, path in file_tracker[mk]["received"]:
                    try:
                        os.remove(path)
                    except OSError:
                        pass

            if incomplete or files_failed:
                print(
                    f"WARNING: {len(incomplete) + files_failed} file(s) were "
                    f"not written:"
                )
                for mk in incomplete[:10]:
                    tracked = file_tracker[mk]
                    print(
                        f"  {mk} (received {len(tracked['received'])}/"
                        f"{tracked['expected']} chunks)"
                    )
                if len(incomplete) > 10:
                    print(f"  ... and {len(incomplete) - 10} more")

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
            # Match, mutate, and save in a single critical section against a
            # fresh read. Matching under one lock and saving under a later one
            # lets another process commit in between, and the save would then
            # write this process's older copy back over it.
            self.load_manifest()

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
                print(f"No tracked files found matching '{directory}'.")
                return

            removed_hashes = {
                file_path: self.manifest["files"].pop(file_path, None)
                for file_path in files_to_remove
            }
            self.save_manifest()

        # S3 deletion is network I/O and must not hold the manifest lock.
        if not keep_in_s3:
            for file_path, file_hash in removed_hashes.items():
                if not file_hash:
                    continue
                self._delete_asset(self._asset_base_key(file_path, file_hash))

        count = len(files_to_remove)
        print(
            f"Removed tracking for {count} file{'s' if count != 1 else ''} matching '{directory}'."
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
                print(f"S3 credentials are valid for bucket '{self.bucket_name}'.")
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

    def _is_internal_path(self, path: Union[str, Path]) -> bool:
        """
        Is this a file s3lfs must never track?

        Covers git's own metadata and s3lfs's own bookkeeping. Filesystem
        enumeration uses rglob("*"), which matches dotfiles, so without this
        `track .` walks into .git/ and also picks up the manifest, the hash
        cache, and the lock file.

        :param path: Absolute or relative file path
        :return: True if the path is internal and must be skipped
        """
        resolved = Path(path).resolve()
        try:
            parts = resolved.relative_to(self.path_resolver.git_root).parts
        except ValueError:
            # Outside the repository; fall back to the whole path.
            parts = resolved.parts

        if ".git" in parts:
            return True
        if ".s3lfs_temp" in parts or self.temp_dir.name in parts:
            return True
        if resolved.name in {self.manifest_file.name, self.cache_file.name}:
            return True
        if resolved.name.endswith(".s3lfs.lock"):
            return True
        return False

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

        # Return absolute paths, less anything internal to git or s3lfs
        return [p.resolve() for p in resolved_files if not self._is_internal_path(p)]

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
        print("Resolving filesystem paths and computing hashes...")
        files_to_track = self._resolve_filesystem_paths(path)

        if not files_to_track:
            print(f"No files found to track for '{path}'.")
            return

        # Compute hashes in parallel with a progress bar
        with tqdm(total=len(files_to_track), desc="Hashing files", unit="file") as pbar:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
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
        print("Locking manifest to determine files needing updates...")
        with self._lock_context():
            files_to_upload = []
            for file_path, current_hash in file_hashes.items():
                stored_hash = self.manifest["files"].get(file_path)
                if current_hash != stored_hash:
                    files_to_upload.append((file_path, current_hash))

        if not files_to_upload:
            print("All files are up-to-date. No uploads needed.")
            return

        print(f"{len(files_to_upload)} files need to be uploaded.")

        # Test S3 credentials once before starting parallel operations
        if not silence:
            print("Testing S3 credentials...")
        self.test_s3_credentials(silence=silence)

        # Phase 3: Upload files needing updates
        print("Uploading files...")
        try:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
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
                        print("Shutdown requested. Cancelling remaining uploads...")
                        return

                    try:
                        future.result()  # Will re-raise exceptions from the worker thread
                    except Exception as e:
                        print(f"An error occurred during upload: {e}")
                        raise

        except KeyboardInterrupt:
            print("\nUpload interrupted by user.")
            return

        with self._lock_context():
            self.load_manifest()
            # Phase 4: Lock the manifest and update it
            for file_path, file_hash in files_to_upload:
                manifest_key = self._get_manifest_key(file_path)
                self.manifest["files"][manifest_key] = file_hash
            self.save_manifest()

        print(f"Successfully tracked and uploaded files for '{path}'.")

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
        print("Resolving paths from manifest...")
        files_to_checkout = self._resolve_manifest_paths(path)

        if not files_to_checkout:
            print(f"No files found in the manifest for '{path}'.")
            return

        print(f"Found {len(files_to_checkout)} files to check out.")

        # Phase 2: Hash files to determine which need to be downloaded
        print("Hashing files to determine which need to be downloaded...")
        files_to_download = []
        file_hashes = {}

        with tqdm(
            total=len(files_to_checkout), desc="Hashing files", unit="file"
        ) as pbar:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
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
            print("All files are up-to-date. No downloads needed.")
            return

        print(f"{len(files_to_download)} files need to be downloaded.")

        file_items = [(f, files_to_checkout[f]) for f in files_to_download]
        self.parallel_download_chunked(file_items, silence=silence)

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

        max_chunk_bytes = self.chunk_size - 1
        with open(file_path, "rb") as f:
            chunk_index = 0
            while True:
                chunk_path = Path(f"{file_path}.chunk{chunk_index}")
                bytes_written = 0
                wrote_any = False

                with open(chunk_path, "wb") as chunk_file:
                    while bytes_written < max_chunk_bytes:
                        to_read = min(
                            DEFAULT_BUFFER_SIZE,
                            max_chunk_bytes - bytes_written,
                        )
                        block = f.read(to_read)
                        if not block:
                            break
                        chunk_file.write(block)
                        bytes_written += len(block)
                        wrote_any = True

                if not wrote_any:
                    # Nothing left to read; remove the empty file
                    chunk_path.unlink(missing_ok=True)
                    break

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
                with metrics.track("hashing", str(filesystem_path)):
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
        print("Resolving filesystem paths...")
        files_to_track = self._resolve_filesystem_paths(path)

        if not files_to_track:
            print(f"No files found to track for '{path}'.")
            if metrics.is_enabled():
                tracker.end_pipeline()
            return

        # Test S3 credentials once before starting parallel operations
        if not silence:
            print("Testing S3 credentials...")
        self.test_s3_credentials(silence=silence)

        print(
            f"Processing {len(files_to_track)} files with interleaved hashing and uploading..."
        )

        # Start tracking stages
        if metrics.is_enabled():
            tracker.start_stage("hashing", max_workers=self.workers)
            tracker.start_stage("compression", max_workers=self.workers)
            tracker.start_stage("s3_upload", max_workers=self.workers)

        # Phase 2: Process files with interleaved hashing and uploading
        files_uploaded = []
        files_processed = 0
        total_bytes_transferred = 0

        try:
            # Create unified progress bars
            with (
                tqdm(
                    total=len(files_to_track),
                    desc="Files processed",
                    unit="file",
                    position=0,
                ) as file_pbar,
                tqdm(
                    total=0,
                    desc="Data transferred",
                    unit="B",
                    unit_scale=True,
                    position=1,
                ) as bytes_pbar,
            ):

                def progress_callback(bytes_chunk):
                    """Callback to update the bytes progress bar"""
                    bytes_pbar.update(bytes_chunk)

                with ThreadPoolExecutor(max_workers=self.workers) as executor:
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
                                "Shutdown requested. Cancelling remaining operations..."
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
            print("\nProcessing interrupted by user.")
            return

        # Phase 3: Update manifest with all changes
        if files_uploaded:
            print(f"Updating manifest with {len(files_uploaded)} uploaded files...")
            with self._lock_context():
                self.load_manifest()
                for file_path, file_hash in files_uploaded:
                    manifest_key = self._get_manifest_key(file_path)
                    self.manifest["files"][manifest_key] = file_hash
                self.save_manifest()

        print(
            f"Successfully processed {files_processed} files ({len(files_uploaded)} uploaded) for '{path}'."
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
        print("Resolving paths from manifest...")
        files_to_checkout = self._resolve_manifest_paths(path)

        if not files_to_checkout:
            print(f"No files found in the manifest for '{path}'.")
            if metrics.is_enabled():
                tracker.end_pipeline()
            return

        # Test S3 credentials once before starting parallel operations
        if not silence:
            print("Testing S3 credentials...")
        self.test_s3_credentials(silence=silence)

        print(
            f"Processing {len(files_to_checkout)} files with interleaved hashing and downloading..."
        )

        # Start tracking stages
        if metrics.is_enabled():
            tracker.start_stage("hashing", max_workers=self.workers)
            tracker.start_stage("s3_download", max_workers=self.workers)
            tracker.start_stage("decompression", max_workers=self.workers)

        # Phase 2: Start processing immediately - discover sizes during download
        # We'll process ALL files to ensure proper progress tracking, even for up-to-date ones
        files_to_process = files_to_checkout

        if not files_to_process:
            if not silence:
                print("No files to process.")
            return

        if not silence:
            print(
                f"Processing {len(files_to_process)} files (calculating sizes during processing...)",
                flush=True,
            )

        # Phase 3: Process files with interleaved hashing and downloading
        files_downloaded = 0
        files_processed = 0
        total_bytes_transferred = 0

        try:
            # Create unified progress bars with dynamic total for bytes
            with (
                tqdm(
                    total=len(files_to_process),
                    desc="Files processed",
                    unit="file",
                    position=0,
                ) as file_pbar,
                tqdm(
                    total=0,
                    desc="Data downloaded",
                    unit="B",
                    unit_scale=True,
                    position=1,
                ) as bytes_pbar,
            ):

                def progress_callback(bytes_chunk, file_size=None):
                    """Callback to update the bytes progress bar and optionally set total"""
                    if file_size is not None:
                        # Update total when we discover a new file size
                        bytes_pbar.total = (bytes_pbar.total or 0) + file_size
                        bytes_pbar.refresh()
                    bytes_pbar.update(bytes_chunk)

                with ThreadPoolExecutor(max_workers=self.workers) as executor:
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
                                "Shutdown requested. Cancelling remaining operations..."
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
            print("\nProcessing interrupted by user.")
        finally:
            print(
                f"Successfully processed {files_processed} files ({files_downloaded} downloaded) for '{path}'."
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
            print(f"File '{file_path}' is not in the manifest.")
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
                        f"Skipping download: '{filesystem_path}' is already up-to-date."
                    )
                return 0  # Skip download if hashes match

        # Proceed with download if file is missing or different
        s3_key = f"{self.repo_prefix}/assets/{expected_hash}/{manifest_key}.gz"

        compressed_path = self.temp_dir / f"{uuid4()}.gz"

        chunk_resp = self._get_s3_client().list_objects_v2(
            Bucket=self.bucket_name, Prefix=f"{s3_key}.chunk"
        )
        chunk_contents = chunk_resp.get("Contents", [])

        # Build key list and size map from list_objects_v2 response
        # (avoids separate head_object calls for chunked files)
        key_sizes = {}
        if chunk_contents:
            for i in range(len(chunk_contents)):
                k = f"{s3_key}.chunk{i}"
                for obj in chunk_contents:
                    if obj["Key"] == k:
                        key_sizes[k] = obj["Size"]
                        break
            keys = list(key_sizes.keys())
        else:
            keys = [s3_key]
            obj = self._get_s3_client().head_object(Bucket=self.bucket_name, Key=s3_key)
            key_sizes[s3_key] = obj["ContentLength"]

        base_directory = os.path.dirname(compressed_path)
        os.makedirs(base_directory, exist_ok=True)

        target_paths = []
        total_file_size = sum(key_sizes.values())

        if progress_callback:
            try:
                progress_callback(0, **{"file_size": total_file_size})
            except TypeError:
                pass

        for idx, key in enumerate(keys):
            try:
                target_path = self.temp_dir / f"{uuid4()}.gz"
                target_paths.append(target_path)
                file_size = key_sizes[key]

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
                    with metrics.track("s3_download", key):
                        with open(target_path, "wb") as f:
                            self._get_s3_client().download_fileobj(
                                Bucket=self.bucket_name,
                                Key=key,
                                Fileobj=f,
                                Callback=download_callback,
                            )
            except Exception as e:
                print(f"Error downloading {key}: {e}")

        if chunk_contents:
            compressed_path = self.merge_files(compressed_path, target_paths)
            for path in target_paths:
                os.remove(path)
        else:
            compressed_path = target_paths[0]

        if os.path.dirname(filesystem_path):
            os.makedirs(os.path.dirname(filesystem_path), exist_ok=True)
        try:
            with metrics.track("decompression", str(filesystem_path)):
                self.decompress_file(compressed_path, filesystem_path)
        except Exception as e:
            print(f"Error decompressing {compressed_path} for key {keys}: {e}")
            raise
        os.remove(compressed_path)  # Ensure temp file is deleted
        if not silence:
            print(f"Downloaded {filesystem_path} from s3://{self.bucket_name}/{s3_key}")

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
                print(f"No tracked files found for '{path}'.")
            return

        if verbose:
            print(f"Found {len(files_to_list)} tracked file(s) for '{path}':")
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
                    status = "" if file_status["cache_valid"] else ""
                else:
                    size_str = "missing"
                    status = ""

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
                print("No files are currently tracked.")
            return

        if verbose:
            print(f"All tracked files ({len(all_files)} total):")
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
                    status = "" if file_status["cache_valid"] else ""
                else:
                    size_str = "missing"
                    status = ""

                print(f"{status} {display_path}")
                print(f"    Hash: {file_hash}")
                print(f"    Size: {size_str}")
                print()
            else:
                print(display_path)
