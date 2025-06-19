import fnmatch
import glob
import gzip
import hashlib
import json
import mmap
import os
import shutil
import signal
import subprocess
import sys
import threading
from concurrent.futures import CancelledError, ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import boto3
import portalocker
from boto3.s3.transfer import TransferConfig
from botocore import UNSIGNED
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    NoCredentialsError,
    PartialCredentialsError,
)
from tqdm import tqdm
from urllib3.exceptions import SSLError

"""
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# Set botocore and boto3 loggers to debug
logging.getLogger('boto3').setLevel(logging.DEBUG)
logging.getLogger('botocore').setLevel(logging.DEBUG)
logging.getLogger('s3transfer').setLevel(logging.DEBUG)
logging.getLogger('urllib3').setLevel(logging.DEBUG)
"""


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
                        "Exception thrown when attempting to run %s, attempt "
                        "%d of %d: %s" % (func, attempt, times, exc)
                    )
                    attempt += 1
            return func(*args, **kwargs)

        return newfn

    return decorator


class S3LFS:
    def __init__(
        self,
        bucket_name=None,
        manifest_file=".s3_manifest.json",
        repo_prefix=None,
        encryption=True,
        no_sign_request=False,
        temp_dir=None,
        chunk_size=5 * 1024 * 1024 * 1024,
        s3_factory=None,
    ):
        """
        :param bucket_name: Name of the S3 bucket (can be stored in manifest)
        :param manifest_file: Path to the local manifest (JSON) file
        :param repo_prefix: A unique prefix to isolate this repository's files
        :param encryption: If True, use AES256 server-side encryption
        :param no_sign_request: If True, use unsigned requests
        :param temp_dir: Path to the temporary directory for compression/decompression
        :param chunk_size: Size of chunks for multipart uploads (default: 5 GB)
        :param s3_factory: Custom S3 client factory function (for testing)
        """
        self.chunk_size = chunk_size
        self.s3_factory = (
            (
                lambda no_sign_request: (
                    boto3.session.Session().client(
                        "s3", config=boto3.session.Config(signature_version=UNSIGNED)
                    )
                    if no_sign_request
                    else boto3.session.Session().client("s3")
                )
            )
            if s3_factory is None
            else s3_factory
        )

        # Set the temporary directory to the base of the repository if not provided
        self.temp_dir = Path(temp_dir or ".s3lfs_temp")
        self.temp_dir.mkdir(parents=True, exist_ok=True)  # Ensure the directory exists

        # Use a file-based lock for cross-process synchronization
        self._lock_file = self.temp_dir / ".s3lfs.lock"

        if no_sign_request:
            # If we're not signing, we can't use multipart. Set the threshold to the max.
            self.config = TransferConfig(
                multipart_threshold=5 * 1024 * 1024 * 1024, max_concurrency=10
            )
        else:
            self.config = TransferConfig(max_concurrency=10)
        self.thread_local = threading.local()
        self.manifest_file = Path(manifest_file)
        self.no_sign_request = no_sign_request
        self.load_manifest()

        # Use the stored bucket name if none is provided
        with self._lock_context():
            if bucket_name:
                self.bucket_name = bucket_name
                self.manifest["bucket_name"] = bucket_name
            else:
                self.bucket_name = self.manifest.get("bucket_name")

        if not self.bucket_name:
            raise ValueError("Bucket name must be provided or stored in the manifest.")

        with self._lock_context():
            if repo_prefix:
                self.repo_prefix = repo_prefix
                self.manifest["repo_prefix"] = repo_prefix
            else:
                self.repo_prefix = self.manifest.get("repo_prefix", "s3lfs")
            self.save_manifest()

        self.encryption = encryption

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
                raise RuntimeError(
                    "AWS credentials are missing. Please configure them or use --no-sign-request."
                )
            except PartialCredentialsError:
                raise RuntimeError(
                    "Incomplete AWS credentials. Check your AWS configuration."
                )
            except ClientError as e:
                if e.response["Error"]["Code"] in [
                    "InvalidAccessKeyId",
                    "SignatureDoesNotMatch",
                ]:
                    raise RuntimeError(
                        "Invalid AWS credentials. Please verify your access key and secret key."
                    )
                raise RuntimeError(f"Error initializing S3 client: {e}")

        return self.thread_local.s3

    def initialize_repo(self):
        """
        Initialize the repository with a bucket name and a repo-specific prefix.

        :param bucket_name: Name of the S3 bucket to use
        :param repo_prefix: A unique prefix for this repository in the bucket
        """
        with self._lock_context():
            self.manifest["bucket_name"] = self.bucket_name
            self.manifest["repo_prefix"] = self.repo_prefix
            self.save_manifest()

        print("✅ Successfully initialized S3LFS with:")
        print(f"   Bucket Name: {self.bucket_name}")
        print(f"   Repo Prefix: {self.repo_prefix}")
        print("Manifest file saved as .s3_manifest.json")

    def track_subtree(self, directory, silence=True):
        """
        Deprecated: Use `track` instead.
        """
        print("⚠️ `track_subtree` is deprecated. Use `track` instead.")
        self.track(directory, silence=silence)

    def load_manifest(self):
        """Load the local manifest (.s3_manifest.json)."""
        if self.manifest_file.exists():
            with open(self.manifest_file, "r") as f:
                self.manifest = json.load(f)
        else:
            self.manifest = {"files": {}}  # Use file paths as keys

    def save_manifest(self):
        """Save the manifest back to disk atomically."""
        temp_file = self.manifest_file.with_suffix(
            ".tmp"
        )  # Temporary file in the same directory
        try:
            # Write the manifest to a temporary file
            with open(temp_file, "w") as f:
                json.dump(self.manifest, f, indent=4, sort_keys=True)

            # Atomically move the temporary file to the target location
            temp_file.replace(self.manifest_file)
        except Exception as e:
            print(f"❌ Failed to save manifest: {e}")
            if temp_file.exists():
                temp_file.unlink()  # Clean up the temporary file

    def hash_file(self, file_path, method="auto"):
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
            elif sys.platform.startswith("linux") and shutil.which("sha256sum"):
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

    def _hash_file_mmap(self, file_path):
        """
        Compute the SHA-256 hash using memory-mapped files.
        """
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                hasher.update(mm)
        return hasher.hexdigest()

    def _hash_file_iter(self, file_path, chunk_size=1024 * 1024):
        """
        Compute the SHA-256 hash by iteratively reading the file in chunks.
        """
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
            if sys.platform.startswith("linux") and shutil.which("gzip"):
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
        compressed_path = self.temp_dir / f"{uuid4()}.gz"
        buffer_size = 1024 * 1024  # 1 MB chunks

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

    def file_exists_in_s3(self, s3_key):
        """Check if a file exists in the S3 bucket."""
        try:
            self._get_s3_client().head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def get_git_commit(self):
        """Retrieve the current Git commit hash to use for tagging in S3."""
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True
        )
        return result.stdout.strip()

    @retry(3, (BotoCoreError, ClientError, SSLError))
    def upload(self, file_path, silence=False, needs_immediate_update=True):
        """
        Upload a file to S3 and update the manifest using the file path as the key.
        """
        file_path = Path(file_path)
        if not file_path.exists():
            print(f"Error: {file_path} does not exist.")
            return

        file_hash = self.hash_file(file_path)
        s3_key = f"{self.repo_prefix}/assets/{file_hash}/{file_path.as_posix()}.gz"

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
                with tqdm(
                    total=file_size,
                    unit="B",
                    unit_scale=True,
                    desc=f"Uploading {path.name}",
                    leave=False,
                ) as progress_bar:
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
                    with open(path, "rb") as f:
                        self._get_s3_client().upload_fileobj(
                            f,
                            self.bucket_name,
                            s3_key if not chunked else f"{s3_key}.chunk{chunk_idx}",
                            ExtraArgs=extra_args,
                            Config=self.config,
                            Callback=progress_bar.update,
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
                self.manifest["files"][str(file_path.as_posix())] = file_hash
                self.save_manifest()
        if not silence:
            print(f"Uploaded {file_path} -> s3://{self.bucket_name}/{s3_key}")

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
            if sys.platform.startswith("linux") and shutil.which("gzip"):
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
        with gzip.open(compressed_path, "rb") as f_in, open(output_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

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
    def download(self, file_path, silence=False):
        """
        Download a file from S3 by its recorded hash, but skip if it already exists and matches.
        """
        file_path = Path(file_path)

        # Get the expected hash for the file
        with self._lock_context():
            expected_hash = self.manifest["files"].get(str(file_path.as_posix()))
        if not expected_hash:
            print(f"⚠️ File '{file_path}' is not in the manifest.")
            return

        # If the file exists, check its hash
        if not silence:
            print(f"file_path exists?: {file_path.exists()}")
        if file_path.exists():
            current_hash = self.hash_file(file_path)
            if not silence:
                print(f"current_hash: {current_hash}")
                print(f"expected_hash: {expected_hash}")
            if current_hash == expected_hash:
                if not silence:
                    print(f"✅ Skipping download: '{file_path}' is already up-to-date.")
                return  # Skip download if hashes match

        # Proceed with download if file is missing or different
        s3_key = f"{self.repo_prefix}/assets/{expected_hash}/{file_path.as_posix()}.gz"

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
        for idx, key in enumerate(keys):
            try:
                target_path = self.temp_dir / f"{uuid4()}.gz"
                target_paths.append(target_path)
                obj = self._get_s3_client().head_object(
                    Bucket=self.bucket_name, Key=key
                )
                file_size = obj["ContentLength"]
                with tqdm(
                    total=file_size,
                    unit="B",
                    unit_scale=True,
                    desc=f"Downloading {os.path.basename(key)}",
                    leave=False,
                ) as progress_bar:
                    print(f"Downloading {key} to {target_path}")
                    with open(target_path, "wb") as f:
                        self._get_s3_client().download_fileobj(
                            Bucket=self.bucket_name,
                            Key=key,
                            Fileobj=f,
                            Callback=progress_bar.update,
                        )
            except Exception as e:
                print(f"❌ Error downloading {key}: {e}")

        if chunk_keys:
            compressed_path = self.merge_files(compressed_path, target_paths)
            for path in target_paths:
                os.remove(path)
        else:
            compressed_path = target_paths[0]

        if os.path.dirname(file_path):
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
        try:
            self.decompress_file(compressed_path, file_path)
        except Exception as e:
            print(f"❌ Error decompressing {compressed_path} for key {keys}: {e}")
            raise
        os.remove(compressed_path)  # Ensure temp file is deleted
        if not silence:
            print(f"📥 Downloaded {file_path} from s3://{self.bucket_name}/{s3_key}")

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
                    parts = key.replace(self.repo_prefix + "/", "").split("/")
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
        with ThreadPoolExecutor(max_workers=8) as executor:
            results = zip(files_to_check, executor.map(self.hash_file, files_to_check))

        # Process results
        for file, current_hash in results:
            with self._lock_context():
                stored_hash = self.manifest.get(file)

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

        with ThreadPoolExecutor(max_workers=8) as executor:
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
            with ThreadPoolExecutor(max_workers=8) as executor:
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

    def sparse_checkout(self, prefix, silence=True):
        """
        Deprecated: Use `checkout` instead.
        """
        print("⚠️ `sparse_checkout` is deprecated. Use `checkout` instead.")
        self.checkout(prefix, silence=silence)

    def integrate_with_git(self):
        """
        Set up Git hooks for a more seamless S3-based large file workflow.
        """
        hook_path = ".git/hooks/pre-commit"
        new_command = "s3lfs track-modified\n"

        # Ensure the hooks directory exists
        os.makedirs(os.path.dirname(hook_path), exist_ok=True)

        # Check if the pre-commit hook already exists
        if os.path.exists(hook_path):
            with open(hook_path, "r") as f:
                existing_content = f.readlines()

            # Avoid duplicate entries
            if new_command not in existing_content:
                with open(hook_path, "a") as f:
                    f.write("\n" + new_command)
        else:
            # Create a new pre-commit hook
            with open(hook_path, "w") as f:
                f.write("#!/bin/sh\n" + new_command)

        # Ensure the hook is executable
        os.chmod(hook_path, 0o755)
        print("Git integration setup completed.")

    def remove_subtree(self, directory, keep_in_s3=True):
        """
        Remove all files under a specified directory from tracking.
        Optionally keep the files in S3 for historical reference.

        :param directory: The directory to remove from tracking.
        :param keep_in_s3: If False, delete the files from S3 as well.
        """
        directory = Path(directory)
        directory_str = str(directory.as_posix())

        with self._lock_context():
            files_to_remove = [
                path
                for path in self.manifest["files"]
                if path.startswith(directory_str)
            ]

        if not files_to_remove:
            print(f"⚠️ No tracked files found in '{directory}'.")
            return

        for file_path in files_to_remove:
            file_hash = self.manifest["files"].pop(file_path, None)
            if not keep_in_s3 and file_hash:
                s3_key = f"{self.repo_prefix}/assets/{file_hash}/{file_path}.gz"
                self._get_s3_client().delete_object(Bucket=self.bucket_name, Key=s3_key)
                print(f"🗑 File removed from S3: s3://{self.bucket_name}/{s3_key}")

        with self._lock_context():
            self.save_manifest()

        print(f"🗑 Removed tracking for all files under '{directory}'.")

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
            raise RuntimeError("AWS credentials are missing. Please configure them.")
        except PartialCredentialsError:
            raise RuntimeError(
                "Incomplete AWS credentials. Check your AWS configuration."
            )
        except ClientError as e:
            if e.response["Error"]["Code"] in [
                "InvalidAccessKeyId",
                "SignatureDoesNotMatch",
                "AccessDenied",
            ]:
                raise RuntimeError(
                    f"Invalid or insufficient AWS credentials for bucket '{self.bucket_name}'."
                )
            raise RuntimeError(f"Error testing S3 credentials: {e}")

    def _resolve_filesystem_paths(self, path):
        """
        Resolve a path pattern to actual filesystem paths.
        Used for tracking operations.

        :param path: Path object that could be a file, directory, or glob pattern
        :return: List of Path objects for files found
        """
        path = Path(path)

        # If it's an existing file, return it directly
        if path.is_file():
            return [path]

        # If it's an existing directory, get all files recursively
        if path.is_dir():
            return [f for f in path.rglob("*") if f.is_file()]

        # Otherwise treat as a glob pattern
        # Handle both absolute and relative patterns properly
        if path.is_absolute():
            # For absolute paths, use glob.glob directly
            matched_paths = glob.glob(str(path), recursive=True)
        else:
            # For relative paths, use Path.glob for better handling
            try:
                if "/" in str(path):
                    # Multi-level glob pattern like "data/**/*.txt"
                    parent = Path(".")
                    pattern = str(path)
                    matched_paths = [str(p) for p in parent.glob(pattern)]
                else:
                    # Simple pattern like "*.txt"
                    matched_paths = glob.glob(str(path))
            except Exception:
                # Fallback to simple glob
                matched_paths = glob.glob(str(path))

        # Filter to only return files, not directories
        return [Path(p) for p in matched_paths if Path(p).is_file()]

    def _resolve_manifest_paths(self, path):
        """
        Resolve a path pattern against the manifest contents.
        Used for checkout operations.

        :param path: Path object that could be a file, directory, or glob pattern
        :return: Dictionary of manifest entries {file_path: hash}
        """
        path_str = str(Path(path).as_posix())

        with self._lock_context():
            manifest_files = self.manifest["files"]

            # Check for exact file match first
            if path_str in manifest_files:
                return {path_str: manifest_files[path_str]}

            # Check if it has glob characters
            has_glob_chars = any(char in path_str for char in ["*", "?", "[", "]"])

            if has_glob_chars:
                # Implement proper filesystem-like glob behavior
                matched_files = {}
                for file_path, file_hash in manifest_files.items():
                    if self._glob_match(file_path, path_str):
                        matched_files[file_path] = file_hash
            else:
                # Treat as directory prefix - match files that start with the path
                # Add trailing slash if not present to avoid partial matches
                prefix = path_str if path_str.endswith("/") else f"{path_str}/"
                matched_files = {
                    file_path: file_hash
                    for file_path, file_hash in manifest_files.items()
                    if file_path.startswith(prefix)
                }

                # If no directory matches found, it might be a file without extension
                # or a directory that was specified without trailing slash
                if not matched_files:
                    # Try matching files that start with the exact path (for files)
                    matched_files = {
                        file_path: file_hash
                        for file_path, file_hash in manifest_files.items()
                        if file_path == path_str
                    }

            return matched_files

    def _glob_match(self, file_path, pattern):
        """
        Custom glob matching that behaves like filesystem glob.

        :param file_path: The file path to test
        :param pattern: The glob pattern
        :return: True if the file path matches the pattern
        """
        # Handle ** recursive patterns
        if "**" in pattern:
            # Convert ** patterns to regex-like behavior
            # Split on ** and handle each part
            parts = pattern.split("**")
            if len(parts) == 2:
                prefix, suffix = parts
                prefix = prefix.rstrip("/")
                suffix = suffix.lstrip("/")

                # Check if file starts with prefix (if any) and ends with suffix pattern
                if prefix and not file_path.startswith(prefix):
                    return False

                # For the suffix, we need to match it against the remaining path
                if suffix:
                    if prefix:
                        remaining_path = file_path[len(prefix) :].lstrip("/")
                    else:
                        remaining_path = file_path

                    # Use fnmatch for the suffix part
                    return fnmatch.fnmatch(remaining_path, suffix)
                else:
                    # Pattern ends with **, so just check prefix
                    return not prefix or file_path.startswith(prefix)
            else:
                # Multiple ** or more complex pattern - fall back to fnmatch
                return fnmatch.fnmatch(file_path, pattern)
        else:
            # For non-recursive patterns, we need to ensure * doesn't cross directories
            # Split both the pattern and file path by / and match segment by segment
            pattern_parts = pattern.split("/")
            file_parts = file_path.split("/")

            # If pattern has fewer parts than file, it can't match (unless it's just *)
            if len(pattern_parts) != len(file_parts):
                return False

            # Match each segment
            for pattern_part, file_part in zip(pattern_parts, file_parts):
                if not fnmatch.fnmatch(file_part, pattern_part):
                    return False

            return True

    def track(self, path, silence=True, interleaved=True):
        """
        Track and upload files, directories, or globs.

        :param path: A file, directory, or glob pattern to track.
        :param silence: Silences verbose logging.
        :param interleaved: If True, use interleaved hashing and uploading for better performance.
        """
        if interleaved:
            return self.track_interleaved(path, silence=silence)

        # Original two-stage implementation
        # Phase 1: Resolve filesystem paths and compute hashes
        print("🔍 Resolving filesystem paths and computing hashes...")
        files_to_track = self._resolve_filesystem_paths(path)

        if not files_to_track:
            print(f"⚠️ No files found to track for '{path}'.")
            return

        # Compute hashes in parallel with a progress bar
        with tqdm(total=len(files_to_track), desc="Hashing files", unit="file") as pbar:
            with ThreadPoolExecutor(max_workers=8) as executor:
                file_hashes = {
                    str(file.as_posix()): hash_result
                    for file, hash_result in zip(
                        files_to_track,
                        executor.map(
                            lambda f: self._hash_with_progress(f, pbar), files_to_track
                        ),
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
            with ThreadPoolExecutor(max_workers=8) as executor:
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
                self.manifest["files"][file_path] = file_hash
            self.save_manifest()

        print(f"✅ Successfully tracked and uploaded files for '{path}'.")

    def _hash_with_progress(self, file_path, progress_bar):
        """
        Helper function to compute the hash of a file and update the progress bar.
        """
        result = self.hash_file(file_path)
        progress_bar.update(1)
        return result

    def checkout(self, path, silence=True, interleaved=True):
        """
        Checkout files, directories, or globs from the manifest.

        :param path: A file, directory, or glob pattern to checkout.
        :param silence: Silences verbose logging.
        :param interleaved: If True, use interleaved hashing and downloading for better performance.
        """
        if interleaved:
            return self.checkout_interleaved(path, silence=silence)

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
            with ThreadPoolExecutor(max_workers=8) as executor:
                future_to_file = {
                    executor.submit(self._hash_with_progress, Path(file), pbar): file
                    for file in files_to_checkout.keys()
                    if Path(file).exists()  # Only hash files that exist on disk
                }

                for future in as_completed(future_to_file):
                    file = future_to_file[future]
                    try:
                        file_hashes[file] = future.result()
                    except Exception as exc:
                        print(f"Error hashing file {file}: {exc}")

        # Add files that don't exist on disk to the download list
        for file in files_to_checkout.keys():
            if not Path(file).exists():
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
            with ThreadPoolExecutor(max_workers=8) as executor:
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

    def _hash_and_upload_worker(self, file_path, silence=True):
        """
        Worker function that hashes a file and uploads it if needed.
        Returns (file_path, hash, uploaded) tuple.
        """
        try:
            current_hash = self.hash_file(file_path)

            # Check if upload is needed
            with self._lock_context():
                stored_hash = self.manifest["files"].get(
                    str(Path(file_path).as_posix())
                )

            if current_hash == stored_hash:
                return (file_path, current_hash, False)  # No upload needed

            # Upload the file
            self.upload(file_path, silence=silence, needs_immediate_update=False)
            return (file_path, current_hash, True)  # Upload completed

        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            raise

    def _hash_and_download_worker(self, file_info, silence=True):
        """
        Worker function that checks if a file needs download and downloads it if needed.
        file_info is (file_path, expected_hash) tuple.
        Returns (file_path, downloaded) tuple.
        """
        file_path, expected_hash = file_info
        try:
            # Check if file exists and has correct hash
            if Path(file_path).exists():
                current_hash = self.hash_file(file_path)
                if current_hash == expected_hash:
                    return (file_path, False)  # No download needed

            # Download the file
            self.download(file_path, silence=silence)
            return (file_path, True)  # Download completed

        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            raise

    def track_interleaved(self, path, silence=True):
        """
        Track and upload files with interleaved hashing and uploading for better performance.

        :param path: A file, directory, or glob pattern to track.
        :param silence: Silences verbose logging.
        """
        # Phase 1: Resolve filesystem paths
        print("🔍 Resolving filesystem paths...")
        files_to_track = self._resolve_filesystem_paths(path)

        if not files_to_track:
            print(f"⚠️ No files found to track for '{path}'.")
            return

        # Test S3 credentials once before starting parallel operations
        if not silence:
            print("🔐 Testing S3 credentials...")
        self.test_s3_credentials(silence=silence)

        print(
            f"🚀 Processing {len(files_to_track)} files with interleaved hashing and uploading..."
        )

        # Phase 2: Process files with interleaved hashing and uploading
        files_uploaded = []
        files_processed = 0

        try:
            with ThreadPoolExecutor(max_workers=8) as executor:
                # Submit all hash-and-upload tasks
                future_to_file = {
                    executor.submit(
                        self._hash_and_upload_worker, str(file.as_posix()), silence
                    ): file
                    for file in files_to_track
                }

                # Process results as they complete
                with tqdm(
                    total=len(files_to_track), desc="Processing files", unit="file"
                ) as pbar:
                    for future in as_completed(future_to_file):
                        if self._shutdown_requested:
                            print(
                                "⚠️ Shutdown requested. Cancelling remaining operations..."
                            )
                            return

                        try:
                            file_path, file_hash, uploaded = future.result()
                            files_processed += 1

                            if uploaded:
                                files_uploaded.append((file_path, file_hash))

                            pbar.update(1)

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
                    self.manifest["files"][file_path] = file_hash
                self.save_manifest()

        print(
            f"✅ Successfully processed {files_processed} files ({len(files_uploaded)} uploaded) for '{path}'."
        )

    def checkout_interleaved(self, path, silence=True):
        """
        Checkout files with interleaved hashing and downloading for better performance.

        :param path: A file, directory, or glob pattern to checkout.
        :param silence: Silences verbose logging.
        """
        # Phase 1: Resolve manifest paths
        print("🔒 Resolving paths from manifest...")
        files_to_checkout = self._resolve_manifest_paths(path)

        if not files_to_checkout:
            print(f"⚠️ No files found in the manifest for '{path}'.")
            return

        # Test S3 credentials once before starting parallel operations
        if not silence:
            print("🔐 Testing S3 credentials...")
        self.test_s3_credentials(silence=silence)

        print(
            f"🚀 Processing {len(files_to_checkout)} files with interleaved hashing and downloading..."
        )

        # Phase 2: Process files with interleaved hashing and downloading
        files_downloaded = 0
        files_processed = 0

        try:
            with ThreadPoolExecutor(max_workers=8) as executor:
                # Submit all hash-and-download tasks
                future_to_file = {
                    executor.submit(
                        self._hash_and_download_worker,
                        (file_path, expected_hash),
                        silence,
                    ): file_path
                    for file_path, expected_hash in files_to_checkout.items()
                }

                # Process results as they complete
                with tqdm(
                    total=len(files_to_checkout), desc="Processing files", unit="file"
                ) as pbar:
                    for future in as_completed(future_to_file):
                        if self._shutdown_requested:
                            print(
                                "⚠️ Shutdown requested. Cancelling remaining operations..."
                            )
                            break

                        try:
                            file_path, downloaded = future.result()
                            files_processed += 1

                            if downloaded:
                                files_downloaded += 1

                            pbar.update(1)

                        except Exception as e:
                            print(f"An error occurred during processing: {e}")
                            raise

        except KeyboardInterrupt:
            print("\n⚠️ Processing interrupted by user.")
        finally:
            print(
                f"✅ Successfully processed {files_processed} files ({files_downloaded} downloaded) for '{path}'."
            )
