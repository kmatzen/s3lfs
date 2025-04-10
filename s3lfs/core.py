import gzip
import hashlib
import json
import mmap
import os
import shutil
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from uuid import uuid4

import boto3
import psutil
from boto3.s3.transfer import TransferConfig
from botocore import UNSIGNED
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    NoCredentialsError,
    PartialCredentialsError,
)
from filelock import FileLock
from tqdm import tqdm
from urllib3.exceptions import SSLError


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
    ):
        """
        :param bucket_name: Name of the S3 bucket (can be stored in manifest)
        :param manifest_file: Path to the local manifest (JSON) file
        :param repo_prefix: A unique prefix to isolate this repository's files
        :param encryption: If True, use AES256 server-side encryption
        :param no_sign_request: If True, use unsigned requests
        :param temp_dir: Path to the temporary directory for compression/decompression
        """
        # Set the temporary directory to the base of the repository if not provided
        self.temp_dir = Path(temp_dir or ".s3lfs_temp")
        self.temp_dir.mkdir(parents=True, exist_ok=True)  # Ensure the directory exists

        # Use a file-based lock for cross-process synchronization
        self.lock_file = self.temp_dir / ".s3lfs.lock"
        self.lock = FileLock(self.lock_file)

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
        with self._acquire_lock():
            if bucket_name:
                self.bucket_name = bucket_name
                self.manifest["bucket_name"] = bucket_name
            else:
                self.bucket_name = self.manifest.get("bucket_name")

        if not self.bucket_name:
            raise ValueError("Bucket name must be provided or stored in the manifest.")

        with self._acquire_lock():
            if repo_prefix:
                self.repo_prefix = repo_prefix
                self.manifest["repo_prefix"] = repo_prefix
            else:
                self.repo_prefix = self.manifest.get("repo_prefix", "s3lfs")

        self.encryption = encryption
        self.save_manifest()

    def _acquire_lock(self):
        """
        Acquire the file-based lock, checking for stale locks.
        If the lock file exists, verify the PID of the process that created it.
        """
        if self.lock_file.exists():
            with open(self.lock_file, "r") as f:
                try:
                    pid = int(f.read().strip())
                    if not psutil.pid_exists(pid):
                        print(f"Stale lock detected (PID {pid}). Removing it.")
                        self.lock_file.unlink()  # Remove the stale lock file
                except ValueError:
                    print("Invalid PID in lock file. Removing it.")
                    self.lock_file.unlink()  # Remove invalid lock file

        # Acquire the lock and write the current PID to the lock file
        context = self.lock.acquire()
        with open(self.lock_file, "w") as f:
            f.write(str(os.getpid()))
        return context

    def _release_lock(self):
        """
        Release the file-based lock and remove the PID from the lock file.
        """
        if self.lock.is_locked:
            self.lock.release()
        if self.lock_file.exists():
            self.lock_file.unlink()

    def _get_s3_client(self):
        """Ensures each thread gets its own instance of the S3 client with appropriate authentication handling."""
        if not hasattr(self.thread_local, "s3"):
            session = boto3.session.Session()
            try:
                if self.no_sign_request:
                    self.thread_local.s3 = session.client(
                        "s3", config=boto3.session.Config(signature_version=UNSIGNED)
                    )
                else:
                    self.thread_local.s3 = session.client("s3")

                # Check if credentials are valid by attempting to list buckets
                if not self.no_sign_request:
                    self.thread_local.s3.list_buckets()

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
        with self.lock:
            self.manifest["bucket_name"] = self.bucket_name
            self.manifest["repo_prefix"] = self.repo_prefix
        self.save_manifest()

        print("✅ Successfully initialized S3LFS with:")
        print(f"   Bucket Name: {self.bucket_name}")
        print(f"   Repo Prefix: {self.repo_prefix}")
        print("Manifest file saved as .s3_manifest.json")

    def track_subtree(self, directory, silence=True):
        """
        Track and upload all files within a given directory (subtree).

        :param directory: The directory to recursively scan and upload.
        :param silence: Silences verbose logging
        """
        directory = Path(directory)

        if not directory.is_dir():
            print(f"Error: {directory} is not a valid directory.")
            return

        files_to_upload = [f for f in directory.rglob("*") if f.is_file()]

        if not files_to_upload:
            print(f"No files found in {directory} to track.")
            return

        print(f"Tracking {len(files_to_upload)} files in {directory}...")

        # Test S3 credentials once before starting the parallel upload
        self.test_s3_credentials()

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [
                executor.submit(self.upload, f, silence=silence)
                for f in files_to_upload
            ]

            for future in tqdm(
                as_completed(futures), total=len(futures), desc="Uploading subtree"
            ):
                try:
                    future.result()  # Will re-raise exceptions from the worker thread
                except Exception as e:
                    print(f"An error occurred during upload: {e}")

        print(f"✅ Successfully tracked and uploaded all files in {directory}")

    def load_manifest(self):
        """Load the local manifest (.s3_manifest.json)."""
        with self._acquire_lock():
            if self.manifest_file.exists():
                with open(self.manifest_file, "r") as f:
                    self.manifest = json.load(f)
            else:
                self.manifest = {"files": {}}  # Use file paths as keys

    def save_manifest(self):
        """Save the manifest back to disk."""
        with self._acquire_lock():
            with open(self.manifest_file, "w") as f:
                json.dump(self.manifest, f, indent=4, sort_keys=True)

    def hash_file(self, file_path):
        """
        Compute a unique SHA-256 hash of the file using its content and relative path.
        This prevents files with identical contents but different paths from colliding.
        """
        hasher = hashlib.sha256()

        file_path = Path(file_path)

        # Include the relative file path as a unique identifier
        relative_path = str(file_path.as_posix()).encode()
        hasher.update(relative_path)

        # Hash the file content
        with open(file_path, "rb") as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                hasher.update(mm)

        return hasher.hexdigest()

    def compress_file(self, file_path):
        """Compress the file using gzip and return the path of the compressed file in the temp directory."""
        compressed_path = self.temp_dir / f"{uuid4()}.gz"

        # Use a larger buffer size (1 MB) for reading/writing.
        buffer_size = 1024 * 1024  # 1 MB chunks
        # Lower the compression level for speed (default is 9)
        with open(file_path, "rb") as f_in, gzip.open(
            compressed_path, "wb", compresslevel=5
        ) as f_out:
            shutil.copyfileobj(f_in, f_out, length=buffer_size)

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
    def upload(self, file_path, silence=False):
        """
        Upload a file to S3 and update the manifest using the file path as the key.
        """
        file_path = Path(file_path)
        if not file_path.exists():
            print(f"Error: {file_path} does not exist.")
            return

        file_hash = self.hash_file(file_path)
        s3_key = f"{self.repo_prefix}/assets/{file_hash}/{file_path.as_posix()}.gz"

        if not self.file_exists_in_s3(s3_key) or (
            file_path.as_posix() not in self.manifest["files"]
        ):
            extra_args = {"ServerSideEncryption": "AES256"} if self.encryption else {}
            compressed_path = self.compress_file(file_path)
            try:
                if not silence:
                    print(f"Uploading {file_path}")
                self._get_s3_client().upload_file(
                    compressed_path,
                    self.bucket_name,
                    s3_key,
                    ExtraArgs=extra_args,
                    Config=self.config,
                )
                if not silence:
                    print(f"{file_path} uploaded")
            finally:
                try:
                    os.remove(compressed_path)
                except OSError:
                    pass

            # Store file path as key, hash as value
            with self.lock:
                self.manifest["files"][str(file_path.as_posix())] = file_hash
            self.save_manifest()
            if not silence:
                print(f"Uploaded {file_path} -> s3://{self.bucket_name}/{s3_key}")
        elif not silence:
            print(
                f"File {file_path} (hash: {file_hash}) already exists in S3. Skipping."
            )

    @retry(3, (BotoCoreError, ClientError, SSLError))
    def download(self, file_path, silence=False):
        """
        Download a file from S3 by its recorded hash, but skip if it already exists and matches.
        """
        file_path = Path(file_path)

        # Get the expected hash for the file
        with self.lock:
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

        try:
            os.makedirs(os.path.dirname(compressed_path), exist_ok=True)
            self._get_s3_client().download_file(
                Bucket=self.bucket_name,
                Key=s3_key,
                Filename=str(compressed_path),
                Config=self.config,
            )
            if os.path.dirname(file_path):
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with gzip.open(compressed_path, "rb") as f_in, open(
                file_path, "wb"
            ) as f_out:
                shutil.copyfileobj(f_in, f_out)
            os.remove(compressed_path)  # Ensure temp file is deleted
            if not silence:
                print(f"📥 Downloaded {file_path} from s3://{self.bucket_name}/{s3_key}")

        except Exception as e:
            print(f"❌ Error downloading {file_path}: {e}")

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

        with self.lock:
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
        with self.lock:
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
        with self.lock:
            files_to_check = list(
                self.manifest["files"].keys()
            )  # Files listed in the manifest

        # Compute hashes in parallel
        with ThreadPoolExecutor(max_workers=8) as executor:
            results = zip(files_to_check, executor.map(self.hash_file, files_to_check))

        # Process results
        for file, current_hash in results:
            with self.lock:
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
            self.save_manifest()
        else:
            print("No modified files needing upload.")

    def parallel_upload(self, files, silence=True):
        """Parallel upload of multiple files using ThreadPoolExecutor."""
        with ThreadPoolExecutor(max_workers=8) as executor:
            # Submit each download task; unpack key from matching_files.items()
            futures = [executor.submit(self.upload, f, silence=silence) for f in files]

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
        with self.lock:
            items = list(
                self.manifest["files"].items()
            )  # File paths as keys, hashes as values

        if not items:
            print("⚠️ Manifest is empty. Nothing to download.")
            return

        print("📥 Starting parallel download of all tracked files...")

        # Test S3 credentials once before starting the parallel download
        self.test_s3_credentials()

        with ThreadPoolExecutor(max_workers=8) as executor:
            # Submit all tasks and collect futures
            futures = [
                executor.submit(self.download, kv[0], silence=silence) for kv in items
            ]

            # Iterate over futures as they complete
            for future in tqdm(
                as_completed(futures), total=len(futures), desc="Downloading files"
            ):
                try:
                    future.result()  # This will re-raise any exceptions from the thread.
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")

        print("✅ All files downloaded.")

    def sparse_checkout(self, prefix, silence=True):
        """
        Sparse checkout: download only files that match a given directory prefix.

        :param prefix: The directory prefix to match.
        """
        prefix = str(Path(prefix).as_posix()) + "/"  # Ensure trailing slash
        with self.lock:
            matching_files = {
                path: hash
                for path, hash in self.manifest["files"].items()
                if path.startswith(prefix)
            }

        if not matching_files:
            print(f"⚠️ No files found for prefix '{prefix}' in the manifest.")
            return

        print(f"📥 Downloading {len(matching_files)} files from '{prefix}'...")

        with ThreadPoolExecutor(max_workers=8) as executor:
            # Submit each download task; unpack key from matching_files.items()
            futures = [
                executor.submit(self.download, key, silence=silence)
                for key, _ in matching_files.items()
            ]

            for future in tqdm(
                as_completed(futures), total=len(futures), desc="Downloading subtree"
            ):
                try:
                    # This will raise the exception if the download failed
                    future.result()
                except Exception as e:
                    # Handle any other exceptions that may occur
                    print(f"An error occurred: {e}")

        print(f"✅ Sparse checkout of '{prefix}' completed.")

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

        with self.lock:
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

        self.save_manifest()

        print(f"🗑 Removed tracking for all files under '{directory}'.")

    def test_s3_credentials(self):
        """
        Test the S3 credentials to ensure they are valid.
        This prevents repeated failures during bulk operations.
        """
        try:
            # Attempt to list buckets to verify credentials
            self._get_s3_client().list_buckets()
            print("✅ S3 credentials are valid.")
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
            ]:
                raise RuntimeError(
                    "Invalid AWS credentials. Please verify your access key and secret key."
                )
            raise RuntimeError(f"Error testing S3 credentials: {e}")
