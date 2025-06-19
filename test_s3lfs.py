import json
import os
import shutil
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import boto3
from botocore.exceptions import ClientError
from moto import mock_s3

from s3lfs import S3LFS


@mock_s3
class TestS3LFS(unittest.TestCase):
    def setUp(self):
        self.s3_mock = mock_s3()
        self.s3_mock.start()

        self.bucket_name = "testbucket"
        self.s3 = boto3.client("s3")
        self.s3.create_bucket(Bucket=self.bucket_name)

        # Create our S3LFS instance
        self.versioner = S3LFS(bucket_name=self.bucket_name)

        self.test_directory = "test_data/"
        os.makedirs(self.test_directory, exist_ok=True)

        # Create a couple of small test files
        self.test_file = os.path.join(self.test_directory, "test_file.txt")
        with open(self.test_file, "w") as f:
            f.write("This is a test file.")

        self.another_test_file = "another_test_file.txt"
        with open(self.another_test_file, "w") as f:
            f.write("Another test file content.")

    def tearDown(self):
        self.s3_mock.stop()

        # Clean up local files
        if os.path.exists(self.test_file):
            os.remove(self.test_file)
        if os.path.exists(self.another_test_file):
            os.remove(self.another_test_file)

        # Clean up the manifest if created
        if os.path.exists(self.versioner.manifest_file):
            os.remove(self.versioner.manifest_file)

        if os.path.exists(self.test_directory):
            os.rmdir(self.test_directory)

    # -------------------------------------------------
    # 1. Basic Upload & Manifest Tracking
    # -------------------------------------------------
    def test_upload_file(self):
        """Test if uploading a file correctly tracks it in the manifest and S3."""
        self.versioner.upload(self.test_file)
        manifest = self.versioner.manifest
        file_hash = self.versioner.hash_file(self.test_file)
        s3_key = f"s3lfs/assets/{file_hash}/{self.test_file}.gz"

        # Check that the manifest correctly tracks the file path
        self.assertIn(self.test_file, manifest["files"])
        self.assertEqual(manifest["files"][self.test_file], file_hash)

        # Check that the file was uploaded to S3
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=s3_key)
        self.assertTrue("Contents" in response and len(response["Contents"]) == 1)

    def test_manifest_tracking(self):
        """Test if uploaded files are correctly tracked in the manifest."""
        self.versioner.upload(self.test_file)
        file_hash = self.versioner.hash_file(self.test_file)

        with open(self.versioner.manifest_file, "r") as f:
            manifest_data = json.load(f)

        # Check that the file path (not hash) is correctly stored in the manifest
        self.assertIn(self.test_file, manifest_data["files"])
        self.assertEqual(manifest_data["files"][self.test_file], file_hash)

    # -------------------------------------------------
    # 2. Download (Single & Multiple)
    # -------------------------------------------------
    def test_download_file(self):
        self.versioner.upload(self.test_file)

        # Re-download to the same path
        self.versioner.download(self.test_file)
        self.assertTrue(os.path.exists(self.test_file))

        with open(self.test_file, "r") as f:
            content = f.read()
        self.assertEqual(content, "This is a test file.")

    def test_multiple_file_upload_download(self):
        self.versioner.upload(self.test_file)
        self.versioner.upload(self.another_test_file)

        os.remove(self.test_file)
        os.remove(self.another_test_file)

        # Download both
        self.versioner.download(self.test_file)
        self.versioner.download(self.another_test_file)

        # Verify contents
        with open(self.test_file, "r") as f:
            content1 = f.read()
        with open(self.another_test_file, "r") as f:
            content2 = f.read()

        self.assertEqual(content1, "This is a test file.")
        self.assertEqual(content2, "Another test file content.")

    def test_chunked_upload_and_download(self):
        chunk_size = self.versioner.chunk_size
        self.versioner.chunk_size = 4

        try:
            self.versioner.upload(self.test_file)

            os.remove(self.test_file)

            self.versioner.download(self.test_file)

            # Verify contents
            with open(self.test_file, "r") as f:
                content1 = f.read()

            self.assertEqual(content1, "This is a test file.")
        finally:
            # Reset chunk size to default
            self.versioner.chunk_size = chunk_size

    # -------------------------------------------------
    # 3. Sparse Checkout
    # -------------------------------------------------
    def test_sparse_checkout(self):
        """Test if sparse_checkout correctly downloads files matching a directory prefix."""
        test_directory = "test_data/"
        self.versioner.upload(self.test_file)

        # Remove local file to simulate a sparse checkout
        os.remove(self.test_file)
        self.assertFalse(os.path.exists(self.test_file))

        # Use checkout with the directory prefix, not the file hash
        self.versioner.checkout(test_directory)

        # Ensure the file has been restored
        self.assertTrue(os.path.exists(self.test_file))

        # Verify file content
        with open(self.test_file, "r") as f:
            content = f.read()
        self.assertEqual(content, "This is a test file.")

    # -------------------------------------------------
    # 4. Encryption (AES256)
    # -------------------------------------------------
    def test_server_side_encryption(self):
        """
        Confirms the object is uploaded with AES256 SSE by checking object metadata.
        (moto does support SSE but occasionally may not store all fields.)
        """
        self.versioner.upload(self.test_file)
        file_hash = self.versioner.hash_file(self.test_file)
        s3_key = f"s3lfs/assets/{file_hash}/{self.test_file}.gz"

        # Retrieve the object's metadata
        head_resp = self.s3.head_object(Bucket=self.bucket_name, Key=s3_key)
        # Check for SSE header
        self.assertEqual(head_resp.get("ServerSideEncryption"), "AES256")

    # -------------------------------------------------
    # 5. Cleanup Unreferenced Files
    # -------------------------------------------------
    def test_cleanup_s3(self):
        """Test if cleanup removes files from S3 that are no longer in the manifest."""
        # Upload the file first
        self.versioner.upload(self.test_file)
        file_hash = self.versioner.hash_file(self.test_file)

        # Remove file entry from manifest to simulate a stale object
        del self.versioner.manifest["files"][self.test_file]
        self.versioner.save_manifest()

        # Cleanup should remove it from S3
        self.versioner.cleanup_s3(force=True)

        s3_key = f"s3lfs/assets/{file_hash}/{self.test_file}.gz"
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=s3_key)

        # Ensure object was deleted (no contents in the response)
        self.assertFalse(
            "Contents" in response or len(response.get("Contents", [])) > 0
        )

    def test_cleanup_chunked_s3(self):
        """Test if cleanup removes files from S3 that are no longer in the manifest."""
        chunk_size = self.versioner.chunk_size
        self.versioner.chunk_size = 4
        try:
            # Upload the file first
            self.versioner.upload(self.test_file)
            file_hash = self.versioner.hash_file(self.test_file)

            # Remove file entry from manifest to simulate a stale object
            del self.versioner.manifest["files"][self.test_file]
            self.versioner.save_manifest()

            # Cleanup should remove it from S3
            self.versioner.cleanup_s3(force=True)

            s3_key = f"s3lfs/assets/{file_hash}/{self.test_file}.gz"
            response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=s3_key)

            # Ensure object was deleted (no contents in the response)
            self.assertFalse(
                "Contents" in response or len(response.get("Contents", [])) > 0
            )
        finally:
            # Reset chunk size to default
            self.versioner.chunk_size = chunk_size

    # -------------------------------------------------
    # 6. Parallel Upload/Download
    # -------------------------------------------------
    def test_parallel_upload(self):
        files = [self.test_file, self.another_test_file]
        self.versioner.parallel_upload(files)

        for file in files:
            file_hash = self.versioner.hash_file(file)
            s3_key = f"s3lfs/assets/{file_hash}/{file}.gz"
            response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=s3_key)
            self.assertTrue("Contents" in response and len(response["Contents"]) == 1)

    def test_parallel_download_all(self):
        # Upload two files
        self.versioner.upload(self.test_file)
        self.versioner.upload(self.another_test_file)

        # Remove local files
        os.remove(self.test_file)
        os.remove(self.another_test_file)
        self.assertFalse(os.path.exists(self.test_file))
        self.assertFalse(os.path.exists(self.another_test_file))

        self.versioner.parallel_download_all()

        # Verify both are restored
        self.assertTrue(os.path.exists(self.test_file))
        self.assertTrue(os.path.exists(self.another_test_file))

    def test_caching(self):
        """Test if redundant downloads are avoided when the file already exists."""
        self.versioner.upload(self.test_file)

        # 1st download
        self.versioner.download(self.test_file)

        # Ensure file exists
        self.assertTrue(os.path.exists(self.test_file))

        # Modify the file to simulate a new version (should trigger re-download)
        with open(self.test_file, "w") as f:
            f.write("Modified content")

        # 2nd download (should fetch from S3 because the file is modified)
        self.versioner.download(self.test_file)

        # Ensure file was updated back to original
        with open(self.test_file, "r") as f:
            content = f.read()
        self.assertEqual(content, "This is a test file.")

        # 3rd download (should NOT fetch from S3 since the file is unchanged)
        with patch.object(self.versioner.thread_local, "s3") as mock_s3:
            self.versioner.download(self.test_file)
            mock_s3.download_file.assert_not_called()  # Ensure no new S3 download happened

    # -------------------------------------------------
    # 9. Compression Before Upload
    # -------------------------------------------------
    def test_compression(self):
        """
        The best we can do in a unit test is:
        - Upload file
        - Confirm it ends up as .gz in the S3 object key
        - Re-download and ensure the content is identical
        """
        self.versioner.upload(self.test_file)
        file_hash = self.versioner.hash_file(self.test_file)

        s3_key = f"s3lfs/assets/{file_hash}/{self.test_file}.gz"
        # Confirm object is .gz by key
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=s3_key)
        self.assertTrue("Contents" in response and len(response["Contents"]) == 1)

        # Confirm re-downloaded file matches original
        self.versioner.download(self.test_file)
        with open(self.test_file, "r") as f:
            content = f.read()
        self.assertEqual(content, "This is a test file.")

    # -------------------------------------------------
    # 10. File Locking / Conflict Resolution
    # -------------------------------------------------
    def test_file_locking(self):
        """
        Upload the same file twice. The second upload should detect
        it already exists in S3 and skip overwriting.
        """
        self.versioner.upload(self.test_file)
        file_hash = self.versioner.hash_file(self.test_file)
        s3_key = f"s3lfs/assets/{file_hash}/{self.test_file}.gz"

        # Re-upload
        self.versioner.upload(self.test_file)
        # There's only one object with that key in S3
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=s3_key)
        self.assertEqual(len(response["Contents"]), 1)

    # -------------------------------------------------
    # 11. Automatic Tracking of Modified Files
    # -------------------------------------------------
    def test_track_modified_files(self):
        third_file = "third_file.txt"
        with open(third_file, "w") as f:
            f.write("Third file content")

        fourth_file = "fourth_file.txt"
        with open(fourth_file, "w") as f:
            f.write("Fourth file content")

        self.versioner.upload(third_file)
        self.versioner.upload(fourth_file)

        # Write two new files and pretend they're both modified
        with open(third_file, "w") as f:
            f.write("Third file content new")
        fourth_file = "fourth_file.txt"
        with open(fourth_file, "w") as f:
            f.write("Fourth file content new")

        self.versioner.track_modified_files()

        # Both should now be in S3
        file_hash_3 = self.versioner.hash_file(third_file)
        file_hash_4 = self.versioner.hash_file(fourth_file)

        s3_key_3 = f"s3lfs/assets/{file_hash_3}/{third_file}.gz"
        s3_key_4 = f"s3lfs/assets/{file_hash_4}/{fourth_file}.gz"

        resp3 = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=s3_key_3)
        resp4 = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=s3_key_4)

        self.assertTrue("Contents" in resp3 and len(resp3["Contents"]) == 1)
        self.assertTrue("Contents" in resp4 and len(resp4["Contents"]) == 1)

        # Clean up the extra test files
        if os.path.exists(third_file):
            os.remove(third_file)
        if os.path.exists(fourth_file):
            os.remove(fourth_file)

    def test_remove_file_updates_manifest(self):
        self.versioner.remove_file(self.test_file, keep_in_s3=True)
        self.assertNotIn(self.test_file, self.versioner.manifest["files"])

    def test_remove_file_deletes_from_s3(self):
        file_hash = self.versioner.hash_file(self.test_file)
        s3_key = f"s3lfs/assets/{file_hash}/{self.test_file}.gz"
        self.versioner.remove_file(self.test_file, keep_in_s3=False)
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=s3_key)
        self.assertFalse("Contents" in response)

    def test_remove_subtree_updates_manifest(self):
        os.makedirs("test_dir", exist_ok=True)
        file_path = "test_dir/nested_file.txt"
        with open(file_path, "w") as f:
            f.write("Nested content")
        self.versioner.upload(file_path)
        self.versioner.remove_subtree("test_dir", keep_in_s3=True)
        self.assertNotIn(file_path, self.versioner.manifest["files"])
        os.remove(file_path)
        shutil.rmtree("test_dir")

    def test_remove_subtree_deletes_from_s3(self):
        file_path = "test_dir/nested_file.txt"
        os.makedirs("test_dir", exist_ok=True)
        with open(file_path, "w") as f:
            f.write("Nested content")
        self.versioner.upload(file_path)
        file_hash = self.versioner.hash_file(file_path)
        s3_key = f"s3lfs/assets/{file_hash}/{file_path}.gz"
        self.versioner.remove_subtree("test_dir", keep_in_s3=False)
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=s3_key)
        self.assertFalse("Contents" in response)
        os.remove(file_path)
        shutil.rmtree("test_dir")

    def test_no_sign_request_upload(self):
        """Test uploading a file with no-sign-request enabled."""
        self.versioner.upload(self.test_file)
        manifest = self.versioner.manifest
        file_hash = self.versioner.hash_file(self.test_file)
        s3_key = f"s3lfs/assets/{file_hash}/{self.test_file}.gz"

        # Check that the manifest correctly tracks the file path
        self.assertIn(self.test_file, manifest["files"])
        self.assertEqual(manifest["files"][self.test_file], file_hash)

        # Check that the file was uploaded to S3
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=s3_key)
        self.assertTrue("Contents" in response and len(response["Contents"]) == 1)

    @mock_s3
    def test_incorrect_credentials(self):
        """Test behavior when incorrect credentials are provided."""
        # Mock the upload_file method to raise a ClientError
        with patch("boto3.client") as mock_boto_client:
            mock_s3_client = MagicMock()
            mock_s3_client.upload_fileobj.side_effect = ClientError(
                error_response={
                    "Error": {
                        "Code": "InvalidAccessKeyId",
                        "Message": "The AWS Access Key Id you provided does not exist in our records.",
                    }
                },
                operation_name="UploadFile",
            )
            mock_boto_client.return_value = mock_s3_client

            # Create an S3LFS instance with the mocked client
            versioner = S3LFS(
                bucket_name=self.bucket_name, s3_factory=lambda _: mock_s3_client
            )

            # Attempt to upload a file
            with self.assertRaises(ClientError) as context:
                versioner.upload(self.test_file)

            # Verify the error is related to authentication
            self.assertIn("InvalidAccessKeyId", str(context.exception))

    @mock_s3
    def test_incorrect_credentials_parallel(self):
        """Test behavior when incorrect credentials are provided."""
        # Mock the upload_file method to raise a ClientError
        with patch("boto3.client") as mock_boto_client:
            mock_s3_client = MagicMock()
            mock_s3_client.upload_fileobj.side_effect = ClientError(
                error_response={
                    "Error": {
                        "Code": "InvalidAccessKeyId",
                        "Message": "The AWS Access Key Id you provided does not exist in our records.",
                    }
                },
                operation_name="UploadFile",
            )
            mock_boto_client.return_value = mock_s3_client

            # Create an S3LFS instance with the mocked client
            versioner = S3LFS(
                bucket_name=self.bucket_name, s3_factory=lambda _: mock_s3_client
            )

            # Attempt to upload a file
            with self.assertRaises(ClientError) as context:
                versioner.track(self.test_file)

            # Verify the error is related to authentication
            self.assertIn("InvalidAccessKeyId", str(context.exception))

    # -------------------------------------------------
    # 13. Globbing Functionality Tests
    # -------------------------------------------------
    def test_track_filesystem_globbing(self):
        """Test that track() uses filesystem-based globbing patterns correctly."""
        # Create a complex directory structure for testing
        os.makedirs("data/subdir", exist_ok=True)
        os.makedirs("logs", exist_ok=True)

        # Create various test files
        files_created = []

        # Root level files
        for fname in ["file1.txt", "file2.txt", "config.json", "test_readme.md"]:
            with open(fname, "w") as f:
                f.write(f"Content of {fname}")
            files_created.append(fname)

        # Data directory files
        for fname in [
            "data/dataset1.txt",
            "data/dataset2.csv",
            "data/subdir/nested.txt",
        ]:
            with open(fname, "w") as f:
                f.write(f"Content of {fname}")
            files_created.append(fname)

        # Logs directory files
        for fname in ["logs/app.log", "logs/error.log"]:
            with open(fname, "w") as f:
                f.write(f"Content of {fname}")
            files_created.append(fname)

        try:
            # Test 1: Simple glob pattern - only root level .txt files
            self.versioner.track("*.txt")
            tracked_files = list(self.versioner.manifest["files"].keys())
            expected_root_txt = ["file1.txt", "file2.txt"]
            for expected in expected_root_txt:
                self.assertIn(expected, tracked_files)
            # Should NOT include nested txt files
            self.assertNotIn("data/dataset1.txt", tracked_files)
            self.assertNotIn("data/subdir/nested.txt", tracked_files)

            # Clear manifest for next test
            self.versioner.manifest["files"] = {}
            self.versioner.save_manifest()

            # Test 2: Directory tracking
            self.versioner.track("data")
            tracked_files = list(self.versioner.manifest["files"].keys())
            expected_data_files = [
                "data/dataset1.txt",
                "data/dataset2.csv",
                "data/subdir/nested.txt",
            ]
            for expected in expected_data_files:
                self.assertIn(expected, tracked_files)
            # Should NOT include root level files
            self.assertNotIn("file1.txt", tracked_files)

            # Clear manifest for next test
            self.versioner.manifest["files"] = {}
            self.versioner.save_manifest()

            # Test 3: Recursive glob pattern
            self.versioner.track("**/*.txt")
            tracked_files = list(self.versioner.manifest["files"].keys())
            expected_all_txt = [
                "file1.txt",
                "file2.txt",
                "data/dataset1.txt",
                "data/subdir/nested.txt",
            ]
            for expected in expected_all_txt:
                self.assertIn(expected, tracked_files)
            # Should NOT include non-txt files
            self.assertNotIn("config.json", tracked_files)
            self.assertNotIn("data/dataset2.csv", tracked_files)

            # Clear manifest for next test
            self.versioner.manifest["files"] = {}
            self.versioner.save_manifest()

            # Test 4: Directory-specific glob
            self.versioner.track("data/*.txt")
            tracked_files = list(self.versioner.manifest["files"].keys())
            self.assertIn("data/dataset1.txt", tracked_files)
            # Should NOT include files in subdirectories of data/
            self.assertNotIn("data/subdir/nested.txt", tracked_files)
            # Should NOT include root level files
            self.assertNotIn("file1.txt", tracked_files)

        finally:
            # Clean up all created files
            for fname in files_created:
                if os.path.exists(fname):
                    os.remove(fname)
            # Clean up directories
            if os.path.exists("data/subdir"):
                os.rmdir("data/subdir")
            if os.path.exists("data"):
                os.rmdir("data")
            if os.path.exists("logs"):
                shutil.rmtree("logs")

    def test_checkout_manifest_globbing(self):
        """Test that checkout() uses manifest-based globbing patterns correctly."""
        # Create test files and upload them
        os.makedirs("data/subdir", exist_ok=True)
        os.makedirs("logs", exist_ok=True)

        files_created = []

        # Create and upload various test files
        test_files = [
            "file1.txt",
            "file2.txt",
            "config.json",
            "data/dataset1.txt",
            "data/dataset2.csv",
            "data/subdir/nested.txt",
            "logs/app.log",
            "logs/error.log",
        ]

        for fname in test_files:
            with open(fname, "w") as f:
                f.write(f"Content of {fname}")
            self.versioner.upload(fname)
            files_created.append(fname)

        try:
            # Remove all local files to test checkout
            for fname in files_created:
                if os.path.exists(fname):
                    os.remove(fname)

            # Test 1: Simple glob pattern - only root level .txt files
            self.versioner.checkout("*.txt")

            # Check which files were downloaded
            expected_root_txt = ["file1.txt", "file2.txt"]
            for expected in expected_root_txt:
                self.assertTrue(
                    os.path.exists(expected), f"{expected} should have been downloaded"
                )

            # Should NOT have downloaded nested txt files with simple glob
            self.assertFalse(
                os.path.exists("data/dataset1.txt"),
                "data/dataset1.txt should NOT have been downloaded",
            )
            self.assertFalse(
                os.path.exists("data/subdir/nested.txt"),
                "data/subdir/nested.txt should NOT have been downloaded",
            )

            # Clean up for next test
            for fname in expected_root_txt:
                if os.path.exists(fname):
                    os.remove(fname)

            # Test 2: Directory checkout
            self.versioner.checkout("data")

            # Check that all files in data/ were downloaded
            expected_data_files = [
                "data/dataset1.txt",
                "data/dataset2.csv",
                "data/subdir/nested.txt",
            ]
            for expected in expected_data_files:
                self.assertTrue(
                    os.path.exists(expected), f"{expected} should have been downloaded"
                )

            # Should NOT have downloaded root level files
            self.assertFalse(
                os.path.exists("file1.txt"), "file1.txt should NOT have been downloaded"
            )

            # Clean up for next test
            for fname in expected_data_files:
                if os.path.exists(fname):
                    os.remove(fname)

            # Test 3: Recursive glob pattern
            self.versioner.checkout("**/*.txt")

            # Check that all .txt files were downloaded
            expected_all_txt = [
                "file1.txt",
                "file2.txt",
                "data/dataset1.txt",
                "data/subdir/nested.txt",
            ]
            for expected in expected_all_txt:
                self.assertTrue(
                    os.path.exists(expected), f"{expected} should have been downloaded"
                )

            # Should NOT have downloaded non-txt files
            self.assertFalse(
                os.path.exists("config.json"),
                "config.json should NOT have been downloaded",
            )
            self.assertFalse(
                os.path.exists("data/dataset2.csv"),
                "data/dataset2.csv should NOT have been downloaded",
            )

            # Clean up for next test
            for fname in expected_all_txt:
                if os.path.exists(fname):
                    os.remove(fname)

            # Test 4: Directory-specific glob
            self.versioner.checkout("data/*.txt")

            # Should download only .txt files directly in data/
            self.assertTrue(
                os.path.exists("data/dataset1.txt"),
                "data/dataset1.txt should have been downloaded",
            )

            # Should NOT download files in subdirectories or other extensions
            self.assertFalse(
                os.path.exists("data/subdir/nested.txt"),
                "data/subdir/nested.txt should NOT have been downloaded",
            )
            self.assertFalse(
                os.path.exists("data/dataset2.csv"),
                "data/dataset2.csv should NOT have been downloaded",
            )
            self.assertFalse(
                os.path.exists("file1.txt"), "file1.txt should NOT have been downloaded"
            )

            # Test 5: Specific file checkout
            if os.path.exists("data/dataset1.txt"):
                os.remove("data/dataset1.txt")

            self.versioner.checkout("data/dataset1.txt")
            self.assertTrue(
                os.path.exists("data/dataset1.txt"),
                "data/dataset1.txt should have been downloaded",
            )

        finally:
            # Clean up all created files
            for fname in files_created:
                if os.path.exists(fname):
                    os.remove(fname)
            # Clean up directories
            if os.path.exists("data/subdir"):
                os.rmdir("data/subdir")
            if os.path.exists("data"):
                os.rmdir("data")
            if os.path.exists("logs"):
                shutil.rmtree("logs")

    def test_glob_match_helper_function(self):
        """Test the internal _glob_match helper function directly."""
        # Test non-recursive patterns
        self.assertTrue(self.versioner._glob_match("file.txt", "*.txt"))
        self.assertFalse(self.versioner._glob_match("dir/file.txt", "*.txt"))
        self.assertTrue(self.versioner._glob_match("dir/file.txt", "dir/*.txt"))
        self.assertFalse(self.versioner._glob_match("dir/subdir/file.txt", "dir/*.txt"))

        # Test recursive patterns
        self.assertTrue(self.versioner._glob_match("file.txt", "**/*.txt"))
        self.assertTrue(self.versioner._glob_match("dir/file.txt", "**/*.txt"))
        self.assertTrue(self.versioner._glob_match("dir/subdir/file.txt", "**/*.txt"))

        # Test prefix recursive patterns
        self.assertTrue(self.versioner._glob_match("data/file.txt", "data/**/*.txt"))
        self.assertTrue(
            self.versioner._glob_match("data/subdir/file.txt", "data/**/*.txt")
        )
        self.assertFalse(self.versioner._glob_match("logs/file.txt", "data/**/*.txt"))

        # Test complex patterns
        self.assertTrue(self.versioner._glob_match("data/test.log", "data/*.log"))
        self.assertFalse(
            self.versioner._glob_match("data/subdir/test.log", "data/*.log")
        )

    def test_resolve_filesystem_paths_helper(self):
        """Test the _resolve_filesystem_paths helper function."""
        # Create test files
        os.makedirs("test_glob/subdir", exist_ok=True)

        test_files = [
            "test_glob/file1.txt",
            "test_glob/file2.txt",
            "test_glob/data.csv",
            "test_glob/subdir/nested.txt",
        ]

        for fname in test_files:
            with open(fname, "w") as f:
                f.write(f"Content of {fname}")

        try:
            # Test single file
            result = self.versioner._resolve_filesystem_paths("test_glob/file1.txt")
            self.assertEqual(len(result), 1)
            self.assertEqual(str(result[0]), "test_glob/file1.txt")

            # Test directory
            result = self.versioner._resolve_filesystem_paths("test_glob")
            self.assertEqual(len(result), 4)
            result_strs = [str(p) for p in result]
            for expected in test_files:
                self.assertIn(expected, result_strs)

            # Test glob pattern
            result = self.versioner._resolve_filesystem_paths("test_glob/*.txt")
            self.assertEqual(len(result), 2)
            result_strs = [str(p) for p in result]
            self.assertIn("test_glob/file1.txt", result_strs)
            self.assertIn("test_glob/file2.txt", result_strs)
            self.assertNotIn(
                "test_glob/subdir/nested.txt", result_strs
            )  # Should not include subdirs

            # Test recursive glob
            result = self.versioner._resolve_filesystem_paths("test_glob/**/*.txt")
            self.assertEqual(len(result), 3)
            result_strs = [str(p) for p in result]
            self.assertIn("test_glob/file1.txt", result_strs)
            self.assertIn("test_glob/file2.txt", result_strs)
            self.assertIn("test_glob/subdir/nested.txt", result_strs)

        finally:
            # Clean up
            for fname in test_files:
                if os.path.exists(fname):
                    os.remove(fname)
            if os.path.exists("test_glob/subdir"):
                os.rmdir("test_glob/subdir")
            if os.path.exists("test_glob"):
                os.rmdir("test_glob")

    def test_resolve_manifest_paths_helper(self):
        """Test the _resolve_manifest_paths helper function."""
        # Setup manifest with test data
        original_manifest = self.versioner.manifest["files"].copy()

        self.versioner.manifest["files"] = {
            "file1.txt": "hash1",
            "file2.txt": "hash2",
            "data/dataset1.txt": "hash3",
            "data/dataset2.csv": "hash4",
            "data/subdir/nested.txt": "hash5",
            "logs/app.log": "hash6",
            "config.json": "hash7",
        }

        try:
            # Test exact file match
            result = self.versioner._resolve_manifest_paths("file1.txt")
            self.assertEqual(result, {"file1.txt": "hash1"})

            # Test directory prefix
            result = self.versioner._resolve_manifest_paths("data")
            expected = {
                "data/dataset1.txt": "hash3",
                "data/dataset2.csv": "hash4",
                "data/subdir/nested.txt": "hash5",
            }
            self.assertEqual(result, expected)

            # Test simple glob pattern
            result = self.versioner._resolve_manifest_paths("*.txt")
            expected = {"file1.txt": "hash1", "file2.txt": "hash2"}
            self.assertEqual(result, expected)

            # Test directory-specific glob
            result = self.versioner._resolve_manifest_paths("data/*.txt")
            expected = {"data/dataset1.txt": "hash3"}
            self.assertEqual(result, expected)

            # Test recursive glob
            result = self.versioner._resolve_manifest_paths("**/*.txt")
            expected = {
                "file1.txt": "hash1",
                "file2.txt": "hash2",
                "data/dataset1.txt": "hash3",
                "data/subdir/nested.txt": "hash5",
            }
            self.assertEqual(result, expected)

            # Test prefix recursive glob
            result = self.versioner._resolve_manifest_paths("data/**/*.txt")
            expected = {"data/dataset1.txt": "hash3", "data/subdir/nested.txt": "hash5"}
            self.assertEqual(result, expected)

            # Test no matches
            result = self.versioner._resolve_manifest_paths("nonexistent/*.txt")
            self.assertEqual(result, {})

        finally:
            # Restore original manifest
            self.versioner.manifest["files"] = original_manifest

    def test_track_checkout_consistency(self):
        """Test that track and checkout work consistently with the same patterns."""
        # Create test files
        os.makedirs("consistency_test/subdir", exist_ok=True)

        test_files = [
            "consistency_test/file1.txt",
            "consistency_test/file2.log",
            "consistency_test/subdir/nested.txt",
        ]

        for fname in test_files:
            with open(fname, "w") as f:
                f.write(f"Content of {fname}")

        try:
            # Track files using glob pattern
            self.versioner.track("consistency_test/*.txt")

            # Verify only the .txt file in the directory was tracked (not subdirs)
            tracked_files = list(self.versioner.manifest["files"].keys())
            self.assertIn("consistency_test/file1.txt", tracked_files)
            self.assertNotIn("consistency_test/file2.log", tracked_files)
            self.assertNotIn("consistency_test/subdir/nested.txt", tracked_files)

            # Remove the tracked file
            os.remove("consistency_test/file1.txt")
            self.assertFalse(os.path.exists("consistency_test/file1.txt"))

            # Checkout using the same pattern
            self.versioner.checkout("consistency_test/*.txt")

            # Verify the file was restored
            self.assertTrue(os.path.exists("consistency_test/file1.txt"))

            # Verify content is correct
            with open("consistency_test/file1.txt", "r") as f:
                content = f.read()
            self.assertEqual(content, "Content of consistency_test/file1.txt")

        finally:
            # Clean up
            for fname in test_files:
                if os.path.exists(fname):
                    os.remove(fname)
            if os.path.exists("consistency_test/subdir"):
                os.rmdir("consistency_test/subdir")
            if os.path.exists("consistency_test"):
                os.rmdir("consistency_test")

    # -------------------------------------------------
    # 15. Interleaved Processing Tests
    # -------------------------------------------------
    def test_track_interleaved(self):
        """Test that interleaved track works correctly and performs better than two-stage."""
        # Create test files
        os.makedirs("data", exist_ok=True)
        files_created = []

        for i in range(3):
            fname = f"test_file_{i}.txt"
            with open(fname, "w") as f:
                f.write(f"Content of file {i}")
            files_created.append(fname)

        try:
            # Test interleaved tracking
            self.versioner.track_interleaved("*.txt")

            # Verify all files are tracked
            for fname in files_created:
                self.assertIn(fname, self.versioner.manifest["files"])

            # Verify files exist in S3
            for fname in files_created:
                file_hash = self.versioner.hash_file(fname)
                s3_key = f"s3lfs/assets/{file_hash}/{fname}.gz"
                response = self.s3.list_objects_v2(
                    Bucket=self.bucket_name, Prefix=s3_key
                )
                self.assertTrue(
                    "Contents" in response and len(response["Contents"]) == 1
                )

        finally:
            # Cleanup
            for fname in files_created:
                try:
                    os.remove(fname)
                except OSError:
                    pass

    def test_checkout_interleaved(self):
        """Test that interleaved checkout works correctly."""
        # First upload some files
        os.makedirs("data", exist_ok=True)
        files_created = []

        for i in range(3):
            fname = f"checkout_test_{i}.txt"
            with open(fname, "w") as f:
                f.write(f"Content for checkout test {i}")
            files_created.append(fname)

        try:
            # Track the files first
            self.versioner.track_interleaved("checkout_test_*.txt")

            # Remove the files locally
            for fname in files_created:
                os.remove(fname)
                self.assertFalse(Path(fname).exists())

            # Test interleaved checkout
            self.versioner.checkout_interleaved("checkout_test_*.txt")

            # Verify all files are restored
            for fname in files_created:
                self.assertTrue(Path(fname).exists())
                with open(fname, "r") as f:
                    content = f.read()
                    self.assertIn("Content for checkout test", content)

        finally:
            # Cleanup
            for fname in files_created:
                try:
                    os.remove(fname)
                except OSError:
                    pass

    def test_interleaved_vs_two_stage_compatibility(self):
        """Test that interleaved and two-stage methods produce the same results."""
        # Create test files
        files_created = []

        for i in range(2):
            fname = f"compat_test_{i}.txt"
            with open(fname, "w") as f:
                f.write(f"Compatibility test content {i}")
            files_created.append(fname)

        try:
            # Track with two-stage method
            self.versioner.track("compat_test_0.txt", interleaved=False)

            # Track with interleaved method
            self.versioner.track("compat_test_1.txt", interleaved=True)

            # Both should be in manifest
            for fname in files_created:
                self.assertIn(fname, self.versioner.manifest["files"])

            # Remove files locally
            for fname in files_created:
                os.remove(fname)

            # Checkout with two-stage method
            self.versioner.checkout("compat_test_0.txt", interleaved=False)

            # Checkout with interleaved method
            self.versioner.checkout("compat_test_1.txt", interleaved=True)

            # Both files should be restored correctly
            for fname in files_created:
                self.assertTrue(Path(fname).exists())
                with open(fname, "r") as f:
                    content = f.read()
                    self.assertIn("Compatibility test content", content)

        finally:
            # Cleanup
            for fname in files_created:
                try:
                    os.remove(fname)
                except OSError:
                    pass

    # -------------------------------------------------
    # 16. Coverage Tests for Edge Cases and Error Conditions
    # -------------------------------------------------
    def test_hash_and_upload_worker_no_upload_needed(self):
        """Test _hash_and_upload_worker when no upload is needed (file already up-to-date)."""
        # Upload file first
        self.versioner.upload(self.test_file)

        # Test the worker function directly - it should return False for uploaded since file is up-to-date
        result = self.versioner._hash_and_upload_worker(self.test_file, silence=True)
        file_path, file_hash, uploaded, bytes_transferred = result

        self.assertEqual(file_path, self.test_file)
        self.assertIsNotNone(file_hash)
        self.assertFalse(uploaded)  # Should be False since no upload was needed
        self.assertEqual(
            bytes_transferred, 0
        )  # No bytes transferred since no upload needed

    def test_hash_and_download_worker_no_download_needed(self):
        """Test _hash_and_download_worker when no download is needed (file already exists and correct)."""
        # Upload file first
        self.versioner.upload(self.test_file)
        expected_hash = self.versioner.hash_file(self.test_file)

        # Test the worker function directly - it should return False for downloaded since file exists and is correct
        result = self.versioner._hash_and_download_worker(
            (self.test_file, expected_hash), silence=True
        )
        file_path, downloaded, bytes_transferred = result

        self.assertEqual(file_path, self.test_file)
        self.assertFalse(downloaded)  # Should be False since no download was needed
        self.assertEqual(
            bytes_transferred, 0
        )  # No bytes transferred since no download needed

    def test_hash_and_upload_worker_error_handling(self):
        """Test _hash_and_upload_worker error handling."""
        # Create a file that will cause an error (non-existent file)
        non_existent_file = "non_existent_file.txt"

        with self.assertRaises(FileNotFoundError):
            self.versioner._hash_and_upload_worker(non_existent_file, silence=True)

    def test_hash_and_download_worker_error_handling(self):
        """Test _hash_and_download_worker error handling."""
        # Create a file and upload it to have it in manifest
        test_file = "error_test_file.txt"
        with open(test_file, "w") as f:
            f.write("test content")

        try:
            self.versioner.upload(test_file)

            # Remove the file locally
            os.remove(test_file)

            # Mock the download method to raise an exception
            with patch.object(
                self.versioner, "download", side_effect=RuntimeError("Download error")
            ):
                with patch("builtins.print") as mock_print:
                    with self.assertRaises(RuntimeError):
                        expected_hash = self.versioner.manifest["files"][test_file]
                        self.versioner._hash_and_download_worker(
                            (test_file, expected_hash), silence=True
                        )

                    # Should print error message
                    calls = [str(call) for call in mock_print.call_args_list]
                    error_calls = [call for call in calls if "Error processing" in call]
                    self.assertTrue(
                        len(error_calls) > 0, "Error message should be printed"
                    )

        finally:
            # Cleanup
            try:
                if os.path.exists(test_file):
                    os.remove(test_file)
            except OSError:
                pass

    def test_track_interleaved_no_files_found(self):
        """Test track_interleaved when no files match the pattern."""
        # Use a pattern that won't match any files
        with patch("builtins.print") as mock_print:
            self.versioner.track_interleaved("*.nonexistent")

            # Should print warning message
            mock_print.assert_any_call(
                "⚠️ No files found to track for '*.nonexistent'."
            )

    def test_checkout_interleaved_no_files_found(self):
        """Test checkout_interleaved when no files match the pattern in manifest."""
        # Use a pattern that won't match any files in manifest
        with patch("builtins.print") as mock_print:
            self.versioner.checkout_interleaved("*.nonexistent")

            # Should print warning message
            mock_print.assert_any_call(
                "⚠️ No files found in the manifest for '*.nonexistent'."
            )

    def test_track_interleaved_with_shutdown_signal(self):
        """Test track_interleaved behavior when shutdown is requested."""
        # Create test files
        files_created = []
        for i in range(3):
            fname = f"shutdown_test_{i}.txt"
            with open(fname, "w") as f:
                f.write(f"Content {i}")
            files_created.append(fname)

        try:
            # Mock the shutdown flag to be True during processing
            original_shutdown = self.versioner._shutdown_requested

            def mock_worker(file_path, silence, progress_callback=None):
                # Set shutdown flag during first call
                self.versioner._shutdown_requested = True
                return self.versioner._hash_and_upload_worker(
                    file_path, silence, progress_callback
                )

            with patch.object(
                self.versioner, "_hash_and_upload_worker", side_effect=mock_worker
            ):
                with patch("builtins.print") as mock_print:
                    self.versioner.track_interleaved("shutdown_test_*.txt")

                    # Should print shutdown message
                    mock_print.assert_any_call(
                        "⚠️ Shutdown requested. Cancelling remaining operations..."
                    )

            # Restore original shutdown state
            self.versioner._shutdown_requested = original_shutdown

        finally:
            # Cleanup
            for fname in files_created:
                try:
                    os.remove(fname)
                except OSError:
                    pass

    def test_checkout_interleaved_with_shutdown_signal(self):
        """Test checkout_interleaved behavior when shutdown is requested."""
        # First upload some files
        files_created = []
        for i in range(3):
            fname = f"shutdown_checkout_test_{i}.txt"
            with open(fname, "w") as f:
                f.write(f"Content {i}")
            files_created.append(fname)
            self.versioner.upload(fname)

        try:
            # Remove files locally
            for fname in files_created:
                os.remove(fname)

            # Mock the shutdown flag to be True during processing
            original_shutdown = self.versioner._shutdown_requested

            def mock_worker(file_info, silence, progress_callback=None):
                # Set shutdown flag during first call
                self.versioner._shutdown_requested = True
                return self.versioner._hash_and_download_worker(
                    file_info, silence, progress_callback
                )

            with patch.object(
                self.versioner, "_hash_and_download_worker", side_effect=mock_worker
            ):
                with patch("builtins.print") as mock_print:
                    self.versioner.checkout_interleaved("shutdown_checkout_test_*.txt")

                    # Should print shutdown message
                    mock_print.assert_any_call(
                        "⚠️ Shutdown requested. Cancelling remaining operations..."
                    )

            # Restore original shutdown state
            self.versioner._shutdown_requested = original_shutdown

        finally:
            # Cleanup
            for fname in files_created:
                try:
                    if os.path.exists(fname):
                        os.remove(fname)
                except OSError:
                    pass

    def test_track_interleaved_keyboard_interrupt(self):
        """Test track_interleaved behavior when KeyboardInterrupt occurs."""
        # Create test files
        files_created = []
        for i in range(2):
            fname = f"interrupt_test_{i}.txt"
            with open(fname, "w") as f:
                f.write(f"Content {i}")
            files_created.append(fname)

        try:
            # Mock ThreadPoolExecutor to raise KeyboardInterrupt
            with patch("s3lfs.core.ThreadPoolExecutor") as mock_executor:
                mock_executor.return_value.__enter__.return_value.submit.side_effect = (
                    KeyboardInterrupt()
                )

                with patch("builtins.print") as mock_print:
                    self.versioner.track_interleaved("interrupt_test_*.txt")

                    # Should print interrupt message
                    mock_print.assert_any_call("\n⚠️ Processing interrupted by user.")

        finally:
            # Cleanup
            for fname in files_created:
                try:
                    os.remove(fname)
                except OSError:
                    pass

    def test_checkout_interleaved_keyboard_interrupt(self):
        """Test checkout_interleaved behavior when KeyboardInterrupt occurs."""
        # First upload some files
        files_created = []
        for i in range(2):
            fname = f"interrupt_checkout_test_{i}.txt"
            with open(fname, "w") as f:
                f.write(f"Content {i}")
            files_created.append(fname)
            self.versioner.upload(fname)

        try:
            # Remove files locally
            for fname in files_created:
                os.remove(fname)

            # Mock ThreadPoolExecutor to raise KeyboardInterrupt
            with patch("s3lfs.core.ThreadPoolExecutor") as mock_executor:
                mock_executor.return_value.__enter__.return_value.submit.side_effect = (
                    KeyboardInterrupt()
                )

                with patch("builtins.print") as mock_print:
                    self.versioner.checkout_interleaved("interrupt_checkout_test_*.txt")

                    # Should print interrupt message
                    mock_print.assert_any_call("\n⚠️ Processing interrupted by user.")

        finally:
            # Cleanup
            for fname in files_created:
                try:
                    if os.path.exists(fname):
                        os.remove(fname)
                except OSError:
                    pass

    def test_track_interleaved_processing_error(self):
        """Test track_interleaved behavior when processing error occurs."""
        # Create test files
        files_created = []
        for i in range(2):
            fname = f"error_test_{i}.txt"
            with open(fname, "w") as f:
                f.write(f"Content {i}")
            files_created.append(fname)

        try:
            # Mock worker to raise an exception
            def mock_worker(file_path, silence, progress_callback=None):
                raise RuntimeError(f"Processing error for {file_path}")

            with patch.object(
                self.versioner, "_hash_and_upload_worker", side_effect=mock_worker
            ):
                with patch("builtins.print") as mock_print:
                    with self.assertRaises(RuntimeError):
                        self.versioner.track_interleaved("error_test_*.txt")

                    # Should print error message - check that at least one error call was made
                    calls = [str(call) for call in mock_print.call_args_list]
                    error_calls = [
                        call
                        for call in calls
                        if "An error occurred during processing:" in call
                    ]
                    self.assertTrue(
                        len(error_calls) > 0,
                        "Error message should be printed during processing",
                    )

        finally:
            # Cleanup
            for fname in files_created:
                try:
                    os.remove(fname)
                except OSError:
                    pass

    def test_checkout_interleaved_processing_error(self):
        """Test checkout_interleaved behavior when processing error occurs."""
        # First upload some files
        files_created = []
        for i in range(2):
            fname = f"error_checkout_test_{i}.txt"
            with open(fname, "w") as f:
                f.write(f"Content {i}")
            files_created.append(fname)
            self.versioner.upload(fname)

        try:
            # Remove files locally
            for fname in files_created:
                os.remove(fname)

            # Mock worker to raise an exception
            def mock_worker(file_info, silence, progress_callback=None):
                file_path, expected_hash = file_info
                raise RuntimeError(f"Processing error for {file_path}")

            with patch.object(
                self.versioner, "_hash_and_download_worker", side_effect=mock_worker
            ):
                with patch("builtins.print") as mock_print:
                    with self.assertRaises(RuntimeError):
                        self.versioner.checkout_interleaved("error_checkout_test_*.txt")

                    # Should print error message - check that at least one error call was made
                    calls = [str(call) for call in mock_print.call_args_list]
                    error_calls = [
                        call
                        for call in calls
                        if "An error occurred during processing:" in call
                    ]
                    self.assertTrue(
                        len(error_calls) > 0,
                        "Error message should be printed during processing",
                    )

        finally:
            # Cleanup
            for fname in files_created:
                try:
                    if os.path.exists(fname):
                        os.remove(fname)
                except OSError:
                    pass

    def test_worker_error_print_and_raise(self):
        """Test that worker functions print errors and re-raise them."""
        # Test _hash_and_upload_worker error handling
        non_existent_file = "definitely_does_not_exist.txt"

        with patch("builtins.print") as mock_print:
            with self.assertRaises(FileNotFoundError):
                self.versioner._hash_and_upload_worker(non_existent_file, silence=True)

            # Should print error message - check that at least one error call was made
            calls = [str(call) for call in mock_print.call_args_list]
            error_calls = [
                call
                for call in calls
                if "Error processing" in call and non_existent_file in call
            ]
            self.assertTrue(
                len(error_calls) > 0,
                f"Error message should be printed for {non_existent_file}",
            )

    def test_checkout_interleaved_finally_block(self):
        """Test that checkout_interleaved finally block executes and prints completion message."""
        # Upload a test file first
        self.versioner.upload(self.test_file)

        # Remove it locally
        os.remove(self.test_file)

        # Mock to cause an exception during processing but ensure finally block runs
        with patch.object(
            self.versioner,
            "_hash_and_download_worker",
            side_effect=RuntimeError("Test error"),
        ):
            with patch("builtins.print") as mock_print:
                with self.assertRaises(RuntimeError):
                    self.versioner.checkout_interleaved(self.test_file)

                # Should print completion message in finally block
                calls = [str(call) for call in mock_print.call_args_list]
                completion_calls = [
                    call for call in calls if "Successfully processed" in call
                ]
                self.assertTrue(
                    len(completion_calls) > 0,
                    "Finally block completion message should be printed",
                )

    # -------------------------------------------------
    # 17. MD5 Hashing Tests
    # -------------------------------------------------
    def test_md5_file_methods(self):
        """Test all MD5 hashing methods produce the same result."""
        # Test with the existing test file
        md5_auto = self.versioner.md5_file(self.test_file, method="auto")
        md5_mmap = self.versioner.md5_file(self.test_file, method="mmap")
        md5_iter = self.versioner.md5_file(self.test_file, method="iter")

        # All methods should produce the same hash
        self.assertEqual(md5_auto, md5_mmap)
        self.assertEqual(md5_auto, md5_iter)

        # MD5 should be 32 characters long
        self.assertEqual(len(md5_auto), 32)

        # Should be valid hex
        try:
            int(md5_auto, 16)
        except ValueError:
            self.fail("MD5 hash is not valid hexadecimal")

    def test_md5_cli_method(self):
        """Test MD5 CLI method if available."""
        try:
            md5_cli = self.versioner.md5_file(self.test_file, method="cli")
            md5_mmap = self.versioner.md5_file(self.test_file, method="mmap")

            # CLI and mmap should produce the same result
            self.assertEqual(md5_cli, md5_mmap)
        except RuntimeError as e:
            # CLI method not available on this system
            self.assertIn("No suitable MD5 CLI utility found", str(e))

    def test_md5_empty_file(self):
        """Test MD5 of empty file."""
        empty_file = "empty_test.txt"
        try:
            # Create empty file
            with open(empty_file, "w") as _:
                pass

            md5_empty = self.versioner.md5_file(empty_file, method="auto")

            # MD5 of empty file should be d41d8cd98f00b204e9800998ecf8427e
            expected_empty_md5 = "d41d8cd98f00b204e9800998ecf8427e"
            self.assertEqual(md5_empty, expected_empty_md5)

        finally:
            if os.path.exists(empty_file):
                os.remove(empty_file)

    def test_md5_vs_sha256(self):
        """Test that MD5 and SHA-256 produce different hashes for the same file."""
        md5_hash = self.versioner.md5_file(self.test_file)
        sha256_hash = self.versioner.hash_file(self.test_file)

        # Should be different algorithms producing different hashes
        self.assertNotEqual(md5_hash, sha256_hash)

        # Different lengths
        self.assertEqual(len(md5_hash), 32)  # MD5 is 128 bits = 32 hex chars
        self.assertEqual(len(sha256_hash), 64)  # SHA-256 is 256 bits = 64 hex chars

    def test_md5_nonexistent_file(self):
        """Test MD5 with non-existent file."""
        with self.assertRaises(FileNotFoundError):
            self.versioner.md5_file("nonexistent_file.txt")

    def test_md5_invalid_method(self):
        """Test MD5 with invalid method."""
        with self.assertRaises(ValueError) as context:
            self.versioner.md5_file(self.test_file, method="invalid")

        self.assertIn("Unsupported MD5 hashing method", str(context.exception))

    def test_md5_large_file_chunks(self):
        """Test MD5 with iterative method on a larger file to test chunking."""
        large_file = "large_test.txt"
        try:
            # Create a larger file (multiple chunks)
            with open(large_file, "w") as f:
                for i in range(1000):
                    f.write(f"Line {i}: This is some test content for MD5 hashing.\n")

            # Test iterative method with different chunk sizes
            md5_iter_default = self.versioner._md5_file_iter(large_file)
            md5_iter_small = self.versioner._md5_file_iter(large_file, chunk_size=64)
            md5_iter_large = self.versioner._md5_file_iter(large_file, chunk_size=8192)

            # All should produce the same result regardless of chunk size
            self.assertEqual(md5_iter_default, md5_iter_small)
            self.assertEqual(md5_iter_default, md5_iter_large)

            # Should match mmap method too
            md5_mmap = self.versioner._md5_file_mmap(large_file)
            self.assertEqual(md5_iter_default, md5_mmap)

        finally:
            if os.path.exists(large_file):
                os.remove(large_file)


if __name__ == "__main__":
    unittest.main()
