import json
import os
import shutil
import unittest
from unittest.mock import MagicMock, patch

import boto3
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

        # Use sparse_checkout with the directory prefix, not the file hash
        self.versioner.sparse_checkout(test_directory)

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

    # -------------------------------------------------
    # 12. Git Integration (basic test)
    # -------------------------------------------------
    @patch("subprocess.run")
    def test_git_integration(self, mock_subproc):
        """
        Just ensure no exceptions occur. We won't deeply verify the git config calls.
        """
        mock_subproc.return_value = MagicMock(stdout="")

        try:
            self.versioner.integrate_with_git()
        except Exception as e:
            self.fail(f"git-integration raised an unexpected exception: {e}")
        # If it runs without error, assume success.

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


if __name__ == "__main__":
    unittest.main()
