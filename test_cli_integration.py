import json
import os
import unittest
from pathlib import Path

import boto3
from click.testing import CliRunner
from moto import mock_s3

from s3lfs.cli import cli as s3lfs_main

TEST_BUCKET = "mock-bucket"


@mock_s3
class TestS3LFSCLIInProcess(unittest.TestCase):
    def setUp(self):
        # Create a mock S3 bucket
        self.s3 = boto3.client("s3", region_name="us-east-1")
        self.s3.create_bucket(Bucket=TEST_BUCKET)

        # Create a local file to upload
        self.test_file = "test_cli_file.txt"
        with open(self.test_file, "w") as f:
            f.write("Hello In-Process Test")

        # Remove any leftover manifest
        self.manifest_path = Path(".s3_manifest.json")
        if self.manifest_path.exists():
            self.manifest_path.unlink()

    def tearDown(self):
        # Cleanup local file
        if os.path.exists(self.test_file):
            os.remove(self.test_file)

        # Cleanup manifest
        if self.manifest_path.exists():
            self.manifest_path.unlink()

    def test_upload_in_process(self):
        """
        Test the 'upload' command by calling s3lfs_main()
        with patched sys.argv, all in-process (mock_s3).
        """
        test_args = [
            "upload",
            self.test_file,
            "--bucket",
            TEST_BUCKET,
        ]
        runner = CliRunner()
        result = runner.invoke(s3lfs_main, test_args)
        self.assertEqual(result.exit_code, 0, "Upload command failed")

        # Now check if the file is in S3
        resp = self.s3.list_objects_v2(Bucket=TEST_BUCKET, Prefix="s3lfs/assets/")
        self.assertIn("Contents", resp)
        self.assertEqual(len(resp["Contents"]), 1, "Expected 1 uploaded object.")

        # Check if manifest is created and contains the file path
        self.assertTrue(self.manifest_path.exists())
        manifest_data = json.loads(self.manifest_path.read_text())

        # Ensure the file path is a key in the manifest
        self.assertIn(
            self.test_file, manifest_data["files"], "File path should be in manifest"
        )

    def test_download_in_process(self):
        """
        Upload a file first, remove it locally, then download it via the CLI again.
        """
        runner = CliRunner()

        # 1. Upload the file via CLI
        result = runner.invoke(
            s3lfs_main,
            [
                "upload",
                self.test_file,
                "--bucket",
                TEST_BUCKET,
            ],
        )
        self.assertEqual(result.exit_code, 0, "Upload command failed")

        # 2. Read the manifest and get the file path
        manifest_data = json.loads(self.manifest_path.read_text())
        self.assertIn(
            self.test_file, manifest_data["files"], "File path should be in manifest"
        )

        # 3. Remove the local file to simulate needing a download
        os.remove(self.test_file)
        self.assertFalse(os.path.exists(self.test_file))

        # 4. Download via CLI (uses file path instead of hash)
        result = runner.invoke(
            s3lfs_main,
            [
                "download",
                self.test_file,  # Now we use the file path, not a hash
                "--bucket",
                TEST_BUCKET,
            ],
        )
        self.assertEqual(result.exit_code, 0, "Download command failed")

        # 5. Check if the file is correctly restored
        self.assertTrue(os.path.exists(self.test_file))

        with open(self.test_file, "r") as f:
            content = f.read()
        self.assertEqual(content, "Hello In-Process Test")

    def test_cleanup_in_process(self):
        """
        Upload a file, remove its entry from the manifest, then run 'cleanup'
        and ensure the object is removed from S3.
        """
        runner = CliRunner()

        # Upload file via CLI
        result = runner.invoke(
            s3lfs_main,
            [
                "upload",
                self.test_file,
                "--bucket",
                TEST_BUCKET,
            ],
        )
        self.assertEqual(result.exit_code, 0, "Upload command failed")

        # Remove the file path from the manifest (not the hash)
        manifest_data = json.loads(self.manifest_path.read_text())
        self.assertIn(
            self.test_file,
            manifest_data["files"],
            "File should be in manifest before cleanup",
        )

        del manifest_data["files"][self.test_file]  # Remove entry by file path
        self.manifest_path.write_text(json.dumps(manifest_data))

        # Run cleanup via CLI
        result = runner.invoke(
            s3lfs_main,
            [
                "cleanup",
                "--bucket",
                TEST_BUCKET,
                "--force",
            ],
        )
        self.assertEqual(result.exit_code, 0, "Cleanup command failed")

        # Check S3 is empty
        resp = self.s3.list_objects_v2(Bucket=TEST_BUCKET, Prefix="s3lfs/assets/")
        self.assertFalse(
            "Contents" in resp or len(resp.get("Contents", [])) > 0,
            "Bucket should be empty after cleanup",
        )

    def test_remove_cli(self):
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["upload", self.test_file, "--bucket", TEST_BUCKET])
        result = runner.invoke(s3lfs_main, ["remove", self.test_file])
        self.assertEqual(result.exit_code, 0)

    def test_remove_subtree_cli(self):
        runner = CliRunner()
        os.makedirs("test_dir", exist_ok=True)
        file_path = "test_dir/nested_cli_file.txt"
        with open(file_path, "w") as f:
            f.write("Nested CLI content")
        runner.invoke(s3lfs_main, ["upload", file_path, "--bucket", TEST_BUCKET])
        result = runner.invoke(s3lfs_main, ["remove-subtree", "test_dir"])
        self.assertEqual(result.exit_code, 0)
        os.remove(file_path)
        os.rmdir("test_dir")

    def test_cleanup_cli(self):
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["upload", self.test_file, "--bucket", TEST_BUCKET])
        result = runner.invoke(
            s3lfs_main, ["cleanup", "--bucket", TEST_BUCKET, "--force"]
        )
        self.assertEqual(result.exit_code, 0)

    def test_upload_with_no_sign_request(self):
        """Test upload using --no-sign-request."""
        test_args = [
            "upload",
            self.test_file,
            "--bucket",
            TEST_BUCKET,
            "--no-sign-request",
        ]
        runner = CliRunner()
        result = runner.invoke(s3lfs_main, test_args)
        self.assertEqual(result.exit_code, 0, "Upload command failed")

        resp = self.s3.list_objects_v2(Bucket=TEST_BUCKET, Prefix="s3lfs/assets/")
        self.assertIn("Contents", resp)
        self.assertEqual(len(resp["Contents"]), 1, "Expected 1 uploaded object.")

        self.assertTrue(self.manifest_path.exists())
        manifest_data = json.loads(self.manifest_path.read_text())
        self.assertIn(
            self.test_file, manifest_data["files"], "File path should be in manifest"
        )


if __name__ == "__main__":
    unittest.main()
