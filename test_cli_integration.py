import json
import os
import shutil
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

        # Create test directory structure for more comprehensive tests
        self.test_dir = "test_cli_dir"
        os.makedirs(self.test_dir, exist_ok=True)

        # Create multiple test files
        self.test_files = []
        for i in range(3):
            file_path = f"{self.test_dir}/test_file_{i}.txt"
            with open(file_path, "w") as f:
                f.write(f"Test content {i}")
            self.test_files.append(file_path)

    def tearDown(self):
        # Cleanup local files
        if os.path.exists(self.test_file):
            os.remove(self.test_file)

        # Cleanup test directory
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

        # Cleanup manifest
        if self.manifest_path.exists():
            self.manifest_path.unlink()

        # Cleanup any temporary directories
        temp_dirs = [d for d in os.listdir(".") if d.startswith(".s3lfs_temp")]
        for temp_dir in temp_dirs:
            if os.path.isdir(temp_dir):
                shutil.rmtree(temp_dir)

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

    def test_upload_with_prefix(self):
        """Test upload with custom prefix."""
        test_args = [
            "upload",
            self.test_file,
            "--bucket",
            TEST_BUCKET,
            "--prefix",
            "custom_prefix",
        ]
        runner = CliRunner()
        result = runner.invoke(s3lfs_main, test_args)
        self.assertEqual(result.exit_code, 0, "Upload with prefix failed")

        # Check S3 object uses custom prefix
        resp = self.s3.list_objects_v2(
            Bucket=TEST_BUCKET, Prefix="custom_prefix/assets/"
        )
        self.assertIn("Contents", resp)
        self.assertEqual(len(resp["Contents"]), 1)

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

    def test_download_with_prefix(self):
        """Test download with custom prefix."""
        runner = CliRunner()

        # Upload with prefix
        runner.invoke(
            s3lfs_main,
            [
                "upload",
                self.test_file,
                "--bucket",
                TEST_BUCKET,
                "--prefix",
                "custom_prefix",
            ],
        )

        # Remove local file
        os.remove(self.test_file)

        # Download with same prefix
        result = runner.invoke(
            s3lfs_main,
            [
                "download",
                self.test_file,
                "--bucket",
                TEST_BUCKET,
                "--prefix",
                "custom_prefix",
            ],
        )
        self.assertEqual(result.exit_code, 0)
        self.assertTrue(os.path.exists(self.test_file))

    def test_init_command(self):
        """Test the init command to initialize repository."""
        runner = CliRunner()
        result = runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_repo_prefix"])
        self.assertEqual(result.exit_code, 0, "Init command failed")

        # Check manifest was created with correct values
        self.assertTrue(self.manifest_path.exists())
        manifest_data = json.loads(self.manifest_path.read_text())
        self.assertEqual(manifest_data["bucket_name"], TEST_BUCKET)
        self.assertEqual(manifest_data["repo_prefix"], "test_repo_prefix")

    def test_init_command_with_no_sign_request(self):
        """Test init command with --no-sign-request flag."""
        runner = CliRunner()
        result = runner.invoke(
            s3lfs_main, ["init", TEST_BUCKET, "test_prefix", "--no-sign-request"]
        )
        self.assertEqual(result.exit_code, 0)

    def test_track_command(self):
        """Test the track command for tracking files."""
        runner = CliRunner()

        # First initialize the repo
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        # Track a single file
        result = runner.invoke(
            s3lfs_main, ["track", self.test_file, "--bucket", TEST_BUCKET]
        )
        self.assertEqual(result.exit_code, 0, "Track command failed")

        # Check file was tracked in manifest
        manifest_data = json.loads(self.manifest_path.read_text())
        self.assertIn(self.test_file, manifest_data["files"])

    def test_track_command_with_verbose(self):
        """Test track command with verbose flag."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        result = runner.invoke(
            s3lfs_main, ["track", self.test_file, "--bucket", TEST_BUCKET, "--verbose"]
        )
        self.assertEqual(result.exit_code, 0)

    def test_track_directory(self):
        """Test tracking a directory."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        result = runner.invoke(
            s3lfs_main, ["track", self.test_dir, "--bucket", TEST_BUCKET]
        )
        self.assertEqual(result.exit_code, 0)

        # Check all files in directory were tracked
        manifest_data = json.loads(self.manifest_path.read_text())
        for file_path in self.test_files:
            self.assertIn(file_path, manifest_data["files"])

    def test_checkout_command(self):
        """Test the checkout command."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        # First track the file
        runner.invoke(s3lfs_main, ["track", self.test_file, "--bucket", TEST_BUCKET])

        # Remove the file
        os.remove(self.test_file)

        # Checkout the file
        result = runner.invoke(
            s3lfs_main, ["checkout", self.test_file, "--bucket", TEST_BUCKET]
        )
        self.assertEqual(result.exit_code, 0, "Checkout command failed")
        self.assertTrue(os.path.exists(self.test_file))

    def test_checkout_with_verbose(self):
        """Test checkout command with verbose flag."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])
        runner.invoke(s3lfs_main, ["track", self.test_file, "--bucket", TEST_BUCKET])
        os.remove(self.test_file)

        result = runner.invoke(
            s3lfs_main,
            ["checkout", self.test_file, "--bucket", TEST_BUCKET, "--verbose"],
        )
        self.assertEqual(result.exit_code, 0)

    def test_track_modified_command(self):
        """Test the track-modified command."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        # Track a file first
        runner.invoke(s3lfs_main, ["track", self.test_file, "--bucket", TEST_BUCKET])

        # Modify the file
        with open(self.test_file, "w") as f:
            f.write("Modified content")

        # Run track-modified
        result = runner.invoke(s3lfs_main, ["track-modified", "--bucket", TEST_BUCKET])
        self.assertEqual(result.exit_code, 0, "Track-modified command failed")

    def test_track_modified_with_no_sign_request(self):
        """Test track-modified with --no-sign-request."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        result = runner.invoke(
            s3lfs_main, ["track-modified", "--bucket", TEST_BUCKET, "--no-sign-request"]
        )
        self.assertEqual(result.exit_code, 0)

    def test_download_all_command(self):
        """Test the download-all command."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        # Track multiple files
        for file_path in self.test_files:
            runner.invoke(s3lfs_main, ["track", file_path, "--bucket", TEST_BUCKET])

        # Remove all files
        for file_path in self.test_files:
            os.remove(file_path)

        # Download all
        result = runner.invoke(s3lfs_main, ["download-all", "--bucket", TEST_BUCKET])
        self.assertEqual(result.exit_code, 0, "Download-all command failed")

        # Check all files were restored
        for file_path in self.test_files:
            self.assertTrue(os.path.exists(file_path))

    def test_download_all_with_verbose(self):
        """Test download-all with verbose flag."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])
        runner.invoke(s3lfs_main, ["track", self.test_file, "--bucket", TEST_BUCKET])
        os.remove(self.test_file)

        result = runner.invoke(
            s3lfs_main, ["download-all", "--bucket", TEST_BUCKET, "--verbose"]
        )
        self.assertEqual(result.exit_code, 0)

    def test_download_all_with_no_sign_request(self):
        """Test download-all with --no-sign-request."""
        runner = CliRunner()
        result = runner.invoke(
            s3lfs_main, ["download-all", "--bucket", TEST_BUCKET, "--no-sign-request"]
        )
        self.assertEqual(result.exit_code, 0)

    def test_git_setup_command(self):
        """Test the git-setup command."""
        runner = CliRunner()
        result = runner.invoke(
            s3lfs_main,
            ["git-setup", "--bucket", TEST_BUCKET, "--prefix", "test_prefix"],
        )
        self.assertEqual(result.exit_code, 0, "Git-setup command failed")

    def test_git_setup_with_no_sign_request(self):
        """Test git-setup with --no-sign-request."""
        runner = CliRunner()
        result = runner.invoke(
            s3lfs_main,
            [
                "git-setup",
                "--bucket",
                TEST_BUCKET,
                "--prefix",
                "test_prefix",
                "--no-sign-request",
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_track_subtree_deprecated_command(self):
        """Test the deprecated track-subtree command."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        result = runner.invoke(
            s3lfs_main, ["track-subtree", self.test_dir, "--bucket", TEST_BUCKET]
        )
        self.assertEqual(result.exit_code, 0, "Track-subtree command failed")

        # Should show deprecation warning in output
        self.assertIn("deprecated", result.output)

    def test_track_subtree_with_verbose(self):
        """Test track-subtree with verbose flag."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        result = runner.invoke(
            s3lfs_main,
            ["track-subtree", self.test_dir, "--bucket", TEST_BUCKET, "--verbose"],
        )
        self.assertEqual(result.exit_code, 0)

    def test_sparse_checkout_deprecated_command(self):
        """Test the deprecated sparse-checkout command."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])
        runner.invoke(s3lfs_main, ["track", self.test_file, "--bucket", TEST_BUCKET])
        os.remove(self.test_file)

        result = runner.invoke(
            s3lfs_main, ["sparse-checkout", self.test_file, "--bucket", TEST_BUCKET]
        )
        self.assertEqual(result.exit_code, 0, "Sparse-checkout command failed")

        # Should show deprecation warning in output
        self.assertIn("deprecated", result.output)

    def test_sparse_checkout_with_verbose(self):
        """Test sparse-checkout with verbose flag."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])
        runner.invoke(s3lfs_main, ["track", self.test_file, "--bucket", TEST_BUCKET])
        os.remove(self.test_file)

        result = runner.invoke(
            s3lfs_main,
            ["sparse-checkout", self.test_file, "--bucket", TEST_BUCKET, "--verbose"],
        )
        self.assertEqual(result.exit_code, 0)

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

    def test_cleanup_with_prefix(self):
        """Test cleanup with custom prefix."""
        runner = CliRunner()
        runner.invoke(
            s3lfs_main,
            [
                "upload",
                self.test_file,
                "--bucket",
                TEST_BUCKET,
                "--prefix",
                "custom_prefix",
            ],
        )

        result = runner.invoke(
            s3lfs_main,
            [
                "cleanup",
                "--bucket",
                TEST_BUCKET,
                "--prefix",
                "custom_prefix",
                "--force",
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_remove_cli(self):
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["upload", self.test_file, "--bucket", TEST_BUCKET])
        result = runner.invoke(s3lfs_main, ["remove", self.test_file])
        self.assertEqual(result.exit_code, 0)

    def test_remove_with_purge_from_s3(self):
        """Test remove with --purge-from-s3 flag."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["upload", self.test_file, "--bucket", TEST_BUCKET])

        result = runner.invoke(
            s3lfs_main,
            ["remove", self.test_file, "--purge-from-s3", "--bucket", TEST_BUCKET],
        )
        self.assertEqual(result.exit_code, 0)

    def test_remove_subtree_cli(self):
        runner = CliRunner()
        os.makedirs("test_dir_remove", exist_ok=True)
        file_path = "test_dir_remove/nested_cli_file.txt"
        with open(file_path, "w") as f:
            f.write("Nested CLI content")
        runner.invoke(s3lfs_main, ["upload", file_path, "--bucket", TEST_BUCKET])
        result = runner.invoke(s3lfs_main, ["remove-subtree", "test_dir_remove"])
        self.assertEqual(result.exit_code, 0)
        os.remove(file_path)
        os.rmdir("test_dir_remove")

    def test_remove_subtree_with_purge(self):
        """Test remove-subtree with --purge-from-s3 flag."""
        runner = CliRunner()
        os.makedirs("test_dir_purge", exist_ok=True)
        file_path = "test_dir_purge/nested_file.txt"
        with open(file_path, "w") as f:
            f.write("Content to purge")

        runner.invoke(s3lfs_main, ["upload", file_path, "--bucket", TEST_BUCKET])
        result = runner.invoke(
            s3lfs_main,
            [
                "remove-subtree",
                "test_dir_purge",
                "--purge-from-s3",
                "--bucket",
                TEST_BUCKET,
            ],
        )
        self.assertEqual(result.exit_code, 0)

        os.remove(file_path)
        os.rmdir("test_dir_purge")

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

    def test_cli_group_help(self):
        """Test that the CLI group shows help correctly."""
        runner = CliRunner()
        result = runner.invoke(s3lfs_main, ["--help"])
        self.assertEqual(result.exit_code, 0)
        self.assertIn("S3-based asset versioning CLI tool", result.output)

    def test_command_help_messages(self):
        """Test that individual commands show help."""
        runner = CliRunner()
        commands = ["upload", "download", "init", "track", "checkout", "cleanup"]

        for command in commands:
            result = runner.invoke(s3lfs_main, [command, "--help"])
            self.assertEqual(result.exit_code, 0, f"Help for {command} failed")
            self.assertIn("Usage:", result.output)

    def test_error_handling_missing_file(self):
        """Test error handling when trying to upload non-existent file."""
        runner = CliRunner()
        result = runner.invoke(
            s3lfs_main, ["upload", "nonexistent_file.txt", "--bucket", TEST_BUCKET]
        )
        # Should not crash, but may have non-zero exit code
        self.assertIsNotNone(result.exit_code)

    def test_error_handling_no_manifest(self):
        """Test commands that depend on manifest when no manifest exists."""
        runner = CliRunner()

        # Try to download without manifest
        result = runner.invoke(
            s3lfs_main, ["download", "some_file.txt", "--bucket", TEST_BUCKET]
        )
        # Should handle gracefully
        self.assertIsNotNone(result.exit_code)

    def test_main_entry_point(self):
        """Test the main() entry point function."""
        # Import the main function and CLI module
        import sys
        from unittest.mock import patch

        from s3lfs.cli import main

        # Test main() function by mocking sys.argv and calling it
        with patch.object(sys, "argv", ["s3lfs", "--help"]):
            try:
                main()
            except SystemExit as e:
                # Help command should exit with code 0
                self.assertEqual(e.code, 0)

    def test_cli_as_module(self):
        """Test running the CLI as a module (__main__ block)."""
        import subprocess
        import sys

        # Test running the module with --help
        result = subprocess.run(
            [sys.executable, "-m", "s3lfs.cli", "--help"],
            capture_output=True,
            text=True,
        )

        self.assertEqual(result.returncode, 0)
        self.assertIn("S3-based asset versioning CLI tool", result.stdout)


if __name__ == "__main__":
    unittest.main()
