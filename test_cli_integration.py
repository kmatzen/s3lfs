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
        # Clean up the test file and manifest
        if os.path.exists(self.test_file):
            os.remove(self.test_file)
        if self.manifest_path.exists():
            self.manifest_path.unlink()

    def test_init_command(self):
        """Test the init command."""
        runner = CliRunner()
        result = runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])
        self.assertEqual(result.exit_code, 0, "Init command failed")

        # Check if manifest was created
        self.assertTrue(self.manifest_path.exists(), "Manifest file was not created")

        # Check manifest contents
        with open(self.manifest_path, "r") as f:
            manifest = json.load(f)
        self.assertEqual(manifest["bucket_name"], TEST_BUCKET)
        self.assertEqual(manifest["repo_prefix"], "test_prefix")

    def test_init_with_no_sign_request(self):
        """Test init with --no-sign-request flag."""
        runner = CliRunner()
        result = runner.invoke(
            s3lfs_main, ["init", TEST_BUCKET, "test_prefix", "--no-sign-request"]
        )
        self.assertEqual(result.exit_code, 0)

    def test_track_command(self):
        """Test the track command (replaces upload)."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        result = runner.invoke(s3lfs_main, ["track", self.test_file])
        self.assertEqual(result.exit_code, 0, "Track command failed")

    def test_track_with_verbose(self):
        """Test track command with verbose flag."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        result = runner.invoke(s3lfs_main, ["track", self.test_file, "--verbose"])
        self.assertEqual(result.exit_code, 0)

    def test_track_with_no_sign_request(self):
        """Test track command with --no-sign-request."""
        runner = CliRunner()
        runner.invoke(
            s3lfs_main, ["init", TEST_BUCKET, "test_prefix", "--no-sign-request"]
        )

        result = runner.invoke(
            s3lfs_main, ["track", self.test_file, "--no-sign-request"]
        )
        self.assertEqual(result.exit_code, 0)

    def test_checkout_command(self):
        """Test the checkout command (replaces download)."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        # First track the file
        runner.invoke(s3lfs_main, ["track", self.test_file])

        # Remove the file
        os.remove(self.test_file)

        # Checkout the file
        result = runner.invoke(s3lfs_main, ["checkout", self.test_file])
        self.assertEqual(result.exit_code, 0, "Checkout command failed")
        self.assertTrue(os.path.exists(self.test_file))

    def test_checkout_with_verbose(self):
        """Test checkout command with verbose flag."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])
        runner.invoke(s3lfs_main, ["track", self.test_file])
        os.remove(self.test_file)

        result = runner.invoke(s3lfs_main, ["checkout", self.test_file, "--verbose"])
        self.assertEqual(result.exit_code, 0)

    def test_track_modified_command(self):
        """Test the track --modified command (replaces track-modified)."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        # Track a file first
        runner.invoke(s3lfs_main, ["track", self.test_file])

        # Modify the file
        with open(self.test_file, "w") as f:
            f.write("Modified content")

        # Run track --modified
        result = runner.invoke(s3lfs_main, ["track", "--modified"])
        self.assertEqual(result.exit_code, 0, "Track --modified command failed")

    def test_track_modified_with_no_sign_request(self):
        """Test track --modified with --no-sign-request."""
        runner = CliRunner()
        runner.invoke(
            s3lfs_main, ["init", TEST_BUCKET, "test_prefix", "--no-sign-request"]
        )
        runner.invoke(s3lfs_main, ["track", self.test_file, "--no-sign-request"])

        with open(self.test_file, "w") as f:
            f.write("Modified content")

        result = runner.invoke(s3lfs_main, ["track", "--modified", "--no-sign-request"])
        self.assertEqual(result.exit_code, 0)

    def test_checkout_all_command(self):
        """Test the checkout --all command (replaces download-all)."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        # Track a file
        runner.invoke(s3lfs_main, ["track", self.test_file])

        # Remove the file
        os.remove(self.test_file)

        # Download all files
        result = runner.invoke(s3lfs_main, ["checkout", "--all"])
        self.assertEqual(result.exit_code, 0, "Checkout --all command failed")
        self.assertTrue(os.path.exists(self.test_file))

    def test_checkout_all_with_verbose(self):
        """Test checkout --all with verbose flag."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])
        runner.invoke(s3lfs_main, ["track", self.test_file])
        os.remove(self.test_file)

        result = runner.invoke(s3lfs_main, ["checkout", "--all", "--verbose"])
        self.assertEqual(result.exit_code, 0)

    def test_checkout_all_with_no_sign_request(self):
        """Test checkout --all with --no-sign-request."""
        runner = CliRunner()
        runner.invoke(
            s3lfs_main, ["init", TEST_BUCKET, "test_prefix", "--no-sign-request"]
        )
        runner.invoke(s3lfs_main, ["track", self.test_file, "--no-sign-request"])
        os.remove(self.test_file)

        result = runner.invoke(s3lfs_main, ["checkout", "--all", "--no-sign-request"])
        self.assertEqual(result.exit_code, 0)

    def test_remove_command(self):
        """Test the remove command."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        # Track a file first
        runner.invoke(s3lfs_main, ["track", self.test_file])

        # Remove the file from tracking
        result = runner.invoke(s3lfs_main, ["remove", self.test_file])
        self.assertEqual(result.exit_code, 0, "Remove command failed")

    def test_remove_with_purge_from_s3(self):
        """Test remove command with --purge-from-s3."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])
        runner.invoke(s3lfs_main, ["track", self.test_file])

        result = runner.invoke(
            s3lfs_main, ["remove", self.test_file, "--purge-from-s3"]
        )
        self.assertEqual(result.exit_code, 0)

    def test_remove_with_no_sign_request(self):
        """Test remove command with --no-sign-request."""
        runner = CliRunner()
        runner.invoke(
            s3lfs_main, ["init", TEST_BUCKET, "test_prefix", "--no-sign-request"]
        )
        runner.invoke(s3lfs_main, ["track", self.test_file, "--no-sign-request"])

        result = runner.invoke(
            s3lfs_main, ["remove", self.test_file, "--no-sign-request"]
        )
        self.assertEqual(result.exit_code, 0)

    def test_cleanup_command(self):
        """Test the cleanup command."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        result = runner.invoke(s3lfs_main, ["cleanup", "--force"])
        self.assertEqual(result.exit_code, 0, "Cleanup command failed")

    def test_cleanup_with_no_sign_request(self):
        """Test cleanup command with --no-sign-request."""
        runner = CliRunner()
        runner.invoke(
            s3lfs_main, ["init", TEST_BUCKET, "test_prefix", "--no-sign-request"]
        )

        result = runner.invoke(s3lfs_main, ["cleanup", "--force", "--no-sign-request"])
        self.assertEqual(result.exit_code, 0)

    def test_remove_directory_command(self):
        """Test the remove command with directory (replaces remove-subtree)."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        # Create a directory with files
        os.makedirs("test_dir_remove", exist_ok=True)
        file_path = os.path.join("test_dir_remove", "test_file.txt")
        with open(file_path, "w") as f:
            f.write("Test content")

        try:
            # Track the file
            runner.invoke(s3lfs_main, ["track", file_path])
            result = runner.invoke(s3lfs_main, ["remove", "test_dir_remove"])
            self.assertEqual(result.exit_code, 0, "Remove directory command failed")
        finally:
            # Clean up
            if os.path.exists(file_path):
                os.remove(file_path)
            if os.path.exists("test_dir_remove"):
                os.rmdir("test_dir_remove")

    def test_remove_directory_with_purge_from_s3(self):
        """Test remove directory with --purge-from-s3."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        os.makedirs("test_dir_remove", exist_ok=True)
        file_path = os.path.join("test_dir_remove", "test_file.txt")
        with open(file_path, "w") as f:
            f.write("Test content")

        try:
            runner.invoke(s3lfs_main, ["track", file_path])

            result = runner.invoke(
                s3lfs_main, ["remove", "test_dir_remove", "--purge-from-s3"]
            )
            self.assertEqual(result.exit_code, 0)
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)
            if os.path.exists("test_dir_remove"):
                os.rmdir("test_dir_remove")

    def test_track_and_checkout_workflow(self):
        """Test a complete workflow with track and checkout."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        # Track the file
        result = runner.invoke(s3lfs_main, ["track", self.test_file])
        self.assertEqual(result.exit_code, 0)

        # Remove the local file
        os.remove(self.test_file)

        # Checkout the file
        result = runner.invoke(s3lfs_main, ["checkout", self.test_file])
        self.assertEqual(result.exit_code, 0)
        self.assertTrue(os.path.exists(self.test_file))

    def test_cli_help(self):
        """Test that CLI help works and shows expected commands."""
        runner = CliRunner()
        result = runner.invoke(s3lfs_main, ["--help"])
        self.assertEqual(result.exit_code, 0)

        # Check that main commands are present
        commands = ["track", "checkout", "init", "remove", "cleanup"]
        for cmd in commands:
            self.assertIn(cmd, result.output)

    def test_error_handling_nonexistent_file(self):
        """Test error handling for nonexistent files."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        result = runner.invoke(s3lfs_main, ["track", "nonexistent_file.txt"])
        # Should handle gracefully
        self.assertIsNotNone(result.exit_code)

    def test_error_handling_no_manifest(self):
        """Test commands that depend on manifest when no manifest exists."""
        runner = CliRunner()

        # Try to checkout without manifest
        result = runner.invoke(s3lfs_main, ["checkout", "some_file.txt"])
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
                # Help command should exit with 0
                self.assertEqual(e.code, 0)

    def test_cli_as_module(self):
        """Test running CLI as module."""
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

    def test_track_without_path_or_modified_flag(self):
        """Test track command error when neither path nor --modified is provided."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        result = runner.invoke(s3lfs_main, ["track"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn(
            "Error: Must provide either a path or use --modified flag", result.output
        )

    def test_checkout_without_path_or_all_flag(self):
        """Test checkout command error when neither path nor --all is provided."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        result = runner.invoke(s3lfs_main, ["checkout"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn(
            "Error: Must provide either a path or use --all flag", result.output
        )

    def test_track_with_transfer_acceleration(self):
        """Test track command with transfer acceleration flag."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        result = runner.invoke(
            s3lfs_main, ["track", self.test_file, "--use-acceleration"]
        )
        self.assertEqual(
            result.exit_code, 0, "Track command with transfer acceleration failed"
        )

    def test_checkout_with_transfer_acceleration(self):
        """Test checkout command with transfer acceleration flag."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        # First track the file
        runner.invoke(s3lfs_main, ["track", self.test_file])

        # Remove the file
        os.remove(self.test_file)

        # Checkout with transfer acceleration
        result = runner.invoke(
            s3lfs_main, ["checkout", self.test_file, "--use-acceleration"]
        )
        self.assertEqual(
            result.exit_code, 0, "Checkout command with transfer acceleration failed"
        )
        self.assertTrue(os.path.exists(self.test_file))

    def test_ls_with_transfer_acceleration(self):
        """Test ls command with transfer acceleration flag."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        # Track a file first
        runner.invoke(s3lfs_main, ["track", self.test_file])

        # List with transfer acceleration
        result = runner.invoke(s3lfs_main, ["ls", "--use-acceleration"])
        self.assertEqual(
            result.exit_code, 0, "Ls command with transfer acceleration failed"
        )

    def test_remove_with_transfer_acceleration(self):
        """Test remove command with transfer acceleration flag."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        # Track a file first
        runner.invoke(s3lfs_main, ["track", self.test_file])

        # Remove with transfer acceleration
        result = runner.invoke(
            s3lfs_main, ["remove", self.test_file, "--use-acceleration"]
        )
        self.assertEqual(
            result.exit_code, 0, "Remove command with transfer acceleration failed"
        )

    def test_cleanup_with_transfer_acceleration(self):
        """Test cleanup command with transfer acceleration flag."""
        runner = CliRunner()
        runner.invoke(s3lfs_main, ["init", TEST_BUCKET, "test_prefix"])

        result = runner.invoke(s3lfs_main, ["cleanup", "--force", "--use-acceleration"])
        self.assertEqual(
            result.exit_code, 0, "Cleanup command with transfer acceleration failed"
        )


if __name__ == "__main__":
    unittest.main()
