import json
import os
import subprocess
import tempfile
import unittest


class TestManifestNotExistsCoverage(unittest.TestCase):
    """Comprehensive tests for manifest path does not exist scenarios."""

    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)

    def tearDown(self):
        """Clean up test environment."""
        os.chdir(self.original_cwd)
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_track_command_manifest_not_exists(self):
        """Test track command when manifest does not exist (lines 143-144)."""
        # Create a git repository without manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Test track command without manifest file
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "track", "test_file.txt"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should fail with error message
        self.assertIn(
            "Error: S3LFS not initialized. Run 's3lfs init' first.", result.stdout
        )
        self.assertNotEqual(result.returncode, 0)

    def test_track_command_manifest_not_exists_with_modified_flag(self):
        """Test track command with --modified flag when manifest does not exist (lines 143-144)."""
        # Create a git repository without manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Test track command with --modified flag without manifest file
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "track", "--modified"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should fail with error message
        self.assertIn(
            "Error: S3LFS not initialized. Run 's3lfs init' first.", result.stdout
        )
        self.assertNotEqual(result.returncode, 0)

    def test_track_command_manifest_not_exists_with_verbose_flag(self):
        """Test track command with --verbose flag when manifest does not exist (lines 143-144)."""
        # Create a git repository without manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Test track command with --verbose flag without manifest file
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "track", "--verbose", "test_file.txt"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should fail with error message
        self.assertIn(
            "Error: S3LFS not initialized. Run 's3lfs init' first.", result.stdout
        )
        self.assertNotEqual(result.returncode, 0)

    def test_track_command_manifest_not_exists_with_no_sign_request_flag(self):
        """Test track command with --no-sign-request flag when manifest does not exist (lines 143-144)."""
        # Create a git repository without manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Test track command with --no-sign-request flag without manifest file
        result = subprocess.run(
            [
                "python",
                "-m",
                "s3lfs.cli",
                "track",
                "--no-sign-request",
                "test_file.txt",
            ],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should fail with error message
        self.assertIn(
            "Error: S3LFS not initialized. Run 's3lfs init' first.", result.stdout
        )
        self.assertNotEqual(result.returncode, 0)

    def test_checkout_command_manifest_not_exists(self):
        """Test checkout command when manifest does not exist (lines 176-177)."""
        # Create a git repository without manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Test checkout command without manifest file
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "checkout", "test_file.txt"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should fail with error message
        self.assertIn(
            "Error: S3LFS not initialized. Run 's3lfs init' first.", result.stdout
        )
        self.assertNotEqual(result.returncode, 0)

    def test_checkout_command_manifest_not_exists_with_all_flag(self):
        """Test checkout command with --all flag when manifest does not exist (lines 176-177)."""
        # Create a git repository without manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Test checkout command with --all flag without manifest file
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "checkout", "--all"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should fail with error message
        self.assertIn(
            "Error: S3LFS not initialized. Run 's3lfs init' first.", result.stdout
        )
        self.assertNotEqual(result.returncode, 0)

    def test_checkout_command_manifest_not_exists_with_verbose_flag(self):
        """Test checkout command with --verbose flag when manifest does not exist (lines 176-177)."""
        # Create a git repository without manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Test checkout command with --verbose flag without manifest file
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "checkout", "--verbose", "test_file.txt"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should fail with error message
        self.assertIn(
            "Error: S3LFS not initialized. Run 's3lfs init' first.", result.stdout
        )
        self.assertNotEqual(result.returncode, 0)

    def test_checkout_command_manifest_not_exists_with_no_sign_request_flag(self):
        """Test checkout command with --no-sign-request flag when manifest does not exist (lines 176-177)."""
        # Create a git repository without manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Test checkout command with --no-sign-request flag without manifest file
        result = subprocess.run(
            [
                "python",
                "-m",
                "s3lfs.cli",
                "checkout",
                "--no-sign-request",
                "test_file.txt",
            ],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should fail with error message
        self.assertIn(
            "Error: S3LFS not initialized. Run 's3lfs init' first.", result.stdout
        )
        self.assertNotEqual(result.returncode, 0)

    def test_ls_command_manifest_not_exists(self):
        """Test ls command when manifest does not exist."""
        # Create a git repository without manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Test ls command without manifest file
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "ls"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should fail with error message
        self.assertIn(
            "Error: S3LFS not initialized. Run 's3lfs init' first.", result.stdout
        )
        self.assertNotEqual(result.returncode, 0)

    def test_ls_command_manifest_not_exists_with_all_flag(self):
        """Test ls command with --all flag when manifest does not exist."""
        # Create a git repository without manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Test ls command with --all flag without manifest file
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "ls", "--all"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should fail with error message
        self.assertIn(
            "Error: S3LFS not initialized. Run 's3lfs init' first.", result.stdout
        )
        self.assertNotEqual(result.returncode, 0)

    def test_ls_command_manifest_not_exists_with_verbose_flag(self):
        """Test ls command with --verbose flag when manifest does not exist."""
        # Create a git repository without manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Test ls command with --verbose flag without manifest file
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "ls", "--verbose"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should fail with error message
        self.assertIn(
            "Error: S3LFS not initialized. Run 's3lfs init' first.", result.stdout
        )
        self.assertNotEqual(result.returncode, 0)

    def test_ls_command_manifest_not_exists_with_no_sign_request_flag(self):
        """Test ls command with --no-sign-request flag when manifest does not exist."""
        # Create a git repository without manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Test ls command with --no-sign-request flag without manifest file
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "ls", "--no-sign-request"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should fail with error message
        self.assertIn(
            "Error: S3LFS not initialized. Run 's3lfs init' first.", result.stdout
        )
        self.assertNotEqual(result.returncode, 0)

    def test_remove_command_manifest_not_exists(self):
        """Test remove command when manifest does not exist."""
        # Create a git repository without manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Test remove command without manifest file
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "remove", "test_file.txt"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should fail with error message
        self.assertIn(
            "Error: S3LFS not initialized. Run 's3lfs init' first.", result.stdout
        )
        self.assertNotEqual(result.returncode, 0)

    def test_remove_command_manifest_not_exists_with_purge_flag(self):
        """Test remove command with --purge-from-s3 flag when manifest does not exist."""
        # Create a git repository without manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Test remove command with --purge-from-s3 flag without manifest file
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "remove", "--purge-from-s3", "test_file.txt"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should fail with error message
        self.assertIn(
            "Error: S3LFS not initialized. Run 's3lfs init' first.", result.stdout
        )
        self.assertNotEqual(result.returncode, 0)

    def test_cleanup_command_manifest_not_exists(self):
        """Test cleanup command when manifest does not exist."""
        # Create a git repository without manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Test cleanup command without manifest file
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "cleanup"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should fail with error message
        self.assertIn(
            "Error: S3LFS not initialized. Run 's3lfs init' first.", result.stdout
        )
        self.assertNotEqual(result.returncode, 0)

    def test_cleanup_command_manifest_not_exists_with_force_flag(self):
        """Test cleanup command with --force flag when manifest does not exist."""
        # Create a git repository without manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Test cleanup command with --force flag without manifest file
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "cleanup", "--force"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should fail with error message
        self.assertIn(
            "Error: S3LFS not initialized. Run 's3lfs init' first.", result.stdout
        )
        self.assertNotEqual(result.returncode, 0)

    def test_init_command_success(self):
        """Test init command success (lines 110-111)."""
        # Create a git repository
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Test init command
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "init", "testbucket", "testprefix"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should succeed and reach lines 110-111
        self.assertEqual(result.returncode, 0)

    def test_init_command_with_no_sign_request(self):
        """Test init command with --no-sign-request flag (lines 110-111)."""
        # Create a git repository
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Test init command with --no-sign-request flag
        result = subprocess.run(
            [
                "python",
                "-m",
                "s3lfs.cli",
                "init",
                "--no-sign-request",
                "testbucket",
                "testprefix",
            ],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should succeed and reach lines 110-111
        self.assertEqual(result.returncode, 0)

    def test_init_command_not_in_git_repo(self):
        """Test init command when not in a git repository."""
        # Import the necessary functions
        from unittest.mock import patch

        # Create a directory that is not a git repository
        non_git_dir = os.path.join(self.temp_dir, "non_git_dir")
        os.makedirs(non_git_dir)
        os.chdir(non_git_dir)

        # Mock find_git_root to return None (no git repo found)
        with patch("s3lfs.cli.find_git_root", return_value=None):
            # Test init command when not in git repo
            result = subprocess.run(
                ["python", "-m", "s3lfs.cli", "init", "testbucket", "testprefix"],
                capture_output=True,
                text=True,
                cwd=non_git_dir,
            )

            # Should fail with error message
            self.assertIn("Error: Not in a git repository", result.stdout)
            self.assertNotEqual(result.returncode, 0)

    def test_init_command_not_in_git_repo_with_no_sign_request(self):
        """Test init command with --no-sign-request flag when not in a git repository."""
        # Import the necessary functions
        from unittest.mock import patch

        # Create a directory that is not a git repository
        non_git_dir = os.path.join(self.temp_dir, "non_git_dir")
        os.makedirs(non_git_dir)
        os.chdir(non_git_dir)

        # Mock find_git_root to return None (no git repo found)
        with patch("s3lfs.cli.find_git_root", return_value=None):
            # Test init command with --no-sign-request flag when not in git repo
            result = subprocess.run(
                [
                    "python",
                    "-m",
                    "s3lfs.cli",
                    "init",
                    "--no-sign-request",
                    "testbucket",
                    "testprefix",
                ],
                capture_output=True,
                text=True,
                cwd=non_git_dir,
            )

            # Should fail with error message
            self.assertIn("Error: Not in a git repository", result.stdout)
            self.assertNotEqual(result.returncode, 0)

    def test_track_command_no_path_no_modified_flag(self):
        """Test track command with no path and no --modified flag (lines 110-111)."""
        # Create a git repository with manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Create manifest file
        manifest_file = os.path.join(git_repo, ".s3_manifest.json")
        manifest = {
            "bucket_name": "testbucket",
            "repo_prefix": "testprefix",
            "files": {"test_file.txt": "test_hash"},
        }
        with open(manifest_file, "w") as f:
            json.dump(manifest, f)

        # Test track command with no path and no --modified flag
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "track"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should fail with error message
        self.assertIn(
            "Error: Must provide either a path or use --modified flag", result.stdout
        )
        self.assertNotEqual(result.returncode, 0)

    def test_checkout_command_no_path_no_all_flag(self):
        """Test checkout command with no path and no --all flag (lines 138-139, 143-144)."""
        # Create a git repository with manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Create manifest file
        manifest_file = os.path.join(git_repo, ".s3_manifest.json")
        manifest = {
            "bucket_name": "testbucket",
            "repo_prefix": "testprefix",
            "files": {"test_file.txt": "test_hash"},
        }
        with open(manifest_file, "w") as f:
            json.dump(manifest, f)

        # Test checkout command with no path and no --all flag
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "checkout"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should fail with error message
        self.assertIn(
            "Error: Must provide either a path or use --all flag", result.stdout
        )
        self.assertNotEqual(result.returncode, 0)

    def test_ls_command_with_all_flag(self):
        """Test ls command with --all flag (lines 176-177)."""
        # Create a git repository with manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Create manifest file
        manifest_file = os.path.join(git_repo, ".s3_manifest.json")
        manifest = {
            "bucket_name": "testbucket",
            "repo_prefix": "testprefix",
            "files": {"test_file.txt": "test_hash"},
        }
        with open(manifest_file, "w") as f:
            json.dump(manifest, f)

        # Test ls command with --all flag
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "ls", "--all"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should succeed
        self.assertEqual(result.returncode, 0)

    def test_ls_command_with_all_flag_and_verbose(self):
        """Test ls command with --all flag and --verbose (lines 176-177)."""
        # Create a git repository with manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Create manifest file
        manifest_file = os.path.join(git_repo, ".s3_manifest.json")
        manifest = {
            "bucket_name": "testbucket",
            "repo_prefix": "testprefix",
            "files": {"test_file.txt": "test_hash"},
        }
        with open(manifest_file, "w") as f:
            json.dump(manifest, f)

        # Test ls command with --all flag and --verbose
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "ls", "--all", "--verbose"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should succeed
        self.assertEqual(result.returncode, 0)

    def test_ls_command_with_all_flag_and_no_sign_request(self):
        """Test ls command with --all flag and --no-sign-request (lines 176-177)."""
        # Create a git repository with manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Create manifest file
        manifest_file = os.path.join(git_repo, ".s3_manifest.json")
        manifest = {
            "bucket_name": "testbucket",
            "repo_prefix": "testprefix",
            "files": {"test_file.txt": "test_hash"},
        }
        with open(manifest_file, "w") as f:
            json.dump(manifest, f)

        # Test ls command with --all flag and --no-sign-request
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "ls", "--all", "--no-sign-request"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should succeed
        self.assertEqual(result.returncode, 0)

    def test_remove_command_pattern_handling(self):
        """Test remove command pattern handling (lines 259-260)."""
        # Create a git repository with manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Create manifest file
        manifest_file = os.path.join(git_repo, ".s3_manifest.json")
        manifest = {
            "bucket_name": "testbucket",
            "repo_prefix": "testprefix",
            "files": {"test_file.txt": "test_hash"},
        }
        with open(manifest_file, "w") as f:
            json.dump(manifest, f)

        # Test remove command with directory pattern
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "remove", "test_dir/"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should succeed (even if no files match)
        self.assertEqual(result.returncode, 0)

    def test_remove_command_glob_pattern_handling(self):
        """Test remove command glob pattern handling (lines 259-260)."""
        # Create a git repository with manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Create manifest file
        manifest_file = os.path.join(git_repo, ".s3_manifest.json")
        manifest = {
            "bucket_name": "testbucket",
            "repo_prefix": "testprefix",
            "files": {"test_file.txt": "test_hash"},
        }
        with open(manifest_file, "w") as f:
            json.dump(manifest, f)

        # Test remove command with glob pattern
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "remove", "*.txt"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should succeed
        self.assertEqual(result.returncode, 0)

    def test_cleanup_command_force_flag(self):
        """Test cleanup command with --force flag (lines 289-290)."""
        # Create a git repository with manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Create manifest file
        manifest_file = os.path.join(git_repo, ".s3_manifest.json")
        manifest = {
            "bucket_name": "testbucket",
            "repo_prefix": "testprefix",
            "files": {"test_file.txt": "test_hash"},
        }
        with open(manifest_file, "w") as f:
            json.dump(manifest, f)

        # Test cleanup command with --force flag
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "cleanup", "--force"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should fail due to S3 credentials, but the important thing is that it reaches lines 289-290
        # The command should fail with S3-related error, not manifest error
        self.assertNotIn("Error: S3LFS not initialized", result.stdout)
        self.assertNotEqual(result.returncode, 0)

    def test_cleanup_command_force_flag_with_no_sign_request(self):
        """Test cleanup command with --force flag and --no-sign-request (lines 289-290)."""
        # Create a git repository with manifest file
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Create manifest file
        manifest_file = os.path.join(git_repo, ".s3_manifest.json")
        manifest = {
            "bucket_name": "testbucket",
            "repo_prefix": "testprefix",
            "files": {"test_file.txt": "test_hash"},
        }
        with open(manifest_file, "w") as f:
            json.dump(manifest, f)

        # Test cleanup command with --force flag and --no-sign-request
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "cleanup", "--force", "--no-sign-request"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should fail due to S3 credentials, but the important thing is that it reaches lines 289-290
        # The command should fail with S3-related error, not manifest error
        self.assertNotIn("Error: S3LFS not initialized", result.stdout)
        self.assertNotEqual(result.returncode, 0)

    def test_track_command_not_in_git_repo(self):
        """Test track command when not in a git repository."""
        # Import the necessary functions
        from unittest.mock import patch

        # Create a directory that is not a git repository
        non_git_dir = os.path.join(self.temp_dir, "non_git_dir")
        os.makedirs(non_git_dir)
        os.chdir(non_git_dir)

        # Mock find_git_root to return None (no git repo found)
        with patch("s3lfs.cli.find_git_root", return_value=None):
            # Test track command when not in git repo
            result = subprocess.run(
                ["python", "-m", "s3lfs.cli", "track", "test_file.txt"],
                capture_output=True,
                text=True,
                cwd=non_git_dir,
            )

            # Should fail with error message
            self.assertIn("Error: Not in a git repository", result.stdout)
            self.assertNotEqual(result.returncode, 0)

    def test_track_command_not_in_git_repo_with_modified_flag(self):
        """Test track command with --modified flag when not in a git repository."""
        # Import the necessary functions
        from unittest.mock import patch

        # Create a directory that is not a git repository
        non_git_dir = os.path.join(self.temp_dir, "non_git_dir")
        os.makedirs(non_git_dir)
        os.chdir(non_git_dir)

        # Mock find_git_root to return None (no git repo found)
        with patch("s3lfs.cli.find_git_root", return_value=None):
            # Test track command with --modified flag when not in git repo
            result = subprocess.run(
                ["python", "-m", "s3lfs.cli", "track", "--modified"],
                capture_output=True,
                text=True,
                cwd=non_git_dir,
            )

            # Should fail with error message
            self.assertIn("Error: Not in a git repository", result.stdout)
            self.assertNotEqual(result.returncode, 0)

    def test_checkout_command_not_in_git_repo(self):
        """Test checkout command when not in a git repository."""
        # Import the necessary functions
        from unittest.mock import patch

        # Create a directory that is not a git repository
        non_git_dir = os.path.join(self.temp_dir, "non_git_dir")
        os.makedirs(non_git_dir)
        os.chdir(non_git_dir)

        # Mock find_git_root to return None (no git repo found)
        with patch("s3lfs.cli.find_git_root", return_value=None):
            # Test checkout command when not in git repo
            result = subprocess.run(
                ["python", "-m", "s3lfs.cli", "checkout", "test_file.txt"],
                capture_output=True,
                text=True,
                cwd=non_git_dir,
            )

            # Should fail with error message
            self.assertIn("Error: Not in a git repository", result.stdout)
            self.assertNotEqual(result.returncode, 0)

    def test_checkout_command_not_in_git_repo_with_all_flag(self):
        """Test checkout command with --all flag when not in a git repository."""
        # Import the necessary functions
        from unittest.mock import patch

        # Create a directory that is not a git repository
        non_git_dir = os.path.join(self.temp_dir, "non_git_dir")
        os.makedirs(non_git_dir)
        os.chdir(non_git_dir)

        # Mock find_git_root to return None (no git repo found)
        with patch("s3lfs.cli.find_git_root", return_value=None):
            # Test checkout command with --all flag when not in git repo
            result = subprocess.run(
                ["python", "-m", "s3lfs.cli", "checkout", "--all"],
                capture_output=True,
                text=True,
                cwd=non_git_dir,
            )

            # Should fail with error message
            self.assertIn("Error: Not in a git repository", result.stdout)
            self.assertNotEqual(result.returncode, 0)

    def test_ls_command_not_in_git_repo(self):
        """Test ls command when not in a git repository."""
        # Import the necessary functions
        from unittest.mock import patch

        # Create a directory that is not a git repository
        non_git_dir = os.path.join(self.temp_dir, "non_git_dir")
        os.makedirs(non_git_dir)
        os.chdir(non_git_dir)

        # Mock find_git_root to return None (no git repo found)
        with patch("s3lfs.cli.find_git_root", return_value=None):
            # Test ls command when not in git repo
            result = subprocess.run(
                ["python", "-m", "s3lfs.cli", "ls"],
                capture_output=True,
                text=True,
                cwd=non_git_dir,
            )

            # Should fail with error message
            self.assertIn("Error: Not in a git repository", result.stdout)
            self.assertNotEqual(result.returncode, 0)

    def test_ls_command_not_in_git_repo_with_all_flag(self):
        """Test ls command with --all flag when not in a git repository."""
        # Import the necessary functions
        from unittest.mock import patch

        # Create a directory that is not a git repository
        non_git_dir = os.path.join(self.temp_dir, "non_git_dir")
        os.makedirs(non_git_dir)
        os.chdir(non_git_dir)

        # Mock find_git_root to return None (no git repo found)
        with patch("s3lfs.cli.find_git_root", return_value=None):
            # Test ls command with --all flag when not in git repo
            result = subprocess.run(
                ["python", "-m", "s3lfs.cli", "ls", "--all"],
                capture_output=True,
                text=True,
                cwd=non_git_dir,
            )

            # Should fail with error message
            self.assertIn("Error: Not in a git repository", result.stdout)
            self.assertNotEqual(result.returncode, 0)

    def test_remove_command_not_in_git_repo(self):
        """Test remove command when not in a git repository."""
        # Import the necessary functions
        from unittest.mock import patch

        # Create a directory that is not a git repository
        non_git_dir = os.path.join(self.temp_dir, "non_git_dir")
        os.makedirs(non_git_dir)
        os.chdir(non_git_dir)

        # Mock find_git_root to return None (no git repo found)
        with patch("s3lfs.cli.find_git_root", return_value=None):
            # Test remove command when not in git repo
            result = subprocess.run(
                ["python", "-m", "s3lfs.cli", "remove", "test_file.txt"],
                capture_output=True,
                text=True,
                cwd=non_git_dir,
            )

            # Should fail with error message
            self.assertIn("Error: Not in a git repository", result.stdout)
            self.assertNotEqual(result.returncode, 0)

    def test_remove_command_not_in_git_repo_with_purge_flag(self):
        """Test remove command with --purge-from-s3 flag when not in a git repository."""
        # Import the necessary functions
        from unittest.mock import patch

        # Create a directory that is not a git repository
        non_git_dir = os.path.join(self.temp_dir, "non_git_dir")
        os.makedirs(non_git_dir)
        os.chdir(non_git_dir)

        # Mock find_git_root to return None (no git repo found)
        with patch("s3lfs.cli.find_git_root", return_value=None):
            # Test remove command with --purge-from-s3 flag when not in git repo
            result = subprocess.run(
                [
                    "python",
                    "-m",
                    "s3lfs.cli",
                    "remove",
                    "--purge-from-s3",
                    "test_file.txt",
                ],
                capture_output=True,
                text=True,
                cwd=non_git_dir,
            )

            # Should fail with error message
            self.assertIn("Error: Not in a git repository", result.stdout)
            self.assertNotEqual(result.returncode, 0)

    def test_cleanup_command_not_in_git_repo(self):
        """Test cleanup command when not in a git repository."""
        # Import the necessary functions
        from unittest.mock import patch

        # Create a directory that is not a git repository
        non_git_dir = os.path.join(self.temp_dir, "non_git_dir")
        os.makedirs(non_git_dir)
        os.chdir(non_git_dir)

        # Mock find_git_root to return None (no git repo found)
        with patch("s3lfs.cli.find_git_root", return_value=None):
            # Test cleanup command when not in git repo
            result = subprocess.run(
                ["python", "-m", "s3lfs.cli", "cleanup"],
                capture_output=True,
                text=True,
                cwd=non_git_dir,
            )

            # Should fail with error message
            self.assertIn("Error: Not in a git repository", result.stdout)
            self.assertNotEqual(result.returncode, 0)

    def test_cleanup_command_not_in_git_repo_with_force_flag(self):
        """Test cleanup command with --force flag when not in a git repository."""
        # Import the necessary functions
        from unittest.mock import patch

        # Create a directory that is not a git repository
        non_git_dir = os.path.join(self.temp_dir, "non_git_dir")
        os.makedirs(non_git_dir)
        os.chdir(non_git_dir)

        # Mock find_git_root to return None (no git repo found)
        with patch("s3lfs.cli.find_git_root", return_value=None):
            # Test cleanup command with --force flag when not in git repo
            result = subprocess.run(
                ["python", "-m", "s3lfs.cli", "cleanup", "--force"],
                capture_output=True,
                text=True,
                cwd=non_git_dir,
            )

            # Should fail with error message
            self.assertIn("Error: Not in a git repository", result.stdout)
            self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
