import json
import os
import subprocess
import tempfile
import unittest


class TestLSCommandCoverage(unittest.TestCase):
    """Comprehensive tests for the ls command to cover missing lines."""

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

    def test_ls_command_not_in_git_repo(self):
        """Test ls command when not in git repository (lines 208-211)."""
        # Create a directory that's not a git repository
        non_git_dir = os.path.join(self.temp_dir, "non_git_dir")
        os.makedirs(non_git_dir)
        os.chdir(non_git_dir)

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

    def test_ls_command_manifest_not_exists(self):
        """Test ls command when manifest doesn't exist (lines 215-216)."""
        # Create a git repository
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Don't create manifest file - it shouldn't exist
        # Test ls command when manifest doesn't exist
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

    def test_ls_command_cwd_outside_git_root(self):
        """Test ls command when cwd is outside git root (lines 222-223)."""
        # Create a git repository
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))

        # Create manifest file
        manifest_file = os.path.join(git_repo, ".s3_manifest.json")
        manifest = {
            "bucket_name": "testbucket",
            "repo_prefix": "testprefix",
            "files": {"test_file.txt": "test_hash"},
        }
        with open(manifest_file, "w") as f:
            json.dump(manifest, f)

        # Change to a directory outside the git repo
        outside_dir = os.path.join(self.temp_dir, "outside_dir")
        os.makedirs(outside_dir)
        os.chdir(outside_dir)

        # Test ls command from outside git repo
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "ls"],
            capture_output=True,
            text=True,
            cwd=outside_dir,
        )

        # Should fail with error message
        self.assertIn("Error: Not in a git repository", result.stdout)
        self.assertNotEqual(result.returncode, 0)

    def test_ls_command_with_all_flag(self):
        """Test ls command with --all flag (lines 230-235)."""
        # Create a git repository
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

    def test_ls_command_with_specific_path(self):
        """Test ls command with specific path (lines 236-239)."""
        # Create a git repository
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

        # Test ls command with specific path
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "ls", "test_file.txt"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should succeed
        self.assertEqual(result.returncode, 0)

    def test_ls_command_with_verbose_flag(self):
        """Test ls command with --verbose flag."""
        # Create a git repository
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

        # Test ls command with --verbose flag
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "ls", "--verbose"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should succeed
        self.assertEqual(result.returncode, 0)

    def test_ls_command_with_no_sign_request(self):
        """Test ls command with --no-sign-request flag."""
        # Create a git repository
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

        # Test ls command with --no-sign-request flag
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "ls", "--no-sign-request"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should succeed
        self.assertEqual(result.returncode, 0)

    def test_ls_command_with_all_flags(self):
        """Test ls command with multiple flags."""
        # Create a git repository
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

        # Test ls command with multiple flags
        result = subprocess.run(
            [
                "python",
                "-m",
                "s3lfs.cli",
                "ls",
                "--all",
                "--verbose",
                "--no-sign-request",
            ],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should succeed
        self.assertEqual(result.returncode, 0)

    def test_ls_command_with_path_and_all(self):
        """Test ls command with both path and --all flag (lines 172-173)."""
        # Create a git repository
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

        # Test ls command with both path and --all flag
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "ls", "test_file.txt", "--all"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should succeed (--all takes precedence)
        self.assertEqual(result.returncode, 0)

    def test_ls_command_with_relative_cwd(self):
        """Test ls command with relative current working directory."""
        # Create a git repository
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Create a subdirectory
        subdir = os.path.join(git_repo, "subdir")
        os.makedirs(subdir)
        os.chdir(subdir)

        # Create manifest file
        manifest_file = os.path.join(git_repo, ".s3_manifest.json")
        manifest = {
            "bucket_name": "testbucket",
            "repo_prefix": "testprefix",
            "files": {"subdir/test_file.txt": "test_hash"},
        }
        with open(manifest_file, "w") as f:
            json.dump(manifest, f)

        # Test ls command from subdirectory
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "ls"],
            capture_output=True,
            text=True,
            cwd=subdir,
        )

        # Should succeed
        self.assertEqual(result.returncode, 0)

    def test_ls_command_with_strip_prefix(self):
        """Test ls command with strip_prefix functionality."""
        # Create a git repository
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Create a subdirectory
        subdir = os.path.join(git_repo, "subdir")
        os.makedirs(subdir)
        os.chdir(subdir)

        # Create manifest file
        manifest_file = os.path.join(git_repo, ".s3_manifest.json")
        manifest = {
            "bucket_name": "testbucket",
            "repo_prefix": "testprefix",
            "files": {"subdir/test_file.txt": "test_hash"},
        }
        with open(manifest_file, "w") as f:
            json.dump(manifest, f)

        # Test ls command with specific path from subdirectory
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "ls", "test_file.txt"],
            capture_output=True,
            text=True,
            cwd=subdir,
        )

        # Should succeed
        self.assertEqual(result.returncode, 0)

    def test_ls_command_empty_manifest(self):
        """Test ls command with empty manifest."""
        # Create a git repository
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))
        os.chdir(git_repo)

        # Create manifest file with no files
        manifest_file = os.path.join(git_repo, ".s3_manifest.json")
        manifest = {
            "bucket_name": "testbucket",
            "repo_prefix": "testprefix",
            "files": {},
        }
        with open(manifest_file, "w") as f:
            json.dump(manifest, f)

        # Test ls command with empty manifest
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "ls"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should succeed
        self.assertEqual(result.returncode, 0)

    def test_ls_command_with_nonexistent_path(self):
        """Test ls command with nonexistent path."""
        # Create a git repository
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

        # Test ls command with nonexistent path
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "ls", "nonexistent_file.txt"],
            capture_output=True,
            text=True,
            cwd=git_repo,
        )

        # Should succeed (no files found)
        self.assertEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
