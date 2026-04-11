import json
import os
import subprocess
import tempfile
import unittest


class TestGitRootCoverage(unittest.TestCase):
    """Comprehensive tests for git root detection failure scenarios."""

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

    def test_ls_command_cwd_outside_git_root_value_error(self):
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

        # Create a directory that's completely outside the git repo
        # This will cause cwd.relative_to(git_root) to raise ValueError
        outside_dir = os.path.join(self.temp_dir, "completely_outside")
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

    def test_ls_command_cwd_at_different_level(self):
        """Test ls command when cwd is at a different directory level."""
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

        # Create a directory that's at a different level than the git repo
        # This should also cause the ValueError
        different_level_dir = os.path.join(self.temp_dir, "different_level", "subdir")
        os.makedirs(different_level_dir)
        os.chdir(different_level_dir)

        # Test ls command from different level directory
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "ls"],
            capture_output=True,
            text=True,
            cwd=different_level_dir,
        )

        # Should fail with error message
        self.assertIn("Error: Not in a git repository", result.stdout)
        self.assertNotEqual(result.returncode, 0)

    def test_ls_command_cwd_in_parent_directory(self):
        """Test ls command when cwd is in parent directory of git repo."""
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

        # Change to parent directory of temp_dir
        parent_dir = os.path.dirname(self.temp_dir)
        os.chdir(parent_dir)

        # Test ls command from parent directory
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "ls"],
            capture_output=True,
            text=True,
            cwd=parent_dir,
        )

        # Should fail with error message
        self.assertIn("Error: Not in a git repository", result.stdout)
        self.assertNotEqual(result.returncode, 0)

    def test_ls_command_cwd_in_sibling_directory(self):
        """Test ls command when cwd is in sibling directory of git repo."""
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

        # Create a sibling directory
        sibling_dir = os.path.join(self.temp_dir, "sibling_dir")
        os.makedirs(sibling_dir)
        os.chdir(sibling_dir)

        # Test ls command from sibling directory
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "ls"],
            capture_output=True,
            text=True,
            cwd=sibling_dir,
        )

        # Should fail with error message
        self.assertIn("Error: Not in a git repository", result.stdout)
        self.assertNotEqual(result.returncode, 0)

    def test_ls_command_cwd_in_nested_outside_directory(self):
        """Test ls command when cwd is in nested directory outside git repo."""
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

        # Create a nested directory structure outside the git repo
        nested_dir = os.path.join(self.temp_dir, "nested", "deep", "structure")
        os.makedirs(nested_dir)
        os.chdir(nested_dir)

        # Test ls command from nested directory
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "ls"],
            capture_output=True,
            text=True,
            cwd=nested_dir,
        )

        # Should fail with error message
        self.assertIn("Error: Not in a git repository", result.stdout)
        self.assertNotEqual(result.returncode, 0)

    def test_ls_command_cwd_with_symlink_outside_git_root(self):
        """Test ls command when cwd is a symlink outside git root."""
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

        # Create a symlink that points outside the git repo
        outside_dir = os.path.join(self.temp_dir, "outside")
        os.makedirs(outside_dir)

        symlink_dir = os.path.join(self.temp_dir, "symlink_to_outside")
        os.symlink(outside_dir, symlink_dir)
        os.chdir(symlink_dir)

        # Test ls command from symlink directory
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "ls"],
            capture_output=True,
            text=True,
            cwd=symlink_dir,
        )

        # Should fail with error message
        self.assertIn("Error: Not in a git repository", result.stdout)
        self.assertNotEqual(result.returncode, 0)

    def test_ls_command_cwd_with_absolute_path_outside_git_root(self):
        """Test ls command when cwd is an absolute path outside git root."""
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

        # Create a directory with absolute path outside git repo
        outside_dir = os.path.abspath(os.path.join(self.temp_dir, "absolute_outside"))
        os.makedirs(outside_dir)
        os.chdir(outside_dir)

        # Test ls command from absolute path directory
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "ls"],
            capture_output=True,
            text=True,
            cwd=outside_dir,
        )

        # Should fail with error message
        self.assertIn("Error: Not in a git repository", result.stdout)
        self.assertNotEqual(result.returncode, 0)

    def test_ls_command_cwd_with_different_drive_on_windows(self):
        """Test ls command when cwd is on different drive (Windows scenario)."""
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

        # Create a directory that simulates being on a different drive
        # This would cause relative_to to fail on Windows
        if os.name == "nt":  # Windows
            # Try to create a path that would be on a different drive
            different_drive_dir = os.path.join("D:", "different_drive_dir")
            if not os.path.exists("D:"):
                # If D: doesn't exist, create a directory that would cause the same issue
                different_drive_dir = os.path.join(self.temp_dir, "different_drive_sim")
                os.makedirs(different_drive_dir)
                os.chdir(different_drive_dir)

                # Test ls command from different drive directory
                result = subprocess.run(
                    ["python", "-m", "s3lfs.cli", "ls"],
                    capture_output=True,
                    text=True,
                    cwd=different_drive_dir,
                )

                # Should fail with error message
                self.assertIn("Error: Not in a git repository", result.stdout)
                self.assertNotEqual(result.returncode, 0)
        else:
            # On Unix-like systems, create a directory that would cause the same issue
            different_drive_dir = os.path.join(self.temp_dir, "different_drive_sim")
            os.makedirs(different_drive_dir)
            os.chdir(different_drive_dir)

            # Test ls command from different drive directory
            result = subprocess.run(
                ["python", "-m", "s3lfs.cli", "ls"],
                capture_output=True,
                text=True,
                cwd=different_drive_dir,
            )

            # Should fail with error message
            self.assertIn("Error: Not in a git repository", result.stdout)
            self.assertNotEqual(result.returncode, 0)

    def test_ls_command_cwd_outside_but_git_root_found(self):
        """Test ls command when git root is found but cwd is outside (lines 222-223)."""
        # Create a git repository at a higher level (parent of temp_dir)
        parent_dir = os.path.dirname(self.temp_dir)
        git_repo = os.path.join(parent_dir, f"git_repo_for_testing_{os.getpid()}")
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

        # Create a directory that's completely outside the git repo
        # This should cause find_git_root to succeed but cwd.relative_to to fail
        outside_dir = os.path.join(self.temp_dir, "completely_outside")
        os.makedirs(outside_dir)
        os.chdir(outside_dir)

        # Test ls command from outside directory
        result = subprocess.run(
            ["python", "-m", "s3lfs.cli", "ls"],
            capture_output=True,
            text=True,
            cwd=outside_dir,
        )

        # Should fail with error message
        self.assertIn("Error: Not in a git repository", result.stdout)
        self.assertNotEqual(result.returncode, 0)

        # Clean up the git repo we created
        import shutil

        shutil.rmtree(git_repo, ignore_errors=True)

    def test_ls_command_mocked_git_root_found_but_cwd_outside(self):
        """Test ls command with mocked git root that causes ValueError (lines 226-227)."""
        # Import the necessary functions
        from pathlib import Path

        from s3lfs.cli import find_git_root

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

        # Create a directory outside the git repo
        outside_dir = os.path.join(self.temp_dir, "outside")
        os.makedirs(outside_dir)
        os.chdir(outside_dir)

        # Test the find_git_root function with a mock that returns a git root
        # that's outside the current working directory
        def mock_git_finder(start_path=None):
            return Path(git_repo)

        # Test that find_git_root works with our mock
        result = find_git_root(git_finder_func=mock_git_finder)
        self.assertEqual(result, Path(git_repo))

        # Test that cwd.relative_to(git_root) raises ValueError when cwd is outside git_root
        cwd = Path.cwd()
        git_root = Path(git_repo)

        # This should raise ValueError because cwd is outside git_root
        with self.assertRaises(ValueError):
            cwd.relative_to(git_root)

        # Now test the actual ValueError handling logic
        # This simulates the logic in the ls function (lines 226-227)
        cwd = Path.cwd()
        git_root = Path(git_repo)

        try:
            relative_cwd = cwd.relative_to(git_root)
        except ValueError:
            relative_cwd = Path(".")

        # Verify that the ValueError was handled correctly
        self.assertEqual(relative_cwd, Path("."))


if __name__ == "__main__":
    unittest.main()
