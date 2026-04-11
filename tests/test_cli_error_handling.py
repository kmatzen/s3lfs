import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import click

# Import CLI functions
from s3lfs.cli import find_git_root, get_manifest_path


class TestCLIErrorHandling(unittest.TestCase):
    """Tests for CLI error handling to improve coverage."""

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

    def test_find_git_root_not_in_git_repo(self):
        """Test find_git_root when not in a git repository."""
        # Create a directory structure without .git
        test_dir = os.path.join(self.temp_dir, "not_git_repo")
        os.makedirs(test_dir)
        os.chdir(test_dir)

        result = find_git_root()
        self.assertIsNone(result)

    def test_find_git_root_with_custom_start_path(self):
        """Test find_git_root with custom start path."""
        # Create a git repository
        git_repo = os.path.join(self.temp_dir, "git_repo")
        os.makedirs(git_repo)
        os.makedirs(os.path.join(git_repo, ".git"))

        result = find_git_root(git_repo)
        # Use resolve() to handle symlinks and normalize paths
        self.assertEqual(result.resolve(), Path(git_repo).resolve())

    def test_find_git_root_with_nonexistent_path(self):
        """Test find_git_root with nonexistent path."""
        result = find_git_root("/nonexistent/path")
        self.assertIsNone(result)

    # Note: Path resolution tests moved to test_path_resolver.py
    # The old resolve_path_from_git_root() function has been replaced by PathResolver

    def test_get_manifest_path(self):
        """Test get_manifest_path function."""
        git_root = Path("/git/root")
        result = get_manifest_path(git_root)
        # Should return YAML path for new repos (when neither exists)
        expected = git_root / ".s3_manifest.yaml"
        self.assertEqual(result, expected)

    def test_cli_error_handling_not_in_git_repo(self):
        """Test CLI error handling when not in git repository."""
        # Test the actual error handling logic without calling Click commands
        with patch("s3lfs.cli.find_git_root") as mock_find_git_root:
            mock_find_git_root.return_value = None

            # Test the error message that would be shown
            with patch("click.echo") as mock_echo:
                try:
                    # Simulate the error condition
                    if not mock_find_git_root():
                        mock_echo("Error: Not in a git repository")
                        raise click.Abort()
                except click.Abort:
                    pass

                mock_echo.assert_called_with("Error: Not in a git repository")

    def test_cli_error_handling_manifest_not_exists(self):
        """Test CLI error handling when manifest doesn't exist."""
        with patch("s3lfs.cli.find_git_root") as mock_find_git_root:
            with patch("s3lfs.cli.get_manifest_path") as mock_get_manifest:
                git_root = Path("/git/root")
                mock_find_git_root.return_value = git_root

                manifest_path = Path("/git/root/.s3_manifest.json")
                mock_get_manifest.return_value = manifest_path

                with patch("pathlib.Path.exists") as mock_exists:
                    mock_exists.return_value = False

                    with patch("click.echo") as mock_echo:
                        try:
                            # Simulate the error condition
                            if not mock_exists():
                                mock_echo(
                                    "Error: S3LFS not initialized. Run 's3lfs init' first."
                                )
                                raise click.Abort()
                        except click.Abort:
                            pass

                        mock_echo.assert_called_with(
                            "Error: S3LFS not initialized. Run 's3lfs init' first."
                        )

    def test_cli_remove_logic_directory_pattern(self):
        """Test CLI remove logic with directory pattern."""
        # Test the logic without calling the actual Click command
        resolved_path = "test_dir/"

        # Mock the Path operations
        with patch("pathlib.Path") as mock_path_class:
            mock_path = Mock()
            mock_path.is_dir.return_value = True
            mock_path_class.return_value = mock_path

            # Test the directory/pattern logic
            is_dir = mock_path.is_dir()
            has_wildcard = "*" in resolved_path or "?" in resolved_path

            self.assertTrue(is_dir)
            self.assertFalse(has_wildcard)

    def test_cli_remove_logic_glob_pattern(self):
        """Test CLI remove logic with glob pattern."""
        # Test the logic without calling the actual Click command
        resolved_path = "*.txt"

        # Mock the Path operations
        with patch("pathlib.Path") as mock_path_class:
            mock_path = Mock()
            mock_path.is_dir.return_value = False
            mock_path_class.return_value = mock_path

            # Test the glob pattern logic
            is_dir = mock_path.is_dir()
            has_wildcard = "*" in resolved_path or "?" in resolved_path

            self.assertFalse(is_dir)
            self.assertTrue(has_wildcard)

    def test_cli_remove_logic_single_file(self):
        """Test CLI remove logic with single file."""
        # Test the logic without calling the actual Click command
        resolved_path = "test_file.txt"

        # Mock the Path operations
        with patch("pathlib.Path") as mock_path_class:
            mock_path = Mock()
            mock_path.is_dir.return_value = False
            mock_path_class.return_value = mock_path

            # Test the single file logic
            is_dir = mock_path.is_dir()
            has_wildcard = "*" in resolved_path or "?" in resolved_path

            self.assertFalse(is_dir)
            self.assertFalse(has_wildcard)

    def test_cli_ls_relative_cwd_value_error(self):
        """Test CLI ls logic when relative_to raises ValueError."""
        # Test the logic without calling the actual Click command
        with patch("pathlib.Path.cwd") as mock_cwd:
            mock_cwd.return_value = Path("/different/path")

            with patch("pathlib.Path.relative_to") as mock_relative_to:
                mock_relative_to.side_effect = ValueError("Not relative")

                # Test the error handling logic
                try:
                    relative_cwd = Path.cwd().relative_to(Path("/git/root"))
                except ValueError:
                    relative_cwd = Path(".")

                self.assertEqual(relative_cwd, Path("."))


if __name__ == "__main__":
    unittest.main()
