"""
Tests for the PathResolver class.

These tests verify that path resolution works correctly across all scenarios:
- Different working directories
- Relative and absolute paths
- Subdirectory contexts
- Edge cases and error conditions
"""

import os
import tempfile
import unittest
from pathlib import Path

from s3lfs.path_resolver import PathResolver


class TestPathResolver(unittest.TestCase):
    """Test the PathResolver class."""

    def setUp(self):
        """Set up test environment with a mock git repository."""
        self.temp_dir = tempfile.mkdtemp()
        self.git_root = Path(self.temp_dir) / "repo"
        self.git_root.mkdir()

        # Create subdirectory structure
        self.subdir = self.git_root / "subdir"
        self.subdir.mkdir()

        self.nested_subdir = self.subdir / "nested"
        self.nested_subdir.mkdir()

        self.resolver = PathResolver(self.git_root)

    def tearDown(self):
        """Clean up test environment."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    # Test to_manifest_key

    def test_to_manifest_key_absolute_path(self):
        """Test converting absolute path to manifest key."""
        abs_path = self.git_root / "data" / "file.txt"
        result = self.resolver.to_manifest_key(abs_path)
        self.assertEqual(result, "data/file.txt")

    def test_to_manifest_key_relative_path(self):
        """Test converting relative path to manifest key."""
        result = self.resolver.to_manifest_key("data/file.txt")
        self.assertEqual(result, "data/file.txt")

    def test_to_manifest_key_path_object(self):
        """Test converting Path object to manifest key."""
        path = Path("data") / "file.txt"
        result = self.resolver.to_manifest_key(path)
        self.assertEqual(result, "data/file.txt")

    def test_to_manifest_key_outside_repo_raises(self):
        """Test that paths outside repo raise ValueError."""
        outside_path = Path(self.temp_dir) / "outside" / "file.txt"
        with self.assertRaises(ValueError) as ctx:
            self.resolver.to_manifest_key(outside_path)
        self.assertIn("outside git repository", str(ctx.exception))

    def test_to_manifest_key_uses_posix_format(self):
        """Test that manifest keys use POSIX separators."""
        result = self.resolver.to_manifest_key("data/subdir/file.txt")
        self.assertNotIn("\\", result)
        self.assertIn("/", result)

    # Test to_filesystem_path

    def test_to_filesystem_path_simple(self):
        """Test converting manifest key to filesystem path."""
        result = self.resolver.to_filesystem_path("data/file.txt")
        expected = (self.git_root / "data" / "file.txt").resolve()
        self.assertEqual(result, expected)

    def test_to_filesystem_path_returns_absolute(self):
        """Test that filesystem paths are always absolute."""
        result = self.resolver.to_filesystem_path("file.txt")
        self.assertTrue(result.is_absolute())

    def test_to_filesystem_path_rejects_absolute(self):
        """Test that absolute paths are rejected as manifest keys."""
        with self.assertRaises(ValueError) as ctx:
            self.resolver.to_filesystem_path("/absolute/path")
        self.assertIn("must be relative", str(ctx.exception))

    def test_to_filesystem_path_rejects_escape(self):
        """Test that paths escaping git root are rejected."""
        with self.assertRaises(ValueError) as ctx:
            self.resolver.to_filesystem_path("../outside")
        self.assertIn("cannot escape", str(ctx.exception))

    # Test from_cli_input

    def test_from_cli_input_at_git_root(self):
        """Test CLI input when at git root."""
        result = self.resolver.from_cli_input("file.txt", cwd=self.git_root)
        self.assertEqual(result, "file.txt")

    def test_from_cli_input_in_subdirectory_relative(self):
        """Test CLI input when in subdirectory with relative path."""
        result = self.resolver.from_cli_input("file.txt", cwd=self.subdir)
        self.assertEqual(result, "subdir/file.txt")

    def test_from_cli_input_in_subdirectory_with_prefix(self):
        """Test CLI input when path already includes subdirectory prefix."""
        result = self.resolver.from_cli_input("subdir/file.txt", cwd=self.subdir)
        self.assertEqual(result, "subdir/file.txt")

    def test_from_cli_input_nested_subdirectory(self):
        """Test CLI input from nested subdirectory."""
        result = self.resolver.from_cli_input("file.txt", cwd=self.nested_subdir)
        self.assertEqual(result, "subdir/nested/file.txt")

    def test_from_cli_input_absolute_path_allowed(self):
        """Test CLI input with absolute path when allowed."""
        abs_path = str(self.git_root / "data" / "file.txt")
        result = self.resolver.from_cli_input(
            abs_path, cwd=self.subdir, allow_absolute=True
        )
        self.assertEqual(result, "data/file.txt")

    def test_from_cli_input_absolute_path_not_allowed(self):
        """Test CLI input with absolute path when not allowed."""
        abs_path = str(self.git_root / "data" / "file.txt")
        with self.assertRaises(ValueError) as ctx:
            self.resolver.from_cli_input(
                abs_path, cwd=self.subdir, allow_absolute=False
            )
        self.assertIn("Absolute paths not allowed", str(ctx.exception))

    def test_from_cli_input_uses_current_cwd_by_default(self):
        """Test that from_cli_input uses current CWD when not specified."""
        original_cwd = os.getcwd()
        try:
            os.chdir(self.subdir)
            result = self.resolver.from_cli_input("file.txt")
            self.assertEqual(result, "subdir/file.txt")
        finally:
            os.chdir(original_cwd)

    def test_from_cli_input_parent_directory_reference(self):
        """Test CLI input with parent directory reference."""
        # From nested subdir, reference file in parent subdir
        result = self.resolver.from_cli_input("../file.txt", cwd=self.nested_subdir)
        self.assertEqual(result, "subdir/file.txt")

    def test_from_cli_input_cwd_outside_repo(self):
        """Test CLI input when CWD is outside the git repository."""
        outside_dir = Path(self.temp_dir) / "outside"
        outside_dir.mkdir()
        # When CWD is outside repo, path should be treated as relative to git root
        result = self.resolver.from_cli_input("file.txt", cwd=outside_dir)
        self.assertEqual(result, "file.txt")

    # Test validate_manifest_key

    def test_validate_manifest_key_valid(self):
        """Test validation of valid manifest keys."""
        self.assertTrue(self.resolver.validate_manifest_key("file.txt"))
        self.assertTrue(self.resolver.validate_manifest_key("data/file.txt"))
        self.assertTrue(self.resolver.validate_manifest_key("a/b/c/file.txt"))

    def test_validate_manifest_key_empty(self):
        """Test validation rejects empty string."""
        self.assertFalse(self.resolver.validate_manifest_key(""))

    def test_validate_manifest_key_absolute(self):
        """Test validation rejects absolute paths."""
        self.assertFalse(self.resolver.validate_manifest_key("/absolute/path"))

    def test_validate_manifest_key_escape(self):
        """Test validation rejects paths that escape."""
        self.assertFalse(self.resolver.validate_manifest_key("../escape"))
        self.assertFalse(self.resolver.validate_manifest_key("data/../escape"))

    def test_validate_manifest_key_backslash(self):
        """Test validation rejects Windows-style paths."""
        self.assertFalse(self.resolver.validate_manifest_key("data\\file.txt"))

    def test_validate_manifest_key_leading_slash(self):
        """Test validation rejects leading slash."""
        self.assertFalse(self.resolver.validate_manifest_key("/data/file.txt"))

    def test_validate_manifest_key_trailing_slash(self):
        """Test validation rejects trailing slash."""
        self.assertFalse(self.resolver.validate_manifest_key("data/"))

    # Test is_within_repo

    def test_is_within_repo_relative_path(self):
        """Test checking if relative path is within repo."""
        self.assertTrue(self.resolver.is_within_repo("data/file.txt"))

    def test_is_within_repo_absolute_path(self):
        """Test checking if absolute path is within repo."""
        path = self.git_root / "data" / "file.txt"
        self.assertTrue(self.resolver.is_within_repo(path))

    def test_is_within_repo_outside_path(self):
        """Test checking if path outside repo returns False."""
        outside = Path(self.temp_dir) / "outside" / "file.txt"
        self.assertFalse(self.resolver.is_within_repo(outside))

    # Test get_relative_cwd

    def test_get_relative_cwd_at_root(self):
        """Test getting relative CWD when at git root."""
        result = self.resolver.get_relative_cwd(self.git_root)
        self.assertEqual(result, Path("."))

    def test_get_relative_cwd_in_subdirectory(self):
        """Test getting relative CWD when in subdirectory."""
        result = self.resolver.get_relative_cwd(self.subdir)
        self.assertEqual(result, Path("subdir"))

    def test_get_relative_cwd_outside_repo(self):
        """Test getting relative CWD when outside repo."""
        outside = Path(self.temp_dir) / "outside"
        result = self.resolver.get_relative_cwd(outside)
        self.assertEqual(result, Path("."))

    def test_get_relative_cwd_uses_current_by_default(self):
        """Test that get_relative_cwd uses current CWD by default."""
        original_cwd = os.getcwd()
        try:
            os.chdir(self.subdir)
            result = self.resolver.get_relative_cwd()
            self.assertEqual(result, Path("subdir"))
        finally:
            os.chdir(original_cwd)

    # Test round-trip conversions

    def test_roundtrip_manifest_to_filesystem_to_manifest(self):
        """Test that manifest key -> filesystem -> manifest key is identity."""
        original = "data/subdir/file.txt"
        filesystem = self.resolver.to_filesystem_path(original)
        result = self.resolver.to_manifest_key(filesystem)
        self.assertEqual(result, original)

    def test_roundtrip_filesystem_to_manifest_to_filesystem(self):
        """Test that filesystem -> manifest -> filesystem is identity."""
        original = (self.git_root / "data" / "file.txt").resolve()
        manifest = self.resolver.to_manifest_key(original)
        result = self.resolver.to_filesystem_path(manifest)
        self.assertEqual(result, original)

    # Test edge cases

    def test_git_root_must_be_absolute(self):
        """Test that PathResolver requires absolute git root."""
        with self.assertRaises(ValueError) as ctx:
            PathResolver(Path("relative/path"))
        self.assertIn("must be absolute", str(ctx.exception))

    def test_repr(self):
        """Test string representation."""
        repr_str = repr(self.resolver)
        self.assertIn("PathResolver", repr_str)
        self.assertIn(str(self.git_root), repr_str)


class TestPathResolverRealWorldScenarios(unittest.TestCase):
    """Test PathResolver with real-world scenarios."""

    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.git_root = Path(self.temp_dir) / "GoProCaptures"
        self.git_root.mkdir()

        self.gopro_processed = self.git_root / "GoProProcessed"
        self.gopro_processed.mkdir()

        self.resolver = PathResolver(self.git_root)

    def tearDown(self):
        """Clean up test environment."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_gopro_bug_scenario_relative_filename(self):
        """
        Test the exact scenario from the bug report.

        User in: /repo/GoProProcessed
        Command: s3lfs checkout capture091
        Expected: File at /repo/GoProProcessed/capture091
        """
        result = self.resolver.from_cli_input(
            "capture091.txt", cwd=self.gopro_processed
        )
        self.assertEqual(result, "GoProProcessed/capture091.txt")

        # Verify filesystem path is correct
        fs_path = self.resolver.to_filesystem_path(result)
        expected = (self.gopro_processed / "capture091.txt").resolve()
        self.assertEqual(fs_path, expected)

    def test_gopro_bug_scenario_full_path(self):
        """
        Test providing full path from subdirectory.

        User in: /repo/GoProProcessed
        Command: s3lfs checkout GoProProcessed/capture091
        Expected: File at /repo/GoProProcessed/capture091 (no duplication)
        """
        result = self.resolver.from_cli_input(
            "GoProProcessed/capture091.txt", cwd=self.gopro_processed
        )
        self.assertEqual(result, "GoProProcessed/capture091.txt")

        # Verify filesystem path is correct (no duplication)
        fs_path = self.resolver.to_filesystem_path(result)
        expected = (self.gopro_processed / "capture091.txt").resolve()
        self.assertEqual(fs_path, expected)


if __name__ == "__main__":
    unittest.main()
