#!/usr/bin/env python3
"""
Error handling, edge cases, and platform-specific functionality tests for S3LFS.

This test suite focuses on:
- Error handling and exception paths
- Platform-specific CLI method selection
- Cache management edge cases
- GitIgnore file management
- Auto-selection logic for different methods
- File handling edge cases (empty files, missing files, etc.)

These tests complement the main test suite by covering code paths that are
difficult to test in normal usage scenarios.
"""

import os
import shutil
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import mock_open, patch

import boto3
from moto import mock_s3

from s3lfs.core import S3LFS


@mock_s3
class TestS3LFSErrorHandlingAndEdgeCases(unittest.TestCase):
    def setUp(self):
        self.s3_mock = mock_s3()
        self.s3_mock.start()

        self.bucket_name = "test-coverage-bucket"
        self.s3 = boto3.client("s3", region_name="us-east-1")
        self.s3.create_bucket(Bucket=self.bucket_name)

        # Create test directory in a temporary location to avoid polluting git root
        self.test_dir = Path(tempfile.mkdtemp(prefix="s3lfs_test_"))

        # Create test file in the test directory
        self.test_file = self.test_dir / "test_file.txt"
        with open(self.test_file, "w") as f:
            f.write("test content")

        # Create S3LFS instance with test directory as base
        # Use a temporary manifest file in the test directory
        self.manifest_file = self.test_dir / ".s3_manifest.json"
        self.versioner = S3LFS(
            bucket_name=self.bucket_name,
            manifest_file=str(self.manifest_file),
            temp_dir=str(self.test_dir / ".s3lfs_temp"),
        )

        # Store original .gitignore content for restoration
        self.original_gitignore_content = None
        self.gitignore_existed = False
        gitignore_path = Path(".gitignore")
        if gitignore_path.exists():
            self.gitignore_existed = True
            with open(gitignore_path, "r") as f:
                self.original_gitignore_content = f.read()

    def tearDown(self):
        self.s3_mock.stop()

        # Clean up test directory completely
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

        # Restore original .gitignore state
        gitignore_path = Path(".gitignore")
        if self.gitignore_existed and self.original_gitignore_content is not None:
            # Restore original content
            with open(gitignore_path, "w") as f:
                f.write(self.original_gitignore_content)
        elif not self.gitignore_existed and gitignore_path.exists():
            # Remove .gitignore if it didn't exist before
            gitignore_path.unlink()

        # Clean up any other test artifacts that might have been created in git root
        test_artifacts = [
            "third_file.txt",
            "fourth_file.txt",
            "error_checkout_test_0.txt",
            "interrupt_checkout_test_0.txt",
            "shutdown_checkout_test_0.txt",
            "cli_test_file.txt.gz",
            ".s3_manifest.json",
            ".s3_manifest_cache.json",
            ".s3_manifest.tmp",
            ".s3_manifest_cache.tmp",
        ]

        for artifact in test_artifacts:
            artifact_path = Path(artifact)
            if artifact_path.exists():
                if artifact_path.is_file():
                    artifact_path.unlink()
                elif artifact_path.is_dir():
                    shutil.rmtree(artifact_path)

        # Clean up test directories that might have been created in git root
        test_dirs = ["test_dir", "test_coverage_data"]
        for test_dir in test_dirs:
            test_dir_path = Path(test_dir)
            if test_dir_path.exists() and test_dir_path.is_dir():
                shutil.rmtree(test_dir_path)

    # Error Handling Tests
    def test_save_manifest_error_handling(self):
        """Test save_manifest raises on write failure and cleans up its temp file."""
        with patch("json.dump", side_effect=Exception("JSON error")):
            with self.assertRaises(RuntimeError) as ctx:
                self.versioner.save_manifest()
        self.assertIn("Failed to save manifest: JSON error", str(ctx.exception))
        leftovers = list(
            self.versioner.manifest_file.parent.glob(
                self.versioner.manifest_file.name + ".*.tmp"
            )
        )
        self.assertEqual(leftovers, [], "temp file was not cleaned up")

    def test_save_manifest_replace_failure_raises(self):
        """A failed atomic replace (e.g. disk full) must propagate: printing a
        warning and returning leaves S3 and the index out of sync with exit
        code 0, which is a data-loss path."""
        self.versioner.load_manifest()
        with patch.object(
            Path, "replace", side_effect=OSError(28, "No space left on device")
        ):
            with self.assertRaises(RuntimeError) as ctx:
                self.versioner.save_manifest()
        self.assertIn("Failed to save manifest", str(ctx.exception))

    def test_save_cache_error_handling(self):
        """Test save_cache error handling and cleanup."""
        self.versioner._cache_dirty = True
        with patch("json.dump", side_effect=Exception("Cache error")):
            with patch("builtins.print") as mock_print:
                self.versioner.save_cache()
                mock_print.assert_any_call("Failed to save cache: Cache error")

    def test_load_cache_json_decode_error(self):
        """Test load_cache with corrupted JSON file."""
        with open(self.versioner.cache_file, "w") as f:
            f.write("invalid json content {")

        with patch("builtins.print") as mock_print:
            self.versioner.load_cache()
            self.assertEqual(self.versioner.hash_cache, {})
            calls = [str(call_args) for call_args in mock_print.call_args_list]
            warning_calls = [
                call for call in calls if "Warning: Failed to load cache file" in call
            ]
            self.assertTrue(len(warning_calls) > 0)

    def test_load_cache_io_error(self):
        """Test load_cache with IO error."""
        with patch("builtins.open", side_effect=IOError("Permission denied")):
            self.versioner.load_cache()
            self.assertEqual(self.versioner.hash_cache, {})

    # CLI Hashing Method Tests
    @patch("sys.platform", "linux")
    @patch("shutil.which")
    def test_hash_file_cli_method_linux(self, mock_which):
        """Test CLI hashing method on Linux."""
        mock_which.return_value = "/usr/bin/sha256sum"

        with patch("subprocess.run") as mock_run:
            # A real-shaped digest: output that does not parse as one now
            # falls back to Python hashing rather than becoming an S3 key.
            mock_run.return_value.stdout = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa /path/to/file\n"

            result = self.versioner.hash_file(self.test_file, method="cli")
            self.assertEqual(
                result,
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )

            mock_run.assert_called_once()
            args = mock_run.call_args[0][0]
            self.assertEqual(args[0], "sha256sum")

    # Compression CLI Method Tests
    @patch("sys.platform", "linux")
    @patch("shutil.which")
    def test_compress_file_cli_method(self, mock_which):
        """Test compression using CLI method."""
        mock_which.return_value = "/usr/bin/gzip"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = None

            result = self.versioner.compress_file(self.test_file, method="cli")

            self.assertTrue(str(result).startswith(str(self.versioner.temp_dir)))
            self.assertTrue(str(result).endswith(".gz"))

            mock_run.assert_called_once()
            args = mock_run.call_args[0][0]
            self.assertEqual(args[:4], ["gzip", "-n", "-c", "-5"])

    @patch("sys.platform", "linux")
    @patch("shutil.which")
    def test_decompress_file_cli_method_error(self, mock_which):
        """Test decompression CLI method error handling.

        The unreachable `returncode != 0` branch behind `check=True` was
        removed; the real contract is that the CLI's CalledProcessError
        propagates to the caller.
        """
        mock_which.return_value = "/usr/bin/gzip"
        compressed_file = self.test_dir / "test.gz"
        compressed_file.touch()

        try:
            with patch("subprocess.run") as mock_run:
                mock_run.side_effect = subprocess.CalledProcessError(1, ["gzip"])

                with patch("builtins.open", mock_open()):
                    with self.assertRaises(subprocess.CalledProcessError):
                        self.versioner.decompress_file(compressed_file, method="cli")
        finally:
            if compressed_file.exists():
                compressed_file.unlink()

    # GitIgnore Tests
    def test_initialize_repo_does_not_write_gitignore(self):
        """The unmanaged .gitignore writer was removed; `s3lfs init` now
        writes anchored exclusions through the managed block in the CLI, so
        initialize_repo() must not create or modify .gitignore in the cwd."""
        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                self.assertFalse(Path(".gitignore").exists())

                self.versioner.initialize_repo()

                self.assertFalse(Path(".gitignore").exists())
            finally:
                os.chdir(original_cwd)

    def test_update_gitignore_is_removed(self):
        """The old writer appended unanchored `*_cache.yaml` / `*_cache.json`
        patterns matching repo-wide; it must be gone, not shadowed."""
        self.assertFalse(hasattr(self.versioner, "_update_gitignore"))

    # Enhanced Cache Management Tests
    def test_cleanup_stale_cache_no_stale_entries(self):
        """Test cleanup_stale_cache when no entries are stale."""
        # Create a real file for the fresh entry
        fresh_file = self.test_dir / "fresh_file.txt"
        fresh_file.write_text("fresh content")

        # Add a fresh cache entry with the actual file path
        fresh_file_path = str(fresh_file)
        self.versioner.hash_cache[fresh_file_path] = {
            "hash": "fresh_hash",
            "metadata": {"size": 100, "mtime": 123456789},
            "timestamp": time.time(),  # Fresh timestamp
        }

        # Save the cache to ensure it's persisted
        self.versioner.save_cache()

        initial_count = len(self.versioner.hash_cache)

        with patch("builtins.print") as mock_print:
            self.versioner.cleanup_stale_cache(max_age_days=30)

            # Should have same number of entries since file exists and is fresh
            final_count = len(self.versioner.hash_cache)

            self.assertEqual(final_count, initial_count)

            # Should not print cleanup message since no stale entries
            calls = [str(call_args) for call_args in mock_print.call_args_list]
            cleanup_calls = [call for call in calls if "Cleaned up" in call]
            self.assertEqual(len(cleanup_calls), 0)

    def test_cleanup_stale_cache_mixed_entries(self):
        """Test cleanup_stale_cache with mix of fresh, old, and missing file entries."""
        import time

        # Create a real file for fresh entry
        fresh_file = self.test_dir / "fresh_file.txt"
        fresh_file.write_text("fresh content")

        # Create a real file for old entry (exists but old in cache)
        old_file = self.test_dir / "old_existing_file.txt"
        old_file.write_text("old content")

        # Add mixed cache entries
        current_time = time.time()
        self.versioner.hash_cache[str(fresh_file)] = {
            "hash": "fresh_hash",
            "metadata": {"size": 100, "mtime": 123456789},
            "timestamp": current_time,  # Fresh
        }

        # Missing file entry (file doesn't exist)
        missing_file_path = str(self.test_dir / "missing_file.txt")
        self.versioner.hash_cache[missing_file_path] = {
            "hash": "missing_hash",
            "metadata": {"size": 200, "mtime": 123456789},
            "timestamp": current_time,  # Fresh but file doesn't exist
        }

        # Old file entry (file exists but cache is old)
        self.versioner.hash_cache[str(old_file)] = {
            "hash": "old_hash",
            "metadata": {"size": 300, "mtime": 123456789},
            "timestamp": current_time - (35 * 24 * 60 * 60),  # 35 days old
        }

        # Save the cache to ensure it's persisted
        self.versioner.save_cache()

        initial_count = len(self.versioner.hash_cache)
        self.assertEqual(initial_count, 3)

        with patch("builtins.print") as mock_print:
            self.versioner.cleanup_stale_cache(max_age_days=30)

            # Should keep fresh existing file, remove missing file and old file
            final_count = len(self.versioner.hash_cache)
            self.assertEqual(final_count, 1)
            self.assertIn(str(fresh_file), self.versioner.hash_cache)
            self.assertNotIn(missing_file_path, self.versioner.hash_cache)
            self.assertNotIn(str(old_file), self.versioner.hash_cache)

            # Should print cleanup message
            calls = [str(call_args) for call_args in mock_print.call_args_list]
            cleanup_calls = [
                call for call in calls if "Cleaned up 2 stale cache entries" in call
            ]
            self.assertTrue(len(cleanup_calls) > 0)

    def test_cleanup_stale_cache_missing_timestamp(self):
        """Test cleanup_stale_cache with entries missing timestamp."""
        # Add cache entry without timestamp (should default to 0)
        self.versioner.hash_cache["no_timestamp_file.txt"] = {
            "hash": "no_timestamp_hash",
            "metadata": {"size": 100, "mtime": 123456789},
            # No timestamp field
        }

        initial_count = len(self.versioner.hash_cache)

        self.versioner.cleanup_stale_cache(max_age_days=30)

        # Entry without timestamp should be removed (defaults to 0, very old)
        final_count = len(self.versioner.hash_cache)
        self.assertLess(final_count, initial_count)
        self.assertNotIn("no_timestamp_file.txt", self.versioner.hash_cache)

    # Tests with Cache Disabled
    def test_track_with_cache_disabled(self):
        """Test track method with use_cache=False."""
        test_file = self.test_dir / "cache_disabled_track.txt"
        test_file.write_text("content for cache disabled test")

        # Track with cache disabled
        self.versioner.track(str(test_file), use_cache=False, interleaved=False)

        # File should be in manifest (as relative path)
        manifest_key = self.versioner.path_resolver.to_manifest_key(test_file)
        self.assertIn(manifest_key, self.versioner.manifest["files"])

        # Cache should be empty since we didn't use it
        self.assertEqual(len(self.versioner.hash_cache), 0)

    def test_track_interleaved_with_cache_disabled(self):
        """Test track_interleaved method with use_cache=False."""
        test_file = self.test_dir / "cache_disabled_interleaved.txt"
        test_file.write_text("content for interleaved cache disabled test")

        # Track with cache disabled
        self.versioner.track_interleaved(str(test_file), use_cache=False)

        # File should be in manifest (as relative path)
        manifest_key = self.versioner.path_resolver.to_manifest_key(test_file)
        self.assertIn(manifest_key, self.versioner.manifest["files"])

        # Cache should be empty since we didn't use it
        self.assertEqual(len(self.versioner.hash_cache), 0)

    def test_checkout_with_cache_disabled(self):
        """Test checkout method with use_cache=False."""
        test_file = self.test_dir / "cache_disabled_checkout.txt"
        test_file.write_text("content for checkout cache disabled test")

        # First track the file
        self.versioner.track(str(test_file), use_cache=False)

        # Remove the local file
        test_file.unlink()
        self.assertFalse(test_file.exists())

        # Checkout with cache disabled
        self.versioner.checkout(str(test_file), use_cache=False, interleaved=False)

        # File should be restored
        self.assertTrue(test_file.exists())
        self.assertEqual(
            test_file.read_text(), "content for checkout cache disabled test"
        )

        # Cache should still be empty
        self.assertEqual(len(self.versioner.hash_cache), 0)

    def test_checkout_interleaved_with_cache_disabled(self):
        """Test checkout_interleaved method with use_cache=False."""
        test_file = self.test_dir / "cache_disabled_checkout_interleaved.txt"
        test_file.write_text("content for interleaved checkout cache disabled test")

        # First track the file
        self.versioner.track_interleaved(str(test_file), use_cache=False)

        # Remove the local file
        test_file.unlink()
        self.assertFalse(test_file.exists())

        # Checkout with cache disabled
        self.versioner.checkout_interleaved(str(test_file), use_cache=False)

        # File should be restored
        self.assertTrue(test_file.exists())
        self.assertEqual(
            test_file.read_text(),
            "content for interleaved checkout cache disabled test",
        )

        # Cache should still be empty
        self.assertEqual(len(self.versioner.hash_cache), 0)

    def test_hash_and_upload_worker_with_cache_disabled(self):
        """Test _hash_and_upload_worker with use_cache=False."""
        test_file = self.test_dir / "worker_cache_disabled.txt"
        test_file.write_text("content for worker cache disabled test")

        # Test the worker function directly with cache disabled
        result = self.versioner._hash_and_upload_worker(
            str(test_file), silence=True, progress_callback=None, use_cache=False
        )

        file_path, file_hash, uploaded, bytes_transferred = result

        # Should have computed hash and uploaded
        # file_path should be returned as-is (the input path)
        self.assertEqual(file_path, str(test_file))
        self.assertIsInstance(file_hash, str)
        self.assertTrue(uploaded)  # Should be uploaded since not in manifest
        self.assertGreater(bytes_transferred, 0)

        # Cache should still be empty
        self.assertEqual(len(self.versioner.hash_cache), 0)

        # Note: _hash_and_upload_worker calls upload with needs_immediate_update=False,
        # so the manifest is not updated immediately. This is by design for batch operations.

    def test_hash_and_download_worker_with_cache_disabled(self):
        """Test _hash_and_download_worker with use_cache=False."""
        test_file = self.test_dir / "download_worker_cache_disabled.txt"
        test_file.write_text("content for download worker cache disabled test")

        # First upload the file
        self.versioner.upload(str(test_file))
        expected_hash = self.versioner.hash_file(str(test_file))

        # Remove local file
        test_file.unlink()

        # Get the manifest key for the file
        manifest_key = self.versioner.path_resolver.to_manifest_key(test_file)

        # Test the worker function directly with cache disabled
        result = self.versioner._hash_and_download_worker(
            (manifest_key, expected_hash),
            silence=True,
            progress_callback=None,
            use_cache=False,
        )

        file_path, downloaded, bytes_transferred = result

        # Should have downloaded the file
        # file_path should be the manifest key (relative path)
        self.assertEqual(file_path, manifest_key)
        self.assertTrue(downloaded)
        self.assertGreater(bytes_transferred, 0)

        # File should be restored
        self.assertTrue(test_file.exists())

        # Cache should still be empty
        self.assertEqual(len(self.versioner.hash_cache), 0)

    def test_cache_disabled_performance_comparison(self):
        """Test that cache disabled mode works correctly and doesn't use cache."""
        test_file = self.test_dir / "performance_test.txt"
        test_file.write_text("content for performance test")

        # Track with cache enabled
        self.versioner.track(str(test_file), use_cache=True)

        # Should have cache entry
        self.assertGreater(len(self.versioner.hash_cache), 0)

        # Clear cache
        self.versioner.clear_hash_cache()
        self.assertEqual(len(self.versioner.hash_cache), 0)

        # Track again with cache disabled
        self.versioner.track(str(test_file), use_cache=False)

        # Cache should still be empty
        self.assertEqual(len(self.versioner.hash_cache), 0)

        # But file should still be tracked properly (as manifest key)
        manifest_key = self.versioner.path_resolver.to_manifest_key(test_file)
        self.assertIn(manifest_key, self.versioner.manifest["files"])

    # Edge Cases and Missing Tests
    def test_hash_file_empty_file_auto_method(self):
        """Test hash_file with empty file using auto method selection."""
        empty_file = self.test_dir / "empty.txt"
        empty_file.touch()

        try:
            result = self.versioner.hash_file(empty_file, method="auto")
            self.assertIsInstance(result, str)
            self.assertEqual(len(result), 64)
        finally:
            if empty_file.exists():
                empty_file.unlink()

    def test_track_modified_files_missing_file(self):
        """Test track_modified_files_cached when a tracked file is missing."""
        self.versioner.manifest["files"]["missing_file.txt"] = "fake_hash"

        with patch("builtins.print") as mock_print:
            # Use the cached version which has better error handling
            self.versioner.track_modified_files_cached()

            calls = [str(call_args) for call_args in mock_print.call_args_list]
            missing_calls = [call for call in calls if "missing" in call.lower()]
            self.assertTrue(len(missing_calls) > 0)

    def test_cleanup_stale_cache_old_entries(self):
        """Test cleanup_stale_cache with old timestamp entries."""
        import time

        # Add an old cache entry for a non-existent file (should be removed)
        old_timestamp = time.time() - (35 * 24 * 60 * 60)  # 35 days ago
        self.versioner.hash_cache["old_nonexistent_file.txt"] = {
            "hash": "old_hash",
            "metadata": {"size": 100, "mtime": 123456789},
            "timestamp": old_timestamp,
        }

        # Count initial entries
        initial_count = len(self.versioner.hash_cache)

        # Call cleanup - should remove the non-existent file entry
        self.versioner.cleanup_stale_cache(max_age_days=30)

        # Should remove old entries
        self.assertNotIn("old_nonexistent_file.txt", self.versioner.hash_cache)

        # Should have cleaned up entries
        final_count = len(self.versioner.hash_cache)
        self.assertLess(final_count, initial_count)

    def test_clear_hash_cache_all_entries(self):
        """Test clear_hash_cache clearing all entries."""
        # Add some cache entries
        self.versioner.hash_cache["file1.txt"] = {"hash": "hash1"}
        self.versioner.hash_cache["file2.txt"] = {"hash": "hash2"}

        with patch("builtins.print") as mock_print:
            self.versioner.clear_hash_cache()  # Clear all

            self.assertEqual(len(self.versioner.hash_cache), 0)

            calls = [str(call_args) for call_args in mock_print.call_args_list]
            clear_calls = [call for call in calls if "Cleared all hash cache" in call]
            self.assertTrue(len(clear_calls) > 0)

    def test_clear_hash_cache_specific_file(self):
        """Test clear_hash_cache clearing specific file."""
        # Add some cache entries and save them
        self.versioner.hash_cache["file1.txt"] = {"hash": "hash1"}
        self.versioner.hash_cache["file2.txt"] = {"hash": "hash2"}
        self.versioner.save_cache()

        with patch("builtins.print") as mock_print:
            self.versioner.clear_hash_cache("file1.txt")

            # Reload cache to see the persisted state
            self.versioner.load_cache()

            self.assertNotIn("file1.txt", self.versioner.hash_cache)
            self.assertIn("file2.txt", self.versioner.hash_cache)

            calls = [str(call_args) for call_args in mock_print.call_args_list]
            clear_calls = [call for call in calls if "Cleared hash cache for" in call]
            self.assertTrue(len(clear_calls) > 0)

    def test_hash_file_unsupported_method(self):
        """Test hash_file with unsupported method."""
        with self.assertRaises(ValueError) as cm:
            self.versioner.hash_file(self.test_file, method="unsupported")

        self.assertIn("Unsupported hashing method", str(cm.exception))

    def test_compress_file_unsupported_method(self):
        """Test compress_file with unsupported method."""
        with self.assertRaises(ValueError) as cm:
            self.versioner.compress_file(self.test_file, method="unsupported")

        self.assertIn("Unsupported compression method", str(cm.exception))

    def test_decompress_file_unsupported_method(self):
        """Test decompress_file with unsupported method."""
        compressed_file = self.test_dir / "test.gz"
        compressed_file.touch()

        try:
            with self.assertRaises(ValueError) as cm:
                self.versioner.decompress_file(compressed_file, method="unsupported")

            self.assertIn("Unsupported decompression method", str(cm.exception))
        finally:
            if compressed_file.exists():
                compressed_file.unlink()

    def test_hash_file_auto_selection_linux_cli(self):
        """Test automatic selection of CLI method on Linux for non-empty files."""
        with (
            patch("sys.platform", "linux"),
            patch("shutil.which", return_value="/usr/bin/sha256sum"),
            patch("subprocess.run") as mock_run,
        ):
            # A real-shaped digest: output that does not parse as one now
            # falls back to Python hashing rather than becoming an S3 key.
            mock_run.return_value.stdout = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa /path/to/file\n"

            # Should select CLI method for non-empty files on Linux
            result = self.versioner.hash_file(self.test_file, method="auto")
            self.assertEqual(
                result,
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )

    def test_compress_file_auto_selection_cli(self):
        """Test automatic selection of CLI compression method."""

        def which_no_pigz(cmd):
            return "/usr/bin/gzip" if cmd == "gzip" else None

        with (
            patch("sys.platform", "linux"),
            patch("shutil.which", side_effect=which_no_pigz),
            patch("builtins.open", mock_open()),
        ):
            result = self.versioner.compress_file(self.test_file, method="auto")
            self.assertTrue(str(result).endswith(".gz"))

    def test_decompress_file_auto_selection_cli(self):
        """Test automatic selection of CLI decompression method."""
        compressed_file = self.test_dir / "test.gz"
        compressed_file.touch()

        def which_no_pigz(cmd):
            return "/usr/bin/gzip" if cmd == "gzip" else None

        try:
            with (
                patch("sys.platform", "linux"),
                patch("shutil.which", side_effect=which_no_pigz),
                patch("subprocess.run") as mock_run,
                patch("builtins.open", mock_open()),
            ):
                mock_run.return_value.returncode = 0

                result = self.versioner.decompress_file(compressed_file, method="auto")
                self.assertEqual(result, compressed_file.with_suffix(""))
        finally:
            if compressed_file.exists():
                compressed_file.unlink()


if __name__ == "__main__":
    unittest.main()
