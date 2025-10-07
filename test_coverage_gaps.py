#!/usr/bin/env python3
"""
Test coverage for previously uncovered code areas.
"""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from s3lfs import S3LFS


class TestCoverageGaps(unittest.TestCase):
    """Test coverage for previously uncovered code areas."""

    def setUp(self):
        """Set up test environment."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: self._cleanup_temp_dir())

    def _cleanup_temp_dir(self):
        """Clean up temporary directory."""
        import shutil

        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)

    def test_manifest_outside_git_repo(self):
        """Test PathResolver when manifest is outside git repo."""
        # Create a manifest file outside any git repo
        manifest_file = self.temp_dir / ".s3_manifest.yaml"
        manifest_content = """
bucket: test-bucket
prefix: test-prefix
files: {}
"""
        with open(manifest_file, "w") as f:
            f.write(manifest_content)

        # Create S3LFS instance - this should trigger the "manifest outside git repo" path
        s3lfs = S3LFS(
            bucket_name="test-bucket",
            manifest_file=str(manifest_file),
            no_sign_request=True,
        )

        # Verify that path_resolver uses manifest directory as base
        self.assertEqual(s3lfs.path_resolver.git_root, manifest_file.parent.resolve())

    def test_mmap_hashing_method(self):
        """Test mmap-based file hashing method."""
        # Create a test file
        test_file = self.temp_dir / "test_file.txt"
        test_content = "This is a test file for mmap hashing"
        test_file.write_text(test_content)

        # Create manifest file
        manifest_file = self.temp_dir / ".s3_manifest.yaml"
        manifest_content = """
bucket: test-bucket
prefix: test-prefix
files: {}
"""
        with open(manifest_file, "w") as f:
            f.write(manifest_content)

        # Create S3LFS instance
        s3lfs = S3LFS(
            bucket_name="test-bucket",
            manifest_file=str(manifest_file),
            no_sign_request=True,
        )

        # Mock the system to prefer mmap method
        with patch("s3lfs.core.mmap") as mock_mmap:
            # Mock mmap to return a mock object
            mock_mmap_instance = Mock()
            mock_mmap_instance.__enter__ = Mock(return_value=test_content.encode())
            mock_mmap_instance.__exit__ = Mock(return_value=None)
            mock_mmap.mmap.return_value = mock_mmap_instance
            mock_mmap.ACCESS_READ = 0

            # Test the hashing
            with patch("s3lfs.metrics.get_tracker") as mock_tracker:
                mock_tracker_instance = Mock()
                mock_tracker.return_value = mock_tracker_instance
                mock_tracker_instance.track_task.return_value.__enter__ = Mock(
                    return_value=None
                )
                mock_tracker_instance.track_task.return_value.__exit__ = Mock(
                    return_value=None
                )

                hash_result = s3lfs.hash_file(test_file)
                self.assertIsInstance(hash_result, str)
                self.assertEqual(len(hash_result), 64)  # SHA256 hex length

    def test_chunked_hashing_method(self):
        """Test chunked file hashing method."""
        # Create a test file
        test_file = self.temp_dir / "test_file.txt"
        test_content = "This is a test file for chunked hashing"
        test_file.write_text(test_content)

        # Create manifest file
        manifest_file = self.temp_dir / ".s3_manifest.yaml"
        manifest_content = """
bucket: test-bucket
prefix: test-prefix
files: {}
"""
        with open(manifest_file, "w") as f:
            f.write(manifest_content)

        # Create S3LFS instance
        s3lfs = S3LFS(
            bucket_name="test-bucket",
            manifest_file=str(manifest_file),
            no_sign_request=True,
        )

        # Mock the system to prefer chunked method
        with patch("s3lfs.core.mmap", side_effect=ImportError("mmap not available")):
            with patch("s3lfs.metrics.get_tracker") as mock_tracker:
                mock_tracker_instance = Mock()
                mock_tracker.return_value = mock_tracker_instance
                mock_tracker_instance.track_task.return_value.__enter__ = Mock(
                    return_value=None
                )
                mock_tracker_instance.track_task.return_value.__exit__ = Mock(
                    return_value=None
                )

                hash_result = s3lfs.hash_file(test_file)
                self.assertIsInstance(hash_result, str)
                self.assertEqual(len(hash_result), 64)  # SHA256 hex length

    def test_compression_with_metrics(self):
        """Test file compression with metrics tracking."""
        # Create a test file
        test_file = self.temp_dir / "test_file.txt"
        test_content = "This is a test file for compression"
        test_file.write_text(test_content)

        # Create manifest file
        manifest_file = self.temp_dir / ".s3_manifest.yaml"
        manifest_content = """
bucket: test-bucket
prefix: test-prefix
files: {}
"""
        with open(manifest_file, "w") as f:
            f.write(manifest_content)

        # Create S3LFS instance
        s3lfs = S3LFS(
            bucket_name="test-bucket",
            manifest_file=str(manifest_file),
            no_sign_request=True,
        )

        with patch("s3lfs.metrics.get_tracker") as mock_tracker:
            mock_tracker_instance = Mock()
            mock_tracker.return_value = mock_tracker_instance
            mock_tracker_instance.track_task.return_value.__enter__ = Mock(
                return_value=None
            )
            mock_tracker_instance.track_task.return_value.__exit__ = Mock(
                return_value=None
            )

            # Test compression
            compressed_path = s3lfs.compress_file(test_file)
            self.assertTrue(compressed_path.exists())
            self.assertTrue(compressed_path.suffix == ".gz")

    def test_decompression_with_metrics(self):
        """Test file decompression with metrics tracking."""
        # Create a test file and compress it
        test_file = self.temp_dir / "test_file.txt"
        test_content = "This is a test file for decompression"
        test_file.write_text(test_content)

        # Create manifest file
        manifest_file = self.temp_dir / ".s3_manifest.yaml"
        manifest_content = """
bucket: test-bucket
prefix: test-prefix
files: {}
"""
        with open(manifest_file, "w") as f:
            f.write(manifest_content)

        s3lfs = S3LFS(
            bucket_name="test-bucket",
            manifest_file=str(manifest_file),
            no_sign_request=True,
        )
        compressed_path = s3lfs.compress_file(test_file)

        # Test decompression
        output_path = self.temp_dir / "decompressed.txt"
        with patch("s3lfs.metrics.get_tracker") as mock_tracker:
            mock_tracker_instance = Mock()
            mock_tracker.return_value = mock_tracker_instance
            mock_tracker_instance.track_task.return_value.__enter__ = Mock(
                return_value=None
            )
            mock_tracker_instance.track_task.return_value.__exit__ = Mock(
                return_value=None
            )

            result_path = s3lfs.decompress_file(compressed_path, output_path)
            self.assertEqual(result_path, output_path)
            self.assertTrue(output_path.exists())
            self.assertEqual(output_path.read_text(), test_content)

    def test_s3_upload_with_metrics(self):
        """Test S3 upload with metrics tracking."""
        # This test focuses on the metrics tracking code path
        from s3lfs.metrics import get_tracker

        # Test that metrics tracking works
        tracker = get_tracker()
        with tracker.track_task("s3_upload", "test-key"):
            # Simulate some work
            pass

    def test_directory_glob_resolution(self):
        """Test directory glob pattern resolution."""
        # Create test directory structure
        test_dir = self.temp_dir / "test_dir"
        test_dir.mkdir()

        # Create subdirectories matching pattern
        for i in range(3):
            subdir = test_dir / f"capture{i:03d}"
            subdir.mkdir()
            (subdir / "data.txt").write_text(f"Data from capture{i:03d}")

        # Create S3LFS instance
        manifest_file = self.temp_dir / ".s3_manifest.yaml"
        manifest_content = """
bucket: test-bucket
prefix: test-prefix
files: {}
"""
        with open(manifest_file, "w") as f:
            f.write(manifest_content)

        s3lfs = S3LFS(
            bucket_name="test-bucket",
            manifest_file=str(manifest_file),
            no_sign_request=True,
        )

        # Test directory glob resolution
        resolved_files = s3lfs._resolve_filesystem_paths("test_dir/capture*")

        # Should find all files in directories matching the pattern
        self.assertGreater(len(resolved_files), 0)
        for file_path in resolved_files:
            self.assertTrue(file_path.is_file())

    def test_metrics_pipeline_tracking(self):
        """Test metrics pipeline tracking."""
        from s3lfs import metrics

        # Enable metrics
        metrics.enable_metrics()

        # Test pipeline tracking
        tracker = metrics.get_tracker()
        tracker.start_pipeline()
        tracker.start_stage("test_stage", max_workers=4)
        tracker.end_stage("test_stage")
        tracker.end_pipeline()
        tracker.print_summary(verbose=True)

    def test_s3_download_with_metrics(self):
        """Test S3 download with metrics tracking."""
        # This test focuses on the metrics tracking code path
        from s3lfs.metrics import get_tracker

        # Test that metrics tracking works
        tracker = get_tracker()
        with tracker.track_task("s3_download", "test-key"):
            # Simulate some work
            pass

    def test_ls_command_path_resolution(self):
        """Test ls command path resolution logic."""
        from s3lfs.path_resolver import PathResolver

        # Create a test git repository
        git_root = self.temp_dir / "test_repo"
        git_root.mkdir()
        (git_root / ".git").mkdir()

        # Create manifest
        manifest_file = git_root / ".s3_manifest.yaml"
        manifest_content = """
bucket: test-bucket
prefix: test-prefix
files: {}
"""
        with open(manifest_file, "w") as f:
            f.write(manifest_content)

        # Create test files
        test_file = git_root / "test_file.txt"
        test_file.write_text("Test content")

        # Test path resolution directly
        path_resolver = PathResolver(git_root)
        manifest_key = path_resolver.from_cli_input("test_file.txt", cwd=git_root)
        self.assertEqual(manifest_key, "test_file.txt")


if __name__ == "__main__":
    unittest.main()
