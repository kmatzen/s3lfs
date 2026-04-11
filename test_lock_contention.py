import os
import shutil
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import Mock, patch

import yaml

from s3lfs.core import S3LFS


class TestCheckCacheHit(unittest.TestCase):
    """Tests for _check_cache_hit helper."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        os.makedirs(".git")

        manifest = {
            "bucket_name": "test-bucket",
            "repo_prefix": "test-prefix",
            "files": {},
        }
        with open(".s3_manifest.yaml", "w") as f:
            yaml.safe_dump(manifest, f)

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_returns_hash_on_match(self):
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket")
            s3lfs.hash_cache = {
                "file.txt": {
                    "hash": "abc123",
                    "metadata": {"size": 100, "mtime": 1.0, "inode": 42},
                }
            }
            result = s3lfs._check_cache_hit(
                "file.txt", {"size": 100, "mtime": 1.0, "inode": 42}
            )
            self.assertEqual(result, "abc123")

    def test_returns_none_on_size_mismatch(self):
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket")
            s3lfs.hash_cache = {
                "file.txt": {
                    "hash": "abc123",
                    "metadata": {"size": 100, "mtime": 1.0, "inode": 42},
                }
            }
            result = s3lfs._check_cache_hit(
                "file.txt", {"size": 200, "mtime": 1.0, "inode": 42}
            )
            self.assertIsNone(result)

    def test_returns_none_on_missing_entry(self):
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket")
            s3lfs.hash_cache = {}
            result = s3lfs._check_cache_hit(
                "file.txt", {"size": 100, "mtime": 1.0, "inode": 42}
            )
            self.assertIsNone(result)


class TestTrackModifiedReducedLocking(unittest.TestCase):
    """Verify track_modified_files_cached minimizes lock acquisitions."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        os.makedirs(".git")

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_loads_cache_once_for_many_files(self):
        """Cache is loaded once at start, not per file."""
        # Create manifest with multiple files
        manifest = {
            "bucket_name": "test-bucket",
            "repo_prefix": "test-prefix",
            "files": {
                "a.txt": "hash_a",
                "b.txt": "hash_b",
                "c.txt": "hash_c",
            },
        }
        with open(".s3_manifest.yaml", "w") as f:
            yaml.safe_dump(manifest, f)

        # Create the files
        for name in ["a.txt", "b.txt", "c.txt"]:
            Path(name).write_text(f"content of {name}")

        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            mock_client.head_object.return_value = {"ContentLength": 10}
            mock_client.upload_fileobj = Mock()

            s3lfs = S3LFS(bucket_name="test-bucket")

            # Count how many times load_cache is called
            load_count = [0]
            original_load = s3lfs.load_cache

            def counting_load(*args, **kwargs):
                load_count[0] += 1
                return original_load(*args, **kwargs)

            with patch.object(s3lfs, "load_cache", side_effect=counting_load):
                s3lfs.track_modified_files_cached(silence=True)

            # Should load cache a small constant number of times,
            # NOT once per file (which would be 3+)
            # The initial __init__ also calls load_cache, so we
            # only count from this point.
            # Expect: 1 load in the method + possibly 1 for save
            self.assertLessEqual(
                load_count[0],
                2,
                f"load_cache called {load_count[0]} times for 3 files, "
                f"expected at most 2",
            )

    def test_saves_cache_once_at_end(self):
        """Cache is saved in a single batch at the end, not per file."""
        manifest = {
            "bucket_name": "test-bucket",
            "repo_prefix": "test-prefix",
            "files": {
                "a.txt": "old_hash",
                "b.txt": "old_hash",
            },
        }
        with open(".s3_manifest.yaml", "w") as f:
            yaml.safe_dump(manifest, f)

        for name in ["a.txt", "b.txt"]:
            Path(name).write_text(f"new content of {name}")

        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            mock_client.head_object.side_effect = Exception("not found")
            mock_client.upload_fileobj = Mock()

            s3lfs = S3LFS(bucket_name="test-bucket")

            save_count = [0]
            original_save = s3lfs.save_cache

            def counting_save():
                save_count[0] += 1
                return original_save()

            with patch.object(s3lfs, "save_cache", side_effect=counting_save):
                s3lfs.track_modified_files_cached(silence=True)

            # At most 1 save_cache call (the batch update at end)
            self.assertLessEqual(
                save_count[0],
                1,
                f"save_cache called {save_count[0]} times, expected at most 1",
            )

    def test_does_not_lock_per_file_for_manifest_reads(self):
        """Manifest stored_hash lookups use the snapshot, not per-file locks."""
        manifest = {
            "bucket_name": "test-bucket",
            "repo_prefix": "test-prefix",
            "files": {"x.txt": "hash_x"},
        }
        with open(".s3_manifest.yaml", "w") as f:
            yaml.safe_dump(manifest, f)

        Path("x.txt").write_text("content")

        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()

            s3lfs = S3LFS(bucket_name="test-bucket")

            original_lock = s3lfs._lock_context
            lock_count = [0]

            @contextmanager
            def counting_lock():
                lock_count[0] += 1
                with original_lock():
                    yield

            with patch.object(s3lfs, "_lock_context", side_effect=counting_lock):
                s3lfs.track_modified_files_cached(silence=True)

            # Should be a small constant (1 for initial load, maybe
            # 1 for cache save), not 1-per-file
            self.assertLessEqual(
                lock_count[0],
                3,
                f"Lock acquired {lock_count[0]} times, expected at most 3",
            )


if __name__ == "__main__":
    unittest.main()
