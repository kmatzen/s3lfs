import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import yaml

from s3lfs.core import S3LFS


class TestCacheDirtyTracking(unittest.TestCase):
    """Tests for cache dirty flag and mtime-based skip."""

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

    def test_save_cache_noop_when_not_dirty(self):
        """save_cache does not write to disk when nothing changed."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket")

            # Cache was just loaded, not modified
            self.assertFalse(s3lfs._cache_dirty)

            # Record cache file state
            if s3lfs.cache_file.exists():
                mtime_before = s3lfs.cache_file.stat().st_mtime
            else:
                mtime_before = None

            s3lfs.save_cache()

            # File should not have been rewritten
            if mtime_before is not None:
                self.assertEqual(s3lfs.cache_file.stat().st_mtime, mtime_before)

    def test_save_cache_writes_when_dirty(self):
        """save_cache writes to disk when cache was modified."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket")

            # Mutate the cache
            s3lfs.hash_cache["test.txt"] = {
                "hash": "abc",
                "metadata": {},
                "timestamp": time.time(),
            }
            s3lfs._cache_dirty = True

            s3lfs.save_cache()

            # Dirty flag should be cleared
            self.assertFalse(s3lfs._cache_dirty)

            # File should contain the new entry
            with open(s3lfs.cache_file) as f:
                saved = yaml.safe_load(f)
            self.assertIn("test.txt", saved)

    def test_load_cache_skips_when_mtime_unchanged(self):
        """load_cache skips disk read when file mtime hasn't changed."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket")

            # Write some cache data
            s3lfs.hash_cache["x.txt"] = {
                "hash": "h",
                "metadata": {},
                "timestamp": 0,
            }
            s3lfs._cache_dirty = True
            s3lfs.save_cache()

            # Modify in-memory cache (simulating ongoing work)
            s3lfs.hash_cache["y.txt"] = {"hash": "h2", "metadata": {}, "timestamp": 0}

            # load_cache should skip re-read (mtime unchanged)
            s3lfs.load_cache()

            # The in-memory addition should still be there because
            # load_cache was a no-op
            self.assertIn("y.txt", s3lfs.hash_cache)

    def test_load_cache_reads_when_mtime_changed(self):
        """load_cache re-reads when the file was modified externally."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket")

            # Write initial cache
            s3lfs.hash_cache["x.txt"] = {
                "hash": "h",
                "metadata": {},
                "timestamp": 0,
            }
            s3lfs._cache_dirty = True
            s3lfs.save_cache()

            # Externally modify the cache file (simulating another process)
            external_cache = {
                "external.txt": {"hash": "ext", "metadata": {}, "timestamp": 0}
            }
            with open(s3lfs.cache_file, "w") as f:
                yaml.safe_dump(external_cache, f)

            # Force a different mtime
            new_mtime = (s3lfs._cache_mtime or 0) + 1
            os.utime(s3lfs.cache_file, (new_mtime, new_mtime))

            s3lfs.load_cache()

            # Should have the external data now
            self.assertIn("external.txt", s3lfs.hash_cache)
            self.assertNotIn("x.txt", s3lfs.hash_cache)

    def test_force_load_ignores_mtime(self):
        """load_cache(force=True) always re-reads from disk."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket")

            # Write cache
            s3lfs.hash_cache["a.txt"] = {"hash": "h", "metadata": {}, "timestamp": 0}
            s3lfs._cache_dirty = True
            s3lfs.save_cache()

            # Add in-memory entry
            s3lfs.hash_cache["b.txt"] = {"hash": "h2", "metadata": {}, "timestamp": 0}

            # Force reload should overwrite in-memory state even though
            # mtime hasn't changed
            s3lfs.load_cache(force=True)

            self.assertIn("a.txt", s3lfs.hash_cache)
            self.assertNotIn("b.txt", s3lfs.hash_cache)

    def test_hash_file_cached_triggers_save(self):
        """hash_file_cached writes to cache file when computing a new hash."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs_obj = S3LFS(bucket_name="test-bucket")

            test_file = Path("test_data.bin")
            test_file.write_bytes(b"hello")

            save_count = [0]
            original_save = s3lfs_obj.save_cache

            def counting_save():
                save_count[0] += 1
                return original_save()

            with patch.object(s3lfs_obj, "save_cache", side_effect=counting_save):
                s3lfs_obj.hash_file_cached(test_file)

            self.assertEqual(save_count[0], 1)
            self.assertFalse(s3lfs_obj._cache_dirty)


if __name__ == "__main__":
    unittest.main()
