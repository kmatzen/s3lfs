"""The hash cache must have exactly one key per file: the manifest key.

Keying by filesystem path in some producers (hash_file_cached,
get_file_status) and by manifest key in others (disk_hashes, record_hashes,
track_modified_files_cached) meant the cache almost never hit across
commands: `s3lfs checkout data/` stored /abs/tmp/repo/data.txt, the next
`s3lfs status` looked up data.txt, missed, re-read every byte, and stored a
second entry forever.

Also covered:
- parallel_download_all (checkout --all / sync fallback) must reuse the
  cache instead of re-reading the entire working copy.
- cleanup_stale_cache must resolve cache keys through the path resolver so
  it is CWD-independent, and `s3lfs cleanup` must actually call it.
"""

import hashlib
import os
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from s3lfs.cli import cli
from s3lfs.core import S3LFS

from .test_git_integration import GitRepoTestCase


def _sha(text):
    return hashlib.sha256(text.encode()).hexdigest()


class TestHashCacheUnifiedKeying(GitRepoTestCase):
    def _s3lfs(self):
        return S3LFS(
            bucket_name="test-bucket-s3lfs-git",
            manifest_file=str(Path(self.temp_dir) / ".s3_manifest.yaml"),
        )

    def _counting_hash(self, s3lfs):
        calls = []
        real = S3LFS.hash_file

        def counting(file_path, method="auto"):
            calls.append(file_path)
            return real(s3lfs, file_path, method)

        return calls, counting

    def test_hash_file_cached_stores_under_manifest_key(self):
        """An absolute path in, a manifest key out."""
        s3lfs = self._s3lfs()
        path = self._write("data.txt", "hello")

        s3lfs.hash_file_cached(path)

        self.assertEqual(list(s3lfs.hash_cache.keys()), ["data.txt"])

    def test_disk_hashes_after_abs_path_hash_file_cached_reuses_entry(self):
        """checkout-style call then status-style scan: one key, zero re-hashes."""
        s3lfs = self._s3lfs()
        path = self._write("data.txt", "hello")

        s3lfs.hash_file_cached(path)

        calls, counting = self._counting_hash(s3lfs)
        with patch.object(s3lfs, "hash_file", side_effect=counting):
            hashes = s3lfs.disk_hashes({"data.txt": _sha("hello")})

        self.assertEqual(hashes, {"data.txt": _sha("hello")})
        self.assertEqual(calls, [], "disk_hashes re-read a cached file")
        self.assertEqual(
            list(s3lfs.hash_cache.keys()),
            ["data.txt"],
            "cache gained a second key for the same file",
        )

    def test_get_file_status_uses_manifest_key(self):
        s3lfs = self._s3lfs()
        path = self._write("data.txt", "hello")

        s3lfs.hash_file_cached(path)
        # ls --verbose / list_all_files call get_file_status with the
        # manifest key; it must find the entry hash_file_cached wrote.
        status = s3lfs.get_file_status("data.txt")

        self.assertTrue(status["exists"])
        self.assertTrue(status["cached"])
        self.assertTrue(status["cache_valid"])
        self.assertEqual(list(s3lfs.hash_cache.keys()), ["data.txt"])

    def test_clear_hash_cache_accepts_filesystem_path(self):
        s3lfs = self._s3lfs()
        path = self._write("data.txt", "hello")

        s3lfs.hash_file_cached(path)
        s3lfs.clear_hash_cache(path)

        self.assertEqual(s3lfs.hash_cache, {})


class TestParallelDownloadAllCacheAware(GitRepoTestCase):
    def _s3lfs(self, files):
        s3lfs = S3LFS(
            bucket_name="test-bucket-s3lfs-git",
            manifest_file=str(Path(self.temp_dir) / ".s3_manifest.yaml"),
        )
        hashes = {}
        for key, content in files.items():
            self._write(key, content)
            hashes[key] = _sha(content)
        with s3lfs._lock_context():
            s3lfs.manifest["files"] = dict(hashes)
        s3lfs.record_hashes(hashes)
        return s3lfs

    def test_unchanged_files_are_never_rehashed(self):
        """checkout --all with a warm cache must not read a single byte."""
        s3lfs = self._s3lfs({"a.bin": "aaa", "b.bin": "bbb"})

        calls = []

        def record(*a, **k):
            calls.append(a)
            return "x" * 64

        with patch.object(s3lfs, "hash_file", side_effect=record):
            n = s3lfs.parallel_download_all(silence=True)

        self.assertEqual(calls, [], "parallel_download_all re-hashed cached files")
        self.assertEqual(n, 0)

    def test_changed_file_is_downloaded(self):
        s3lfs = self._s3lfs({"a.bin": "aaa", "b.bin": "bbb"})
        self._write("a.bin", "changed content")

        with patch.object(
            s3lfs, "parallel_download_chunked", return_value=1
        ) as mock_dl:
            n = s3lfs.parallel_download_all(silence=True)

        self.assertEqual(n, 1)
        downloaded = [key for key, _hash in mock_dl.call_args[0][0]]
        self.assertEqual(downloaded, ["a.bin"])

    def test_missing_file_is_downloaded(self):
        s3lfs = self._s3lfs({"a.bin": "aaa", "b.bin": "bbb"})
        os.remove(Path(self.temp_dir) / "b.bin")

        with patch.object(
            s3lfs, "parallel_download_chunked", return_value=1
        ) as mock_dl:
            n = s3lfs.parallel_download_all(silence=True)

        self.assertEqual(n, 1)
        downloaded = [key for key, _hash in mock_dl.call_args[0][0]]
        self.assertEqual(downloaded, ["b.bin"])

    def test_preserve_modified_keeps_diverged_file(self):
        s3lfs = self._s3lfs({"a.bin": "aaa"})
        self._write("a.bin", "local work")

        with patch.object(s3lfs, "parallel_download_chunked") as mock_dl:
            n = s3lfs.parallel_download_all(silence=True, preserve_modified=True)

        mock_dl.assert_not_called()
        self.assertEqual(n, 0)
        self.assertEqual(Path(self.temp_dir, "a.bin").read_text(), "local work")


class TestCleanupStaleCache(GitRepoTestCase):
    def _s3lfs(self):
        return S3LFS(
            bucket_name="test-bucket-s3lfs-git",
            manifest_file=str(Path(self.temp_dir) / ".s3_manifest.yaml"),
        )

    def _seed(self, s3lfs):
        entry = {
            "hash": "h",
            "metadata": {"size": 1, "mtime": 1.0, "inode": 1},
            "timestamp": time.time(),
        }
        s3lfs.hash_cache.update(
            {
                "keep.txt": dict(entry),
                "sub/keep2.txt": dict(entry),
                "gone.txt": dict(entry),
            }
        )
        s3lfs._cache_dirty = True
        s3lfs.save_cache()

    def test_prunes_by_manifest_key_from_a_subdirectory(self):
        """Entries are repo-relative; resolving them against CWD wipes the
        whole cache when the command runs from a subdirectory."""
        s3lfs = self._s3lfs()
        self._write("keep.txt", "k")
        self._write("sub/keep2.txt", "k2")
        self._seed(s3lfs)

        os.chdir(Path(self.temp_dir) / "sub")
        dropped = s3lfs.cleanup_stale_cache(max_age_days=30)

        self.assertIn("keep.txt", s3lfs.hash_cache)
        self.assertIn("sub/keep2.txt", s3lfs.hash_cache)
        self.assertNotIn("gone.txt", s3lfs.hash_cache)
        self.assertEqual(dropped, 1)

    def test_cleanup_command_prunes_hash_cache(self):
        self._write("keep.txt", "k")
        s3lfs = self._s3lfs()
        self._seed(s3lfs)

        with patch.object(
            S3LFS, "cleanup_stale_cache", autospec=True, return_value=1
        ) as mock_cleanup:
            result = CliRunner().invoke(cli, ["cleanup", "--force"])

        self.assertEqual(result.exit_code, 0, msg=result.output)
        mock_cleanup.assert_called_once()


if __name__ == "__main__":
    unittest.main()
