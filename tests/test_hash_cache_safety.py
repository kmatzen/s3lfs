import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml

from s3lfs.core import S3LFS


class TestHashCacheSafety(unittest.TestCase):
    """The hash cache must never return a hash matching no version of a file.

    The cache key is (size, mtime, inode). Metadata was read before hashing
    and stored alongside the result afterwards, so a file modified during
    hashing produced an entry whose hash belonged to neither the old nor the
    new content. Filesystem mtime is also often 1-second granular, so a file
    modified within the same second can be indistinguishable from an
    unmodified one.
    """

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        self.root = Path(self.temp_dir).resolve()
        os.makedirs(".git")

        self.manifest_path = self.root / ".s3_manifest.yaml"
        with open(self.manifest_path, "w") as f:
            yaml.safe_dump(
                {
                    "bucket_name": "test-bucket",
                    "repo_prefix": "test-prefix",
                    "files": {},
                },
                f,
            )

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _s3lfs(self):
        with patch("boto3.client"):
            return S3LFS(
                bucket_name="test-bucket",
                manifest_file=str(self.manifest_path),
                s3_factory=lambda no_sign: MagicMock(),
            )

    def _settled_file(self, name, content):
        """A file old enough that mtime is decisive."""
        path = Path(name)
        path.write_bytes(content)
        old = time.time() - 60
        os.utime(path, (old, old))
        return path

    def test_file_changed_during_hashing_is_not_cached(self):
        s3lfs = self._s3lfs()
        path = self._settled_file("racy.bin", b"original")

        real_hash_file = s3lfs.hash_file

        def hash_and_mutate(file_path, method="auto"):
            result = real_hash_file(file_path, method)
            # The file changes while we are hashing it.
            Path(file_path).write_bytes(b"modified during hashing")
            return result

        with patch.object(s3lfs, "hash_file", side_effect=hash_and_mutate):
            s3lfs.hash_file_cached(path)

        self.assertNotIn(
            str(path.as_posix()),
            s3lfs.hash_cache,
            "a hash computed over a file being modified was cached",
        )

    def test_racy_entry_is_stored_but_not_trusted(self):
        """A file hashed within mtime granularity must be revalidated.

        The entry is still written — refusing to cache would make the cache
        useless for the common case of tracking files just after writing them
        — but it is recomputed once rather than trusted on metadata alone.
        """
        s3lfs = self._s3lfs()
        path = Path("fresh.bin")
        path.write_bytes(b"just written")

        s3lfs.hash_file_cached(path)
        entry = s3lfs.hash_cache[str(path.as_posix())]

        self.assertTrue(s3lfs._entry_is_racy(entry))

        # A racy entry must not be served from metadata alone.
        recomputed = []
        real_hash_file = s3lfs.hash_file

        def counting(file_path, method="auto"):
            recomputed.append(file_path)
            return real_hash_file(file_path, method)

        with patch.object(s3lfs, "hash_file", side_effect=counting):
            s3lfs.hash_file_cached(path)

        self.assertEqual(len(recomputed), 1, "racy entry was trusted without a rehash")

    def test_racy_entry_becomes_trusted_after_revalidation(self):
        """Revalidating once must settle the entry, not loop forever."""
        s3lfs = self._s3lfs()
        path = Path("settling.bin")
        path.write_bytes(b"content")

        s3lfs.hash_file_cached(path)  # racy
        # Once the file's mtime is comfortably in the past, the refreshed
        # entry is trustworthy.
        old = time.time() - 60
        os.utime(path, (old, old))
        s3lfs.hash_file_cached(path)  # recompute, store non-racy

        with patch.object(s3lfs, "hash_file", side_effect=AssertionError("recomputed")):
            s3lfs.hash_file_cached(path)

    def test_settled_file_is_cached_and_reused(self):
        s3lfs = self._s3lfs()
        path = self._settled_file("stable.bin", b"stable content")

        first = s3lfs.hash_file_cached(path)
        self.assertIn(str(path.as_posix()), s3lfs.hash_cache)

        # A second call must not recompute.
        with patch.object(s3lfs, "hash_file", side_effect=AssertionError("recomputed")):
            second = s3lfs.hash_file_cached(path)

        self.assertEqual(first, second)

    def test_cached_hash_matches_actual_content(self):
        s3lfs = self._s3lfs()
        path = self._settled_file("verify.bin", b"content to verify")

        cached = s3lfs.hash_file_cached(path)
        direct = s3lfs.hash_file(path)

        self.assertEqual(cached, direct)

    def test_manifest_temp_file_name_is_unique(self):
        """Concurrent writers must not share one temp file."""
        a = self._s3lfs()
        b = self._s3lfs()

        names = set()
        real_replace = Path.replace

        def capture(self_path, target):
            names.add(self_path.name)
            return real_replace(self_path, target)

        with patch.object(Path, "replace", capture):
            with a._lock_context():
                a.save_manifest()
            with b._lock_context():
                b.save_manifest()

        self.assertEqual(len(names), 2, f"temp file name was reused: {names}")


if __name__ == "__main__":
    unittest.main()
