import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml

from s3lfs.core import S3LFS


class TestPathAwareGC(unittest.TestCase):
    """Reachability must be judged on hash *and* path.

    Objects are stored at assets/{hash}/{manifest_key}.gz, so the storage
    unit is hash + path. Garbage collection previously judged reachability on
    the hash alone, so an object stayed reachable as long as any path shared
    its content -- a removed or renamed path leaked its object permanently.
    """

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        self.root = Path(self.temp_dir).resolve()
        os.makedirs(".git")
        self.manifest_path = self.root / ".s3_manifest.yaml"
        self.store = {}

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _write_manifest(self, files):
        with open(self.manifest_path, "w") as f:
            yaml.safe_dump(
                {
                    "bucket_name": "test-bucket",
                    "repo_prefix": "test-prefix",
                    "files": files,
                },
                f,
            )

    def _s3lfs(self):
        store = self.store

        def factory(no_sign_request):
            client = MagicMock()

            def list_objects_v2(Bucket=None, Prefix=None, **kwargs):
                keys = sorted(k for k in store if k.startswith(Prefix or ""))
                return {"Contents": [{"Key": k} for k in keys]} if keys else {}

            client.list_objects_v2.side_effect = list_objects_v2

            def get_paginator(_):
                paginator = MagicMock()
                paginator.paginate.side_effect = lambda **kw: [list_objects_v2(**kw)]
                return paginator

            client.get_paginator.side_effect = get_paginator
            client.delete_object.side_effect = lambda Bucket=None, Key=None, **kw: (
                store.pop(Key, None)
            )
            return client

        with patch("boto3.client"):
            return S3LFS(
                bucket_name="test-bucket",
                manifest_file=str(self.manifest_path),
                s3_factory=factory,
            )

    def test_duplicate_content_removed_path_is_collected(self):
        """Two paths share a hash; removing one must free that path's object."""
        self._write_manifest({"b/x.bin": "samehash"})
        removed = "test-prefix/assets/samehash/a/x.bin.gz"
        kept = "test-prefix/assets/samehash/b/x.bin.gz"
        self.store[removed] = b"data"
        self.store[kept] = b"data"

        self._s3lfs().cleanup_s3(force=True)

        self.assertNotIn(removed, self.store, "orphaned duplicate leaked")
        self.assertIn(kept, self.store, "GC deleted a referenced object")

    def test_renamed_path_object_is_collected(self):
        self._write_manifest({"new/name.bin": "h1"})
        stale = "test-prefix/assets/h1/old/name.bin.gz"
        current = "test-prefix/assets/h1/new/name.bin.gz"
        self.store[stale] = b"data"
        self.store[current] = b"data"

        self._s3lfs().cleanup_s3(force=True)

        self.assertNotIn(stale, self.store, "object for the old path leaked")
        self.assertIn(current, self.store)

    def test_chunks_of_referenced_file_are_kept(self):
        self._write_manifest({"big.bin": "h2"})
        base = "test-prefix/assets/h2/big.bin.gz"
        for i in range(3):
            self.store[f"{base}.chunk{i}"] = b"chunk"

        self._s3lfs().cleanup_s3(force=True)

        self.assertEqual(len(self.store), 3, "chunks of a tracked file were deleted")

    def test_chunks_of_unreferenced_file_are_collected(self):
        self._write_manifest({})
        base = "test-prefix/assets/h3/gone.bin.gz"
        for i in range(3):
            self.store[f"{base}.chunk{i}"] = b"chunk"

        self._s3lfs().cleanup_s3(force=True)

        self.assertEqual(self.store, {}, "orphaned chunks were not collected")

    def test_remove_purges_all_chunks(self):
        self._write_manifest({"big.bin": "h4"})
        base = "test-prefix/assets/h4/big.bin.gz"
        for i in range(4):
            self.store[f"{base}.chunk{i}"] = b"chunk"

        self._s3lfs().remove_file("big.bin", keep_in_s3=False)

        self.assertEqual(self.store, {}, "chunks were left behind by remove")

    def test_inflight_claim_is_path_aware(self):
        self._write_manifest({})
        base = "test-prefix/assets/h5/pending.bin.gz"
        self.store[base] = b"data"
        other = "test-prefix/assets/h5/unrelated.bin.gz"
        self.store[other] = b"data"

        s3lfs = self._s3lfs()
        s3lfs._claim_inflight(base)
        s3lfs.cleanup_s3(force=True)

        self.assertIn(base, self.store, "claimed asset was collected")
        self.assertNotIn(
            other, self.store, "a claim on one path protected a different path"
        )

    def test_key_matching_rejects_lookalikes(self):
        s3lfs = self._s3lfs()
        base = "test-prefix/assets/h/a.bin.gz"

        self.assertTrue(s3lfs._key_covered_by(base, {base}))
        self.assertTrue(s3lfs._key_covered_by(f"{base}.chunk12", {base}))
        # A longer path that merely starts with the same characters.
        self.assertFalse(
            s3lfs._key_covered_by("test-prefix/assets/h/a.bin.gz2", {base})
        )
        self.assertFalse(s3lfs._key_covered_by(f"{base}.chunkX", {base}))


if __name__ == "__main__":
    unittest.main()
