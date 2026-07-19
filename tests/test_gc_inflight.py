import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml

from s3lfs.core import S3LFS


class TestGCInflightRegistry(unittest.TestCase):
    """Garbage collection must not delete objects an upload is about to use.

    An uploader PUTs chunks before publishing its manifest entry, so there is
    a window in which an object exists in S3 and nothing references it. A
    sweep running in that window would delete it and leave the manifest
    pointing at a missing object. Uploaders therefore claim the asset key
    before any bytes are sent, and GC treats claimed keys as live.
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
        self.store = {}

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

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

    def test_claimed_asset_survives_cleanup(self):
        s3lfs = self._s3lfs()
        key = "test-prefix/assets/claimed_hash/file.bin.gz"
        self.store[key] = b"data"

        s3lfs._claim_inflight(key)
        s3lfs.cleanup_s3(force=True)

        self.assertIn(key, self.store, "GC deleted an in-flight object")

    def test_unreferenced_hash_is_collected(self):
        s3lfs = self._s3lfs()
        key = "test-prefix/assets/orphan_hash/file.bin.gz"
        self.store[key] = b"data"

        s3lfs.cleanup_s3(force=True)

        self.assertNotIn(key, self.store, "GC failed to collect genuine garbage")

    def test_released_claim_no_longer_protects(self):
        s3lfs = self._s3lfs()
        key = "test-prefix/assets/tmp_hash/file.bin.gz"
        self.store[key] = b"data"

        s3lfs._claim_inflight(key)
        s3lfs._release_inflight({key})
        s3lfs.cleanup_s3(force=True)

        self.assertNotIn(key, self.store)

    def test_expired_claim_no_longer_protects(self):
        """A crashed uploader's claim must not pin objects forever."""
        s3lfs = self._s3lfs()
        key = "test-prefix/assets/stale_hash/file.bin.gz"
        self.store[key] = b"data"

        with s3lfs._lock_context():
            expired = time.time() - S3LFS.INFLIGHT_TTL_SECONDS - 60
            s3lfs._save_inflight({key: expired})

        s3lfs.cleanup_s3(force=True)

        self.assertNotIn(key, self.store, "an aged-out claim still pinned an object")

    def test_asset_key_shape_is_validated(self):
        """Keys that are not recognisably assets are left alone."""
        s3lfs = self._s3lfs()

        self.assertTrue(
            s3lfs._is_asset_key("test-prefix/assets/abc123/test-prefix/nested.bin.gz")
        )
        self.assertFalse(s3lfs._is_asset_key("other/assets/abc123/f.gz"))
        self.assertFalse(s3lfs._is_asset_key("test-prefix/short"))
        self.assertFalse(s3lfs._is_asset_key("test-prefix/assets/onlyhash"))

    def test_corrupt_registry_does_not_block(self):
        s3lfs = self._s3lfs()
        s3lfs._inflight_file.write_text("{{{ not yaml")

        self.assertEqual(s3lfs._live_inflight_keys(), set())

    def test_registry_is_not_enumerated_as_a_user_file(self):
        s3lfs = self._s3lfs()
        s3lfs._claim_inflight("some_hash")

        found = s3lfs._resolve_filesystem_paths(str(self.root))
        names = [str(Path(p).relative_to(self.root)) for p in found]

        self.assertNotIn(".s3lfs_temp/.s3lfs_inflight.yaml", names)


if __name__ == "__main__":
    unittest.main()
