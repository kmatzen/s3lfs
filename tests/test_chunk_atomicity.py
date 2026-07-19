import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import yaml

from s3lfs.core import S3LFS


class TestChunkAtomicity(unittest.TestCase):
    """A manifest entry must imply every one of its chunks exists.

    Chunk discovery infers the chunk count from the objects that happen to
    exist and then reads indices 0..n-1. A partial upload missing its tail
    therefore reassembles into a shorter but otherwise well-formed file, so
    recording an incomplete upload turns into silent truncation at checkout.
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

        # Incompressible, so it genuinely splits into several chunks.
        self.payload = os.urandom(30000)
        Path("big.bin").write_bytes(self.payload)

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _s3lfs(self, fail_chunks_after=None):
        """Build an S3LFS whose chunk uploads fail after N successes."""
        store: dict = {}

        def factory(no_sign_request):
            client = Mock()

            def upload_fileobj(fileobj, bucket, key, **kwargs):
                # Decide from the chunk index, not from how many chunks have
                # landed so far: uploads run concurrently, so a count-based
                # rule lets every chunk pass the check before any is recorded
                # and the failure never fires.
                if fail_chunks_after is not None and ".chunk" in key:
                    index = int(key.rsplit(".chunk", 1)[1])
                    if index >= fail_chunks_after:
                        raise RuntimeError("simulated S3 failure")
                store[key] = fileobj.read()

            client.upload_fileobj.side_effect = upload_fileobj
            client.head_object.side_effect = Exception("not found")

            def list_objects_v2(Bucket=None, Prefix=None, **kwargs):
                keys = sorted(k for k in store if k.startswith(Prefix or ""))
                return {"Contents": [{"Key": k} for k in keys]} if keys else {}

            client.list_objects_v2.side_effect = list_objects_v2
            return client

        with patch("boto3.client"):
            s3lfs = S3LFS(
                bucket_name="test-bucket",
                manifest_file=str(self.manifest_path),
                s3_factory=factory,
            )
        s3lfs.chunk_size = 10000
        return s3lfs, store

    def _manifest_files(self):
        with open(self.manifest_path) as f:
            return yaml.safe_load(f).get("files", {})

    def test_partial_upload_is_not_recorded(self):
        s3lfs, store = self._s3lfs(fail_chunks_after=1)
        s3lfs.parallel_upload_chunked(["big.bin"], silence=True)

        uploaded_chunks = [k for k in store if ".chunk" in k]
        self.assertGreater(len(uploaded_chunks), 0, "test should upload some chunks")
        self.assertNotIn(
            "big.bin",
            self._manifest_files(),
            "an incomplete upload was recorded in the manifest",
        )

    def test_complete_upload_is_recorded(self):
        s3lfs, _ = self._s3lfs()
        s3lfs.parallel_upload_chunked(["big.bin"], silence=True)

        self.assertIn("big.bin", self._manifest_files())

    def test_finalize_rejects_checksum_mismatch(self):
        s3lfs, _ = self._s3lfs()
        compressed = s3lfs.compress_file(Path("big.bin"))

        with self.assertRaises(RuntimeError) as ctx:
            s3lfs._finalize_file(
                "big.bin", [compressed], False, expected_hash="not_the_right_hash"
            )

        self.assertIn("Checksum mismatch", str(ctx.exception))
        self.assertFalse(
            Path("big.bin").exists(), "corrupt output should be removed, not left"
        )

    def test_finalize_accepts_matching_checksum(self):
        s3lfs, _ = self._s3lfs()
        expected = s3lfs.hash_file(Path("big.bin"))
        compressed = s3lfs.compress_file(Path("big.bin"))

        s3lfs._finalize_file("big.bin", [compressed], False, expected_hash=expected)

        self.assertEqual(Path("big.bin").read_bytes(), self.payload)


if __name__ == "__main__":
    unittest.main()
