import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml

from s3lfs.core import S3LFS, ShutdownRequested


class TestShutdownStopsQueuedWork(unittest.TestCase):
    """An interrupt must stop work that has not started yet.

    Only the drain loops checked _shutdown_requested, so every task already
    submitted to the pool ran to completion. On a large transfer that makes
    Ctrl-C look like a hang.
    """

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        os.makedirs(".git")
        with open(".s3_manifest.yaml", "w") as f:
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

    def _s3lfs(self, client=None):
        with patch("boto3.client"):
            return S3LFS(
                bucket_name="test-bucket",
                s3_factory=lambda no_sign: client or MagicMock(),
            )

    def test_upload_chunk_declines_after_shutdown(self):
        uploaded = []
        client = MagicMock()
        client.upload_fileobj.side_effect = lambda *a, **kw: uploaded.append(1)

        s3lfs = self._s3lfs(client)
        chunk_path = Path("chunk0.gz")
        chunk_path.write_bytes(b"payload")
        chunk_info = {
            "path": chunk_path,
            "s3_key": "test-prefix/assets/h/f.gz.chunk0",
            "chunk_index": 0,
            "extra_args": {},
        }

        s3lfs._shutdown_requested = True

        with self.assertRaises(ShutdownRequested):
            s3lfs._upload_chunk(chunk_info)

        self.assertEqual(uploaded, [], "a cancelled chunk was still uploaded")
        self.assertFalse(chunk_path.exists(), "cancelled chunk file leaked")

    def test_download_chunk_declines_after_shutdown(self):
        downloaded = []
        client = MagicMock()
        client.download_fileobj.side_effect = lambda **kw: downloaded.append(1)

        s3lfs = self._s3lfs(client)
        s3lfs._shutdown_requested = True

        chunk_info = {
            "manifest_key": "f.bin",
            "s3_key": "test-prefix/assets/h/f.gz",
            "chunk_index": 0,
            "is_chunked": False,
            "num_chunks": 1,
        }

        with self.assertRaises(ShutdownRequested):
            s3lfs._download_chunk(chunk_info, Path("out.gz"))

        self.assertEqual(downloaded, [], "a cancelled chunk was still downloaded")

    def test_work_proceeds_when_not_shutting_down(self):
        uploaded = []
        client = MagicMock()
        client.upload_fileobj.side_effect = lambda *a, **kw: uploaded.append(1)

        s3lfs = self._s3lfs(client)
        chunk_path = Path("chunk0.gz")
        chunk_path.write_bytes(b"payload")
        chunk_info = {
            "path": chunk_path,
            "s3_key": "test-prefix/assets/h/f.gz.chunk0",
            "chunk_index": 0,
            "extra_args": {},
        }

        s3lfs._upload_chunk(chunk_info)

        self.assertEqual(len(uploaded), 1)


if __name__ == "__main__":
    unittest.main()
