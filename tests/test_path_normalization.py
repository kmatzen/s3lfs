import hashlib
import os
import shutil
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml

from s3lfs.core import S3LFS


class TestRemoveFilePathNormalization(unittest.TestCase):
    """`remove` must accept the same path spellings as every other command.

    Manifest keys are relative to the git root. remove_file built its key
    from the raw argument, so equivalent spellings of a tracked path were
    reported as untracked.
    """

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        self.root = Path(self.temp_dir).resolve()
        os.makedirs(".git")
        os.makedirs("data")
        Path("data/asset.bin").write_bytes(b"payload")

        self.manifest_path = self.root / ".s3_manifest.yaml"
        with open(self.manifest_path, "w") as f:
            yaml.safe_dump(
                {
                    "bucket_name": "test-bucket",
                    "repo_prefix": "test-prefix",
                    "files": {"data/asset.bin": "abc123"},
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

    def _tracked(self):
        with open(self.manifest_path) as f:
            return yaml.safe_load(f)["files"]

    def test_removes_with_plain_relative_path(self):
        s3lfs = self._s3lfs()
        s3lfs.remove_file("data/asset.bin")
        self.assertNotIn("data/asset.bin", self._tracked())

    def test_removes_with_dot_slash_prefix(self):
        s3lfs = self._s3lfs()
        s3lfs.remove_file("./data/asset.bin")
        self.assertNotIn("data/asset.bin", self._tracked())

    def test_removes_with_absolute_path(self):
        s3lfs = self._s3lfs()
        s3lfs.remove_file(str(self.root / "data" / "asset.bin"))
        self.assertNotIn("data/asset.bin", self._tracked())

    def test_deletes_the_object_actually_stored(self):
        """The S3 key must match how the object was uploaded."""
        deleted = []

        def factory(no_sign_request):
            client = MagicMock()
            client.delete_object.side_effect = lambda Bucket=None, Key=None, **kw: (
                deleted.append(Key)
            )
            return client

        with patch("boto3.client"):
            s3lfs = S3LFS(
                bucket_name="test-bucket",
                manifest_file=str(self.manifest_path),
                s3_factory=factory,
            )

        s3lfs.remove_file("./data/asset.bin", keep_in_s3=False)

        self.assertEqual(deleted, ["test-prefix/assets/abc123/data/asset.bin"])


class TestConstructionOffMainThread(unittest.TestCase):
    """S3LFS must be constructible from a worker thread.

    signal.signal raises ValueError off the main thread, which made the
    class unusable as a library component inside one.
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

    def test_constructs_in_worker_thread(self):
        result: dict = {}

        def build():
            try:
                with patch("boto3.client"):
                    result["obj"] = S3LFS(
                        bucket_name="test-bucket",
                        s3_factory=lambda no_sign: MagicMock(),
                    )
            except Exception as exc:  # pragma: no cover - failure path
                result["error"] = exc

        thread = threading.Thread(target=build)
        thread.start()
        thread.join(timeout=30)

        self.assertNotIn("error", result, f"construction failed: {result.get('error')}")
        self.assertIsNotNone(result.get("obj"))


class TestHashIsContentOnly(unittest.TestCase):
    """The documented contract: the digest covers content, not path."""

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

    def test_same_content_different_paths_same_hash(self):
        with patch("boto3.client"):
            s3lfs = S3LFS(
                bucket_name="test-bucket", s3_factory=lambda no_sign: MagicMock()
            )

        content = b"identical bytes"
        Path("a.bin").write_bytes(content)
        os.makedirs("sub")
        Path("sub/b.bin").write_bytes(content)

        expected = hashlib.sha256(content).hexdigest()
        self.assertEqual(s3lfs.hash_file("a.bin"), expected)
        self.assertEqual(s3lfs.hash_file("sub/b.bin"), expected)


if __name__ == "__main__":
    unittest.main()
