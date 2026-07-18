import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import yaml

from s3lfs.core import S3LFS


class TestManifestReadModifyWrite(unittest.TestCase):
    """Manifest writers must re-read under the lock before saving.

    An S3LFS instance loads the manifest once at construction. If a writer
    saves without re-reading, it writes that stale snapshot back over
    anything another process committed in the meantime, silently dropping
    entries whose S3 objects then become unreferenced.
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
                    "files": {"old.bin": "hash_old"},
                },
                f,
            )

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _s3lfs(self):
        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            mock_client.list_objects_v2.return_value = {}
            return S3LFS(
                bucket_name="test-bucket", manifest_file=str(self.manifest_path)
            )

    def _files_on_disk(self):
        with open(self.manifest_path) as f:
            return yaml.safe_load(f)["files"]

    def _commit_from_another_process(self, key, value):
        """Simulate a second process committing an entry."""
        other = self._s3lfs()
        with other._lock_context():
            other.load_manifest()
            other.manifest["files"][key] = value
            other.save_manifest()

    def test_remove_file_preserves_concurrent_commit(self):
        stale = self._s3lfs()  # snapshot taken before the commit below
        self._commit_from_another_process("new.bin", "hash_new")

        stale.remove_file("old.bin")

        files = self._files_on_disk()
        self.assertIn("new.bin", files, "concurrent commit was erased")
        self.assertNotIn("old.bin", files)

    def test_remove_subtree_preserves_concurrent_commit(self):
        with open(self.manifest_path, "w") as f:
            yaml.safe_dump(
                {
                    "bucket_name": "test-bucket",
                    "repo_prefix": "test-prefix",
                    "files": {"data/a.bin": "hash_a", "data/b.bin": "hash_b"},
                },
                f,
            )

        stale = self._s3lfs()
        self._commit_from_another_process("new.bin", "hash_new")

        stale.remove_subtree("data")

        files = self._files_on_disk()
        self.assertIn("new.bin", files, "concurrent commit was erased")
        self.assertNotIn("data/a.bin", files)
        self.assertNotIn("data/b.bin", files)

    def test_remove_file_untracked_leaves_manifest_alone(self):
        stale = self._s3lfs()
        self._commit_from_another_process("new.bin", "hash_new")

        stale.remove_file("nonexistent.bin")

        files = self._files_on_disk()
        self.assertIn("new.bin", files)
        self.assertIn("old.bin", files)


if __name__ == "__main__":
    unittest.main()
