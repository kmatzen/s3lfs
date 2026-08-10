import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import yaml

from s3lfs.core import S3LFS


class TestInternalPathExclusion(unittest.TestCase):
    """Filesystem enumeration must not pick up git or s3lfs internals.

    _resolve_filesystem_paths uses rglob("*"), which matches dotfiles, so
    tracking the repository root would otherwise walk into .git/ and also
    collect the manifest, the hash cache, and the lock file.
    """

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        self.root = Path(self.temp_dir).resolve()

        os.makedirs(".git/hooks")
        Path(".git/config").write_text("[core]\n")
        Path(".git/hooks/pre-commit.sample").write_text("#!/bin/sh\n")

        os.makedirs("data")
        Path("data/asset.bin").write_text("payload")

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

    def _s3lfs(self):
        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            mock_client.list_objects_v2.return_value = {}
            return S3LFS(bucket_name="test-bucket")

    def test_repo_root_enumeration_excludes_internals(self):
        """Tracking the repo root yields user files only."""
        s3lfs = self._s3lfs()
        # Materialize the lock file, as any real operation would.
        with s3lfs._lock_context():
            pass

        found = s3lfs._resolve_filesystem_paths(str(self.root))
        names = sorted(Path(p).relative_to(self.root).as_posix() for p in found)

        self.assertEqual(names, ["data/asset.bin"])

    def test_git_directory_is_internal(self):
        s3lfs = self._s3lfs()
        self.assertTrue(s3lfs._is_internal_path(".git/config"))
        self.assertTrue(s3lfs._is_internal_path(".git/hooks/pre-commit.sample"))

    def test_s3lfs_bookkeeping_is_internal(self):
        s3lfs = self._s3lfs()
        self.assertTrue(s3lfs._is_internal_path(".s3_manifest.yaml"))
        self.assertTrue(s3lfs._is_internal_path(".s3_manifest_cache.yaml"))
        self.assertTrue(s3lfs._is_internal_path(".s3lfs_temp/.s3lfs.lock"))
        self.assertTrue(s3lfs._is_internal_path(".s3lfs_temp/abc123.gz"))

    def test_user_files_are_not_internal(self):
        s3lfs = self._s3lfs()
        self.assertFalse(s3lfs._is_internal_path("data/asset.bin"))
        # A user directory merely containing "git" in its name is not .git
        self.assertFalse(s3lfs._is_internal_path("gitignore_docs/readme.md"))
        self.assertFalse(s3lfs._is_internal_path(".github/workflows/ci.yml"))


if __name__ == "__main__":
    unittest.main()
