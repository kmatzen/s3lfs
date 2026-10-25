"""Tests for the audit cleanup phase.

Pins the two behaviour changes made while deleting dead code:

- verbose `ls` lists every tracked file (top-level keys included) with a
  size resolved against the repository root, and never hashes the
  working copy to do it.
- `download` no longer prints the `file_path exists?: ...` debug line.
"""

import io
import os
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from click.testing import CliRunner

from s3lfs.cli import cli

from .test_git_integration import TEST_BUCKET, GitRepoTestCase


class TestLsVerbose(GitRepoTestCase):
    """`ls --verbose` lists tracked files and sizes; it does not hash them."""

    def _ls_verbose(self):
        return self.runner.invoke(cli, ["ls", "--verbose"]).output

    def test_lists_every_tracked_file_with_size(self):
        self._write("data/ok.bin", "content")
        self._write("top.bin", "x" * 7)
        self.runner.invoke(cli, ["track", "data/ok.bin"])
        self.runner.invoke(cli, ["track", "top.bin"])
        out = self._ls_verbose()
        self.assertIn("data/ok.bin", out)
        # A top-level key lives in the root shard; listing only shards
        # dropped every file that is not inside a directory.
        self.assertIn("top.bin", out)
        self.assertIn("7 bytes", out)

    def test_missing_file_reports_missing_size(self):
        self._write("data/gone.bin", "content")
        self.runner.invoke(cli, ["track", "data/gone.bin"])
        Path("data/gone.bin").unlink()
        out = self._ls_verbose()
        self.assertIn("data/gone.bin", out)
        self.assertIn("Size: missing", out)

    def test_verbose_from_subdirectory_resolves_paths_at_repo_root(self):
        # `ls` hands manifest keys to get_file_status(); resolving those
        # against the CWD made every tracked file look missing from a
        # subdirectory.
        self._write("data/ok.bin", "content")
        self.runner.invoke(cli, ["track", "data/ok.bin"])
        os.chdir(Path.cwd() / "data")
        out = self._ls_verbose()
        self.assertIn("ok.bin", out)  # displayed relative to the CWD
        self.assertIn("Size: 7 bytes", out)
        self.assertNotIn("missing", out)

    def test_verbose_does_not_read_the_working_copy(self):
        # The hash cache is the only way to tell a listing from a status
        # check: if `ls` hashed, it would add an entry for a file it has
        # never seen. `s3lfs status` is the command that does that.
        self._write("data/ok.bin", "content")
        self.runner.invoke(cli, ["track", "data/ok.bin"])
        cache = Path(".s3lfs/cache")
        before = cache.read_text() if cache.exists() else ""
        self._write("data/untracked.bin", "y" * 100)
        self.runner.invoke(cli, ["ls", "--verbose"])
        after = cache.read_text() if cache.exists() else ""
        self.assertEqual(before, after)


class TestDownloadNoDebugPrint(GitRepoTestCase):
    def test_download_does_not_print_debug_line(self):
        self._write("data/dl.bin", "payload")
        self.runner.invoke(cli, ["track", "data/dl.bin"])
        Path("data/dl.bin").unlink()

        from s3lfs.core import S3LFS

        versioner = S3LFS(bucket_name=TEST_BUCKET, manifest_file=".s3_manifest.yaml")
        buf = io.StringIO()
        with redirect_stdout(buf):
            versioner.download("data/dl.bin")
        self.assertIn("data/dl.bin", buf.getvalue())  # download still reports
        self.assertNotIn("file_path exists?", buf.getvalue())


if __name__ == "__main__":
    unittest.main()
