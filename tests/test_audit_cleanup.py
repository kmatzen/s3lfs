"""Tests for the audit cleanup phase.

Pins the two behaviour changes made while deleting dead code:

- verbose `ls` prints a real status per file ("ok" / "modified" /
  "missing") instead of the blank column produced by the old
  `status = "" if cache_valid else ""` expression.
- `download` no longer prints the `file_path exists?: ...` debug line.
"""

import io
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from click.testing import CliRunner

from s3lfs.cli import cli

from .test_git_integration import TEST_BUCKET, GitRepoTestCase


class TestLsVerboseStatus(GitRepoTestCase):
    """`ls --verbose` must show a real per-file status, not a blank column."""

    def _ls_verbose(self):
        return self.runner.invoke(cli, ["ls", "--verbose"]).output

    def test_matching_file_reports_ok(self):
        self._write("data/ok.bin", "content")
        self.runner.invoke(cli, ["track", "data/ok.bin"])
        out = self._ls_verbose()
        self.assertIn("ok data/ok.bin", out)

    def test_modified_file_reports_modified(self):
        self._write("data/mod.bin", "content")
        self.runner.invoke(cli, ["track", "data/mod.bin"])
        # Change the file on disk after tracking; the hash cache records
        # the tracked bytes, so the disk copy is now modified.
        self._write("data/mod.bin", "content and more")
        out = self._ls_verbose()
        self.assertIn("modified data/mod.bin", out)
        self.assertNotIn("ok data/mod.bin", out)

    def test_missing_file_reports_missing(self):
        self._write("data/gone.bin", "content")
        self.runner.invoke(cli, ["track", "data/gone.bin"])
        Path("data/gone.bin").unlink()
        out = self._ls_verbose()
        self.assertIn("missing data/gone.bin", out)
        self.assertIn("Size: missing", out)


class TestDownloadNoDebugPrint(GitRepoTestCase):
    def test_download_does_not_print_debug_line(self):
        self._write("data/dl.bin", "payload")
        s3lfs_runner = CliRunner()
        s3lfs_runner.invoke(cli, ["track", "data/dl.bin"])
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
