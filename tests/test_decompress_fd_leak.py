"""The gzip/pigz CLI decompress helpers must close the output file handle.

`_decompress_file_cli` / `_decompress_file_pigz` passed
`stdout=open(output_path, "wb")` straight into subprocess.run without
keeping a reference, so the fd leaked on every call — and `check=True`
made the failure path leak too, with a partial file behind it. A checkout
of thousands of compressed assets died with "Too many open files".
"""

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from s3lfs.core import S3LFS


class TestDecompressClosesOutputHandle(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.manifest = self.temp_dir / ".s3_manifest.yaml"
        self.manifest.write_text("bucket_name: b\nrepo_prefix: p\nfiles: {}\n")
        self.s3lfs = S3LFS(
            bucket_name="b",
            manifest_file=str(self.manifest),
            temp_dir=str(self.temp_dir / "tmp"),
            s3_factory=lambda no_sign: Mock(),
        )
        self.compressed = self.temp_dir / "in.gz"
        self.compressed.write_bytes(b"\x1f\x8bstub")
        self.output = self.temp_dir / "out.bin"

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _run_capture_handle(self, method, fail):
        captured = {}

        def fake_run(cmd, stdout=None, **kwargs):
            captured["handle"] = stdout
            if fail:
                raise subprocess.CalledProcessError(1, cmd)
            return subprocess.CompletedProcess(cmd, 0)

        with patch("s3lfs.core.subprocess.run", side_effect=fake_run):
            if fail:
                with self.assertRaises(subprocess.CalledProcessError):
                    self.s3lfs.decompress_file(
                        self.compressed, self.output, method=method
                    )
            else:
                self.s3lfs.decompress_file(self.compressed, self.output, method=method)
        return captured["handle"]

    def test_cli_closes_handle_on_success(self):
        handle = self._run_capture_handle("cli", fail=False)
        self.assertTrue(handle.closed, "output handle left open after success")

    def test_cli_closes_handle_on_failure(self):
        handle = self._run_capture_handle("cli", fail=True)
        self.assertTrue(handle.closed, "output handle leaked on the failure path")

    def test_pigz_closes_handle_on_success(self):
        handle = self._run_capture_handle("pigz", fail=False)
        self.assertTrue(handle.closed, "output handle left open after success")

    def test_pigz_closes_handle_on_failure(self):
        handle = self._run_capture_handle("pigz", fail=True)
        self.assertTrue(handle.closed, "output handle leaked on the failure path")


if __name__ == "__main__":
    unittest.main()
