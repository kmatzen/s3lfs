import gzip
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import yaml

from s3lfs.core import S3LFS


class TestPigzCompression(unittest.TestCase):
    """Tests for pigz parallel compression support."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        os.makedirs(".git")

        manifest = {
            "bucket_name": "test-bucket",
            "repo_prefix": "test-prefix",
            "files": {},
        }
        with open(".s3_manifest.yaml", "w") as f:
            yaml.safe_dump(manifest, f)

        self.test_data = b"hello world " * 1000
        self.test_file = Path(self.temp_dir) / "testdata.bin"
        self.test_file.write_bytes(self.test_data)

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_auto_prefers_pigz_over_gzip(self):
        """When pigz is available, auto mode selects it."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket")

            with patch("shutil.which") as mock_which:
                mock_which.side_effect = lambda cmd: (
                    "/usr/bin/pigz" if cmd == "pigz" else None
                )

                with patch.object(
                    s3lfs, "_compress_file_pigz", return_value=Path("x.gz")
                ) as mock_pigz:
                    s3lfs.compress_file(self.test_file, method="auto")
                    mock_pigz.assert_called_once_with(self.test_file)

    def test_auto_falls_back_to_gzip(self):
        """When pigz is not available, auto mode falls back to gzip CLI."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket")

            with patch("shutil.which") as mock_which:
                mock_which.side_effect = lambda cmd: (
                    "/usr/bin/gzip" if cmd == "gzip" else None
                )

                with patch.object(
                    s3lfs, "_compress_file_cli", return_value=Path("x.gz")
                ) as mock_cli:
                    s3lfs.compress_file(self.test_file, method="auto")
                    mock_cli.assert_called_once_with(self.test_file)

    def test_auto_falls_back_to_python(self):
        """When no CLI tools available, auto mode uses Python gzip."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket")

            with patch("shutil.which", return_value=None):
                with patch.object(
                    s3lfs, "_compress_file_python", return_value=Path("x.gz")
                ) as mock_py:
                    s3lfs.compress_file(self.test_file, method="auto")
                    mock_py.assert_called_once_with(self.test_file)

    @unittest.skipUnless(shutil.which("pigz"), "pigz not installed")
    def test_pigz_produces_valid_gzip(self):
        """pigz output is decompressible by standard gzip."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket")

            compressed = s3lfs._compress_file_pigz(self.test_file)
            self.assertTrue(compressed.exists())

            decompressed = gzip.decompress(compressed.read_bytes())
            self.assertEqual(decompressed, self.test_data)

    @unittest.skipUnless(shutil.which("pigz"), "pigz not installed")
    def test_pigz_roundtrip(self):
        """Compress with pigz, decompress with pigz."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket")

            compressed = s3lfs._compress_file_pigz(self.test_file)
            output = Path(self.temp_dir) / "roundtrip.bin"
            s3lfs._decompress_file_pigz(compressed, output)

            self.assertEqual(output.read_bytes(), self.test_data)

    @unittest.skipUnless(shutil.which("pigz"), "pigz not installed")
    def test_pigz_compress_gzip_decompress(self):
        """Compress with pigz, decompress with standard gzip CLI."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket")

            compressed = s3lfs._compress_file_pigz(self.test_file)
            output = Path(self.temp_dir) / "cross.bin"
            s3lfs._decompress_file_cli(compressed, output)

            self.assertEqual(output.read_bytes(), self.test_data)

    def test_explicit_pigz_method(self):
        """compress_file(method='pigz') calls _compress_file_pigz."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket")

            with patch.object(
                s3lfs, "_compress_file_pigz", return_value=Path("x.gz")
            ) as mock_pigz:
                s3lfs.compress_file(self.test_file, method="pigz")
                mock_pigz.assert_called_once()


class TestPigzDecompression(unittest.TestCase):
    """Tests for pigz parallel decompression support."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        os.makedirs(".git")

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

    def test_decompress_auto_prefers_pigz(self):
        """When pigz is available, auto decompression selects it."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket")

            compressed = Path(self.temp_dir) / "test.gz"
            with gzip.open(compressed, "wb") as f:
                f.write(b"data")
            output = Path(self.temp_dir) / "out.bin"

            with patch("shutil.which") as mock_which:
                mock_which.side_effect = lambda cmd: (
                    "/usr/bin/pigz" if cmd == "pigz" else None
                )

                with patch.object(
                    s3lfs,
                    "_decompress_file_pigz",
                    return_value=output,
                ) as mock_pigz:
                    s3lfs.decompress_file(compressed, output, method="auto")
                    mock_pigz.assert_called_once()

    def test_explicit_pigz_decompress_method(self):
        """decompress_file(method='pigz') calls _decompress_file_pigz."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket")

            compressed = Path(self.temp_dir) / "test.gz"
            with gzip.open(compressed, "wb") as f:
                f.write(b"data")
            output = Path(self.temp_dir) / "out.bin"

            with patch.object(
                s3lfs,
                "_decompress_file_pigz",
                return_value=output,
            ) as mock_pigz:
                s3lfs.decompress_file(compressed, output, method="pigz")
                mock_pigz.assert_called_once()


if __name__ == "__main__":
    unittest.main()
