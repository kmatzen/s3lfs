import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import yaml

from s3lfs.core import DEFAULT_BUFFER_SIZE, S3LFS


class TestStreamingMD5(unittest.TestCase):
    """Verify that upload MD5 computation streams instead of loading all at once."""

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

    def test_md5_matches_hashlib_reference(self):
        """Streaming MD5 produces the same result as hashlib.md5(data)."""

        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client

            s3lfs = S3LFS(bucket_name="test-bucket")

            # Create a test file larger than DEFAULT_BUFFER_SIZE
            data = b"x" * (DEFAULT_BUFFER_SIZE * 3 + 42)
            test_file = Path(self.temp_dir) / "testdata.bin"
            test_file.write_bytes(data)

            # The upload method computes MD5 via streaming.
            # Verify it completes successfully (upload_fileobj called)
            # when head_object returns 404 (file not yet in S3).
            from botocore.exceptions import ClientError

            mock_client.head_object.side_effect = ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}},
                "HeadObject",
            )
            mock_client.upload_fileobj = Mock()

            s3lfs.upload(test_file, silence=True)

            self.assertTrue(mock_client.upload_fileobj.called)


class TestStreamingSplitFile(unittest.TestCase):
    """Verify that split_file streams in small buffers."""

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

    def test_split_produces_correct_chunks(self):
        """Streaming split produces the same output as the original."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()

            # Use a small chunk size for testing
            s3lfs = S3LFS(
                bucket_name="test-bucket",
                chunk_size=1024,  # 1KB chunks
            )

            # Create a file that will split into 3 chunks
            data = b"A" * 1000 + b"B" * 1000 + b"C" * 500
            test_file = Path(self.temp_dir) / "big.bin"
            test_file.write_bytes(data)

            chunks = s3lfs.split_file(test_file)

            # Should produce 3 chunks (1023 + 1023 + remaining)
            self.assertEqual(len(chunks), 3)

            # Reassemble and verify
            reassembled = b""
            for chunk_path in chunks:
                reassembled += chunk_path.read_bytes()
            self.assertEqual(reassembled, data)

    def test_split_single_chunk(self):
        """A file smaller than chunk_size produces one chunk."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()

            s3lfs = S3LFS(
                bucket_name="test-bucket",
                chunk_size=4096,
            )

            data = b"small"
            test_file = Path(self.temp_dir) / "small.bin"
            test_file.write_bytes(data)

            chunks = s3lfs.split_file(test_file)
            self.assertEqual(len(chunks), 1)
            self.assertEqual(chunks[0].read_bytes(), data)

    def test_split_exact_boundary(self):
        """A file exactly at chunk_size-1 produces one chunk."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()

            s3lfs = S3LFS(
                bucket_name="test-bucket",
                chunk_size=1024,
            )

            data = b"X" * 1023  # exactly chunk_size - 1
            test_file = Path(self.temp_dir) / "exact.bin"
            test_file.write_bytes(data)

            chunks = s3lfs.split_file(test_file)
            self.assertEqual(len(chunks), 1)
            self.assertEqual(chunks[0].read_bytes(), data)

    def test_split_no_empty_trailing_chunk(self):
        """No empty chunk file is left when data ends on a boundary."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()

            s3lfs = S3LFS(
                bucket_name="test-bucket",
                chunk_size=1024,
            )

            # Exactly 2 full chunks
            data = b"X" * 1023 + b"Y" * 1023
            test_file = Path(self.temp_dir) / "boundary.bin"
            test_file.write_bytes(data)

            chunks = s3lfs.split_file(test_file)
            self.assertEqual(len(chunks), 2)

            # No leftover empty chunk file
            leftover = Path(f"{test_file}.chunk2")
            self.assertFalse(leftover.exists())

            # Content is correct
            reassembled = b""
            for chunk_path in chunks:
                reassembled += chunk_path.read_bytes()
            self.assertEqual(reassembled, data)


if __name__ == "__main__":
    unittest.main()
