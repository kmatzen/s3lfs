import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import yaml

from s3lfs.core import S3LFS


class TestPrepareFileForUpload(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        os.makedirs(".git")

        manifest = {
            "bucket_name": "test-bucket",
            "repo_prefix": "pfx",
            "files": {},
        }
        with open(".s3_manifest.yaml", "w") as f:
            yaml.safe_dump(manifest, f)

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_returns_chunks_for_new_file(self):
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket")

            Path("data.bin").write_bytes(b"hello world")
            result = s3lfs._prepare_file_for_upload("data.bin")

            self.assertIsNotNone(result)
            manifest_key, file_hash, chunks = result
            self.assertEqual(manifest_key, "data.bin")
            self.assertGreater(len(chunks), 0)
            self.assertTrue(chunks[0]["path"].exists())

    def test_returns_none_when_hash_matches(self):
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()

            Path("data.bin").write_bytes(b"content")
            s3lfs = S3LFS(bucket_name="test-bucket")

            # Compute hash and store in manifest
            h = s3lfs.hash_file("data.bin")
            with s3lfs._lock_context():
                s3lfs.manifest["files"]["data.bin"] = h
                s3lfs.save_manifest()

            result = s3lfs._prepare_file_for_upload("data.bin")
            self.assertIsNone(result)

    def test_chunk_has_correct_s3_key(self):
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket")

            # Tiny content: gzip would inflate it, so it is stored raw
            # under its natural name -- fetchable by any S3 tool.
            Path("file.txt").write_bytes(b"data")
            result = s3lfs._prepare_file_for_upload("file.txt")

            _, file_hash, chunks = result
            expected_key = f"pfx/assets/{file_hash}/file.txt"
            self.assertEqual(chunks[0]["s3_key"], expected_key)

    def test_compressible_content_still_gets_gz_key(self):
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket")

            Path("big.txt").write_bytes(b"the same line again\n" * 20000)
            result = s3lfs._prepare_file_for_upload("big.txt")

            _, file_hash, chunks = result
            expected_key = f"pfx/assets/{file_hash}/big.txt.gz"
            self.assertEqual(chunks[0]["s3_key"], expected_key)


class TestUploadChunk(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        os.makedirs(".git")

        manifest = {
            "bucket_name": "test-bucket",
            "repo_prefix": "pfx",
            "files": {},
        }
        with open(".s3_manifest.yaml", "w") as f:
            yaml.safe_dump(manifest, f)

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_uploads_and_cleans_up(self):
        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            mock_client.upload_fileobj = Mock()

            s3lfs = S3LFS(bucket_name="test-bucket")

            chunk_path = Path(self.temp_dir) / "chunk.gz"
            chunk_path.write_bytes(b"compressed data")

            chunk_info = {
                "path": chunk_path,
                "s3_key": "pfx/assets/h/file.gz",
                "chunk_index": 0,
                "extra_args": {},
            }

            s3_key, bytes_uploaded = s3lfs._upload_chunk(chunk_info)

            self.assertEqual(s3_key, "pfx/assets/h/file.gz")
            self.assertEqual(bytes_uploaded, 15)
            mock_client.upload_fileobj.assert_called_once()
            # Chunk file should be cleaned up
            self.assertFalse(chunk_path.exists())


class TestParallelUploadChunked(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        os.makedirs(".git")

        manifest = {
            "bucket_name": "test-bucket",
            "repo_prefix": "pfx",
            "files": {},
        }
        with open(".s3_manifest.yaml", "w") as f:
            yaml.safe_dump(manifest, f)

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_empty_file_list(self):
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket")
            s3lfs.parallel_upload_chunked([], silence=True)

    def test_uploads_multiple_files(self):
        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            mock_client.upload_fileobj = Mock()
            mock_client.head_object.return_value = {"ContentLength": 10}

            s3lfs = S3LFS(bucket_name="test-bucket")

            Path("a.txt").write_bytes(b"file a content")
            Path("b.txt").write_bytes(b"file b content")

            s3lfs.parallel_upload_chunked(["a.txt", "b.txt"], silence=True)

            # Both files should have been uploaded
            self.assertEqual(mock_client.upload_fileobj.call_count, 2)

            # Manifest should be updated
            self.assertIn("a.txt", s3lfs.manifest["files"])
            self.assertIn("b.txt", s3lfs.manifest["files"])

    def test_skips_unchanged_files(self):
        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            mock_client.upload_fileobj = Mock()

            s3lfs = S3LFS(bucket_name="test-bucket")

            Path("a.txt").write_bytes(b"content")
            h = s3lfs.hash_file("a.txt")
            with s3lfs._lock_context():
                s3lfs.manifest["files"]["a.txt"] = h
                s3lfs.save_manifest()

            s3lfs.parallel_upload_chunked(["a.txt"], silence=True)

            # No upload should have happened
            mock_client.upload_fileobj.assert_not_called()


if __name__ == "__main__":
    unittest.main()
