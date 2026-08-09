import gzip
import hashlib
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import yaml

from s3lfs.core import S3LFS


class TestDiscoverChunksForFile(unittest.TestCase):
    """Tests for _discover_chunks_for_file method."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        os.makedirs(".git")

        manifest = {
            "bucket_name": "test-bucket",
            "repo_prefix": "test-prefix",
            "files": {"data/file.bin": "abc123"},
        }
        with open(".s3_manifest.yaml", "w") as f:
            yaml.safe_dump(manifest, f)

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_single_unchunked_file(self):
        """A file stored as a single object produces one chunk entry."""
        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            key = "test-prefix/assets/abc123/data/file.bin.gz"
            mock_client.list_objects_v2.return_value = {
                "Contents": [{"Key": key, "Size": 10}]
            }

            s3lfs = S3LFS(bucket_name="test-bucket")
            chunks = s3lfs._discover_chunks_for_file("data/file.bin", "abc123")

            self.assertEqual(len(chunks), 1)
            self.assertFalse(chunks[0]["is_chunked"])
            self.assertEqual(chunks[0]["chunk_index"], 0)
            self.assertEqual(chunks[0]["num_chunks"], 1)
            self.assertTrue(chunks[0]["compressed"])

    def test_single_raw_file(self):
        """An uncompressed object is discovered and flagged as raw."""
        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            key = "test-prefix/assets/abc123/data/file.bin"
            mock_client.list_objects_v2.return_value = {
                "Contents": [{"Key": key, "Size": 10}]
            }

            s3lfs = S3LFS(bucket_name="test-bucket")
            chunks = s3lfs._discover_chunks_for_file("data/file.bin", "abc123")

            self.assertEqual(len(chunks), 1)
            self.assertEqual(chunks[0]["s3_key"], key)
            self.assertFalse(chunks[0]["compressed"])

    def test_missing_object_is_loud(self):
        """No stored object must raise, not fabricate a key that 404s."""
        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            mock_client.list_objects_v2.return_value = {}

            s3lfs = S3LFS(bucket_name="test-bucket")
            with self.assertRaises(RuntimeError):
                s3lfs._discover_chunks_for_file("data/file.bin", "abc123")

    def test_chunked_file(self):
        """A file stored as multiple chunks produces one entry per chunk."""
        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            s3_key = "test-prefix/assets/abc123/data/file.bin.gz"
            mock_client.list_objects_v2.return_value = {
                "Contents": [
                    {"Key": f"{s3_key}.chunk0"},
                    {"Key": f"{s3_key}.chunk1"},
                    {"Key": f"{s3_key}.chunk2"},
                ]
            }

            s3lfs = S3LFS(bucket_name="test-bucket")
            chunks = s3lfs._discover_chunks_for_file("data/file.bin", "abc123")

            self.assertEqual(len(chunks), 3)
            self.assertEqual(chunks[0]["num_chunks"], 3)
            for i, chunk in enumerate(chunks):
                self.assertTrue(chunk["is_chunked"])
                self.assertEqual(chunk["chunk_index"], i)


class TestDownloadChunk(unittest.TestCase):
    """Tests for _download_chunk method."""

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

    def test_downloads_to_target_path(self):
        """Chunk is downloaded to the specified target path."""
        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client

            def fake_download(Bucket, Key, Fileobj):
                Fileobj.write(b"compressed data here")

            mock_client.download_fileobj.side_effect = fake_download

            s3lfs = S3LFS(bucket_name="test-bucket")
            target = Path(self.temp_dir) / "chunk.gz"
            chunk_info = {
                "manifest_key": "data/file.bin",
                "file_hash": "abc123",
                "s3_key": "test-prefix/assets/abc123/data/file.bin.gz",
                "chunk_index": 0,
                "is_chunked": False,
                "num_chunks": 1,
            }

            mk, idx, path, size, is_chunked, num = s3lfs._download_chunk(
                chunk_info, target
            )

            self.assertEqual(mk, "data/file.bin")
            self.assertEqual(idx, 0)
            self.assertEqual(path, target)
            self.assertGreater(size, 0)
            self.assertTrue(target.exists())

    def test_returns_correct_bytes(self):
        """Returns the number of bytes written."""
        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client

            content = b"x" * 1024

            def fake_download(Bucket, Key, Fileobj):
                Fileobj.write(content)

            mock_client.download_fileobj.side_effect = fake_download

            s3lfs = S3LFS(bucket_name="test-bucket")
            target = Path(self.temp_dir) / "chunk.gz"
            chunk_info = {
                "manifest_key": "file.bin",
                "file_hash": "h",
                "s3_key": "key",
                "chunk_index": 0,
                "is_chunked": False,
                "num_chunks": 1,
            }

            _, _, _, size, _, _ = s3lfs._download_chunk(chunk_info, target)
            self.assertEqual(size, 1024)


class TestFinalizeFile(unittest.TestCase):
    """Tests for _finalize_file method."""

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

    def test_single_chunk_decompresses(self):
        """Single unchunked file is decompressed directly."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()

            s3lfs = S3LFS(bucket_name="test-bucket")

            chunk_path = Path(self.temp_dir) / "chunk0.gz"
            with gzip.open(chunk_path, "wb") as f:
                f.write(b"hello world")

            s3lfs._finalize_file("output.txt", [chunk_path], is_chunked=False)

            output_path = s3lfs.path_resolver.to_filesystem_path("output.txt")
            self.assertTrue(output_path.exists())
            self.assertEqual(output_path.read_bytes(), b"hello world")

    def test_multi_chunk_merges_and_decompresses(self):
        """Multiple chunks are merged then decompressed."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()

            s3lfs = S3LFS(bucket_name="test-bucket")

            full_content = b"hello world from chunked file"
            compressed = gzip.compress(full_content)
            mid = len(compressed) // 2

            chunk0 = Path(self.temp_dir) / "c0.gz"
            chunk1 = Path(self.temp_dir) / "c1.gz"
            chunk0.write_bytes(compressed[:mid])
            chunk1.write_bytes(compressed[mid:])

            s3lfs._finalize_file("output.txt", [chunk0, chunk1], is_chunked=True)

            output_path = s3lfs.path_resolver.to_filesystem_path("output.txt")
            self.assertTrue(output_path.exists())
            self.assertEqual(output_path.read_bytes(), full_content)

    def test_cleans_up_chunk_files(self):
        """Chunk temp files are deleted after finalization."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()

            s3lfs = S3LFS(bucket_name="test-bucket")

            chunk_path = Path(self.temp_dir) / "to_delete.gz"
            with gzip.open(chunk_path, "wb") as f:
                f.write(b"data")

            s3lfs._finalize_file("out.txt", [chunk_path], is_chunked=False)
            self.assertFalse(chunk_path.exists())


class TestParallelDownloadChunked(unittest.TestCase):
    """Tests for parallel_download_chunked method."""

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

    def test_empty_file_list(self):
        """Empty file list prints message and returns."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()

            s3lfs = S3LFS(bucket_name="test-bucket")
            s3lfs.parallel_download_chunked([], silence=True)

    def test_downloads_all_chunks_dynamically(self):
        """Discovery and download run in the same pool."""
        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client

            content = b"test content"
            compressed = gzip.compress(content)
            # Finalization verifies the reassembled file against the manifest
            # hash, so the fixture must carry the real digest.
            digest = hashlib.sha256(content).hexdigest()

            # Discovery lists the stem; serve each file's .gz object.
            def fake_list(Bucket=None, Prefix=None, **kw):
                return {"Contents": [{"Key": Prefix + ".gz", "Size": 100}]}

            mock_client.list_objects_v2.side_effect = fake_list

            def fake_download(Bucket, Key, Fileobj):
                Fileobj.write(compressed)

            mock_client.download_fileobj.side_effect = fake_download

            s3lfs = S3LFS(bucket_name="test-bucket")
            s3lfs.parallel_download_chunked(
                [("file1.txt", digest), ("file2.txt", digest)],
                silence=True,
            )

            self.assertEqual(mock_client.download_fileobj.call_count, 2)

            p1 = s3lfs.path_resolver.to_filesystem_path("file1.txt")
            p2 = s3lfs.path_resolver.to_filesystem_path("file2.txt")
            self.assertTrue(p1.exists())
            self.assertTrue(p2.exists())

    def test_finalizes_when_all_chunks_arrive(self):
        """A chunked file is finalized once all its chunks are downloaded."""
        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client

            full_content = b"chunked file data"
            compressed = gzip.compress(full_content)
            mid = len(compressed) // 2
            part0 = compressed[:mid]
            part1 = compressed[mid:]

            # Finalization verifies the reassembled file against the manifest
            # hash, so the fixture must carry the real digest.
            digest = hashlib.sha256(full_content).hexdigest()
            s3_key = f"test-prefix/assets/{digest}/big.bin.gz"
            mock_client.list_objects_v2.return_value = {
                "Contents": [
                    {"Key": f"{s3_key}.chunk0"},
                    {"Key": f"{s3_key}.chunk1"},
                ]
            }

            call_count = [0]

            def fake_download(Bucket, Key, Fileobj):
                if "chunk0" in Key:
                    Fileobj.write(part0)
                elif "chunk1" in Key:
                    Fileobj.write(part1)
                call_count[0] += 1

            mock_client.download_fileobj.side_effect = fake_download

            s3lfs = S3LFS(bucket_name="test-bucket")
            s3lfs.parallel_download_chunked([("big.bin", digest)], silence=True)

            output = s3lfs.path_resolver.to_filesystem_path("big.bin")
            self.assertTrue(output.exists())
            self.assertEqual(output.read_bytes(), full_content)
            self.assertEqual(call_count[0], 2)


if __name__ == "__main__":
    unittest.main()
