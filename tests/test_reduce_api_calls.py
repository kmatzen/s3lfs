import gzip
import hashlib
import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

import yaml

from s3lfs.core import S3LFS


class TestDownloadUsesListSizes(unittest.TestCase):
    """Verify download() gets sizes from list_objects_v2 for chunked files."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        os.makedirs(".git")

        manifest = {
            "bucket_name": "test-bucket",
            "repo_prefix": "pfx",
            "files": {"data.bin": "hash1"},
        }
        with open(".s3_manifest.yaml", "w") as f:
            yaml.safe_dump(manifest, f)

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_chunked_download_no_head_object(self):
        """For chunked files, sizes come from list_objects_v2, not head_object."""
        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client

            full_data = b"chunked file content here"
            digest = hashlib.sha256(full_data).hexdigest()
            compressed = gzip.compress(full_data)
            mid = len(compressed) // 2

            s3_key = f"pfx/assets/{digest}/data.bin.gz"
            mock_client.list_objects_v2.return_value = {
                "Contents": [
                    {"Key": f"{s3_key}.chunk0", "Size": mid},
                    {"Key": f"{s3_key}.chunk1", "Size": len(compressed) - mid},
                ]
            }

            call_count = [0]

            def fake_download(Bucket, Key, Fileobj, **kwargs):
                if "chunk0" in Key:
                    Fileobj.write(compressed[:mid])
                else:
                    Fileobj.write(compressed[mid:])
                call_count[0] += 1

            mock_client.download_fileobj.side_effect = fake_download

            s3lfs = S3LFS(bucket_name="test-bucket")
            s3lfs.download("data.bin", silence=True, expected_hash=digest)

            # head_object should NOT have been called for chunked files
            mock_client.head_object.assert_not_called()

    def test_unchunked_download_no_head_object(self):
        """Sizes come from the discovery listing; head_object is never
        needed, even for unchunked files."""
        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client

            data = b"single file"
            compressed = gzip.compress(data)

            def fake_list(Bucket=None, Prefix=None, **kw):
                return {"Contents": [{"Key": Prefix + ".gz", "Size": len(compressed)}]}

            mock_client.list_objects_v2.side_effect = fake_list

            def fake_download(Bucket, Key, Fileobj, **kwargs):
                Fileobj.write(compressed)

            mock_client.download_fileobj.side_effect = fake_download

            s3lfs = S3LFS(bucket_name="test-bucket")
            s3lfs.download(
                "data.bin",
                silence=True,
                expected_hash=hashlib.sha256(data).hexdigest(),
            )

            self.assertEqual(mock_client.head_object.call_count, 0)


if __name__ == "__main__":
    unittest.main()
