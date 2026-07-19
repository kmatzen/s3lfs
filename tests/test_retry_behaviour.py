import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml
from botocore.exceptions import ClientError

from s3lfs.core import S3LFS, _is_retryable, retry


def _client_error(code, status=400):
    response: dict = {
        "Error": {"Code": code, "Message": code},
        "ResponseMetadata": {"HTTPStatusCode": status},
    }
    return ClientError(response, "PutObject")  # type: ignore[arg-type]


class TestRetryClassification(unittest.TestCase):
    """Retrying an error that cannot succeed only delays the real message."""

    def test_permission_errors_are_not_retryable(self):
        for code in ("AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"):
            self.assertFalse(_is_retryable(_client_error(code, 403)), code)

    def test_missing_bucket_is_not_retryable(self):
        self.assertFalse(_is_retryable(_client_error("NoSuchBucket", 404)))

    def test_throttling_is_retryable(self):
        self.assertTrue(_is_retryable(_client_error("SlowDown", 503)))
        self.assertTrue(_is_retryable(_client_error("RequestTimeout", 400)))

    def test_server_errors_are_retryable(self):
        self.assertTrue(_is_retryable(_client_error("InternalError", 500)))

    def test_non_client_errors_are_retryable(self):
        self.assertTrue(_is_retryable(OSError("connection reset")))


class TestRetryMechanics(unittest.TestCase):
    def test_gives_up_immediately_on_non_retryable(self):
        calls = []

        @retry(3, (ClientError,))
        def always_denied():
            calls.append(1)
            raise _client_error("AccessDenied", 403)

        with self.assertRaises(ClientError):
            always_denied()

        self.assertEqual(len(calls), 1, "a non-retryable error was retried")

    def test_retries_then_succeeds(self):
        calls = []

        @retry(3, (ClientError,))
        def flaky():
            calls.append(1)
            if len(calls) < 3:
                raise _client_error("InternalError", 500)
            return "ok"

        with patch("time.sleep"):
            self.assertEqual(flaky(), "ok")
        self.assertEqual(len(calls), 3)

    def test_backoff_is_jittered(self):
        """Fixed backoff makes every worker retry in lockstep."""
        delays: list = []

        @retry(4, (ClientError,))
        def always_fails():
            raise _client_error("InternalError", 500)

        with patch("time.sleep", side_effect=delays.append):
            with self.assertRaises(ClientError):
                always_fails()

        self.assertEqual(len(delays), 3)
        # Full jitter: each delay lies in [0, 2**(attempt+1)].
        for attempt, delay in enumerate(delays):
            self.assertGreaterEqual(delay, 0)
            self.assertLessEqual(delay, 2 ** (attempt + 1))
        # Not the old fixed schedule.
        self.assertNotEqual(delays, [2, 4, 8])

    def test_preserves_function_metadata(self):
        @retry(2, (ClientError,))
        def named_function():
            return 1

        self.assertEqual(named_function.__name__, "named_function")


class TestChunkTransferRetries(unittest.TestCase):
    """The chunked pipeline handles the largest files and had no retry."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        os.makedirs(".git")
        with open(".s3_manifest.yaml", "w") as f:
            yaml.safe_dump(
                {
                    "bucket_name": "test-bucket",
                    "repo_prefix": "test-prefix",
                    "files": {},
                },
                f,
            )

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _s3lfs(self, client):
        with patch("boto3.client"):
            return S3LFS(
                bucket_name="test-bucket",
                s3_factory=lambda no_sign: client,
            )

    def test_upload_chunk_survives_a_transient_failure(self):
        """The retry must still find its input file.

        The chunk was previously deleted in a finally that ran on the first
        failure, so any retry could only fail with FileNotFoundError.
        """
        attempts: list = []
        uploaded: dict = {}

        client = MagicMock()

        def upload_fileobj(fileobj, bucket, key, **kwargs):
            attempts.append(key)
            if len(attempts) == 1:
                raise _client_error("InternalError", 500)
            uploaded[key] = fileobj.read()

        client.upload_fileobj.side_effect = upload_fileobj
        s3lfs = self._s3lfs(client)

        chunk_path = Path("chunk0.gz")
        chunk_path.write_bytes(b"chunk payload")
        chunk_info = {
            "path": chunk_path,
            "s3_key": "test-prefix/assets/h/f.gz.chunk0",
            "chunk_index": 0,
            "extra_args": {},
        }

        with patch("time.sleep"):
            key, size = s3lfs._upload_chunk(chunk_info)

        self.assertEqual(len(attempts), 2, "the chunk upload was not retried")
        self.assertEqual(uploaded[key], b"chunk payload")
        self.assertFalse(chunk_path.exists(), "chunk file should be cleaned up once")

    def test_upload_chunk_cleans_up_after_permanent_failure(self):
        client = MagicMock()
        client.upload_fileobj.side_effect = _client_error("AccessDenied", 403)
        s3lfs = self._s3lfs(client)

        chunk_path = Path("chunk0.gz")
        chunk_path.write_bytes(b"chunk payload")
        chunk_info = {
            "path": chunk_path,
            "s3_key": "test-prefix/assets/h/f.gz.chunk0",
            "chunk_index": 0,
            "extra_args": {},
        }

        with self.assertRaises(ClientError):
            s3lfs._upload_chunk(chunk_info)

        self.assertFalse(chunk_path.exists(), "chunk file leaked after failure")

    def test_download_chunk_survives_a_transient_failure(self):
        attempts: list = []
        client = MagicMock()

        def download_fileobj(Bucket=None, Key=None, Fileobj=None, **kwargs):
            attempts.append(Key)
            if len(attempts) == 1:
                raise _client_error("InternalError", 500)
            Fileobj.write(b"downloaded")

        client.download_fileobj.side_effect = download_fileobj
        s3lfs = self._s3lfs(client)

        target = Path("out.gz")
        chunk_info = {
            "manifest_key": "f.bin",
            "s3_key": "test-prefix/assets/h/f.gz",
            "chunk_index": 0,
            "is_chunked": False,
            "num_chunks": 1,
        }

        with patch("time.sleep"):
            s3lfs._download_chunk(chunk_info, target)

        self.assertEqual(len(attempts), 2, "the chunk download was not retried")
        self.assertEqual(target.read_bytes(), b"downloaded")


if __name__ == "__main__":
    unittest.main()
