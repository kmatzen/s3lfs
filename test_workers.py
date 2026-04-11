import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

import yaml
from click.testing import CliRunner

from s3lfs.core import DEFAULT_THREAD_POOL_SIZE, S3LFS, _default_workers


class TestDefaultWorkers(unittest.TestCase):
    """Tests for the _default_workers() helper."""

    def test_uses_cpu_count(self):
        with patch("os.cpu_count", return_value=8):
            self.assertEqual(_default_workers(), 12)  # min(32, 8+4)

    def test_caps_at_32(self):
        with patch("os.cpu_count", return_value=64):
            self.assertEqual(_default_workers(), 32)

    def test_fallback_when_cpu_count_none(self):
        with patch("os.cpu_count", return_value=None):
            self.assertEqual(_default_workers(), DEFAULT_THREAD_POOL_SIZE)

    def test_single_cpu(self):
        with patch("os.cpu_count", return_value=1):
            self.assertEqual(_default_workers(), 5)  # min(32, 1+4)


class TestWorkersParameter(unittest.TestCase):
    """Tests for the workers parameter on S3LFS."""

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

    def test_explicit_workers(self):
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket", workers=16)
            self.assertEqual(s3lfs.workers, 16)

    def test_default_workers_auto_detected(self):
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            with patch("os.cpu_count", return_value=8):
                s3lfs = S3LFS(bucket_name="test-bucket")
                self.assertEqual(s3lfs.workers, 12)

    def test_workers_one(self):
        """Workers=1 should be valid (sequential mode)."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket", workers=1)
            self.assertEqual(s3lfs.workers, 1)

    def test_max_concurrency_aligns_with_workers(self):
        """boto3 max_concurrency should be at least as large as workers."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket", workers=32)
            concurrency = s3lfs.config.max_concurrency  # type: ignore[attr-defined]
            self.assertGreaterEqual(concurrency, 32)

    def test_max_concurrency_minimum_preserved(self):
        """max_concurrency should not go below DEFAULT_MAX_CONCURRENCY."""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()
            s3lfs = S3LFS(bucket_name="test-bucket", workers=2)
            concurrency = s3lfs.config.max_concurrency  # type: ignore[attr-defined]
            self.assertGreaterEqual(concurrency, 15)


class TestWorkersCLI(unittest.TestCase):
    """Tests for --workers CLI flag."""

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

    def test_track_accepts_workers(self):
        from s3lfs.cli import cli

        with open("testfile.txt", "w") as f:
            f.write("hello")

        runner = CliRunner()
        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            mock_client.upload_fileobj = Mock()
            mock_client.head_object = Mock(side_effect=Exception("not found"))

            result = runner.invoke(cli, ["track", "testfile.txt", "--workers", "4"])
            self.assertNotIn("no such option", result.output.lower())

    def test_checkout_accepts_workers(self):
        from s3lfs.cli import cli

        runner = CliRunner()
        with patch("boto3.client") as mock_boto3:
            mock_boto3.return_value = Mock()

            result = runner.invoke(cli, ["checkout", "--all", "--workers", "4"])
            self.assertNotIn("no such option", result.output.lower())

    def test_cleanup_accepts_workers(self):
        from s3lfs.cli import cli

        runner = CliRunner()
        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            mock_client.list_objects_v2 = Mock(return_value={"Contents": []})

            result = runner.invoke(cli, ["cleanup", "--force", "--workers", "4"])
            self.assertNotIn("no such option", result.output.lower())

    def test_track_workers_passed_to_s3lfs(self):
        """Verify --workers value reaches the S3LFS constructor."""
        from s3lfs.cli import cli

        with open("testfile.txt", "w") as f:
            f.write("hello")

        runner = CliRunner()
        with patch("s3lfs.cli.S3LFS") as mock_cls:
            mock_instance = Mock()
            mock_cls.return_value = mock_instance
            mock_instance.manifest = {
                "bucket_name": "test-bucket",
                "repo_prefix": "test-prefix",
                "files": {},
            }
            mock_instance.track = Mock()

            runner.invoke(cli, ["track", "testfile.txt", "--workers", "24"])

            call_kwargs = mock_cls.call_args[1]
            self.assertEqual(call_kwargs["workers"], 24)

    def test_default_workers_is_none_without_flag(self):
        """Without --workers, None is passed so S3LFS auto-detects."""
        from s3lfs.cli import cli

        runner = CliRunner()
        with patch("s3lfs.cli.S3LFS") as mock_cls:
            mock_instance = Mock()
            mock_cls.return_value = mock_instance
            mock_instance.manifest = {
                "bucket_name": "test-bucket",
                "repo_prefix": "test-prefix",
                "files": {},
            }
            mock_instance.parallel_download_all = Mock()

            runner.invoke(cli, ["checkout", "--all"])

            call_kwargs = mock_cls.call_args[1]
            self.assertIsNone(call_kwargs["workers"])


if __name__ == "__main__":
    unittest.main()
