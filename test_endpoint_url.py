import os
import tempfile
import unittest
from unittest.mock import Mock, patch

import yaml

from s3lfs.core import S3LFS


class TestEndpointUrl(unittest.TestCase):
    """Tests for custom S3 endpoint URL functionality."""

    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)

        # Create a git repository
        os.makedirs(".git")

        # Create manifest file
        self.manifest_file = ".s3_manifest.yaml"
        manifest = {
            "bucket_name": "test-bucket",
            "repo_prefix": "test-prefix",
            "files": {},
        }
        with open(self.manifest_file, "w") as f:
            yaml.safe_dump(manifest, f)

    def tearDown(self):
        """Clean up test environment."""
        os.chdir(self.original_cwd)
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_endpoint_url_passed_to_boto3_client(self):
        """Test that endpoint_url is passed through to boto3.client."""
        with patch("boto3.client") as mock_boto3_client:
            mock_client = Mock()
            mock_boto3_client.return_value = mock_client

            s3lfs = S3LFS(
                bucket_name="test-bucket",
                repo_prefix="test-prefix",
                endpoint_url="http://localhost:9000",
            )

            s3lfs._get_s3_client()

            mock_boto3_client.assert_called_once()
            call_args = mock_boto3_client.call_args
            self.assertEqual(call_args[1]["endpoint_url"], "http://localhost:9000")

    def test_endpoint_url_with_unsigned_requests(self):
        """Test that endpoint_url works with --no-sign-request."""
        with patch("boto3.client") as mock_boto3_client:
            mock_client = Mock()
            mock_boto3_client.return_value = mock_client

            s3lfs = S3LFS(
                bucket_name="test-bucket",
                repo_prefix="test-prefix",
                endpoint_url="http://localhost:9000",
                no_sign_request=True,
            )

            s3lfs._get_s3_client()

            mock_boto3_client.assert_called_once()
            call_args = mock_boto3_client.call_args
            self.assertEqual(call_args[1]["endpoint_url"], "http://localhost:9000")

    def test_endpoint_url_with_acceleration(self):
        """Test that endpoint_url works alongside transfer acceleration."""
        with patch("boto3.client") as mock_boto3_client:
            mock_client = Mock()
            mock_boto3_client.return_value = mock_client

            s3lfs = S3LFS(
                bucket_name="test-bucket",
                repo_prefix="test-prefix",
                endpoint_url="http://localhost:9000",
                use_acceleration=True,
            )

            s3lfs._get_s3_client()

            mock_boto3_client.assert_called_once()
            call_args = mock_boto3_client.call_args
            self.assertEqual(call_args[1]["endpoint_url"], "http://localhost:9000")
            config = call_args[1]["config"]
            self.assertTrue(config.s3["use_accelerate_endpoint"])

    def test_no_endpoint_url_by_default(self):
        """Test that endpoint_url is not passed when not specified."""
        with patch("boto3.client") as mock_boto3_client:
            mock_client = Mock()
            mock_boto3_client.return_value = mock_client

            s3lfs = S3LFS(
                bucket_name="test-bucket",
                repo_prefix="test-prefix",
            )

            s3lfs._get_s3_client()

            mock_boto3_client.assert_called_once()
            call_args = mock_boto3_client.call_args
            self.assertNotIn("endpoint_url", call_args[1])

    def test_endpoint_url_stored_in_manifest(self):
        """Test that endpoint_url is persisted to the manifest."""
        with patch("boto3.client") as mock_boto3_client:
            mock_client = Mock()
            mock_boto3_client.return_value = mock_client

            S3LFS(
                bucket_name="test-bucket",
                repo_prefix="test-prefix",
                endpoint_url="http://localhost:9000",
            )

            # Read manifest and check endpoint_url is stored
            with open(self.manifest_file, "r") as f:
                manifest = yaml.safe_load(f)

            self.assertEqual(manifest["endpoint_url"], "http://localhost:9000")

    def test_endpoint_url_loaded_from_manifest(self):
        """Test that endpoint_url is loaded from manifest when not passed as parameter."""
        # Write manifest with endpoint_url
        manifest = {
            "bucket_name": "test-bucket",
            "repo_prefix": "test-prefix",
            "endpoint_url": "http://minio.internal:9000",
            "files": {},
        }
        with open(self.manifest_file, "w") as f:
            yaml.safe_dump(manifest, f)

        with patch("boto3.client") as mock_boto3_client:
            mock_client = Mock()
            mock_boto3_client.return_value = mock_client

            s3lfs = S3LFS(
                bucket_name="test-bucket",
                repo_prefix="test-prefix",
            )

            self.assertEqual(s3lfs.endpoint_url, "http://minio.internal:9000")

            s3lfs._get_s3_client()

            call_args = mock_boto3_client.call_args
            self.assertEqual(call_args[1]["endpoint_url"], "http://minio.internal:9000")

    def test_endpoint_url_parameter_overrides_manifest(self):
        """Test that endpoint_url parameter takes precedence over manifest value."""
        # Write manifest with one endpoint_url
        manifest = {
            "bucket_name": "test-bucket",
            "repo_prefix": "test-prefix",
            "endpoint_url": "http://old-endpoint:9000",
            "files": {},
        }
        with open(self.manifest_file, "w") as f:
            yaml.safe_dump(manifest, f)

        with patch("boto3.client") as mock_boto3_client:
            mock_client = Mock()
            mock_boto3_client.return_value = mock_client

            s3lfs = S3LFS(
                bucket_name="test-bucket",
                repo_prefix="test-prefix",
                endpoint_url="http://new-endpoint:9000",
            )

            self.assertEqual(s3lfs.endpoint_url, "http://new-endpoint:9000")

            # Manifest should be updated with the new value
            with open(self.manifest_file, "r") as f:
                saved_manifest = yaml.safe_load(f)
            self.assertEqual(saved_manifest["endpoint_url"], "http://new-endpoint:9000")

    def test_endpoint_url_stored_by_initialize_repo(self):
        """Test that initialize_repo persists endpoint_url to manifest."""
        # Start with a clean manifest (no endpoint_url)
        os.remove(self.manifest_file)

        with patch("boto3.client") as mock_boto3_client:
            mock_client = Mock()
            mock_boto3_client.return_value = mock_client

            s3lfs = S3LFS(
                bucket_name="test-bucket",
                repo_prefix="test-prefix",
                endpoint_url="https://r2.cloudflarestorage.com",
            )
            s3lfs.initialize_repo()

            with open(self.manifest_file, "r") as f:
                manifest = yaml.safe_load(f)

            self.assertEqual(
                manifest["endpoint_url"], "https://r2.cloudflarestorage.com"
            )

    def test_endpoint_url_not_stored_when_none(self):
        """Test that endpoint_url key is not added to manifest when not set."""
        with patch("boto3.client") as mock_boto3_client:
            mock_client = Mock()
            mock_boto3_client.return_value = mock_client

            s3lfs = S3LFS(
                bucket_name="test-bucket",
                repo_prefix="test-prefix",
            )
            s3lfs.initialize_repo()

            with open(self.manifest_file, "r") as f:
                manifest = yaml.safe_load(f)

            self.assertNotIn("endpoint_url", manifest)


class TestEndpointUrlCli(unittest.TestCase):
    """Tests for endpoint URL CLI integration."""

    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)

        # Create a git repository
        os.makedirs(".git")

        # Create manifest file
        self.manifest_file = ".s3_manifest.yaml"
        manifest = {
            "bucket_name": "test-bucket",
            "repo_prefix": "test-prefix",
            "files": {},
        }
        with open(self.manifest_file, "w") as f:
            yaml.safe_dump(manifest, f)

    def tearDown(self):
        """Clean up test environment."""
        os.chdir(self.original_cwd)
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_init_accepts_endpoint_url(self):
        """Test that the init command accepts --endpoint-url."""
        from click.testing import CliRunner

        from s3lfs.cli import cli

        os.remove(self.manifest_file)

        runner = CliRunner()
        with patch("boto3.client") as mock_boto3_client:
            mock_client = Mock()
            mock_boto3_client.return_value = mock_client

            result = runner.invoke(
                cli,
                [
                    "init",
                    "my-bucket",
                    "my-prefix",
                    "--endpoint-url",
                    "http://localhost:9000",
                ],
            )

            self.assertEqual(result.exit_code, 0, msg=result.output)
            self.assertIn("Repository initialized", result.output)

            # Verify manifest has endpoint_url
            with open(self.manifest_file, "r") as f:
                manifest = yaml.safe_load(f)
            self.assertEqual(manifest["endpoint_url"], "http://localhost:9000")

    def test_track_accepts_endpoint_url(self):
        """Test that the track command accepts --endpoint-url."""
        from click.testing import CliRunner

        from s3lfs.cli import cli

        # Create a file to track
        with open("test_file.txt", "w") as f:
            f.write("hello")

        runner = CliRunner()
        with patch("boto3.client") as mock_boto3_client:
            mock_client = Mock()
            mock_boto3_client.return_value = mock_client
            mock_client.upload_fileobj = Mock()
            mock_client.head_object = Mock(side_effect=Exception("not found"))

            result = runner.invoke(
                cli,
                [
                    "track",
                    "test_file.txt",
                    "--endpoint-url",
                    "http://localhost:9000",
                ],
            )

            # Should not fail due to unrecognized option
            self.assertNotIn("no such option", result.output.lower())

    def test_checkout_accepts_endpoint_url(self):
        """Test that the checkout command accepts --endpoint-url."""
        from click.testing import CliRunner

        from s3lfs.cli import cli

        runner = CliRunner()
        with patch("boto3.client") as mock_boto3_client:
            mock_client = Mock()
            mock_boto3_client.return_value = mock_client

            result = runner.invoke(
                cli,
                [
                    "checkout",
                    "--all",
                    "--endpoint-url",
                    "http://localhost:9000",
                ],
            )

            self.assertNotIn("no such option", result.output.lower())

    def test_ls_accepts_endpoint_url(self):
        """Test that the ls command accepts --endpoint-url."""
        from click.testing import CliRunner

        from s3lfs.cli import cli

        runner = CliRunner()
        with patch("boto3.client") as mock_boto3_client:
            mock_client = Mock()
            mock_boto3_client.return_value = mock_client

            result = runner.invoke(
                cli,
                ["ls", "--endpoint-url", "http://localhost:9000"],
            )

            self.assertEqual(result.exit_code, 0, msg=result.output)

    def test_remove_accepts_endpoint_url(self):
        """Test that the remove command accepts --endpoint-url."""
        from click.testing import CliRunner

        from s3lfs.cli import cli

        runner = CliRunner()
        with patch("boto3.client") as mock_boto3_client:
            mock_client = Mock()
            mock_boto3_client.return_value = mock_client

            result = runner.invoke(
                cli,
                [
                    "remove",
                    "somefile.txt",
                    "--endpoint-url",
                    "http://localhost:9000",
                ],
            )

            # May fail because file doesn't exist, but should not fail
            # due to unrecognized option
            self.assertNotIn("no such option", result.output.lower())

    def test_cleanup_accepts_endpoint_url(self):
        """Test that the cleanup command accepts --endpoint-url."""
        from click.testing import CliRunner

        from s3lfs.cli import cli

        runner = CliRunner()
        with patch("boto3.client") as mock_boto3_client:
            mock_client = Mock()
            mock_boto3_client.return_value = mock_client
            mock_client.list_objects_v2 = Mock(return_value={"Contents": []})

            result = runner.invoke(
                cli,
                [
                    "cleanup",
                    "--force",
                    "--endpoint-url",
                    "http://localhost:9000",
                ],
            )

            self.assertNotIn("no such option", result.output.lower())


if __name__ == "__main__":
    unittest.main()
