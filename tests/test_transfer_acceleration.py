import json
import os
import tempfile
import unittest
from unittest.mock import Mock, patch

from s3lfs.core import S3LFS


class TestTransferAcceleration(unittest.TestCase):
    """Tests for S3 Transfer Acceleration functionality."""

    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)

        # Create a git repository
        os.makedirs(".git")

        # Create manifest file
        self.manifest_file = ".s3_manifest.json"
        manifest = {
            "bucket_name": "test-bucket",
            "repo_prefix": "test-prefix",
            "files": {},
        }
        with open(self.manifest_file, "w") as f:
            json.dump(manifest, f)

    def tearDown(self):
        """Clean up test environment."""
        os.chdir(self.original_cwd)
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_transfer_acceleration_enabled(self):
        """Test that transfer acceleration is properly configured when enabled."""
        with patch("boto3.client") as mock_boto3_client:
            mock_client = Mock()
            mock_boto3_client.return_value = mock_client

            # Create S3LFS with transfer acceleration enabled
            s3lfs = S3LFS(
                bucket_name="test-bucket",
                repo_prefix="test-prefix",
                use_acceleration=True,
            )

            # Get the S3 client to trigger the factory call
            s3lfs._get_s3_client()

            # Verify boto3.client was called with acceleration config
            mock_boto3_client.assert_called_once()
            call_args = mock_boto3_client.call_args

            # Check that the config includes use_accelerate_endpoint=True
            config = call_args[1]["config"]
            self.assertTrue(config.s3["use_accelerate_endpoint"])

    def test_transfer_acceleration_disabled(self):
        """Test that transfer acceleration is not configured when disabled."""
        with patch("boto3.client") as mock_boto3_client:
            mock_client = Mock()
            mock_boto3_client.return_value = mock_client

            # Create S3LFS with transfer acceleration disabled
            s3lfs = S3LFS(
                bucket_name="test-bucket",
                repo_prefix="test-prefix",
                use_acceleration=False,
            )

            # Get the S3 client to trigger the factory call
            s3lfs._get_s3_client()

            # Verify boto3.client was called without acceleration config
            mock_boto3_client.assert_called_once()
            call_args = mock_boto3_client.call_args

            # Check that the config does not include use_accelerate_endpoint
            # When no config is passed, boto3.client is called without config parameter
            if "config" in call_args[1]:
                config = call_args[1]["config"]
                self.assertFalse(
                    hasattr(config, "s3")
                    or (
                        hasattr(config, "s3")
                        and "use_accelerate_endpoint" not in config.s3
                    )
                )
            else:
                # No config passed, which is correct for disabled acceleration
                pass

    def test_transfer_acceleration_with_unsigned_requests_fails(self):
        """Test that transfer acceleration fails with unsigned requests."""
        with self.assertRaises(RuntimeError) as context:
            s3lfs = S3LFS(
                bucket_name="test-bucket",
                repo_prefix="test-prefix",
                no_sign_request=True,
                use_acceleration=True,
            )
            # Trigger S3 client creation which will cause the error
            s3lfs._get_s3_client()

        self.assertIn(
            "Transfer acceleration is not supported for unsigned requests",
            str(context.exception),
        )

    def test_transfer_acceleration_with_unsigned_requests_disabled(self):
        """Test that unsigned requests work when transfer acceleration is disabled."""
        with patch("boto3.client") as mock_boto3_client:
            mock_client = Mock()
            mock_boto3_client.return_value = mock_client

            # This should not raise an exception
            s3lfs = S3LFS(
                bucket_name="test-bucket",
                repo_prefix="test-prefix",
                no_sign_request=True,
                use_acceleration=False,
            )

            # Get the S3 client to trigger the factory call
            s3lfs._get_s3_client()

            # Verify boto3.client was called with unsigned config
            mock_boto3_client.assert_called_once()
            call_args = mock_boto3_client.call_args

            # Check that the config includes signature_version=UNSIGNED
            config = call_args[1]["config"]
            from botocore import UNSIGNED

            self.assertEqual(config.signature_version, UNSIGNED)

    def test_transfer_acceleration_default_factory(self):
        """Test the default S3 factory function with transfer acceleration."""
        with patch("boto3.client") as mock_boto3_client:
            mock_client = Mock()
            mock_boto3_client.return_value = mock_client

            s3lfs = S3LFS(
                bucket_name="test-bucket",
                repo_prefix="test-prefix",
                use_acceleration=True,
            )

            # Test the default factory function
            factory = s3lfs.s3_factory

            # Test with signed requests and acceleration
            factory(no_sign_request=False)

            # Verify the call was made with acceleration config
            mock_boto3_client.assert_called()
            call_args = mock_boto3_client.call_args_list[-1]
            config = call_args[1]["config"]
            self.assertTrue(config.s3["use_accelerate_endpoint"])

    def test_transfer_acceleration_custom_factory(self):
        """Test that custom S3 factory can work with transfer acceleration."""

        def custom_factory(no_sign_request):
            if no_sign_request:
                return Mock()  # Custom unsigned client
            else:
                return Mock()  # Custom signed client

        s3lfs = S3LFS(
            bucket_name="test-bucket",
            repo_prefix="test-prefix",
            use_acceleration=True,
            s3_factory=custom_factory,
        )

        # The custom factory should be used instead of the default
        self.assertEqual(s3lfs.s3_factory, custom_factory)

    def test_transfer_acceleration_error_message(self):
        """Test that the error message for acceleration with unsigned requests is correct."""
        from s3lfs.core import ERROR_MESSAGES

        self.assertIn("acceleration_not_supported", ERROR_MESSAGES)
        self.assertIn(
            "Transfer acceleration is not supported for unsigned requests",
            ERROR_MESSAGES["acceleration_not_supported"],
        )

    def test_transfer_acceleration_parameter_storage(self):
        """Test that the use_acceleration parameter is properly stored."""
        s3lfs = S3LFS(
            bucket_name="test-bucket",
            repo_prefix="test-prefix",
            use_acceleration=True,
        )

        self.assertTrue(s3lfs.use_acceleration)

        s3lfs_no_accel = S3LFS(
            bucket_name="test-bucket",
            repo_prefix="test-prefix",
            use_acceleration=False,
        )

        self.assertFalse(s3lfs_no_accel.use_acceleration)


if __name__ == "__main__":
    unittest.main()
