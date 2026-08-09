import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import yaml
from click.testing import CliRunner

from s3lfs.config import CONFIG_FILENAME, apply_config, find_config, load_config


class TestFindConfig(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_returns_path_when_exists(self):
        config_path = Path(self.temp_dir) / CONFIG_FILENAME
        config_path.write_text("no_sign_request: true\n")
        result = find_config(self.temp_dir)
        self.assertEqual(result, config_path)

    def test_returns_none_when_missing(self):
        result = find_config(self.temp_dir)
        self.assertIsNone(result)


class TestLoadConfig(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_loads_recognized_keys(self):
        config_path = Path(self.temp_dir) / CONFIG_FILENAME
        config_path.write_text("no_sign_request: true\nuse_acceleration: true\n")
        config = load_config(self.temp_dir)
        self.assertEqual(config, {"no_sign_request": True, "use_acceleration": True})

    def test_ignores_unknown_keys(self):
        config_path = Path(self.temp_dir) / CONFIG_FILENAME
        config_path.write_text("no_sign_request: true\nfuture_option: some_value\n")
        config = load_config(self.temp_dir)
        self.assertNotIn("future_option", config)
        self.assertTrue(config["no_sign_request"])

    def test_returns_empty_when_missing(self):
        config = load_config(self.temp_dir)
        self.assertEqual(config, {})

    def test_returns_empty_for_empty_file(self):
        config_path = Path(self.temp_dir) / CONFIG_FILENAME
        config_path.write_text("")
        config = load_config(self.temp_dir)
        self.assertEqual(config, {})

    def test_partial_config(self):
        config_path = Path(self.temp_dir) / CONFIG_FILENAME
        config_path.write_text("use_acceleration: true\n")
        config = load_config(self.temp_dir)
        self.assertEqual(config, {"use_acceleration": True})
        self.assertNotIn("no_sign_request", config)


class TestApplyConfig(unittest.TestCase):
    def test_cli_true_overrides_config_false(self):
        config = {"no_sign_request": False, "use_acceleration": False}
        cli_kwargs = {"no_sign_request": True, "use_acceleration": False}
        merged = apply_config(config, cli_kwargs)
        self.assertTrue(merged["no_sign_request"])
        self.assertFalse(merged["use_acceleration"])

    def test_config_true_used_when_cli_false(self):
        config = {"no_sign_request": True, "use_acceleration": True}
        cli_kwargs = {"no_sign_request": False, "use_acceleration": False}
        merged = apply_config(config, cli_kwargs)
        self.assertTrue(merged["no_sign_request"])
        self.assertTrue(merged["use_acceleration"])

    def test_defaults_when_no_config_no_cli(self):
        config: dict = {}
        cli_kwargs = {"no_sign_request": False, "use_acceleration": False}
        merged = apply_config(config, cli_kwargs)
        self.assertFalse(merged["no_sign_request"])
        self.assertFalse(merged["use_acceleration"])

    def test_empty_cli_kwargs(self):
        config = {"no_sign_request": True}
        merged = apply_config(config, {})
        self.assertTrue(merged["no_sign_request"])


class TestConfigCLIIntegration(unittest.TestCase):
    """Test that .s3lfsconfig is picked up by CLI commands."""

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

    def test_ls_uses_config_no_sign_request(self):
        """ls command picks up no_sign_request from .s3lfsconfig."""
        # Write config
        Path(CONFIG_FILENAME).write_text("no_sign_request: true\n")

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
            mock_instance.list_all_files = Mock()

            runner.invoke(cli, ["ls"])

            # S3LFS should have been called with no_sign_request=True
            call_kwargs = mock_cls.call_args[1]
            self.assertTrue(call_kwargs["no_sign_request"])

    def test_ls_cli_flag_overrides_config(self):
        """CLI --no-sign-request overrides config value of false."""
        # Config says false
        Path(CONFIG_FILENAME).write_text("no_sign_request: false\n")

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
            mock_instance.list_all_files = Mock()

            runner.invoke(cli, ["ls", "--no-sign-request"])

            call_kwargs = mock_cls.call_args[1]
            self.assertTrue(call_kwargs["no_sign_request"])

    def test_track_uses_config_use_acceleration(self):
        """track command picks up use_acceleration from .s3lfsconfig."""
        Path(CONFIG_FILENAME).write_text("use_acceleration: true\n")

        # Create a file to track
        Path("testfile.txt").write_text("hello")

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
            mock_instance.track = Mock()

            runner.invoke(cli, ["track", "testfile.txt"])

            call_kwargs = mock_cls.call_args[1]
            self.assertTrue(call_kwargs["use_acceleration"])

    def test_commands_work_without_config_file(self):
        """Commands work fine when .s3lfsconfig doesn't exist."""
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
            mock_instance.list_all_files = Mock()

            result = runner.invoke(cli, ["ls"])
            self.assertEqual(result.exit_code, 0, msg=result.output)

            call_kwargs = mock_cls.call_args[1]
            self.assertFalse(call_kwargs["no_sign_request"])
            self.assertFalse(call_kwargs["use_acceleration"])

    def test_checkout_uses_config(self):
        """checkout command picks up config."""
        Path(CONFIG_FILENAME).write_text(
            "no_sign_request: true\nuse_acceleration: false\n"
        )

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
            self.assertTrue(call_kwargs["no_sign_request"])
            self.assertFalse(call_kwargs["use_acceleration"])

    def test_cleanup_uses_config(self):
        """cleanup command picks up config."""
        Path(CONFIG_FILENAME).write_text("no_sign_request: true\n")

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
            mock_instance.cleanup_s3 = Mock()

            runner.invoke(cli, ["cleanup", "--force"])

            call_kwargs = mock_cls.call_args[1]
            self.assertTrue(call_kwargs["no_sign_request"])

    def test_remove_uses_config(self):
        """remove command picks up config."""
        Path(CONFIG_FILENAME).write_text("no_sign_request: true\n")

        # Add a file to the manifest so remove has something to work with
        manifest = {
            "bucket_name": "test-bucket",
            "repo_prefix": "test-prefix",
            "files": {"somefile.txt": "abc123"},
        }
        with open(".s3_manifest.yaml", "w") as f:
            yaml.safe_dump(manifest, f)

        from s3lfs.cli import cli

        runner = CliRunner()
        with patch("s3lfs.cli.S3LFS") as mock_cls:
            mock_instance = Mock()
            mock_cls.return_value = mock_instance
            mock_instance.manifest = manifest
            mock_instance.remove_file = Mock()
            mock_instance.remove_subtree = Mock()
            mock_instance.path_resolver = Mock()

            runner.invoke(cli, ["remove", "somefile.txt"])

            call_kwargs = mock_cls.call_args[1]
            self.assertTrue(call_kwargs["no_sign_request"])


if __name__ == "__main__":
    unittest.main()


class TestConfigKeysTeamsNeed(unittest.TestCase):
    """endpoint_url and workers used to be dropped silently, so a team on
    MinIO/R2 that set them in .s3lfsconfig had every request go to AWS."""

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_endpoint_url_and_workers_are_loaded(self):
        (self.temp_dir / ".s3lfsconfig").write_text(
            "endpoint_url: http://minio.internal:9000\nworkers: 4\n"
        )
        config = load_config(self.temp_dir)
        self.assertEqual(config["endpoint_url"], "http://minio.internal:9000")
        self.assertEqual(config["workers"], 4)

    def test_encryption_false_is_loaded(self):
        """MinIO without KMS rejects the SSE header, so a team on MinIO
        needs encryption: false to be honoured -- the CLI has no flag
        for it."""
        (self.temp_dir / ".s3lfsconfig").write_text("encryption: false\n")
        config = load_config(self.temp_dir)
        self.assertIs(config["encryption"], False)

    def test_encryption_config_reaches_s3lfs(self):
        from s3lfs.cli import _make_s3lfs

        (self.temp_dir / ".s3lfsconfig").write_text("encryption: false\n")
        with patch("s3lfs.cli.S3LFS") as mock_cls:
            _make_s3lfs(self.temp_dir, self.temp_dir / ".s3_manifest.yaml")
        self.assertIs(mock_cls.call_args[1]["encryption"], False)

    def test_encryption_defaults_to_on(self):
        """No config value must not pass encryption at all, leaving the
        S3LFS constructor default (on) in charge."""
        from s3lfs.cli import _make_s3lfs

        with patch("s3lfs.cli.S3LFS") as mock_cls:
            _make_s3lfs(self.temp_dir, self.temp_dir / ".s3_manifest.yaml")
        self.assertNotIn("encryption", mock_cls.call_args[1])

    def test_unknown_keys_are_reported(self):
        (self.temp_dir / ".s3lfsconfig").write_text("endpint_url: typo\n")
        with patch("builtins.print") as mock_print:
            config = load_config(self.temp_dir)
        self.assertEqual(config, {})
        self.assertTrue(
            any("endpint_url" in str(c) for c in mock_print.call_args_list),
            "a misspelled key that changes where data goes was ignored silently",
        )
