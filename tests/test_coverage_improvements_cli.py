"""
Additional CLI tests targeting specific uncovered lines in s3lfs/cli.py:

- 185, 261: --metrics flag enabling metrics collection in track/checkout
- 324-334: ls command's test-mode git_finder_func branch
- 595-597: _find_lfs_files exact (non-glob) path branch
- 691: migrate-from-lfs truncated pointer-file listing ("... and N more")
- 712-714: migrate-from-lfs error handling when initialize_repo() raises
- 750: _human_size PB formatting
- 835: _get_hooks_dir relative core.hooksPath resolution
- 837-838: _get_hooks_dir exception handling around subprocess.run
- 877-880: _write_hook_atomically exception cleanup path
"""

import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import yaml
from click.testing import CliRunner

from s3lfs import metrics
from s3lfs.cli import (_find_lfs_files, _get_hooks_dir, _human_size,
                       _write_hook_atomically, cli, ls)


class TestMetricsFlag(unittest.TestCase):
    """Covers the --metrics flag branches in track (185) and checkout (261)."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.test_path = Path(self.test_dir)
        (self.test_path / ".git").mkdir()
        self.original_cwd = os.getcwd()
        os.chdir(self.test_path)
        self.runner = CliRunner()
        # Ensure clean metrics state
        metrics._global_tracker = None

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.test_dir, ignore_errors=True)
        metrics._global_tracker = None

    def test_track_metrics_flag_enables_metrics(self):
        """track --metrics should call metrics.enable_metrics() (line 185)."""
        manifest = self.test_path / ".s3_manifest.yaml"
        with open(manifest, "w") as f:
            yaml.safe_dump({"bucket_name": "b", "repo_prefix": "p", "files": {}}, f)

        self.assertFalse(metrics.is_enabled())

        # No path and no --modified -> command aborts, but metrics is enabled
        # before that check runs.
        result = self.runner.invoke(cli, ["track", "--metrics", "--no-sign-request"])

        self.assertTrue(metrics.is_enabled())
        self.assertNotEqual(result.exit_code, 0)

    def test_checkout_metrics_flag_enables_metrics(self):
        """checkout --metrics should call metrics.enable_metrics() (line 261)."""
        manifest = self.test_path / ".s3_manifest.yaml"
        with open(manifest, "w") as f:
            yaml.safe_dump({"bucket_name": "b", "repo_prefix": "p", "files": {}}, f)

        self.assertFalse(metrics.is_enabled())

        result = self.runner.invoke(cli, ["checkout", "--metrics", "--no-sign-request"])

        self.assertTrue(metrics.is_enabled())
        self.assertNotEqual(result.exit_code, 0)


class TestLsGitFinderFuncBranch(unittest.TestCase):
    """Covers the test-mode git_finder_func branch in ls() (lines 324-334)."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.test_path = Path(self.test_dir)
        self.original_cwd = os.getcwd()
        os.chdir(self.test_path)

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_git_finder_func_no_git_root(self):
        """git_finder_func returning None hits the 'not a git repo' abort (325-327)."""

        def finder(start_path=None):
            return None

        with self.assertRaises(Exception):
            ls.callback(
                path=None,
                no_sign_request=True,
                use_acceleration=False,
                verbose=False,
                all=False,
                endpoint_url=None,
                git_finder_func=finder,
            )

    def test_git_finder_func_manifest_missing(self):
        """git_finder_func returns a root without a manifest (329-331)."""

        def finder(start_path=None):
            return self.test_path

        with self.assertRaises(Exception):
            ls.callback(
                path=None,
                no_sign_request=True,
                use_acceleration=False,
                verbose=False,
                all=False,
                endpoint_url=None,
                git_finder_func=finder,
            )

    def test_git_finder_func_success_with_path(self):
        """git_finder_func returns a valid root with a manifest and a path arg,
        exercising the full 322-336 block including the from_cli_input branch."""
        manifest = self.test_path / ".s3_manifest.yaml"
        with open(manifest, "w") as f:
            yaml.safe_dump(
                {
                    "bucket_name": "b",
                    "repo_prefix": "p",
                    "files": {"test.txt": "hash1"},
                },
                f,
            )

        def finder(start_path=None):
            return self.test_path

        mock_s3lfs_instance = Mock()
        with patch("s3lfs.cli.S3LFS", return_value=mock_s3lfs_instance):
            ls.callback(
                path="test.txt",
                no_sign_request=True,
                use_acceleration=False,
                verbose=False,
                all=False,
                endpoint_url=None,
                git_finder_func=finder,
            )

        # manifest_key resolved via path_resolver.from_cli_input, so list_files
        # (not list_all_files) should have been invoked.
        mock_s3lfs_instance.list_files.assert_called_once()

    def test_git_finder_func_success_no_path(self):
        """git_finder_func success branch without a path arg (manifest_key None)."""
        manifest = self.test_path / ".s3_manifest.yaml"
        with open(manifest, "w") as f:
            yaml.safe_dump(
                {"bucket_name": "b", "repo_prefix": "p", "files": {}},
                f,
            )

        def finder(start_path=None):
            return self.test_path

        mock_s3lfs_instance = Mock()
        with patch("s3lfs.cli.S3LFS", return_value=mock_s3lfs_instance):
            ls.callback(
                path=None,
                no_sign_request=True,
                use_acceleration=False,
                verbose=False,
                all=False,
                endpoint_url=None,
                git_finder_func=finder,
            )

        mock_s3lfs_instance.list_all_files.assert_called_once()


class TestFindLfsFilesExactPath(unittest.TestCase):
    """Covers the non-glob exact-path branch in _find_lfs_files (595-597)."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_exact_path_match(self):
        git_root = Path(self.temp_dir)
        target = git_root / "exact_file.bin"
        target.write_bytes(b"content")

        files = _find_lfs_files(git_root, ["exact_file.bin"])

        self.assertEqual(files, [target])

    def test_exact_path_no_match(self):
        git_root = Path(self.temp_dir)

        files = _find_lfs_files(git_root, ["does_not_exist.bin"])

        self.assertEqual(files, [])


class TestMigrateFromLfsExtra(unittest.TestCase):
    """Covers migrate-from-lfs error paths: truncated pointer listing (691)
    and initialize_repo failure handling (712-714)."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        os.makedirs(".git")

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_more_than_ten_pointer_files_truncates_listing(self):
        Path(".gitattributes").write_text("*.bin filter=lfs diff=lfs -text\n")

        pointer_body = (
            "version https://git-lfs.github.com/spec/v1\n"
            "oid sha256:abc\n"
            "size 100\n"
        )
        for i in range(11):
            Path(f"file{i}.bin").write_text(pointer_body)

        runner = CliRunner()
        result = runner.invoke(cli, ["migrate-from-lfs", "bucket", "prefix"])

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("... and 1 more", result.output)

    def test_initialize_repo_failure_aborts(self):
        Path(".gitattributes").write_text("*.bin filter=lfs diff=lfs -text\n")
        Path("data.bin").write_bytes(b"real binary content")

        runner = CliRunner()
        mock_instance = Mock()
        mock_instance.initialize_repo.side_effect = RuntimeError("boom")
        with patch("s3lfs.cli.S3LFS", return_value=mock_instance):
            result = runner.invoke(cli, ["migrate-from-lfs", "bucket", "prefix"])

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Error initializing s3lfs", result.output)
        self.assertIn("boom", result.output)


class TestHumanSize(unittest.TestCase):
    """Covers the petabyte fallback formatting in _human_size (750)."""

    def test_petabyte_scale(self):
        result = _human_size(1024**5)
        self.assertEqual(result, "1.0 PB")

    def test_multi_petabyte_scale(self):
        result = _human_size(2 * 1024**5)
        self.assertEqual(result, "2.0 PB")


class TestGetHooksDir(unittest.TestCase):
    """Covers relative core.hooksPath resolution (835) and exception
    handling around subprocess.run (837-838)."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_relative_hooks_path_is_resolved_against_git_root(self):
        git_root = Path(self.temp_dir)

        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "relative-hooks\n"

        with patch("subprocess.run", return_value=mock_result):
            hooks_dir = _get_hooks_dir(git_root)

        self.assertEqual(hooks_dir, git_root / "relative-hooks")

    def test_subprocess_exception_falls_back_to_default(self):
        git_root = Path(self.temp_dir)

        with patch("subprocess.run", side_effect=OSError("git not found")):
            hooks_dir = _get_hooks_dir(git_root)

        self.assertEqual(hooks_dir, git_root / ".git" / "hooks")


class TestWriteHookAtomically(unittest.TestCase):
    """Covers the exception cleanup path in _write_hook_atomically (877-880)."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_cleans_up_temp_file_on_failure(self):
        hook_path = Path(self.temp_dir) / "post-merge"

        with patch.object(Path, "chmod", side_effect=OSError("chmod failed")):
            with self.assertRaises(OSError):
                _write_hook_atomically(hook_path, "#!/bin/sh\necho hi\n")

        # No leftover temp files and the target hook was not created.
        leftovers = list(Path(self.temp_dir).glob("*.tmp"))
        self.assertEqual(leftovers, [])
        self.assertFalse(hook_path.exists())


if __name__ == "__main__":
    unittest.main()
