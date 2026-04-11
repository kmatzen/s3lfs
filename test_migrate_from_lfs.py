import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import yaml
from botocore.exceptions import ClientError
from click.testing import CliRunner

from s3lfs.cli import (
    _find_lfs_files,
    _is_lfs_pointer,
    _parse_lfs_patterns,
    _remove_lfs_from_gitattributes,
    cli,
)


class TestIsLfsPointer(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_detects_pointer_file(self):
        p = Path(self.temp_dir) / "pointer.bin"
        p.write_text(
            "version https://git-lfs.github.com/spec/v1\n"
            "oid sha256:abc123\n"
            "size 12345\n"
        )
        self.assertTrue(_is_lfs_pointer(p))

    def test_real_content_not_detected(self):
        p = Path(self.temp_dir) / "real.bin"
        p.write_bytes(b"\x89PNG\r\n\x1a\nsome image data here")
        self.assertFalse(_is_lfs_pointer(p))

    def test_empty_file(self):
        p = Path(self.temp_dir) / "empty"
        p.write_bytes(b"")
        self.assertFalse(_is_lfs_pointer(p))

    def test_nonexistent_file(self):
        self.assertFalse(_is_lfs_pointer(Path(self.temp_dir) / "nope"))


class TestParseLfsPatterns(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_parses_standard_lfs_entries(self):
        ga = Path(self.temp_dir) / ".gitattributes"
        ga.write_text(
            "*.bin filter=lfs diff=lfs merge=lfs -text\n"
            "*.psd filter=lfs diff=lfs merge=lfs -text\n"
            "# a comment\n"
            "*.txt text\n"
        )
        patterns = _parse_lfs_patterns(self.temp_dir)
        self.assertEqual(patterns, ["*.bin", "*.psd"])

    def test_no_lfs_entries(self):
        ga = Path(self.temp_dir) / ".gitattributes"
        ga.write_text("*.txt text\n")
        patterns = _parse_lfs_patterns(self.temp_dir)
        self.assertEqual(patterns, [])

    def test_no_gitattributes(self):
        patterns = _parse_lfs_patterns(self.temp_dir)
        self.assertEqual(patterns, [])

    def test_empty_lines_and_comments(self):
        ga = Path(self.temp_dir) / ".gitattributes"
        ga.write_text("\n# comment\n\n*.dat filter=lfs diff=lfs -text\n\n")
        patterns = _parse_lfs_patterns(self.temp_dir)
        self.assertEqual(patterns, ["*.dat"])

    def test_directory_pattern(self):
        ga = Path(self.temp_dir) / ".gitattributes"
        ga.write_text("data/**/*.bin filter=lfs diff=lfs merge=lfs -text\n")
        patterns = _parse_lfs_patterns(self.temp_dir)
        self.assertEqual(patterns, ["data/**/*.bin"])


class TestFindLfsFiles(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.git_root = Path(self.temp_dir)
        # Create some files
        (self.git_root / "a.bin").write_bytes(b"binary a")
        (self.git_root / "b.bin").write_bytes(b"binary b")
        (self.git_root / "c.txt").write_text("text")
        os.makedirs(self.git_root / "data")
        (self.git_root / "data" / "d.bin").write_bytes(b"binary d")

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_finds_glob_matches(self):
        files = _find_lfs_files(self.git_root, ["*.bin"])
        names = {f.name for f in files}
        self.assertIn("a.bin", names)
        self.assertIn("b.bin", names)
        self.assertNotIn("c.txt", names)

    def test_recursive_glob(self):
        files = _find_lfs_files(self.git_root, ["**/*.bin"])
        names = {f.name for f in files}
        self.assertIn("d.bin", names)

    def test_no_matches(self):
        files = _find_lfs_files(self.git_root, ["*.xyz"])
        self.assertEqual(files, [])

    def test_deduplicates(self):
        files = _find_lfs_files(self.git_root, ["*.bin", "*.bin"])
        names = [f.name for f in files]
        self.assertEqual(len([n for n in names if n == "a.bin"]), 1)

    def test_excludes_git_dir(self):
        os.makedirs(self.git_root / ".git" / "lfs")
        (self.git_root / ".git" / "lfs" / "test.bin").write_bytes(b"lfs obj")
        files = _find_lfs_files(self.git_root, ["**/*.bin"])
        for f in files:
            self.assertFalse(
                str(f.relative_to(self.git_root)).startswith(".git"),
                f"Should exclude .git files: {f}",
            )


class TestRemoveLfsFromGitattributes(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.ga_path = Path(self.temp_dir) / ".gitattributes"

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_removes_lfs_lines(self):
        self.ga_path.write_text(
            "*.bin filter=lfs diff=lfs merge=lfs -text\n"
            "*.txt text\n"
        )
        _remove_lfs_from_gitattributes(self.ga_path, ["*.bin"])
        content = self.ga_path.read_text()
        self.assertNotIn("filter=lfs", content)
        self.assertIn("*.txt text", content)

    def test_preserves_comments(self):
        self.ga_path.write_text(
            "# Important comment\n"
            "*.bin filter=lfs diff=lfs -text\n"
        )
        _remove_lfs_from_gitattributes(self.ga_path, ["*.bin"])
        content = self.ga_path.read_text()
        self.assertIn("# Important comment", content)

    def test_removes_multiple_patterns(self):
        self.ga_path.write_text(
            "*.bin filter=lfs diff=lfs -text\n"
            "*.psd filter=lfs diff=lfs -text\n"
            "*.txt text\n"
        )
        _remove_lfs_from_gitattributes(self.ga_path, ["*.bin", "*.psd"])
        content = self.ga_path.read_text()
        self.assertNotIn("*.bin", content)
        self.assertNotIn("*.psd", content)
        self.assertIn("*.txt", content)


class TestMigrateFromLfsCommand(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        os.makedirs(".git")

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_fails_without_gitattributes(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["migrate-from-lfs", "bucket", "prefix"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("No .gitattributes", result.output)

    def test_fails_without_lfs_patterns(self):
        Path(".gitattributes").write_text("*.txt text\n")
        runner = CliRunner()
        result = runner.invoke(cli, ["migrate-from-lfs", "bucket", "prefix"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("No Git LFS patterns", result.output)

    def test_fails_if_already_initialized(self):
        Path(".gitattributes").write_text("*.bin filter=lfs diff=lfs -text\n")
        Path(".s3_manifest.yaml").write_text("bucket_name: x\n")
        runner = CliRunner()
        result = runner.invoke(cli, ["migrate-from-lfs", "bucket", "prefix"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("already initialized", result.output)

    def test_fails_on_pointer_files(self):
        Path(".gitattributes").write_text("*.bin filter=lfs diff=lfs -text\n")
        Path("data.bin").write_text(
            "version https://git-lfs.github.com/spec/v1\n"
            "oid sha256:abc\n"
            "size 100\n"
        )
        runner = CliRunner()
        result = runner.invoke(cli, ["migrate-from-lfs", "bucket", "prefix"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("pointer files", result.output)
        self.assertIn("git lfs pull", result.output)

    def test_no_matching_files(self):
        Path(".gitattributes").write_text("*.bin filter=lfs diff=lfs -text\n")
        runner = CliRunner()
        result = runner.invoke(cli, ["migrate-from-lfs", "bucket", "prefix"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertIn("No files matching", result.output)

    def test_dry_run(self):
        Path(".gitattributes").write_text("*.bin filter=lfs diff=lfs -text\n")
        Path("data.bin").write_bytes(b"real binary content here")

        runner = CliRunner()
        result = runner.invoke(
            cli, ["migrate-from-lfs", "bucket", "prefix", "--dry-run"]
        )
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertIn("Dry run complete", result.output)
        self.assertIn("data.bin", result.output)
        # Should not have created manifest
        self.assertFalse(Path(".s3_manifest.yaml").exists())

    def test_successful_migration(self):
        Path(".gitattributes").write_text("*.bin filter=lfs diff=lfs -text\n")
        Path("data.bin").write_bytes(b"real binary content")

        runner = CliRunner()
        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            mock_client.upload_fileobj = Mock()
            mock_client.head_object = Mock(
                side_effect=ClientError(
                    {"Error": {"Code": "404", "Message": "Not Found"}},
                    "HeadObject",
                )
            )

            result = runner.invoke(
                cli, ["migrate-from-lfs", "my-bucket", "my-prefix"]
            )

            self.assertEqual(result.exit_code, 0, msg=result.output)
            self.assertIn("Migration complete", result.output)
            # Manifest should exist
            self.assertTrue(Path(".s3_manifest.yaml").exists())
            with open(".s3_manifest.yaml") as f:
                manifest = yaml.safe_load(f)
            self.assertEqual(manifest["bucket_name"], "my-bucket")
            self.assertEqual(manifest["repo_prefix"], "my-prefix")

    def test_remove_lfs_flag(self):
        Path(".gitattributes").write_text(
            "*.bin filter=lfs diff=lfs -text\n"
            "*.txt text\n"
        )
        Path("data.bin").write_bytes(b"real binary content")

        runner = CliRunner()
        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            mock_client.upload_fileobj = Mock()
            mock_client.head_object = Mock(
                side_effect=ClientError(
                    {"Error": {"Code": "404", "Message": "Not Found"}},
                    "HeadObject",
                )
            )

            result = runner.invoke(
                cli,
                ["migrate-from-lfs", "bucket", "prefix", "--remove-lfs"],
            )

            self.assertEqual(result.exit_code, 0, msg=result.output)
            content = Path(".gitattributes").read_text()
            self.assertNotIn("filter=lfs", content)
            self.assertIn("*.txt text", content)

    def test_fails_outside_git_repo(self):
        shutil.rmtree(".git")
        runner = CliRunner()
        result = runner.invoke(cli, ["migrate-from-lfs", "bucket", "prefix"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Not in a git repository", result.output)

    def test_shows_file_sizes(self):
        Path(".gitattributes").write_text("*.bin filter=lfs diff=lfs -text\n")
        Path("data.bin").write_bytes(b"x" * 2048)

        runner = CliRunner()
        result = runner.invoke(
            cli, ["migrate-from-lfs", "bucket", "prefix", "--dry-run"]
        )
        self.assertIn("KB", result.output)


if __name__ == "__main__":
    unittest.main()
