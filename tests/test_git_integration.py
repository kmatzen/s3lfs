"""Tests for the git-workflow integration: .gitignore protection of tracked
paths, the pre-commit command, and push-time verification."""

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import boto3
from click.testing import CliRunner
from moto import mock_s3

from s3lfs.cli import (
    S3LFS_GITIGNORE_END,
    S3LFS_GITIGNORE_START,
    _add_gitignore_entry,
    _load_gitignore_block,
    _remove_gitignore_entry,
    cli,
)

TEST_BUCKET = "test-bucket-s3lfs-git"


class TestGitignoreBlockHelpers(unittest.TestCase):
    """The s3lfs block in .gitignore must not disturb surrounding content."""

    def setUp(self):
        self.temp_dir = Path(os.path.realpath(tempfile.mkdtemp()))

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_add_creates_block(self):
        added = _add_gitignore_entry(self.temp_dir, "/data/")
        self.assertTrue(added)
        content = (self.temp_dir / ".gitignore").read_text()
        self.assertIn(S3LFS_GITIGNORE_START, content)
        self.assertIn("/data/", content)
        self.assertIn(S3LFS_GITIGNORE_END, content)

    def test_add_is_idempotent(self):
        _add_gitignore_entry(self.temp_dir, "/data/")
        added = _add_gitignore_entry(self.temp_dir, "/data/")
        self.assertFalse(added)
        content = (self.temp_dir / ".gitignore").read_text()
        self.assertEqual(content.count("/data/"), 1)

    def test_add_preserves_existing_content(self):
        (self.temp_dir / ".gitignore").write_text("*.pyc\n")
        _add_gitignore_entry(self.temp_dir, "/data/")
        content = (self.temp_dir / ".gitignore").read_text()
        self.assertIn("*.pyc", content)
        self.assertLess(content.index("*.pyc"), content.index("/data/"))

    def test_remove_deletes_entry_and_empty_block(self):
        (self.temp_dir / ".gitignore").write_text("*.pyc\n")
        _add_gitignore_entry(self.temp_dir, "/data/")
        removed = _remove_gitignore_entry(self.temp_dir, {"/data/", "/data"})
        self.assertTrue(removed)
        content = (self.temp_dir / ".gitignore").read_text()
        self.assertIn("*.pyc", content)
        self.assertNotIn("/data/", content)
        self.assertNotIn(S3LFS_GITIGNORE_START, content)

    def test_remove_keeps_other_entries(self):
        _add_gitignore_entry(self.temp_dir, "/data/")
        _add_gitignore_entry(self.temp_dir, "/models/big.bin")
        _remove_gitignore_entry(self.temp_dir, {"/data/", "/data"})
        content = (self.temp_dir / ".gitignore").read_text()
        self.assertNotIn("/data/", content)
        self.assertIn("/models/big.bin", content)

    def test_remove_missing_entry_returns_false(self):
        _add_gitignore_entry(self.temp_dir, "/data/")
        self.assertFalse(_remove_gitignore_entry(self.temp_dir, {"/other"}))

    def test_content_after_block_is_preserved(self):
        _add_gitignore_entry(self.temp_dir, "/data/")
        with open(self.temp_dir / ".gitignore", "a") as f:
            f.write("*.log\n")
        _add_gitignore_entry(self.temp_dir, "/models/")
        before, entries, after = _load_gitignore_block(self.temp_dir)
        self.assertEqual(entries, ["/data/", "/models/"])
        self.assertIn("*.log", after)


class GitRepoTestCase(unittest.TestCase):
    """Base: a real temp git repo with an initialized s3lfs manifest.

    moto is started explicitly here (not via the class decorator) because
    the decorator does not wrap a setUp inherited from a base class.
    """

    def setUp(self):
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        self.addCleanup(self.mock_s3.stop)

        self.temp_dir = os.path.realpath(tempfile.mkdtemp())
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)

        subprocess.run(["git", "init"], capture_output=True, check=True)
        subprocess.run(
            ["git", "config", "user.email", "test@test.com"], capture_output=True
        )
        subprocess.run(["git", "config", "user.name", "Test"], capture_output=True)

        self.s3 = boto3.client("s3", region_name="us-east-1")
        self.s3.create_bucket(Bucket=TEST_BUCKET)

        self.runner = CliRunner()
        result = self.runner.invoke(cli, ["init", TEST_BUCKET, "test-prefix"])
        assert result.exit_code == 0, result.output

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _git(self, *args):
        return subprocess.run(
            ["git", *args], capture_output=True, text=True, cwd=self.temp_dir
        )

    def _git_files(self):
        return set(self._git("ls-files").stdout.splitlines())

    def _commit_all(self, message="commit"):
        self._git("add", "-A")
        self._git("commit", "-m", message)

    def _write(self, rel_path, content):
        path = Path(self.temp_dir) / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        return path


class TestTrackProtectsFromGit(GitRepoTestCase):
    """Tracking a path must keep it out of git: ignored and de-indexed."""

    def test_track_file_adds_gitignore_entry(self):
        self._write("data/big.bin", "payload")
        result = self.runner.invoke(cli, ["track", "data/big.bin"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        content = Path(".gitignore").read_text()
        self.assertIn(S3LFS_GITIGNORE_START, content)
        self.assertIn("/data/big.bin", content)
        # git must now consider the file ignored
        check = self._git("check-ignore", "data/big.bin")
        self.assertEqual(check.returncode, 0, "tracked file is not gitignored")

    def test_track_directory_adds_anchored_dir_entry(self):
        self._write("data/a.bin", "a")
        self._write("data/sub/b.bin", "b")
        result = self.runner.invoke(cli, ["track", "data"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        _, entries, _ = _load_gitignore_block(Path(self.temp_dir))
        self.assertIn("/data/", entries)
        self.assertEqual(self._git("check-ignore", "data/sub/b.bin").returncode, 0)

    def test_track_removes_committed_file_from_index(self):
        """A file already committed to git is de-indexed when handed to s3lfs.

        .gitignore has no effect on files git already tracks, so without
        this step git would keep versioning the large file alongside S3.
        """
        self._write("data/big.bin", "payload")
        self._commit_all("add big file to git")
        self.assertIn("data/big.bin", self._git_files())

        result = self.runner.invoke(cli, ["track", "data/big.bin"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        self.assertNotIn("data/big.bin", self._git_files())
        self.assertTrue(Path("data/big.bin").exists(), "file must stay on disk")
        self.assertIn("Removed 1 s3lfs-tracked file(s)", result.output)

    def test_track_deindexes_file_with_staged_changes(self):
        """git rm --cached needs --force when staged content differs from
        HEAD; the de-index must still work and must not delete the file."""
        self._write("data/big.bin", "v1")
        self._commit_all("add big file to git")
        self._write("data/big.bin", "v2")
        self._git("add", "data/big.bin")

        result = self.runner.invoke(cli, ["track", "data/big.bin"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        self.assertNotIn("data/big.bin", self._git_files())
        self.assertEqual(Path("data/big.bin").read_text(), "v2")

    def test_track_never_tracked_path_leaves_git_alone(self):
        """Tracking a path that matches nothing must not touch .gitignore."""
        self.runner.invoke(cli, ["track", "no/such/file.bin"])

        _, entries, _ = _load_gitignore_block(Path(self.temp_dir))
        self.assertEqual(entries, [])

    def test_remove_drops_gitignore_entry(self):
        self._write("data/big.bin", "payload")
        self.runner.invoke(cli, ["track", "data/big.bin"])
        result = self.runner.invoke(cli, ["remove", "data/big.bin"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        _, entries, _ = _load_gitignore_block(Path(self.temp_dir))
        self.assertNotIn("/data/big.bin", entries)


class TestPreCommitCommand(GitRepoTestCase):
    """s3lfs pre-commit: block staged tracked files, upload, stage manifest."""

    def test_blocks_staged_tracked_file(self):
        self._write("data/big.bin", "payload")
        self.runner.invoke(cli, ["track", "data/big.bin"])
        # Force-stage the tracked file past .gitignore
        self._git("add", "-f", "data/big.bin")

        result = self.runner.invoke(cli, ["pre-commit"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("data/big.bin", result.output)
        self.assertIn("git rm --cached", result.output)

    def test_uploads_modified_content_and_stages_manifest(self):
        self._write("data/big.bin", "v1")
        self.runner.invoke(cli, ["track", "data/big.bin"])
        self._commit_all("track big file")

        self._write("data/big.bin", "v2")
        result = self.runner.invoke(cli, ["pre-commit"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        # Manifest reflects the new content and is staged for the commit
        staged = self._git("diff", "--cached", "--name-only").stdout.splitlines()
        self.assertIn(".s3_manifest.yaml", staged)
        # The new content must already be in S3: verify succeeds
        verify = self.runner.invoke(cli, ["verify"])
        self.assertEqual(verify.exit_code, 0, msg=verify.output)

    def test_noop_when_nothing_modified(self):
        self._write("data/big.bin", "v1")
        self.runner.invoke(cli, ["track", "data/big.bin"])
        self._commit_all("track big file")

        result = self.runner.invoke(cli, ["pre-commit"])
        self.assertEqual(result.exit_code, 0, msg=result.output)


class TestVerifyCommand(GitRepoTestCase):
    """s3lfs verify: does the manifest reference content that exists in S3?"""

    def _delete_all_objects(self):
        resp = self.s3.list_objects_v2(Bucket=TEST_BUCKET)
        for obj in resp.get("Contents", []):
            self.s3.delete_object(Bucket=TEST_BUCKET, Key=obj["Key"])

    def test_verify_passes_after_track(self):
        self._write("data/big.bin", "payload")
        self.runner.invoke(cli, ["track", "data/big.bin"])

        result = self.runner.invoke(cli, ["verify"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertIn("verified present", result.output)

    def test_verify_fails_when_content_missing(self):
        self._write("data/big.bin", "payload")
        self.runner.invoke(cli, ["track", "data/big.bin"])
        self._delete_all_objects()

        result = self.runner.invoke(cli, ["verify"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("data/big.bin", result.output)

    def test_verify_revision_reads_committed_manifest(self):
        self._write("data/big.bin", "payload")
        self.runner.invoke(cli, ["track", "data/big.bin"])
        self._commit_all("track big file")

        result = self.runner.invoke(cli, ["verify", "--revision", "HEAD"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertIn("verified present", result.output)

    def test_verify_base_skips_unchanged_entries(self):
        self._write("data/big.bin", "payload")
        self.runner.invoke(cli, ["track", "data/big.bin"])
        self._commit_all("track big file")

        # Base == revision: every entry is unchanged, nothing to check,
        # even if the content has vanished from S3.
        self._delete_all_objects()
        result = self.runner.invoke(
            cli, ["verify", "--revision", "HEAD", "--base", "HEAD"]
        )
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertIn("No manifest entries to verify", result.output)

    def test_verify_base_catches_changed_entries(self):
        self._write("data/big.bin", "v1")
        self.runner.invoke(cli, ["track", "data/big.bin"])
        self._commit_all("track v1")

        self._write("data/big.bin", "v2")
        self.runner.invoke(cli, ["track", "data/big.bin"])
        self._commit_all("track v2")
        self._delete_all_objects()

        result = self.runner.invoke(
            cli, ["verify", "--revision", "HEAD", "--base", "HEAD~1"]
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("data/big.bin", result.output)

    def test_verify_revision_without_manifest(self):
        """A revision predating s3lfs init has nothing to verify."""
        # Remove the manifest and commit an unrelated file
        Path(".s3_manifest.yaml").unlink()
        self._write("readme.txt", "hello")
        self._commit_all("no manifest yet")
        # Re-initialize so the working tree has a manifest again
        self.runner.invoke(cli, ["init", TEST_BUCKET, "test-prefix"])

        result = self.runner.invoke(cli, ["verify", "--revision", "HEAD"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertIn("No manifest at revision", result.output)


if __name__ == "__main__":
    unittest.main()
