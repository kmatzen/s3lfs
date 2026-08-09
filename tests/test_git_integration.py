"""Tests for the git-workflow integration: .gitignore protection of tracked
paths, the pre-commit command, and push-time verification."""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import boto3
import yaml
from click.testing import CliRunner
from moto import mock_s3

from s3lfs.cli import (
    S3LFS_GITIGNORE_END,
    S3LFS_GITIGNORE_START,
    _add_gitignore_entry,
    _install_merge_driver,
    _load_gitignore_block,
    _merge_gitignore,
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

    def test_track_directory_ignores_each_tracked_file(self):
        """A directory spec must not ignore the directory wholesale.

        `/data/` would hide any source file added there later from git,
        while s3lfs would not be tracking it either -- the file would live
        on one machine only.
        """
        self._write("data/a.bin", "a")
        self._write("data/sub/b.bin", "b")
        result = self.runner.invoke(cli, ["track", "data"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        _, entries, _ = _load_gitignore_block(Path(self.temp_dir))
        self.assertEqual(entries, ["/data/a.bin", "/data/sub/b.bin"])
        self.assertEqual(self._git("check-ignore", "data/sub/b.bin").returncode, 0)

        # A file added under the tracked directory afterwards stays visible
        self._write("data/schema.json", "{}")
        self.assertNotEqual(
            self._git("check-ignore", "data/schema.json").returncode,
            0,
            "a newly added source file was hidden from git",
        )

    def test_track_glob_keeps_the_glob(self):
        """A glob is already precise, so it is used as-is."""
        self._write("clips/one.mp4", "a")
        self._write("clips/notes.txt", "b")
        self.runner.invoke(cli, ["track", "clips/*.mp4"])

        _, entries, _ = _load_gitignore_block(Path(self.temp_dir))
        self.assertEqual(entries, ["/clips/*.mp4"])
        self.assertNotEqual(self._git("check-ignore", "clips/notes.txt").returncode, 0)

    def test_track_escapes_gitignore_metacharacters(self):
        """Expanded entries must be literal, even for bracketed paths.

        `/data/runs[2024]/a.bin` is a character class to gitignore and
        matches nothing, so the file would stay visible to git and get
        committed -- the outcome the .gitignore block exists to prevent.
        """
        self._write("data/runs[2024]/a.bin", "a")
        result = self.runner.invoke(cli, ["track", "data"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        _, entries, _ = _load_gitignore_block(Path(self.temp_dir))
        self.assertEqual(entries, [r"/data/runs\[2024\]/a.bin"])
        self.assertEqual(
            self._git("check-ignore", "data/runs[2024]/a.bin").returncode,
            0,
            "tracked file under a bracketed directory is not ignored",
        )

    def test_track_reports_when_nothing_matched(self):
        """Silence would be indistinguishable from success."""
        result = self.runner.invoke(cli, ["track", "no/such/file.bin"])
        self.assertIn("nothing was tracked", result.output.lower())

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


class TestReadOnlyCommandsLeaveTreeClean(GitRepoTestCase):
    """The manifest is git-tracked, so commands that only read must not
    rewrite it -- that dirties the tree on every sync hook and breaks
    clean-tree checks in CI."""

    def test_read_only_commands_do_not_touch_the_manifest(self):
        self._commit_all("initial")
        self.assertEqual(self._git("status", "--porcelain").stdout.strip(), "")

        for args in (["status"], ["sparse"], ["ls"], ["verify"]):
            self.runner.invoke(cli, args)

        self.assertEqual(
            self._git("status", "--porcelain").stdout.strip(),
            "",
            "a read-only command modified the tracked manifest",
        )

    def test_init_still_writes_configuration(self):
        """The guard must not stop a real configuration change landing."""
        manifest = yaml.safe_load(Path(".s3_manifest.yaml").read_text())
        self.assertEqual(manifest["bucket_name"], TEST_BUCKET)
        self.assertEqual(manifest["repo_prefix"], "test-prefix")


class TestSyncCommand(GitRepoTestCase):
    """s3lfs sync: manifest-diff-based branch switching."""

    def _track_and_commit(self, rel_path, content, message):
        self._write(rel_path, content)
        self.runner.invoke(cli, ["track", rel_path])
        self._commit_all(message)

    def test_sync_downloads_only_changed_entries(self):
        """The diff, not the whole manifest, decides what gets downloaded.

        Both files are deleted from disk; syncing from the revision that
        only lacked the second one must restore that one alone. A full
        checkout would restore both.
        """
        self._track_and_commit("data/a.bin", "a", "track a")
        self._track_and_commit("data/b.bin", "b", "track b")

        Path("data/a.bin").unlink()
        Path("data/b.bin").unlink()

        result = self.runner.invoke(cli, ["sync", "--from", "HEAD~1"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        self.assertTrue(Path("data/b.bin").exists(), "changed entry not restored")
        self.assertFalse(
            Path("data/a.bin").exists(),
            "unchanged entry was downloaded; sync did a full checkout",
        )

    def test_sync_updates_changed_content(self):
        self._track_and_commit("data/a.bin", "v1", "track v1")
        self._write("data/a.bin", "v2")
        self.runner.invoke(cli, ["track", "data/a.bin"])
        self._commit_all("track v2")

        # Put the previous revision's content back on disk. That is the
        # clean state -- it matches what the old manifest recorded -- so
        # updating it to v2 loses nothing.
        self._write("data/a.bin", "v1")
        result = self.runner.invoke(cli, ["sync", "--from", "HEAD~1"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertEqual(Path("data/a.bin").read_text(), "v2")

    def test_sync_keeps_locally_modified_file(self):
        """Tracked files are gitignored, so git never warns that one is
        dirty. sync running from a hook must not be what destroys it."""
        self._track_and_commit("data/a.bin", "v1", "track v1")
        self._write("data/a.bin", "v2")
        self.runner.invoke(cli, ["track", "data/a.bin"])
        self._commit_all("track v2")

        self._write("data/a.bin", "PRECIOUS UNSAVED WORK")
        result = self.runner.invoke(cli, ["sync", "--from", "HEAD~1"])

        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertEqual(Path("data/a.bin").read_text(), "PRECIOUS UNSAVED WORK")
        self.assertIn("not in S3", result.output)

    def test_sync_force_overwrites_locally_modified_file(self):
        self._track_and_commit("data/a.bin", "v1", "track v1")
        self._write("data/a.bin", "v2")
        self.runner.invoke(cli, ["track", "data/a.bin"])
        self._commit_all("track v2")

        self._write("data/a.bin", "discard me")
        result = self.runner.invoke(cli, ["sync", "--from", "HEAD~1", "--force"])

        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertEqual(Path("data/a.bin").read_text(), "v2")

    def test_full_checkout_fallback_keeps_locally_modified_file(self):
        """The no-baseline path cannot tell clean from edited, so it must
        take the conservative option."""
        self._track_and_commit("data/a.bin", "v1", "track v1")
        self._write("data/a.bin", "PRECIOUS UNSAVED WORK")

        result = self.runner.invoke(cli, ["sync"])

        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertEqual(Path("data/a.bin").read_text(), "PRECIOUS UNSAVED WORK")
        self.assertIn("locally modified", result.output)

    def test_sync_reports_when_already_in_sync(self):
        self._track_and_commit("data/a.bin", "a", "track a")
        result = self.runner.invoke(cli, ["sync", "--from", "HEAD"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertIn("already in sync", result.output)

    def test_sync_prunes_untracked_files(self):
        """A file dropped from the manifest is removed from disk, as git
        does for files absent from the branch being switched to."""
        self._track_and_commit("data/a.bin", "a", "track a")
        self.runner.invoke(cli, ["remove", "data/a.bin"])
        self._commit_all("untrack a")

        result = self.runner.invoke(cli, ["sync", "--from", "HEAD~1"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertFalse(Path("data/a.bin").exists())
        self.assertIn("Removed 1 file(s)", result.output)

    def test_sync_prune_removes_emptied_directories(self):
        self._track_and_commit("data/nested/a.bin", "a", "track a")
        self.runner.invoke(cli, ["remove", "data/nested/a.bin"])
        self._commit_all("untrack a")

        self.runner.invoke(cli, ["sync", "--from", "HEAD~1"])
        self.assertFalse(Path("data/nested").exists())

    def test_sync_keeps_locally_modified_file_when_pruning(self):
        """Pruning must never destroy content that is not in S3."""
        self._track_and_commit("data/a.bin", "a", "track a")
        self.runner.invoke(cli, ["remove", "data/a.bin"])
        self._commit_all("untrack a")
        self._write("data/a.bin", "local edits worth keeping")

        result = self.runner.invoke(cli, ["sync", "--from", "HEAD~1"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertTrue(Path("data/a.bin").exists())
        self.assertEqual(Path("data/a.bin").read_text(), "local edits worth keeping")
        self.assertIn("not in S3", result.output)

    def test_sync_no_prune_keeps_dropped_files(self):
        self._track_and_commit("data/a.bin", "a", "track a")
        self.runner.invoke(cli, ["remove", "data/a.bin"])
        self._commit_all("untrack a")

        result = self.runner.invoke(cli, ["sync", "--from", "HEAD~1", "--no-prune"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertTrue(Path("data/a.bin").exists())

    def test_sync_falls_back_to_full_checkout_without_baseline(self):
        """An unknown revision (fresh clone, shallow history) must still
        produce correct files, just without the diff optimization."""
        self._track_and_commit("data/a.bin", "a", "track a")
        Path("data/a.bin").unlink()

        zero = "0" * 40
        result = self.runner.invoke(cli, ["sync", "--from", zero])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertIn("falling back to a full checkout", result.output)
        self.assertTrue(Path("data/a.bin").exists())

    def test_sync_without_from_does_full_checkout(self):
        self._track_and_commit("data/a.bin", "a", "track a")
        Path("data/a.bin").unlink()

        result = self.runner.invoke(cli, ["sync"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertTrue(Path("data/a.bin").exists())


class TestStatusCommand(GitRepoTestCase):
    """s3lfs status: the view git status cannot give for ignored files."""

    def test_status_reports_up_to_date(self):
        self._write("data/a.bin", "a")
        self.runner.invoke(cli, ["track", "data/a.bin"])

        result = self.runner.invoke(cli, ["status"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertIn("1 up-to-date", result.output)

    def test_status_reports_modified(self):
        self._write("data/a.bin", "a")
        self.runner.invoke(cli, ["track", "data/a.bin"])
        self._write("data/a.bin", "changed")

        result = self.runner.invoke(cli, ["status"])
        self.assertIn("1 modified", result.output)
        self.assertIn("Modified", result.output)
        self.assertIn("data/a.bin", result.output)

    def test_status_reports_missing(self):
        self._write("data/a.bin", "a")
        self.runner.invoke(cli, ["track", "data/a.bin"])
        Path("data/a.bin").unlink()

        result = self.runner.invoke(cli, ["status"])
        self.assertIn("1 missing", result.output)
        self.assertIn("Missing from disk", result.output)

    def test_status_porcelain_codes(self):
        self._write("data/a.bin", "a")
        self._write("data/b.bin", "b")
        self.runner.invoke(cli, ["track", "data"])
        self._write("data/a.bin", "changed")
        Path("data/b.bin").unlink()

        result = self.runner.invoke(cli, ["status", "--porcelain"])
        lines = sorted(result.output.splitlines())
        self.assertEqual(lines, ["D data/b.bin", "M data/a.bin"])

    def test_status_path_filter(self):
        self._write("data/a.bin", "a")
        self._write("other/b.bin", "b")
        self.runner.invoke(cli, ["track", "data"])
        self.runner.invoke(cli, ["track", "other"])

        result = self.runner.invoke(cli, ["status", "data", "--porcelain", "--all"])
        self.assertIn("data/a.bin", result.output)
        self.assertNotIn("other/b.bin", result.output)

    def test_status_with_nothing_tracked(self):
        result = self.runner.invoke(cli, ["status"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertIn("No files are tracked", result.output)


class TestMergeDriver(unittest.TestCase):
    """The manifest merge driver: union by key, conflict only on real
    disagreement about the same path."""

    def setUp(self):
        self.temp_dir = Path(os.path.realpath(tempfile.mkdtemp()))
        self.runner = CliRunner()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _manifest(self, name, files, **extra):
        import yaml as _yaml

        path = self.temp_dir / name
        data = {"bucket_name": "b", "repo_prefix": "p", "files": files}
        data.update(extra)
        path.write_text(_yaml.safe_dump(data))
        return path

    def _merged_files(self):
        import yaml as _yaml

        return _yaml.safe_load((self.temp_dir / "ours.yaml").read_text())["files"]

    def test_disjoint_additions_merge_cleanly(self):
        base = self._manifest("base.yaml", {"shared.bin": "h0"})
        ours = self._manifest("ours.yaml", {"shared.bin": "h0", "a.bin": "ha"})
        theirs = self._manifest("theirs.yaml", {"shared.bin": "h0", "b.bin": "hb"})

        result = self.runner.invoke(
            cli, ["merge-driver", str(base), str(ours), str(theirs)]
        )
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertEqual(
            self._merged_files(),
            {"shared.bin": "h0", "a.bin": "ha", "b.bin": "hb"},
        )

    def test_identical_change_on_both_sides_is_not_a_conflict(self):
        base = self._manifest("base.yaml", {"a.bin": "h0"})
        ours = self._manifest("ours.yaml", {"a.bin": "h1"})
        theirs = self._manifest("theirs.yaml", {"a.bin": "h1"})

        result = self.runner.invoke(
            cli, ["merge-driver", str(base), str(ours), str(theirs)]
        )
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertEqual(self._merged_files(), {"a.bin": "h1"})

    def test_one_sided_change_is_taken(self):
        base = self._manifest("base.yaml", {"a.bin": "h0"})
        ours = self._manifest("ours.yaml", {"a.bin": "h0"})
        theirs = self._manifest("theirs.yaml", {"a.bin": "h2"})

        result = self.runner.invoke(
            cli, ["merge-driver", str(base), str(ours), str(theirs)]
        )
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertEqual(self._merged_files(), {"a.bin": "h2"})

    def test_one_sided_deletion_is_taken(self):
        base = self._manifest("base.yaml", {"a.bin": "h0", "b.bin": "hb"})
        ours = self._manifest("ours.yaml", {"a.bin": "h0", "b.bin": "hb"})
        theirs = self._manifest("theirs.yaml", {"b.bin": "hb"})

        result = self.runner.invoke(
            cli, ["merge-driver", str(base), str(ours), str(theirs)]
        )
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertEqual(self._merged_files(), {"b.bin": "hb"})

    def test_same_path_different_hashes_conflicts(self):
        base = self._manifest("base.yaml", {"a.bin": "h0"})
        ours = self._manifest("ours.yaml", {"a.bin": "h1"})
        theirs = self._manifest("theirs.yaml", {"a.bin": "h2"})

        result = self.runner.invoke(
            cli, ["merge-driver", str(base), str(ours), str(theirs)]
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("a.bin", result.output)
        # Our side is kept so the file stays a valid manifest to edit
        self.assertEqual(self._merged_files(), {"a.bin": "h1"})

    def test_json_output_format_from_target_name(self):
        base = self._manifest("base.yaml", {})
        ours = self._manifest("ours.yaml", {"a.bin": "ha"})
        theirs = self._manifest("theirs.yaml", {"b.bin": "hb"})

        result = self.runner.invoke(
            cli,
            [
                "merge-driver",
                str(base),
                str(ours),
                str(theirs),
                ".s3_manifest.json",
            ],
        )
        self.assertEqual(result.exit_code, 0, msg=result.output)
        data = json.loads((self.temp_dir / "ours.yaml").read_text())
        self.assertEqual(data["files"], {"a.bin": "ha", "b.bin": "hb"})


class TestMergeDriverEndToEnd(GitRepoTestCase):
    """git itself must invoke the driver and merge divergent branches.

    Only the merge driver is registered here, not the hooks: the hooks
    would shell out to s3lfs for real S3 work in a subprocess that moto
    does not patch. The merge driver touches no network.
    """

    def setUp(self):
        super().setUp()
        # Put an `s3lfs` on PATH for git to invoke as the merge driver.
        self.bin_dir = Path(os.path.realpath(tempfile.mkdtemp()))
        self.addCleanup(shutil.rmtree, self.bin_dir, ignore_errors=True)
        repo_root = Path(__file__).resolve().parent.parent
        shim = self.bin_dir / "s3lfs"
        shim.write_text(
            "#!/bin/sh\n"
            f'PYTHONPATH="{repo_root}" exec "{sys.executable}" '
            '-m s3lfs.cli "$@"\n'
        )
        shim.chmod(0o755)
        self.env = dict(os.environ)
        self.env["PATH"] = f"{self.bin_dir}:{self.env['PATH']}"

        _install_merge_driver(Path(self.temp_dir))

    def _git_with_shim(self, *args):
        return subprocess.run(
            ["git", *args],
            capture_output=True,
            text=True,
            cwd=self.temp_dir,
            env=self.env,
        )

    def test_git_merge_unions_divergent_branches(self):
        self._commit_all("register merge driver")
        default_branch = self._git("branch", "--show-current").stdout.strip()

        self._write("data/a.bin", "a")
        self.runner.invoke(cli, ["track", "data/a.bin"])
        self._commit_all("track a")

        self._git("checkout", "-b", "feature", default_branch + "~1")
        self._write("data/b.bin", "b")
        self.runner.invoke(cli, ["track", "data/b.bin"])
        self._commit_all("track b")

        self._git("checkout", default_branch)
        merge = self._git_with_shim("merge", "feature", "--no-edit")
        self.assertEqual(merge.returncode, 0, msg=merge.stdout + merge.stderr)

        files = yaml.safe_load(Path(".s3_manifest.yaml").read_text())["files"]
        self.assertIn("data/a.bin", files)
        self.assertIn("data/b.bin", files)

        # The .gitignore block must union too, and stay a single block
        _, entries, _ = _load_gitignore_block(Path(self.temp_dir))
        self.assertIn("/data/a.bin", entries)
        self.assertIn("/data/b.bin", entries)
        content = Path(".gitignore").read_text()
        self.assertEqual(content.count(S3LFS_GITIGNORE_START), 1)

    def test_git_merge_reports_real_manifest_conflict(self):
        """Both branches changing the same path to different content is a
        genuine conflict and must still stop the merge."""
        self._write("data/a.bin", "base")
        self.runner.invoke(cli, ["track", "data/a.bin"])
        self._commit_all("track a")
        default_branch = self._git("branch", "--show-current").stdout.strip()

        self._git("checkout", "-b", "feature")
        self._write("data/a.bin", "theirs")
        self.runner.invoke(cli, ["track", "data/a.bin"])
        self._commit_all("their a")

        self._git("checkout", default_branch)
        self._write("data/a.bin", "ours")
        self.runner.invoke(cli, ["track", "data/a.bin"])
        self._commit_all("our a")

        merge = self._git_with_shim("merge", "feature", "--no-edit")
        self.assertNotEqual(merge.returncode, 0, "real conflict was silently merged")
        self.assertIn("data/a.bin", merge.stdout + merge.stderr)


class TestGitignoreMerge(unittest.TestCase):
    """Unit-level checks on the .gitignore three-way merge."""

    def setUp(self):
        self.temp_dir = Path(os.path.realpath(tempfile.mkdtemp()))

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _side(self, name, text):
        path = self.temp_dir / name
        path.write_text(text)
        return path

    def _block(self, *entries):
        lines = [S3LFS_GITIGNORE_START, *entries, S3LFS_GITIGNORE_END]
        return "\n".join(lines) + "\n"

    def test_unions_block_entries(self):
        base = self._side("base", "*.pyc\n")
        ours = self._side("ours", "*.pyc\n\n" + self._block("/a.bin"))
        theirs = self._side("theirs", "*.pyc\n\n" + self._block("/b.bin"))

        merged, conflict = _merge_gitignore(base, ours, theirs)

        self.assertFalse(conflict)
        self.assertIn("*.pyc", merged)
        self.assertIn("/a.bin", merged)
        self.assertIn("/b.bin", merged)
        self.assertEqual(merged.count(S3LFS_GITIGNORE_START), 1)

    def test_entry_removed_on_one_side_stays_removed(self):
        base = self._side("base", self._block("/a.bin", "/b.bin"))
        ours = self._side("ours", self._block("/a.bin", "/b.bin"))
        theirs = self._side("theirs", self._block("/b.bin"))

        merged, conflict = _merge_gitignore(base, ours, theirs)

        self.assertFalse(conflict)
        self.assertNotIn("/a.bin", merged)
        self.assertIn("/b.bin", merged)

    def test_conflicting_user_edits_are_reported(self):
        base = self._side("base", "shared\n")
        ours = self._side("ours", "ours-only\n")
        theirs = self._side("theirs", "theirs-only\n")

        merged, conflict = _merge_gitignore(base, ours, theirs)

        self.assertTrue(conflict, "conflicting user edits should not merge silently")
        self.assertIn("<<<<<<<", merged)


class TestInstallRegistersMergeDriver(GitRepoTestCase):
    def test_install_writes_gitattributes_and_config(self):
        result = self.runner.invoke(cli, ["install"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        attributes = Path(".gitattributes").read_text()
        self.assertIn(".s3_manifest.yaml merge=s3lfs", attributes)

        driver = self._git("config", "merge.s3lfs.driver").stdout.strip()
        self.assertIn("s3lfs merge-driver", driver)

    def test_uninstall_removes_registration(self):
        self.runner.invoke(cli, ["install"])
        result = self.runner.invoke(cli, ["uninstall"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        attributes = Path(".gitattributes")
        if attributes.exists():
            self.assertNotIn("merge=s3lfs", attributes.read_text())
        self.assertEqual(self._git("config", "merge.s3lfs.driver").stdout.strip(), "")

    def test_install_preserves_existing_gitattributes(self):
        Path(".gitattributes").write_text("*.txt text\n")
        self.runner.invoke(cli, ["install"])
        self.runner.invoke(cli, ["uninstall"])

        self.assertIn("*.txt text", Path(".gitattributes").read_text())


class TestCloneCommand(GitRepoTestCase):
    """s3lfs clone: clone + hooks + download in one command."""

    def _make_source_repo(self):
        self._write("data/a.bin", "payload")
        self.runner.invoke(cli, ["track", "data/a.bin"])
        self._commit_all("track a")
        return self.temp_dir

    def test_clone_installs_hooks_and_downloads_files(self):
        source = self._make_source_repo()
        dest = Path(os.path.realpath(tempfile.mkdtemp())) / "cloned"
        self.addCleanup(shutil.rmtree, dest.parent, ignore_errors=True)

        result = self.runner.invoke(cli, ["clone", source, str(dest)])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        # The tracked file is gitignored, so it can only be here via S3
        self.assertTrue((dest / "data/a.bin").exists())
        self.assertEqual((dest / "data/a.bin").read_text(), "payload")
        self.assertTrue((dest / ".git/hooks/pre-commit").exists())

    def test_clone_no_checkout_skips_download(self):
        source = self._make_source_repo()
        dest = Path(os.path.realpath(tempfile.mkdtemp())) / "cloned"
        self.addCleanup(shutil.rmtree, dest.parent, ignore_errors=True)

        result = self.runner.invoke(cli, ["clone", source, str(dest), "--no-checkout"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertFalse((dest / "data/a.bin").exists())
        self.assertTrue((dest / ".git/hooks/pre-commit").exists())

    def test_clone_non_s3lfs_repo_reports_and_stops(self):
        self._write("readme.txt", "hello")
        Path(".s3_manifest.yaml").unlink()
        self._commit_all("plain repo")

        dest = Path(os.path.realpath(tempfile.mkdtemp())) / "cloned"
        self.addCleanup(shutil.rmtree, dest.parent, ignore_errors=True)

        result = self.runner.invoke(cli, ["clone", self.temp_dir, str(dest)])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertIn("not s3lfs-initialized", result.output)
        self.assertTrue((dest / "readme.txt").exists())

    def test_clone_restores_cwd(self):
        source = self._make_source_repo()
        dest = Path(os.path.realpath(tempfile.mkdtemp())) / "cloned"
        self.addCleanup(shutil.rmtree, dest.parent, ignore_errors=True)

        before = Path.cwd()
        self.runner.invoke(cli, ["clone", source, str(dest)])
        self.assertEqual(Path.cwd(), before)


if __name__ == "__main__":
    unittest.main()


class TestHooksDirResolution(GitRepoTestCase):
    """Hooks must be found via git, not by assuming .git is a directory."""

    def test_install_works_in_a_linked_worktree(self):
        """In a linked worktree .git is a *file*, so guessing
        git_root/.git/hooks fails with NotADirectoryError."""
        self._commit_all("initial")
        worktree = Path(self.temp_dir).parent / "s3lfs-wt"
        self.addCleanup(shutil.rmtree, worktree, ignore_errors=True)
        made = self._git("worktree", "add", "-q", str(worktree))
        self.assertEqual(made.returncode, 0, msg=made.stdout + made.stderr)

        os.chdir(worktree)
        try:
            self.assertTrue((worktree / ".git").is_file())
            result = self.runner.invoke(cli, ["install"])
            self.assertEqual(result.exit_code, 0, msg=result.output)
            self.assertNotIn("Traceback", result.output)
        finally:
            os.chdir(self.temp_dir)


class TestSyncOnBranchWithoutManifest(GitRepoTestCase):
    def test_reports_instead_of_failing(self):
        """Checking out a branch from before s3lfs is normal; the
        post-checkout hook must not report an error for it."""
        self._write("data/a.bin", "a")
        self.runner.invoke(cli, ["track", "data/a.bin"])
        self._commit_all("track a")

        self._git("checkout", "-q", "-b", "pre-s3lfs")
        self._git("rm", "-q", ".s3_manifest.yaml")
        self._git("commit", "-qm", "before s3lfs")

        result = self.runner.invoke(cli, ["sync", "--from", "HEAD~1"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertIn("nothing to sync", result.output)


class TestRevisionManifestRobustness(GitRepoTestCase):
    def test_unparseable_baseline_is_treated_as_no_baseline(self):
        """A manifest committed with conflict markers -- what a teammate
        without the merge driver produces -- must not raise a traceback out
        of a post-checkout hook."""
        good = Path(".s3_manifest.yaml").read_text()
        Path(".s3_manifest.yaml").write_text(
            "<<<<<<< HEAD\nfiles: {}\n=======\nfiles: {}\n>>>>>>> other\n"
        )
        self._git("add", "-f", ".s3_manifest.yaml")
        self._git("commit", "-qm", "conflicted manifest")
        # Working tree is fine again; only the committed baseline is broken.
        Path(".s3_manifest.yaml").write_text(good)

        result = self.runner.invoke(cli, ["sync", "--from", "HEAD"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertNotIn("Traceback", result.output)

    def test_conflicted_working_manifest_explains_itself(self):
        """A bare YAML traceback does not tell the user what to do."""
        Path(".s3_manifest.yaml").write_text(
            "<<<<<<< HEAD\nfiles: {}\n=======\nfiles: {}\n>>>>>>> other\n"
        )
        result = self.runner.invoke(cli, ["status"])

        self.assertNotEqual(result.exit_code, 0)
        message = str(result.exception) if result.exception else result.output
        self.assertIn("merge conflict markers", message)
        self.assertIn("s3lfs install", message)


class TestPreCommitStaging(GitRepoTestCase):
    def test_stages_gitignore_alongside_the_manifest(self):
        """track writes both; committing one without the other leaves the
        ignore rules and the tracked set out of step."""
        self._commit_all("initial")
        self._write("data/a.bin", "a")
        self.runner.invoke(cli, ["track", "data/a.bin"])

        result = self.runner.invoke(cli, ["pre-commit"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        staged = self._git("diff", "--cached", "--name-only").stdout.split()
        self.assertIn(".s3_manifest.yaml", staged)
        self.assertIn(".gitignore", staged)


class TestMergeDriverRobustness(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(os.path.realpath(tempfile.mkdtemp()))
        self.runner = CliRunner()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _write(self, name, text):
        path = self.temp_dir / name
        path.write_text(text)
        return path

    def test_null_valued_key_is_not_dropped(self):
        """Absence and a null value are different things."""
        doc = "bucket_name: b\nrepo_prefix: p\nendpoint_url: null\nfiles: {}\n"
        base = self._write("base.yaml", doc)
        ours = self._write("ours.yaml", doc.replace("files: {}", "files:\n  a: h1"))
        theirs = self._write("theirs.yaml", doc.replace("files: {}", "files:\n  b: h2"))

        result = self.runner.invoke(
            cli, ["merge-driver", str(base), str(ours), str(theirs), "m.yaml"]
        )
        self.assertEqual(result.exit_code, 0, msg=result.output)

        merged = yaml.safe_load(ours.read_text())
        self.assertIn("endpoint_url", merged)
        self.assertIsNone(merged["endpoint_url"])

    def test_gitignore_detected_without_the_path_argument(self):
        """A stale driver config may not pass %P; a .gitignore must not be
        parsed as -- or overwritten by -- a manifest."""

        def block(*entries):
            lines = [S3LFS_GITIGNORE_START, *entries, S3LFS_GITIGNORE_END]
            return "\n".join(lines) + "\n"

        base = self._write("base.gi", block("/a.bin"))
        ours = self._write("ours.gi", block("/a.bin", "/ours.bin"))
        theirs = self._write("theirs.gi", block("/a.bin", "/theirs.bin"))

        result = self.runner.invoke(
            cli, ["merge-driver", str(base), str(ours), str(theirs)]
        )
        self.assertEqual(result.exit_code, 0, msg=result.output)

        merged = ours.read_text()
        self.assertIn("/ours.bin", merged)
        self.assertIn("/theirs.bin", merged)
        self.assertIn(S3LFS_GITIGNORE_START, merged)

    def test_unmergeable_input_does_not_claim_git_falls_back(self):
        base = self._write("base.yaml", "not: [a, manifest\n")
        ours = self._write("ours.yaml", "files: {}\n")
        theirs = self._write("theirs.yaml", "files: {}\n")

        result = self.runner.invoke(
            cli, ["merge-driver", str(base), str(ours), str(theirs), "m.yaml"]
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertNotIn("falling back to git", result.output)
        self.assertIn("resolve it by hand", result.output)


class TestSyncNeverRemovesUnrecoverableContent(GitRepoTestCase):
    """The rule verified in specs/S3lfsWorkingCopy.tla: only take bytes off
    disk when those bytes can be fetched back from S3.

    Matching the hash the manifest recorded is not enough -- the object
    behind that hash may since have been garbage-collected, making the copy
    on disk the last one. TLC found this trace against the weaker rule.
    """

    def test_prune_keeps_a_file_whose_object_was_collected(self):
        # track -> commit -> remove -> cleanup -> sync
        self._write("data/a.bin", "the only copy")
        self.runner.invoke(cli, ["track", "data/a.bin"])
        self._commit_all("track a")

        self.runner.invoke(cli, ["remove", "data/a.bin"])
        self._commit_all("untrack a")
        cleanup = self.runner.invoke(cli, ["cleanup", "--force"])
        self.assertEqual(cleanup.exit_code, 0, msg=cleanup.output)

        result = self.runner.invoke(cli, ["sync", "--from", "HEAD~1"])

        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertTrue(
            Path("data/a.bin").exists(),
            "sync deleted the last copy of content that is no longer in S3",
        )
        self.assertEqual(Path("data/a.bin").read_text(), "the only copy")
        self.assertIn("not in S3", result.output)

    def test_force_still_removes_it(self):
        self._write("data/a.bin", "the only copy")
        self.runner.invoke(cli, ["track", "data/a.bin"])
        self._commit_all("track a")
        self.runner.invoke(cli, ["remove", "data/a.bin"])
        self._commit_all("untrack a")
        self.runner.invoke(cli, ["cleanup", "--force"])

        result = self.runner.invoke(cli, ["sync", "--from", "HEAD~1", "--force"])

        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertFalse(Path("data/a.bin").exists())


class TestDeletionsPropagate(GitRepoTestCase):
    """A tracked file the user deletes must stop being tracked.

    Otherwise the deletion never reaches collaborators: their next sync
    downloads the file again, and keeps doing so forever.
    """

    def test_deleting_a_tracked_file_untracks_it(self):
        self._write("data/a.bin", "a")
        self._write("data/b.bin", "b")
        self.runner.invoke(cli, ["track", "data"])
        self._commit_all("track both")

        Path("data/a.bin").unlink()
        result = self.runner.invoke(cli, ["track", "--modified"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        files = yaml.safe_load(Path(".s3_manifest.yaml").read_text())["files"]
        self.assertNotIn("data/a.bin", files)
        self.assertIn("data/b.bin", files)
        self.assertIn("data/a.bin", result.output)

    def test_never_downloaded_files_are_not_untracked(self):
        """The dangerous case: a fresh clone has every tracked file absent.

        Untracking on absence alone would wipe the manifest and delete
        everyone else's data. Only files this working copy has actually
        hashed count as deleted.
        """
        self._write("data/a.bin", "a")
        self.runner.invoke(cli, ["track", "data/a.bin"])
        self._commit_all("track a")

        # Simulate a clone: file absent, and no local hash cache
        Path("data/a.bin").unlink()
        for cache in Path(".").glob(".s3_manifest_cache.*"):
            cache.unlink()

        result = self.runner.invoke(cli, ["track", "--modified"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        files = yaml.safe_load(Path(".s3_manifest.yaml").read_text())["files"]
        self.assertIn("data/a.bin", files)

    def test_no_prune_deleted_keeps_the_entry(self):
        self._write("data/a.bin", "a")
        self.runner.invoke(cli, ["track", "data/a.bin"])
        self._commit_all("track a")

        Path("data/a.bin").unlink()
        result = self.runner.invoke(cli, ["track", "--modified", "--no-prune-deleted"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        files = yaml.safe_load(Path(".s3_manifest.yaml").read_text())["files"]
        self.assertIn("data/a.bin", files)


class TestShardedManifest(GitRepoTestCase):
    """One flat manifest is parsed and rewritten in full by every command,
    and lands a fresh copy in git history on every track."""

    def _shard_files(self):
        return sorted(p.name for p in Path(".s3lfs_manifest").glob("*.yaml"))

    def test_shard_splits_by_top_level_directory(self):
        self._write("data/a.bin", "a")
        self._write("models/b.bin", "b")
        self._write("root.bin", "c")
        self.runner.invoke(cli, ["track", "data"])
        self.runner.invoke(cli, ["track", "models"])
        self.runner.invoke(cli, ["track", "root.bin"])

        result = self.runner.invoke(cli, ["shard", "--force"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        self.assertEqual(
            self._shard_files(), ["_root.yaml", "data.yaml", "models.yaml"]
        )
        root = yaml.safe_load(Path(".s3_manifest.yaml").read_text())
        self.assertEqual(root.get("manifest_format"), "sharded")
        self.assertNotIn("files", root)

    def test_entries_survive_the_round_trip(self):
        self._write("data/a.bin", "a")
        self._write("models/b.bin", "b")
        self.runner.invoke(cli, ["track", "data"])
        self.runner.invoke(cli, ["track", "models"])
        before = self.runner.invoke(cli, ["ls"]).output

        self.runner.invoke(cli, ["shard", "--force"])
        self.assertEqual(self.runner.invoke(cli, ["ls"]).output, before)

        self.runner.invoke(cli, ["shard", "--undo", "--force"])
        self.assertEqual(self.runner.invoke(cli, ["ls"]).output, before)
        self.assertFalse(Path(".s3lfs_manifest").exists())

    def test_tracking_rewrites_only_the_affected_shard(self):
        """The point of sharding: a change under one directory does not
        produce a new copy of the whole manifest in git history."""
        self._write("data/a.bin", "a")
        self._write("models/b.bin", "b")
        self.runner.invoke(cli, ["track", "data"])
        self.runner.invoke(cli, ["track", "models"])
        self.runner.invoke(cli, ["shard", "--force"])

        untouched = Path(".s3lfs_manifest/models.yaml")
        before = untouched.read_bytes()
        stamp = untouched.stat().st_mtime_ns

        self._write("data/c.bin", "c")
        self.runner.invoke(cli, ["track", "data/c.bin"])

        self.assertIn("data/c.bin", Path(".s3lfs_manifest/data.yaml").read_text())
        self.assertEqual(untouched.read_bytes(), before)
        self.assertEqual(untouched.stat().st_mtime_ns, stamp)

    def test_shards_are_not_themselves_tracked(self):
        self._write("data/a.bin", "a")
        self.runner.invoke(cli, ["track", "data/a.bin"])
        self.runner.invoke(cli, ["shard", "--force"])

        self.runner.invoke(cli, ["track", "."])
        listed = self.runner.invoke(cli, ["ls"]).output
        self.assertNotIn(".s3lfs_manifest", listed)
