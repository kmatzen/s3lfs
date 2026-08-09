"""Tests for sparse working copies: s3lfs applies git's sparse-checkout
rules to tracked files, which git cannot do itself because those files are
gitignored and so absent from its index."""

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import boto3
from click.testing import CliRunner
from moto import mock_s3

from s3lfs.cli import cli
from s3lfs.sparse import SparseProfile

TEST_BUCKET = "test-bucket-s3lfs-sparse"


class SparseRepoTestCase(unittest.TestCase):
    """A git repo with s3lfs initialized and two asset directories."""

    def setUp(self):
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        self.addCleanup(self.mock_s3.stop)

        self.temp_dir = os.path.realpath(tempfile.mkdtemp())
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        self.addCleanup(self._restore_cwd)

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

    def _restore_cwd(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _git(self, *args):
        return subprocess.run(
            ["git", *args], capture_output=True, text=True, cwd=self.temp_dir
        )

    def _commit_all(self, message="commit"):
        self._git("add", "-A")
        self._git("commit", "-m", message)

    def _write(self, rel_path, content):
        path = Path(self.temp_dir) / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        return path

    def _track_both_dirs(self):
        """Track one file in `keep/` and one in `drop/`, and commit."""
        self._write("keep/in.bin", "keep-v1")
        self._write("drop/out.bin", "drop-v1")
        self.runner.invoke(cli, ["track", "keep/in.bin"])
        self.runner.invoke(cli, ["track", "drop/out.bin"])
        self._commit_all("track both")

    def _enable_sparse(self, *dirs):
        result = self._git("sparse-checkout", "set", "--cone", *dirs)
        assert result.returncode == 0, result.stdout + result.stderr


class TestSparseProfileDetection(SparseRepoTestCase):
    def test_inactive_when_not_sparse(self):
        profile = SparseProfile.detect(Path(self.temp_dir))
        self.assertFalse(profile.active)
        self.assertIsNone(profile.degraded_reason)
        self.assertEqual(
            profile.select(["a.bin", "deep/b.bin"]), {"a.bin", "deep/b.bin"}
        )

    def test_active_and_matches_cone_rules(self):
        self._write("root.txt", "x")
        self._commit_all("initial")
        self._enable_sparse("keep")

        profile = SparseProfile.detect(Path(self.temp_dir))
        self.assertTrue(profile.active)

        selected = profile.select(
            ["keep/in.bin", "keep/deep/nested.bin", "drop/out.bin", "root.bin"]
        )
        self.assertIn("keep/in.bin", selected)
        self.assertIn("keep/deep/nested.bin", selected)
        # Cone mode always materializes files at the repository root
        self.assertIn("root.bin", selected)
        self.assertNotIn("drop/out.bin", selected)

    def test_partition_splits_manifest(self):
        self._write("root.txt", "x")
        self._commit_all("initial")
        self._enable_sparse("keep")

        profile = SparseProfile.detect(Path(self.temp_dir))
        inside, outside = profile.partition({"keep/in.bin": "h1", "drop/out.bin": "h2"})
        self.assertEqual(inside, {"keep/in.bin": "h1"})
        self.assertEqual(outside, {"drop/out.bin": "h2"})

    def test_contains_single_path(self):
        self._write("root.txt", "x")
        self._commit_all("initial")
        self._enable_sparse("keep")

        profile = SparseProfile.detect(Path(self.temp_dir))
        self.assertTrue(profile.contains("keep/in.bin"))
        self.assertFalse(profile.contains("drop/out.bin"))

    def test_degrades_to_everything_when_rules_unreadable(self):
        """A half-configured sparse checkout must not silently hide files.

        core.sparseCheckout on with no patterns file is what a too-old git
        or a broken setup looks like from here: s3lfs falls back to
        treating everything as wanted, which can only over-download.
        """
        self._git("config", "core.sparseCheckout", "true")

        profile = SparseProfile.detect(Path(self.temp_dir))
        self.assertFalse(profile.active)
        self.assertIsNotNone(profile.degraded_reason)
        self.assertEqual(profile.select(["drop/out.bin"]), {"drop/out.bin"})

    def test_batching_covers_all_paths(self):
        """Selection is batched; every path must still be classified."""
        self._write("root.txt", "x")
        self._commit_all("initial")
        self._enable_sparse("keep")

        profile = SparseProfile.detect(Path(self.temp_dir))
        # Just over CHECK_RULES_BATCH, so more than one batch runs without
        # making the test pay for tens of thousands of paths.
        per_side = 3000
        keys = [f"keep/f{i}.bin" for i in range(per_side)]
        keys += [f"drop/f{i}.bin" for i in range(per_side)]
        selected = profile.select(keys)

        self.assertEqual(len(selected), per_side)
        self.assertTrue(all(k.startswith("keep/") for k in selected))


class TestSyncRespectsSparseProfile(SparseRepoTestCase):
    def test_sync_does_not_download_out_of_profile_changes(self):
        """The whole point: a branch switch must not refill the slice the
        user deliberately does not have."""
        self._track_both_dirs()

        # Both files change in a later commit
        self._write("keep/in.bin", "keep-v2")
        self._write("drop/out.bin", "drop-v2")
        self.runner.invoke(cli, ["track", "keep/in.bin"])
        self.runner.invoke(cli, ["track", "drop/out.bin"])
        self._commit_all("update both")

        # Narrow to keep/ and remove the out-of-profile file from disk.
        # keep/in.bin goes back to the previous revision's content, which is
        # the clean state sync is allowed to update.
        self._enable_sparse("keep")
        Path("drop/out.bin").unlink()
        self._write("keep/in.bin", "keep-v1")

        result = self.runner.invoke(cli, ["sync", "--from", "HEAD~1"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        self.assertEqual(Path("keep/in.bin").read_text(), "keep-v2")
        self.assertFalse(
            Path("drop/out.bin").exists(),
            "sync refilled a file outside the sparse profile",
        )

    def test_sync_prunes_files_that_left_the_profile(self):
        """Narrowing the profile should reclaim the space."""
        self._track_both_dirs()
        self.assertTrue(Path("drop/out.bin").exists())

        self._enable_sparse("keep")
        result = self.runner.invoke(cli, ["sync", "--from", "HEAD"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        self.assertFalse(Path("drop/out.bin").exists())
        self.assertTrue(Path("keep/in.bin").exists())

    def test_sync_keeps_out_of_profile_file_with_local_edits(self):
        self._track_both_dirs()
        self._enable_sparse("keep")
        self._write("drop/out.bin", "unsaved local work")

        result = self.runner.invoke(cli, ["sync", "--from", "HEAD"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        self.assertTrue(Path("drop/out.bin").exists())
        self.assertEqual(Path("drop/out.bin").read_text(), "unsaved local work")
        self.assertIn("not in S3", result.output)

    def test_sync_no_prune_keeps_out_of_profile_files(self):
        self._track_both_dirs()
        self._enable_sparse("keep")

        result = self.runner.invoke(cli, ["sync", "--from", "HEAD", "--no-prune"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertTrue(Path("drop/out.bin").exists())

    def test_full_checkout_fallback_respects_profile(self):
        """Even the no-baseline path must not materialize the whole repo."""
        self._track_both_dirs()
        self._enable_sparse("keep")
        Path("keep/in.bin").unlink()
        Path("drop/out.bin").unlink()

        result = self.runner.invoke(cli, ["sync"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        self.assertTrue(Path("keep/in.bin").exists())
        self.assertFalse(Path("drop/out.bin").exists())


class TestCheckoutRespectsSparseProfile(SparseRepoTestCase):
    def test_checkout_all_skips_out_of_profile(self):
        self._track_both_dirs()
        self._enable_sparse("keep")
        Path("keep/in.bin").unlink()
        Path("drop/out.bin").unlink()

        result = self.runner.invoke(cli, ["checkout", "--all"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        self.assertTrue(Path("keep/in.bin").exists())
        self.assertFalse(Path("drop/out.bin").exists())
        self.assertIn("outside your sparse profile", result.output)

    def test_explicit_path_outside_profile_is_honored(self):
        """An explicit request is explicit intent; it downloads, with a note."""
        self._track_both_dirs()
        self._enable_sparse("keep")
        Path("drop/out.bin").unlink()

        result = self.runner.invoke(cli, ["checkout", "drop/out.bin"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        self.assertTrue(Path("drop/out.bin").exists())
        self.assertIn("outside your sparse profile", result.output)


class TestStatusRespectsSparseProfile(SparseRepoTestCase):
    def test_out_of_profile_files_are_not_reported_missing(self):
        self._track_both_dirs()
        self._enable_sparse("keep")
        Path("drop/out.bin").unlink()

        result = self.runner.invoke(cli, ["status"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        self.assertIn("1 tracked file(s)", result.output)
        self.assertIn("0 missing", result.output)
        self.assertIn("1 more outside your sparse profile", result.output)
        self.assertNotIn("drop/out.bin", result.output)

    def test_porcelain_omits_out_of_profile(self):
        self._track_both_dirs()
        self._enable_sparse("keep")
        Path("drop/out.bin").unlink()
        self._write("keep/in.bin", "changed")

        result = self.runner.invoke(cli, ["status", "--porcelain"])
        self.assertEqual(result.output.strip(), "M keep/in.bin")

    def test_path_filter_entirely_outside_profile(self):
        self._track_both_dirs()
        self._enable_sparse("keep")

        result = self.runner.invoke(cli, ["status", "drop"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertIn("outside your sparse profile", result.output)


class TestPreCommitRespectsSparseProfile(SparseRepoTestCase):
    def test_pre_commit_does_not_walk_out_of_profile_files(self):
        """Absent-by-design files must not be stat-ed or warned about;
        otherwise every commit costs the size of the whole repository."""
        self._track_both_dirs()
        self._enable_sparse("keep")
        Path("drop/out.bin").unlink()

        result = self.runner.invoke(cli, ["pre-commit"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertNotIn("drop/out.bin", result.output)

    def test_pre_commit_still_uploads_in_profile_changes(self):
        self._track_both_dirs()
        self._enable_sparse("keep")
        Path("drop/out.bin").unlink()
        self._write("keep/in.bin", "keep-v2")

        result = self.runner.invoke(cli, ["pre-commit"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        verify = self.runner.invoke(cli, ["verify"])
        self.assertEqual(verify.exit_code, 0, msg=verify.output)

    def test_pre_commit_still_blocks_staged_tracked_file(self):
        """The staging guard covers the whole manifest, not just the slice.

        git refuses to stage out-of-cone paths without --sparse, so that
        flag is the only way to reach this state -- and it must still be
        caught, since the file would otherwise be committed into git.
        """
        self._track_both_dirs()
        self._enable_sparse("keep")
        staged = self._git("add", "--sparse", "-f", "drop/out.bin")
        self.assertEqual(staged.returncode, 0, msg=staged.stdout + staged.stderr)

        result = self.runner.invoke(cli, ["pre-commit"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("drop/out.bin", result.output)


class TestSparseCommand(SparseRepoTestCase):
    def test_reports_inactive(self):
        self._track_both_dirs()
        result = self.runner.invoke(cli, ["sparse"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertIn("not enabled", result.output)

    def test_reports_patterns_and_counts(self):
        self._track_both_dirs()
        self._enable_sparse("keep")

        result = self.runner.invoke(cli, ["sparse"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertIn("keep", result.output)
        self.assertIn("1 of 2 tracked file(s)", result.output)

    def test_porcelain_marks_each_file(self):
        self._track_both_dirs()
        self._enable_sparse("keep")

        result = self.runner.invoke(cli, ["sparse", "--porcelain"])
        lines = sorted(result.output.splitlines())
        self.assertEqual(lines, ["+ keep/in.bin", "- drop/out.bin"])


if __name__ == "__main__":
    unittest.main()


class TestSparsePruneDoesNotUntrack(SparseRepoTestCase):
    """s3lfs pruning a file is not the user deleting it.

    sync hashes a file just before pruning it, which put the file in the
    hash cache -- the very record used to tell a user deletion from a file
    that was never materialized. A later `track --modified` then untracked
    every out-of-profile file, removing it from the manifest for everyone.
    """

    def _tracked(self):
        import yaml as _yaml

        return sorted(_yaml.safe_load(Path(".s3_manifest.yaml").read_text())["files"])

    def test_narrowing_then_tracking_keeps_out_of_profile_entries(self):
        self._track_both_dirs()
        self.assertEqual(self._tracked(), ["drop/out.bin", "keep/in.bin"])

        self._enable_sparse("keep")
        self.runner.invoke(cli, ["sync", "--from", "HEAD"])
        self.assertFalse(Path("drop/out.bin").exists())

        result = self.runner.invoke(cli, ["track", "--modified"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertEqual(
            self._tracked(),
            ["drop/out.bin", "keep/in.bin"],
            "pruning a file out of the sparse profile untracked it",
        )

    def test_a_real_deletion_inside_the_profile_still_propagates(self):
        self._track_both_dirs()
        self._enable_sparse("keep")
        self.runner.invoke(cli, ["sync", "--from", "HEAD"])

        Path("keep/in.bin").unlink()
        result = self.runner.invoke(cli, ["track", "--modified"])

        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertEqual(self._tracked(), ["drop/out.bin"])


class TestShardsStayVisible(SparseRepoTestCase):
    """Manifest shards are git-tracked files under a directory, so enabling
    a sparse checkout removes them from the working copy unless that
    directory is in the cone -- and s3lfs then reports that nothing is
    tracked at all."""

    def _shard_count(self):
        return len(list(Path(".s3lfs_manifest").glob("*.yaml")))

    def test_sparse_checkout_does_not_hide_the_manifest(self):
        self._track_both_dirs()
        self.runner.invoke(cli, ["shard", "--force"])
        self._commit_all("shard the manifest")
        self.assertEqual(self._shard_count(), 2)

        # Narrow to keep/: git removes .s3lfs_manifest/ from the worktree
        self._enable_sparse("keep")

        # Any command must notice and restore it rather than reporting
        # an empty repository.
        result = self.runner.invoke(cli, ["sparse"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertEqual(self._shard_count(), 2, "the manifest stayed hidden")
        self.assertIn("2 tracked file(s)", result.output)

    def test_shard_command_adds_the_directory_to_the_cone(self):
        self._track_both_dirs()
        self._commit_all("track both")
        self._enable_sparse("keep")

        result = self.runner.invoke(cli, ["shard", "--force"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        cone = self._git("sparse-checkout", "list").stdout
        self.assertIn(".s3lfs_manifest", cone)
        self.assertEqual(self._shard_count(), 2)
