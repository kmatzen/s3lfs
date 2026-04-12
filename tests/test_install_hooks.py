import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml
from click.testing import CliRunner

from s3lfs.cli import (
    S3LFS_HOOK_END,
    S3LFS_HOOK_START,
    _get_hooks_dir,
    _install_hook,
    _uninstall_hook,
    cli,
)


class TestInstallHookHelper(unittest.TestCase):
    """Tests for the _install_hook helper function."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.hooks_dir = Path(self.temp_dir) / "hooks"
        self.hooks_dir.mkdir()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_creates_new_hook_file(self):
        """A new hook file is created with shebang when none exists."""
        block = f"{S3LFS_HOOK_START}\necho hello\n{S3LFS_HOOK_END}"
        path = _install_hook(self.hooks_dir, "post-merge", block)

        self.assertTrue(path.exists())
        content = path.read_text()
        self.assertTrue(content.startswith("#!/bin/sh\n"))
        self.assertIn("echo hello", content)

    def test_new_hook_is_executable(self):
        """Newly created hook file has executable permission."""
        block = f"{S3LFS_HOOK_START}\necho hello\n{S3LFS_HOOK_END}"
        path = _install_hook(self.hooks_dir, "post-merge", block)

        mode = path.stat().st_mode
        self.assertTrue(mode & stat.S_IXUSR)

    def test_appends_to_existing_hook(self):
        """s3lfs block is appended to an existing hook file."""
        hook_path = self.hooks_dir / "post-merge"
        hook_path.write_text("#!/bin/sh\necho existing\n")

        block = f"{S3LFS_HOOK_START}\necho s3lfs\n{S3LFS_HOOK_END}"
        _install_hook(self.hooks_dir, "post-merge", block)

        content = hook_path.read_text()
        self.assertIn("echo existing", content)
        self.assertIn("echo s3lfs", content)
        # Existing content should come before s3lfs block
        self.assertLess(content.index("echo existing"), content.index("echo s3lfs"))

    def test_replaces_existing_s3lfs_block(self):
        """Running install twice replaces the block rather than duplicating."""
        block_v1 = f"{S3LFS_HOOK_START}\necho v1\n{S3LFS_HOOK_END}"
        block_v2 = f"{S3LFS_HOOK_START}\necho v2\n{S3LFS_HOOK_END}"

        _install_hook(self.hooks_dir, "post-merge", block_v1)
        _install_hook(self.hooks_dir, "post-merge", block_v2)

        content = (self.hooks_dir / "post-merge").read_text()
        self.assertNotIn("echo v1", content)
        self.assertIn("echo v2", content)
        # Only one start marker
        self.assertEqual(content.count(S3LFS_HOOK_START), 1)

    def test_preserves_existing_hook_content_on_replace(self):
        """Existing non-s3lfs content is preserved when replacing."""
        hook_path = self.hooks_dir / "post-merge"
        old_block = f"{S3LFS_HOOK_START}\necho old\n{S3LFS_HOOK_END}"
        hook_path.write_text(f"#!/bin/sh\necho keeper\n\n{old_block}\n")

        new_block = f"{S3LFS_HOOK_START}\necho new\n{S3LFS_HOOK_END}"
        _install_hook(self.hooks_dir, "post-merge", new_block)

        content = hook_path.read_text()
        self.assertIn("echo keeper", content)
        self.assertIn("echo new", content)
        self.assertNotIn("echo old", content)


class TestUninstallHookHelper(unittest.TestCase):
    """Tests for the _uninstall_hook helper function."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.hooks_dir = Path(self.temp_dir) / "hooks"
        self.hooks_dir.mkdir()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_removes_s3lfs_block(self):
        """Removes the s3lfs block from a hook with other content."""
        hook_path = self.hooks_dir / "post-merge"
        block = f"{S3LFS_HOOK_START}\necho s3lfs\n{S3LFS_HOOK_END}"
        hook_path.write_text(f"#!/bin/sh\necho keeper\n\n{block}\n")

        result = _uninstall_hook(self.hooks_dir, "post-merge")

        self.assertTrue(result)
        content = hook_path.read_text()
        self.assertIn("echo keeper", content)
        self.assertNotIn("echo s3lfs", content)
        self.assertNotIn(S3LFS_HOOK_START, content)

    def test_removes_file_if_only_s3lfs(self):
        """Removes the entire file if it only contained the s3lfs block."""
        hook_path = self.hooks_dir / "post-merge"
        block = f"{S3LFS_HOOK_START}\necho s3lfs\n{S3LFS_HOOK_END}"
        hook_path.write_text(f"#!/bin/sh\n\n{block}\n")

        result = _uninstall_hook(self.hooks_dir, "post-merge")

        self.assertTrue(result)
        self.assertFalse(hook_path.exists())

    def test_returns_false_when_no_hook_file(self):
        """Returns False when the hook file doesn't exist."""
        result = _uninstall_hook(self.hooks_dir, "post-merge")
        self.assertFalse(result)

    def test_returns_false_when_no_s3lfs_block(self):
        """Returns False when the hook exists but has no s3lfs block."""
        hook_path = self.hooks_dir / "post-merge"
        hook_path.write_text("#!/bin/sh\necho other\n")

        result = _uninstall_hook(self.hooks_dir, "post-merge")

        self.assertFalse(result)
        # File should be untouched
        self.assertEqual(hook_path.read_text(), "#!/bin/sh\necho other\n")


class TestGetHooksDir(unittest.TestCase):
    """Tests for _get_hooks_dir."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        os.makedirs(".git/hooks")

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_defaults_to_git_hooks(self):
        """Returns .git/hooks by default."""
        git_root = Path(self.temp_dir)
        hooks_dir = _get_hooks_dir(git_root)
        self.assertEqual(hooks_dir, git_root / ".git" / "hooks")

    def test_respects_core_hookspath(self):
        """Respects core.hooksPath git config."""
        git_root = Path(self.temp_dir)
        custom_hooks = git_root / "custom-hooks"
        custom_hooks.mkdir()

        # Initialize a real git repo so git config works
        os.system("git init >/dev/null 2>&1")
        os.system(f"git config core.hooksPath {custom_hooks}")

        hooks_dir = _get_hooks_dir(git_root)
        self.assertEqual(hooks_dir, custom_hooks)


class TestInstallCommand(unittest.TestCase):
    """Tests for the 's3lfs install' CLI command."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        os.makedirs(".git/hooks")

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

    def test_install_creates_all_hooks(self):
        """Install command creates post-merge, post-checkout, and pre-push hooks."""
        runner = CliRunner()
        result = runner.invoke(cli, ["install"])

        self.assertEqual(result.exit_code, 0, msg=result.output)

        hooks_dir = Path(self.temp_dir) / ".git" / "hooks"
        for hook_name in ["post-merge", "post-checkout", "pre-push"]:
            hook_path = hooks_dir / hook_name
            self.assertTrue(hook_path.exists(), f"{hook_name} not created")
            content = hook_path.read_text()
            self.assertIn(S3LFS_HOOK_START, content)
            self.assertIn(S3LFS_HOOK_END, content)

    def test_install_output_lists_hooks(self):
        """Install command output mentions all hook names."""
        runner = CliRunner()
        result = runner.invoke(cli, ["install"])

        self.assertIn("post-merge", result.output)
        self.assertIn("post-checkout", result.output)
        self.assertIn("pre-push", result.output)

    def test_install_fails_without_manifest(self):
        """Install fails if s3lfs is not initialized."""
        os.remove(".s3_manifest.yaml")

        runner = CliRunner()
        result = runner.invoke(cli, ["install"])

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("not initialized", result.output)

    def test_install_fails_outside_git_repo(self):
        """Install fails if not in a git repository."""
        shutil.rmtree(".git")

        runner = CliRunner()
        result = runner.invoke(cli, ["install"])

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Not in a git repository", result.output)

    def test_install_idempotent(self):
        """Running install twice doesn't duplicate hooks."""
        runner = CliRunner()
        runner.invoke(cli, ["install"])
        runner.invoke(cli, ["install"])

        hook_path = Path(self.temp_dir) / ".git" / "hooks" / "post-merge"
        content = hook_path.read_text()
        self.assertEqual(content.count(S3LFS_HOOK_START), 1)

    def test_post_checkout_only_runs_on_branch_checkout(self):
        """Post-checkout hook checks $3 == 1 (branch checkout, not file checkout)."""
        runner = CliRunner()
        runner.invoke(cli, ["install"])

        hook_path = Path(self.temp_dir) / ".git" / "hooks" / "post-checkout"
        content = hook_path.read_text()
        self.assertIn('"$3" = "1"', content)

    def test_hooks_warn_on_failure(self):
        """Hook scripts use || echo to warn rather than fail the git operation."""
        runner = CliRunner()
        runner.invoke(cli, ["install"])

        for hook_name in ["post-merge", "post-checkout", "pre-push"]:
            hook_path = Path(self.temp_dir) / ".git" / "hooks" / hook_name
            content = hook_path.read_text()
            self.assertIn("|| echo", content, f"{hook_name} should warn on failure")

    def test_hooks_check_s3lfs_exists(self):
        """Hook scripts check that s3lfs command exists before running."""
        runner = CliRunner()
        runner.invoke(cli, ["install"])

        for hook_name in ["post-merge", "post-checkout", "pre-push"]:
            hook_path = Path(self.temp_dir) / ".git" / "hooks" / hook_name
            content = hook_path.read_text()
            self.assertIn("command -v s3lfs", content)


class TestUninstallCommand(unittest.TestCase):
    """Tests for the 's3lfs uninstall' CLI command."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        os.makedirs(".git/hooks")

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

    def test_uninstall_removes_hooks(self):
        """Uninstall removes all s3lfs hooks."""
        runner = CliRunner()
        runner.invoke(cli, ["install"])
        result = runner.invoke(cli, ["uninstall"])

        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertIn("Removed", result.output)

        hooks_dir = Path(self.temp_dir) / ".git" / "hooks"
        for hook_name in ["post-merge", "post-checkout", "pre-push"]:
            hook_path = hooks_dir / hook_name
            if hook_path.exists():
                content = hook_path.read_text()
                self.assertNotIn(S3LFS_HOOK_START, content)

    def test_uninstall_preserves_other_hooks(self):
        """Uninstall preserves non-s3lfs content in hook files."""
        hooks_dir = Path(self.temp_dir) / ".git" / "hooks"

        # Create a hook with custom content, then install s3lfs
        hook_path = hooks_dir / "post-merge"
        hook_path.write_text("#!/bin/sh\necho my-custom-hook\n")

        runner = CliRunner()
        runner.invoke(cli, ["install"])
        runner.invoke(cli, ["uninstall"])

        content = hook_path.read_text()
        self.assertIn("echo my-custom-hook", content)
        self.assertNotIn(S3LFS_HOOK_START, content)

    def test_uninstall_no_hooks_found(self):
        """Uninstall reports when no hooks are found."""
        runner = CliRunner()
        result = runner.invoke(cli, ["uninstall"])

        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertIn("No s3lfs hooks found", result.output)

    def test_uninstall_outside_git_repo(self):
        """Uninstall fails if not in a git repository."""
        shutil.rmtree(".git")

        runner = CliRunner()
        result = runner.invoke(cli, ["uninstall"])

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Not in a git repository", result.output)


class TestHooksEndToEnd(unittest.TestCase):
    """Verify git actually invokes the installed hooks."""

    def setUp(self):
        self.temp_dir = os.path.realpath(tempfile.mkdtemp())
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)

        # Create a real git repo (not just .git dir)
        subprocess.run(["git", "init"], capture_output=True, check=True)
        subprocess.run(
            ["git", "config", "user.email", "test@test.com"], capture_output=True
        )
        subprocess.run(["git", "config", "user.name", "Test"], capture_output=True)

        manifest = {
            "bucket_name": "test-bucket",
            "repo_prefix": "test-prefix",
            "files": {},
        }
        with open(".s3_manifest.yaml", "w") as f:
            yaml.safe_dump(manifest, f)

        # Initial commit so we have a branch to work with
        Path("initial.txt").write_text("init")
        subprocess.run(["git", "add", "."], capture_output=True)
        subprocess.run(["git", "commit", "-m", "initial"], capture_output=True)

        # Record default branch name before any checkouts
        result = subprocess.run(
            ["git", "branch", "--show-current"],
            capture_output=True,
            text=True,
        )
        self.default_branch = result.stdout.strip()

        # Markers dir must be outside the git repo so git checkout
        # doesn't remove it when switching branches.
        self.markers_dir = Path(os.path.realpath(tempfile.mkdtemp()))

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        shutil.rmtree(self.markers_dir, ignore_errors=True)

    def _write_marker_hooks(self):
        """Write minimal hook scripts that create marker files.

        These replace the s3lfs hooks with simple shell scripts that
        only write marker files.  The goal is to test that git invokes
        hooks at the correct times, independent of whether s3lfs is on
        PATH or has a working S3 backend.

        For post-checkout, the marker is guarded by $3==1 (branch
        checkout), matching s3lfs's real hook behavior.
        """
        hooks_dir = Path(self.temp_dir) / ".git" / "hooks"
        hooks_dir.mkdir(parents=True, exist_ok=True)

        for hook_name in ["post-merge", "post-checkout", "pre-push"]:
            marker = self.markers_dir / f"{hook_name}.fired"
            hook_path = hooks_dir / hook_name

            if hook_name == "post-checkout":
                # Guard with $3==1 (branch checkout only)
                guard = '[ "$3" = "1" ]'
                script = "\n".join(
                    [
                        "#!/bin/sh",
                        "if %s; then" % guard,
                        '    touch "%s"' % marker,
                        "fi",
                        "",
                    ]
                )
            else:
                script = '#!/bin/sh\ntouch "%s"\n' % marker

            hook_path.write_text(script)
            hook_path.chmod(0o755)

    def test_post_checkout_fires_on_branch_switch(self):
        """post-checkout hook fires when switching branches."""
        self._write_marker_hooks()

        # Create and switch to a new branch, then switch back
        subprocess.run(["git", "checkout", "-b", "feature"], capture_output=True)
        Path("feature.txt").write_text("feature")
        subprocess.run(["git", "add", "."], capture_output=True)
        subprocess.run(["git", "commit", "-m", "feature"], capture_output=True)

        # Clear any marker from the branch creation
        marker = self.markers_dir / "post-checkout.fired"
        if marker.exists():
            marker.unlink()

        # Switch back -- this is a branch checkout ($3 == 1)
        subprocess.run(["git", "checkout", self.default_branch], capture_output=True)

        self.assertTrue(
            marker.exists(),
            "post-checkout hook did not fire on branch switch",
        )

    def test_post_merge_fires_on_merge(self):
        """post-merge hook fires when merging a branch."""
        self._write_marker_hooks()

        # Create a branch with a commit
        subprocess.run(["git", "checkout", "-b", "to-merge"], capture_output=True)
        Path("merged.txt").write_text("merged content")
        subprocess.run(["git", "add", "."], capture_output=True)
        subprocess.run(["git", "commit", "-m", "to merge"], capture_output=True)

        # Switch back and merge
        subprocess.run(["git", "checkout", self.default_branch], capture_output=True)

        marker = self.markers_dir / "post-merge.fired"
        if marker.exists():
            marker.unlink()

        subprocess.run(["git", "merge", "to-merge", "--no-edit"], capture_output=True)

        self.assertTrue(
            marker.exists(),
            "post-merge hook did not fire on merge",
        )

    def test_pre_push_fires_on_push(self):
        """pre-push hook fires when pushing to a remote."""
        self._write_marker_hooks()

        # Create a local bare repo as remote
        bare_dir = Path(self.temp_dir) / "bare.git"
        subprocess.run(["git", "init", "--bare", str(bare_dir)], capture_output=True)
        subprocess.run(
            ["git", "remote", "add", "test-remote", str(bare_dir)],
            capture_output=True,
        )

        marker = self.markers_dir / "pre-push.fired"
        if marker.exists():
            marker.unlink()

        # Detect default branch name
        result = subprocess.run(
            ["git", "branch", "--show-current"],
            capture_output=True,
            text=True,
        )
        branch = result.stdout.strip()

        subprocess.run(
            ["git", "push", "test-remote", branch],
            capture_output=True,
        )

        self.assertTrue(
            marker.exists(),
            "pre-push hook did not fire on push",
        )

    def test_post_checkout_does_not_fire_on_file_checkout(self):
        """post-checkout hook does not fire on file-level checkout ($3 != 1)."""
        self._write_marker_hooks()

        # Modify a file and restore it with git checkout -- file
        Path("initial.txt").write_text("modified")
        marker = self.markers_dir / "post-checkout.fired"
        if marker.exists():
            marker.unlink()

        subprocess.run(["git", "checkout", "--", "initial.txt"], capture_output=True)

        self.assertFalse(
            marker.exists(),
            "post-checkout hook should not fire on file checkout",
        )


if __name__ == "__main__":
    unittest.main()
