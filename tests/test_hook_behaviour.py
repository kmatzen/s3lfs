import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

from s3lfs.cli import HOOK_SCRIPTS, _install_hook, _uninstall_hook


class TestHookExitStatus(unittest.TestCase):
    """pre-push must abort the push when tracking fails.

    pre-push uploads the content the manifest being pushed refers to. If the
    upload fails and the push proceeds, collaborators fetch a manifest whose
    hashes have no objects behind them.
    """

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        self.hooks_dir = Path(self.temp_dir) / "hooks"
        self.hooks_dir.mkdir()
        self.bin_dir = Path(self.temp_dir) / "bin"
        self.bin_dir.mkdir()

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _fake_s3lfs(self, exit_code):
        """A stand-in s3lfs on PATH that exits with the given status."""
        script = self.bin_dir / "s3lfs"
        script.write_text(f"#!/bin/sh\necho 'fake s3lfs ran'\nexit {exit_code}\n")
        script.chmod(0o755)
        env = dict(os.environ)
        env["PATH"] = f"{self.bin_dir}:{env['PATH']}"
        return env

    def _run_hook(self, hook_name, exit_code, args=()):
        env = self._fake_s3lfs(exit_code)
        hook_path = _install_hook(self.hooks_dir, hook_name, HOOK_SCRIPTS[hook_name])
        return subprocess.run(
            ["/bin/sh", str(hook_path), *args],
            capture_output=True,
            text=True,
            env=env,
        )

    def test_pre_push_fails_when_track_fails(self):
        result = self._run_hook("pre-push", exit_code=1)
        self.assertNotEqual(
            result.returncode, 0, "pre-push masked a failed track and allowed the push"
        )
        self.assertIn("aborting push", result.stderr)

    def test_pre_push_succeeds_when_track_succeeds(self):
        result = self._run_hook("pre-push", exit_code=0)
        self.assertEqual(result.returncode, 0)

    def test_post_merge_does_not_fail_the_merge(self):
        """The merge has already happened; failing here helps nobody."""
        result = self._run_hook("post-merge", exit_code=1)
        self.assertEqual(result.returncode, 0)
        self.assertIn("ERROR", result.stderr)

    def test_post_checkout_reports_failure_on_stderr(self):
        # $3 == 1 marks a branch checkout
        result = self._run_hook("post-checkout", exit_code=1, args=("a", "b", "1"))
        self.assertEqual(result.returncode, 0)
        self.assertIn("ERROR", result.stderr)


class TestHookInstallAtomicity(unittest.TestCase):
    """Installing a hook must not damage a pre-existing one."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.hooks_dir = Path(self.temp_dir) / "hooks"
        self.hooks_dir.mkdir()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_preserves_existing_hook_content(self):
        hook_path = self.hooks_dir / "pre-push"
        hook_path.write_text("#!/bin/sh\necho 'user hook'\n")
        hook_path.chmod(0o755)

        _install_hook(self.hooks_dir, "pre-push", HOOK_SCRIPTS["pre-push"])

        content = hook_path.read_text()
        self.assertIn("user hook", content)
        self.assertIn("s3lfs", content)

    def test_installed_hook_is_executable(self):
        _install_hook(self.hooks_dir, "pre-push", HOOK_SCRIPTS["pre-push"])
        mode = (self.hooks_dir / "pre-push").stat().st_mode
        self.assertTrue(mode & stat.S_IXUSR)

    def test_uninstall_restores_user_content(self):
        hook_path = self.hooks_dir / "pre-push"
        hook_path.write_text("#!/bin/sh\necho 'user hook'\n")
        hook_path.chmod(0o755)

        _install_hook(self.hooks_dir, "pre-push", HOOK_SCRIPTS["pre-push"])
        _uninstall_hook(self.hooks_dir, "pre-push")

        content = hook_path.read_text()
        self.assertIn("user hook", content)
        self.assertNotIn("s3lfs", content)

    def test_no_temp_files_left_behind(self):
        _install_hook(self.hooks_dir, "pre-push", HOOK_SCRIPTS["pre-push"])
        leftovers = [
            p.name for p in self.hooks_dir.iterdir() if p.name.endswith(".tmp")
        ]
        self.assertEqual(leftovers, [])


if __name__ == "__main__":
    unittest.main()
