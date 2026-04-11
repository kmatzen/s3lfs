"""
Integration tests against a real S3-compatible server (MinIO).

These tests exercise the full HTTP path through boto3, including
multipart handling, ETags, compression, and parallel transfers.
They do NOT use moto -- all S3 operations hit a real server.

Requires environment variables:
    S3LFS_TEST_ENDPOINT  - MinIO endpoint (e.g. http://localhost:9000)
    S3LFS_TEST_BUCKET    - Pre-created bucket name

Skip automatically when the endpoint is not available.
"""

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from click.testing import CliRunner

from s3lfs.cli import cli
from s3lfs.core import S3LFS

ENDPOINT = os.environ.get("S3LFS_TEST_ENDPOINT")
BUCKET = os.environ.get("S3LFS_TEST_BUCKET")

_skip_reason = "S3LFS_TEST_ENDPOINT and S3LFS_TEST_BUCKET not set"


def _init_repo_no_encryption(prefix):
    """Initialize s3lfs via the Python API with encryption disabled.

    MinIO does not support AES256 server-side encryption by default,
    so integration tests must disable it.
    """
    s3 = S3LFS(
        bucket_name=BUCKET,
        repo_prefix=prefix,
        endpoint_url=ENDPOINT,
        encryption=False,
    )
    s3.initialize_repo()
    return s3


@unittest.skipUnless(ENDPOINT and BUCKET, _skip_reason)
class TestMinIOWorkflow(unittest.TestCase):
    """Full workflow against a real MinIO server using the Python API.

    Uses the Python API (not CLI) for all S3 operations so we can
    disable encryption, which MinIO does not support by default.
    CLI commands like ls, remove, install/uninstall that don't touch
    S3 directly still use the CLI.
    """

    def setUp(self):
        self.temp_dir = os.path.realpath(tempfile.mkdtemp())
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)

        subprocess.run(["git", "init"], capture_output=True, check=True)
        subprocess.run(
            ["git", "config", "user.email", "test@test.com"],
            capture_output=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Test"],
            capture_output=True,
        )

        import uuid

        self.prefix = f"test-{uuid.uuid4().hex[:8]}"
        self.s3lfs = _init_repo_no_encryption(self.prefix)

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_init_track_checkout_roundtrip(self):
        """Track files, delete them, checkout, verify content."""
        os.makedirs("data", exist_ok=True)
        Path("data/hello.txt").write_text("hello world")
        Path("data/binary.bin").write_bytes(b"\x00\x01\x02" * 1000)

        self.s3lfs.track("data/", silence=True)

        self.assertIn("data/hello.txt", self.s3lfs.manifest["files"])
        self.assertIn("data/binary.bin", self.s3lfs.manifest["files"])

        os.remove("data/hello.txt")
        os.remove("data/binary.bin")

        self.s3lfs.parallel_download_all(silence=True)

        self.assertEqual(Path("data/hello.txt").read_text(), "hello world")
        self.assertEqual(Path("data/binary.bin").read_bytes(), b"\x00\x01\x02" * 1000)

    def test_selective_checkout(self):
        """Checkout a single file by path."""
        Path("a.txt").write_text("file a")
        Path("b.txt").write_text("file b")

        self.s3lfs.upload("a.txt", silence=True)
        self.s3lfs.upload("b.txt", silence=True)

        os.remove("a.txt")
        os.remove("b.txt")

        self.s3lfs.download("a.txt", silence=True)
        self.assertTrue(Path("a.txt").exists())
        self.assertFalse(Path("b.txt").exists())
        self.assertEqual(Path("a.txt").read_text(), "file a")

    def test_track_modified(self):
        """track_modified_files_cached detects and re-uploads changed files."""
        Path("doc.txt").write_text("version 1")
        self.s3lfs.upload("doc.txt", silence=True)
        hash_v1 = self.s3lfs.manifest["files"]["doc.txt"]

        Path("doc.txt").write_text("version 2")
        self.s3lfs.track_modified_files_cached(silence=True)
        hash_v2 = self.s3lfs.manifest["files"]["doc.txt"]

        self.assertNotEqual(hash_v1, hash_v2)

        os.remove("doc.txt")
        self.s3lfs.download("doc.txt", silence=True)
        self.assertEqual(Path("doc.txt").read_text(), "version 2")

    def test_ls(self):
        """ls lists tracked files via CLI."""
        Path("x.txt").write_text("x")
        Path("y.txt").write_text("y")
        self.s3lfs.upload("x.txt", silence=True)
        self.s3lfs.upload("y.txt", silence=True)

        runner = CliRunner()
        result = runner.invoke(cli, ["ls"])
        self.assertEqual(result.exit_code, 0)
        self.assertIn("x.txt", result.output)
        self.assertIn("y.txt", result.output)

    def test_remove(self):
        """remove stops tracking a file."""
        Path("temp.txt").write_text("temporary")
        self.s3lfs.upload("temp.txt", silence=True)

        runner = CliRunner()
        result = runner.invoke(cli, ["remove", "temp.txt"])
        self.assertEqual(result.exit_code, 0)

        result = runner.invoke(cli, ["ls"])
        self.assertNotIn("temp.txt", result.output)

    def test_deduplication(self):
        """Two files with identical content share the same hash."""
        content = b"identical content across files"
        Path("copy1.bin").write_bytes(content)
        Path("copy2.bin").write_bytes(content)

        self.s3lfs.upload("copy1.bin", silence=True)
        self.s3lfs.upload("copy2.bin", silence=True)

        self.assertEqual(
            self.s3lfs.manifest["files"]["copy1.bin"],
            self.s3lfs.manifest["files"]["copy2.bin"],
        )

        os.remove("copy1.bin")
        os.remove("copy2.bin")
        self.s3lfs.parallel_download_all(silence=True)
        self.assertEqual(Path("copy1.bin").read_bytes(), content)
        self.assertEqual(Path("copy2.bin").read_bytes(), content)

    def test_install_uninstall_hooks(self):
        """install/uninstall create and remove git hooks."""
        runner = CliRunner()

        result = runner.invoke(cli, ["install"])
        self.assertEqual(result.exit_code, 0)
        self.assertTrue(Path(".git/hooks/post-merge").exists())
        self.assertTrue(Path(".git/hooks/post-checkout").exists())
        self.assertTrue(Path(".git/hooks/pre-push").exists())

        result = runner.invoke(cli, ["uninstall"])
        self.assertEqual(result.exit_code, 0)
        self.assertFalse(Path(".git/hooks/post-merge").exists())
        self.assertFalse(Path(".git/hooks/post-checkout").exists())
        self.assertFalse(Path(".git/hooks/pre-push").exists())


@unittest.skipUnless(ENDPOINT and BUCKET, _skip_reason)
class TestMinIOCoreAPI(unittest.TestCase):
    """Test the S3LFS Python API against a real MinIO server."""

    def setUp(self):
        self.temp_dir = os.path.realpath(tempfile.mkdtemp())
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)

        subprocess.run(["git", "init"], capture_output=True, check=True)

        import uuid

        self.prefix = f"test-{uuid.uuid4().hex[:8]}"

        self.s3lfs = _init_repo_no_encryption(self.prefix)

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_upload_download_roundtrip(self):
        """Upload via API, download, verify byte-level correctness."""
        data = os.urandom(50000)  # 50KB of random data
        Path("random.bin").write_bytes(data)

        self.s3lfs.upload("random.bin", silence=True)

        os.remove("random.bin")
        self.s3lfs.download("random.bin", silence=True)

        self.assertEqual(Path("random.bin").read_bytes(), data)

    def test_hash_integrity(self):
        """Manifest hash matches the file's actual SHA-256."""
        import hashlib

        content = b"integrity check content"
        Path("check.txt").write_bytes(content)

        self.s3lfs.upload("check.txt", silence=True)

        expected_hash = hashlib.sha256(content).hexdigest()
        stored_hash = self.s3lfs.manifest["files"].get("check.txt")
        self.assertEqual(stored_hash, expected_hash)

    def test_skip_upload_when_unchanged(self):
        """Uploading the same file twice does not re-upload."""
        Path("stable.txt").write_text("stable")
        self.s3lfs.upload("stable.txt", silence=True)

        # Track the S3 client calls
        client = self.s3lfs._get_s3_client()
        original_upload = client.upload_fileobj

        upload_count = [0]

        def counting_upload(*args, **kwargs):
            upload_count[0] += 1
            return original_upload(*args, **kwargs)

        client.upload_fileobj = counting_upload

        # Upload again -- should skip (matching MD5)
        self.s3lfs.upload("stable.txt", silence=True)

        self.assertEqual(
            upload_count[0],
            0,
            "File was re-uploaded even though content unchanged",
        )

    def test_parallel_download_all(self):
        """parallel_download_all restores multiple files."""
        for i in range(5):
            Path(f"file{i}.txt").write_text(f"content {i}")
            self.s3lfs.upload(f"file{i}.txt", silence=True)

        for i in range(5):
            os.remove(f"file{i}.txt")

        self.s3lfs.parallel_download_all(silence=True)

        for i in range(5):
            self.assertTrue(Path(f"file{i}.txt").exists())
            self.assertEqual(Path(f"file{i}.txt").read_text(), f"content {i}")


if __name__ == "__main__":
    unittest.main()
