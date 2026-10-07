"""Tests for the restore command and atomic checkout finalization.

Restore: the target bucket uses S3 Intelligent-Tiering, so cold objects
age into archive tiers where GetObject fails with InvalidObjectState.
The restore command must request a Standard-tier restore (without a Days
parameter, which S3 rejects for Intelligent-Tiering objects) and poll
until the objects accept reads again.

Atomic checkout: a download that fails partway (decompress error,
InvalidObjectState mid-transfer) must not destroy a previously-good copy.
Finalization now writes a temp file in the destination directory and only
os.replace()s it onto the destination after full success.
"""

import contextlib
import gzip
import hashlib
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import yaml
from botocore.exceptions import ClientError

from s3lfs.core import S3LFS


def _client_error(code):
    return ClientError({"Error": {"Code": code, "Message": code}}, "GetObject")


class _RepoTestCase(unittest.TestCase):
    """A git-style working tree with a manifest, plus helpers to build S3LFS."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        os.makedirs(".git")

        self.manifest_path = Path(self.temp_dir) / ".s3_manifest.yaml"
        with open(self.manifest_path, "w") as f:
            yaml.safe_dump(
                {
                    "bucket_name": "test-bucket",
                    "repo_prefix": "test-prefix",
                    "files": {},
                },
                f,
            )

        self.payload = b"A" * 1024
        self.digest = hashlib.sha256(self.payload).hexdigest()
        self.key = "data/file.bin"
        self.base_key = f"test-prefix/assets/{self.digest}/{self.key}"
        self.gz = gzip.compress(self.payload)

    def tearDown(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _manifest_files(self):
        with open(self.manifest_path) as f:
            return yaml.safe_load(f).get("files", {})

    def _s3lfs(self, client):
        with patch("boto3.client"):
            s3lfs = S3LFS(
                bucket_name="test-bucket",
                manifest_file=str(self.manifest_path),
                s3_factory=lambda no_sign: client,
            )
        return s3lfs

    def _list_client(self, objects):
        """A client that serves list_objects_v2 from a fixed object map."""
        client = Mock()

        def list_objects_v2(Bucket=None, Prefix=None, **kwargs):
            keys = sorted(k for k in objects if k.startswith(Prefix or ""))
            return {"Contents": [{"Key": k} for k in keys]} if keys else {}

        client.list_objects_v2.side_effect = list_objects_v2
        return client


class TestRestoreRequest(_RepoTestCase):
    """The restore command's S3 request shape and skip behaviour."""

    def _archived_client(self, objects, restore_side_effect=None):
        """Objects that all refuse reads until restore_object is called."""
        client = self._list_client(objects)
        state: dict = {"restored": {}}

        def get_object(Bucket=None, Key=None, Range=None, **kwargs):
            if not state["restored"].get(Key):
                raise _client_error("InvalidObjectState")
            return {"Body": Mock(read=lambda n: b"")}

        client.get_object.side_effect = get_object
        if restore_side_effect is None:
            client.restore_object.side_effect = lambda **kw: state["restored"].__setitem__(kw["Key"], True)
        else:
            client.restore_object.side_effect = restore_side_effect
        return client

    def test_restore_request_has_no_days_key(self):
        """Intelligent-Tiering rejects Days; the request must omit it."""
        with open(self.manifest_path, "w") as f:
            yaml.safe_dump(
                {
                    "bucket_name": "test-bucket",
                    "repo_prefix": "test-prefix",
                    "files": {self.key: self.digest},
                },
                f,
            )
        client = self._archived_client({self.base_key: 1024})
        s3lfs = self._s3lfs(client)

        failed = s3lfs.restore({self.key: self.digest}, timeout=5, interval=0)

        self.assertEqual(failed, 0)
        self.assertEqual(client.restore_object.call_count, 1)
        kwargs = client.restore_object.call_args.kwargs
        self.assertEqual(kwargs["Bucket"], "test-bucket")
        self.assertEqual(kwargs["Key"], self.base_key)
        request = kwargs["RestoreRequest"]
        self.assertNotIn("Days", request, "Days is rejected for Intelligent-Tiering")
        self.assertEqual(
            request, {"GlacierJobParameters": {"Tier": "Standard"}}
        )

    def test_restore_marks_already_readable_as_skipped(self):
        """Objects that accept a ranged GET are not restore-requested."""
        with open(self.manifest_path, "w") as f:
            yaml.safe_dump(
                {
                    "bucket_name": "test-bucket",
                    "repo_prefix": "test-prefix",
                    "files": {self.key: self.digest},
                },
                f,
            )
        client = self._list_client({self.base_key: 1024})
        client.get_object.return_value = {"Body": Mock(read=lambda n: b"")}
        s3lfs = self._s3lfs(client)

        failed = s3lfs.restore({self.key: self.digest}, timeout=5, interval=0)

        self.assertEqual(failed, 0)
        client.restore_object.assert_not_called()
        client.get_object.assert_called()

    def test_restore_missing_object_fails(self):
        """A 404 on the probe reports the file and never calls restore."""
        with open(self.manifest_path, "w") as f:
            yaml.safe_dump(
                {
                    "bucket_name": "test-bucket",
                    "repo_prefix": "test-prefix",
                    "files": {self.key: self.digest},
                },
                f,
            )
        client = self._list_client({self.base_key: 1024})
        client.get_object.side_effect = _client_error("NoSuchKey")
        s3lfs = self._s3lfs(client)

        failed = s3lfs.restore({self.key: self.digest}, timeout=5, interval=0)

        self.assertEqual(failed, 1)
        client.restore_object.assert_not_called()

    def test_restore_concurrent_in_progress_is_tolerated(self):
        """A concurrent restore (InvalidArgument) is treated as in flight."""
        with open(self.manifest_path, "w") as f:
            yaml.safe_dump(
                {
                    "bucket_name": "test-bucket",
                    "repo_prefix": "test-prefix",
                    "files": {self.key: self.digest},
                },
                f,
            )
        client = self._archived_client(
            {self.base_key: 1024},
            restore_side_effect=_client_error("InvalidArgument"),
        )
        s3lfs = self._s3lfs(client)

        failed = s3lfs.restore({self.key: self.digest}, timeout=5, interval=0)

        # The object never becomes readable, so the timeout is hit and the
        # concurrent restore is reported as a failure; that is the contract.
        self.assertEqual(failed, 1)
        client.restore_object.assert_called_once()

    def test_restore_restores_every_chunk(self):
        """A chunked object needs one RestoreObject call per chunk."""
        with open(self.manifest_path, "w") as f:
            yaml.safe_dump(
                {
                    "bucket_name": "test-bucket",
                    "repo_prefix": "test-prefix",
                    "files": {self.key: self.digest},
                },
                f,
            )
        chunk_keys = [f"{self.base_key}.chunk{i}" for i in range(3)]
        client = self._archived_client(dict.fromkeys(chunk_keys, 1024))
        s3lfs = self._s3lfs(client)

        failed = s3lfs.restore({self.key: self.digest}, timeout=5, interval=0)

        self.assertEqual(failed, 0)
        self.assertEqual(client.restore_object.call_count, 3)
        restored = {c.kwargs["Key"] for c in client.restore_object.call_args_list}
        self.assertEqual(restored, set(chunk_keys))

    def test_restore_timeout_reports_still_archived(self):
        """Objects that never become readable fail when the timeout is hit."""
        with open(self.manifest_path, "w") as f:
            yaml.safe_dump(
                {
                    "bucket_name": "test-bucket",
                    "repo_prefix": "test-prefix",
                    "files": {self.key: self.digest},
                },
                f,
            )
        # restore_object succeeds but the object never becomes readable.
        client = self._archived_client(
            {self.base_key: 1024},
            restore_side_effect=lambda **kw: None,
        )
        s3lfs = self._s3lfs(client)

        failed = s3lfs.restore({self.key: self.digest}, timeout=0, interval=0)

        self.assertEqual(failed, 1)
        client.restore_object.assert_called_once()


class TestAtomicFinalize(_RepoTestCase):
    """Checkout finalization must never destroy a pre-existing file."""

    def test_decompress_failure_preserves_preexisting_file(self):
        """A gzip that fails mid-stream leaves the old file intact."""
        good = Path(self.key)
        good.parent.mkdir(parents=True)
        good.write_bytes(self.payload)

        # Not a gzip stream at all: decompressing dies partway and would
        # have truncated the destination to whatever it had written.
        corrupt = Path(self.temp_dir) / "corrupt.gz"
        corrupt.write_bytes(b"\x00\x01garbage-not-gzip")

        client = self._list_client({self.base_key + ".gz": 20})
        s3lfs = self._s3lfs(client)

        with self.assertRaises((OSError, RuntimeError, subprocess.CalledProcessError)):
            # The gzip CLI (or Python fallback) raises on a bad stream.
            s3lfs._finalize_file(
                self.key,
                [corrupt],
                False,
                expected_hash=self.digest,
                compressed=True,
            )

        self.assertEqual(
            good.read_bytes(), self.payload, "pre-existing file was destroyed"
        )
        self.assertGreater(good.stat().st_size, 0)
        leftovers = list(good.parent.glob("*.s3lfs-tmp"))
        self.assertEqual(leftovers, [], "staging temp file was left behind")

    def test_successful_decompress_replaces_preexisting_file(self):
        """A clean finalize still lands the new content at the destination."""
        good = Path(self.key)
        good.parent.mkdir(parents=True)
        old_content = b"old bytes"
        good.write_bytes(old_content)

        stored = Path(self.temp_dir) / "stored.gz"
        stored.write_bytes(self.gz)

        client = self._list_client({self.base_key + ".gz": len(self.gz)})
        s3lfs = self._s3lfs(client)

        s3lfs._finalize_file(
            self.key,
            [stored],
            False,
            expected_hash=self.digest,
            compressed=True,
        )

        self.assertEqual(good.read_bytes(), self.payload)

    def test_checksum_mismatch_preserves_preexisting_file(self):
        """A valid gzip with the wrong content must not clobber the old file."""
        good = Path(self.key)
        good.parent.mkdir(parents=True)
        good.write_bytes(self.payload)

        other = b"B" * 1024
        stored = Path(self.temp_dir) / "stored.gz"
        stored.write_bytes(gzip.compress(other))

        client = self._list_client({self.base_key + ".gz": len(other)})
        s3lfs = self._s3lfs(client)

        with self.assertRaises(RuntimeError) as ctx:
            s3lfs._finalize_file(
                self.key,
                [stored],
                False,
                expected_hash=self.digest,
                compressed=True,
            )

        self.assertIn("Checksum mismatch", str(ctx.exception))
        self.assertEqual(
            good.read_bytes(), self.payload, "pre-existing file was destroyed"
        )
        leftovers = list(good.parent.glob("*.s3lfs-tmp"))
        self.assertEqual(leftovers, [], "staging temp file was left behind")

    def test_download_failure_preserves_preexisting_file(self):
        """An InvalidObjectState mid-transfer leaves the old copy intact.

        The full checkout download path: the object disappears (archive
        tier) mid-download, the old destination must survive.
        """
        good = Path(self.key)
        good.parent.mkdir(parents=True)
        good.write_bytes(self.payload)

        # Manifest says this file is stored, so the download proceeds.
        with open(self.manifest_path, "w") as f:
            yaml.safe_dump(
                {
                    "bucket_name": "test-bucket",
                    "repo_prefix": "test-prefix",
                    "files": {self.key: self.digest},
                },
                f,
            )

        client = self._list_client({self.base_key: len(self.payload)})
        # Discovery lists fine, but the ranged download hits the archive.
        client.download_fileobj.side_effect = _client_error("InvalidObjectState")
        s3lfs = self._s3lfs(client)

        # The old copy is up-to-date hash-wise, so force the download by
        # making it stale on disk.
        good.write_bytes(b"stale")

        with contextlib.suppress(Exception):
            # InvalidObjectState is non-retryable; download re-raises it.
            s3lfs.download(self.key, expected_hash=self.digest)

        # The destination must be either the old good copy (never touched
        # after the attempt began) or the stale one -- never 0 bytes.
        self.assertIn(
            good.read_bytes(),
            [self.payload, b"stale"],
            "destination was destroyed by the failed download",
        )
        self.assertGreater(good.stat().st_size, 0)
        leftovers = list(good.parent.glob("*.s3lfs-tmp"))
        self.assertEqual(leftovers, [], "staging temp file was left behind")

    def test_download_success_writes_new_content(self):
        """The normal download path still lands the content."""
        client = self._list_client({self.base_key: len(self.payload)})

        def fake_download(Bucket, Key, Fileobj, **kwargs):
            Fileobj.write(self.payload)

        client.download_fileobj.side_effect = fake_download
        s3lfs = self._s3lfs(client)

        with open(self.manifest_path, "w") as f:
            yaml.safe_dump(
                {
                    "bucket_name": "test-bucket",
                    "repo_prefix": "test-prefix",
                    "files": {self.key: self.digest},
                },
                f,
            )

        s3lfs.download(self.key, expected_hash=self.digest)
        self.assertEqual(Path(self.key).read_bytes(), self.payload)


if __name__ == "__main__":
    unittest.main()
