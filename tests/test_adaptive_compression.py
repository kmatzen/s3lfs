"""Adaptive compression: incompressible files are stored raw under their
natural key, so the bucket is usable without s3lfs -- and gzip time is not
spent achieving nothing."""

import gzip
import hashlib
import os
import random
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import boto3
import yaml
from click.testing import CliRunner
from moto import mock_s3

from s3lfs.cli import cli
from s3lfs.core import S3LFS

TEST_BUCKET = "test-bucket-s3lfs-adaptive"


class AdaptiveRepoTestCase(unittest.TestCase):
    def setUp(self):
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        self.addCleanup(self.mock_s3.stop)

        self.temp_dir = os.path.realpath(tempfile.mkdtemp())
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        self.addCleanup(self._cleanup)

        subprocess.run(["git", "init", "-q", "."], check=True)
        self.s3 = boto3.client("s3", region_name="us-east-1")
        self.s3.create_bucket(Bucket=TEST_BUCKET)

        self.runner = CliRunner()
        result = self.runner.invoke(cli, ["init", TEST_BUCKET, "pfx"])
        assert result.exit_code == 0, result.output

        random.seed(7)
        self.raw_bytes = random.randbytes(300 * 1024)  # incompressible
        self.text_bytes = b"the same line over and over\n" * 20000  # compressible

    def _cleanup(self):
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _keys(self):
        resp = self.s3.list_objects_v2(Bucket=TEST_BUCKET)
        return sorted(o["Key"] for o in resp.get("Contents", []))

    def _manifest_hash(self, key):
        return yaml.safe_load(Path(".s3_manifest.yaml").read_text())["files"][key]


class TestStorageFormatChoice(AdaptiveRepoTestCase):
    def test_incompressible_is_stored_raw_and_byte_identical(self):
        """The transparency promise: a raw object keeps the file's natural
        name and exact bytes, fetchable by any S3 tool."""
        Path("photo.jpg").write_bytes(self.raw_bytes)
        self.runner.invoke(cli, ["track", "photo.jpg"])

        h = self._manifest_hash("photo.jpg")
        key = f"pfx/assets/{h}/photo.jpg"
        self.assertIn(key, self._keys())

        body = self.s3.get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read()
        self.assertEqual(body, self.raw_bytes, "stored object is not the raw file")

    def test_compressible_is_stored_gzipped(self):
        Path("table.csv").write_bytes(self.text_bytes)
        self.runner.invoke(cli, ["track", "table.csv"])

        h = self._manifest_hash("table.csv")
        key = f"pfx/assets/{h}/table.csv.gz"
        self.assertIn(key, self._keys())

        body = self.s3.get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read()
        self.assertLess(len(body), len(self.text_bytes) // 10)
        self.assertEqual(gzip.decompress(body), self.text_bytes)

    def test_both_roundtrip_through_checkout(self):
        Path("photo.jpg").write_bytes(self.raw_bytes)
        Path("table.csv").write_bytes(self.text_bytes)
        self.runner.invoke(cli, ["track", "photo.jpg"])
        self.runner.invoke(cli, ["track", "table.csv"])

        os.remove("photo.jpg")
        os.remove("table.csv")
        result = self.runner.invoke(cli, ["checkout", "--all"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        self.assertEqual(Path("photo.jpg").read_bytes(), self.raw_bytes)
        self.assertEqual(Path("table.csv").read_bytes(), self.text_bytes)

    def test_legacy_gz_objects_still_check_out(self):
        """Buckets written before adaptive compression hold only .gz
        objects; a new client must read them unchanged."""
        content = self.raw_bytes
        h = hashlib.sha256(content).hexdigest()
        # Plant an old-format object directly: gzipped despite being
        # incompressible, as every pre-0.6 version stored it.
        self.s3.put_object(
            Bucket=TEST_BUCKET,
            Key=f"pfx/assets/{h}/old.bin.gz",
            Body=gzip.compress(content),
        )
        manifest = yaml.safe_load(Path(".s3_manifest.yaml").read_text())
        manifest["files"]["old.bin"] = h
        Path(".s3_manifest.yaml").write_text(yaml.safe_dump(manifest))

        result = self.runner.invoke(cli, ["checkout", "--all"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertEqual(Path("old.bin").read_bytes(), content)

    def test_compression_never_and_always_are_respected(self):
        Path(".s3lfsconfig").write_text("compression: never\n")
        Path("table.csv").write_bytes(self.text_bytes)
        self.runner.invoke(cli, ["track", "table.csv"])
        h = self._manifest_hash("table.csv")
        self.assertIn(f"pfx/assets/{h}/table.csv", self._keys())

        Path(".s3lfsconfig").write_text("compression: always\n")
        Path("photo.jpg").write_bytes(self.raw_bytes)
        self.runner.invoke(cli, ["track", "photo.jpg"])
        h2 = self._manifest_hash("photo.jpg")
        self.assertIn(f"pfx/assets/{h2}/photo.jpg.gz", self._keys())


class TestEscapeHatch(AdaptiveRepoTestCase):
    def test_restore_with_plain_boto3_and_the_manifest_only(self):
        """The anti-lock-in promise, encoded as a test.

        Given nothing but the committed manifest and any S3 client, a user
        can reconstruct their files without s3lfs: the manifest maps path
        to hash, the key is prefix/assets/<hash>/<path>[.gz], and a .gz
        suffix means gunzip. If this test breaks, the documented recovery
        recipe in the README breaks with it.
        """
        Path("photo.jpg").write_bytes(self.raw_bytes)
        Path("table.csv").write_bytes(self.text_bytes)
        self.runner.invoke(cli, ["track", "photo.jpg"])
        self.runner.invoke(cli, ["track", "table.csv"])

        manifest = yaml.safe_load(Path(".s3_manifest.yaml").read_text())
        restored = {}
        for path, digest in manifest["files"].items():
            stem = f"{manifest['repo_prefix']}/assets/{digest}/{path}"
            listed = self.s3.list_objects_v2(Bucket=TEST_BUCKET, Prefix=stem)
            keys = {o["Key"] for o in listed.get("Contents", [])}
            if stem in keys:
                restored[path] = self.s3.get_object(Bucket=TEST_BUCKET, Key=stem)[
                    "Body"
                ].read()
            elif stem + ".gz" in keys:
                restored[path] = gzip.decompress(
                    self.s3.get_object(Bucket=TEST_BUCKET, Key=stem + ".gz")[
                        "Body"
                    ].read()
                )

        self.assertEqual(restored["photo.jpg"], self.raw_bytes)
        self.assertEqual(restored["table.csv"], self.text_bytes)
        for path, content in restored.items():
            self.assertEqual(
                hashlib.sha256(content).hexdigest(), manifest["files"][path]
            )


class TestLifecycleWithRawObjects(AdaptiveRepoTestCase):
    def test_verify_sees_raw_objects(self):
        Path("photo.jpg").write_bytes(self.raw_bytes)
        self.runner.invoke(cli, ["track", "photo.jpg"])

        result = self.runner.invoke(cli, ["verify"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertIn("verified present", result.output)

    def test_gc_keeps_live_raw_objects_and_collects_dead_ones(self):
        Path("keep.jpg").write_bytes(self.raw_bytes)
        Path("drop.jpg").write_bytes(self.raw_bytes[::-1])
        self.runner.invoke(cli, ["track", "keep.jpg"])
        self.runner.invoke(cli, ["track", "drop.jpg"])
        self.runner.invoke(cli, ["remove", "drop.jpg"])

        result = self.runner.invoke(cli, ["cleanup", "--force"])
        self.assertEqual(result.exit_code, 0, msg=result.output)

        keys = self._keys()
        self.assertTrue(any(k.endswith("/keep.jpg") for k in keys), keys)
        self.assertFalse(any("drop.jpg" in k for k in keys), keys)

    def test_chunked_raw_roundtrip(self):
        """A large incompressible file is chunked without a .gz layer."""
        big = random.randbytes(1024 * 1024)
        Path("big.bin").write_bytes(big)

        s3lfs = S3LFS(
            bucket_name=TEST_BUCKET,
            manifest_file=".s3_manifest.yaml",
            chunk_size=256 * 1024,
        )
        s3lfs.parallel_upload(["big.bin"])

        h = hashlib.sha256(big).hexdigest()
        chunk_keys = [k for k in self._keys() if f"{h}/big.bin.chunk" in k]
        self.assertGreaterEqual(len(chunk_keys), 2, "expected a chunked object")
        self.assertFalse(any(".gz" in k for k in chunk_keys))
        # Contiguous indices from zero, which is what reassembly relies on
        indices = sorted(int(k.rpartition(".chunk")[2]) for k in chunk_keys)
        self.assertEqual(indices, list(range(len(chunk_keys))))

        os.remove("big.bin")
        s3lfs.parallel_download_chunked([("big.bin", h)])
        self.assertEqual(Path("big.bin").read_bytes(), big)


if __name__ == "__main__":
    unittest.main()


class TestDownloadIntegrity(AdaptiveRepoTestCase):
    """Transport-level checksums are off (they break ranged downloads on
    S3-compatible backends); the SHA-256 in the manifest is the integrity
    check, so it must actually run on every download path -- and a failed
    download must fail the command."""

    def test_client_requests_checksums_only_when_required(self):
        s3lfs = S3LFS(bucket_name=TEST_BUCKET, manifest_file=".s3_manifest.yaml")
        config = s3lfs._get_s3_client().meta.config
        self.assertEqual(config.response_checksum_validation, "when_required")
        self.assertEqual(config.request_checksum_calculation, "when_required")

    def test_single_download_rejects_corrupt_object(self):
        """download() must verify the manifest hash, not trust transport."""
        Path("photo.jpg").write_bytes(self.raw_bytes)
        self.runner.invoke(cli, ["track", "photo.jpg"])
        h = self._manifest_hash("photo.jpg")

        # Corrupt the stored object in place
        self.s3.put_object(
            Bucket=TEST_BUCKET, Key=f"pfx/assets/{h}/photo.jpg", Body=b"tampered"
        )
        os.remove("photo.jpg")

        s3lfs = S3LFS(bucket_name=TEST_BUCKET, manifest_file=".s3_manifest.yaml")
        with self.assertRaises(RuntimeError) as caught:
            s3lfs.download("photo.jpg", silence=True)
        self.assertIn("Checksum mismatch", str(caught.exception))
        self.assertFalse(
            Path("photo.jpg").exists(), "a corrupt file was left looking valid"
        )

    def test_checkout_all_exits_nonzero_when_content_is_missing(self):
        """A checkout that could not materialize the working copy must say
        so in its exit code, not just in scrollback."""
        Path("photo.jpg").write_bytes(self.raw_bytes)
        self.runner.invoke(cli, ["track", "photo.jpg"])
        h = self._manifest_hash("photo.jpg")
        self.s3.delete_object(Bucket=TEST_BUCKET, Key=f"pfx/assets/{h}/photo.jpg")
        os.remove("photo.jpg")

        result = self.runner.invoke(cli, ["checkout", "--all"])
        self.assertNotEqual(result.exit_code, 0, msg=result.output)
        self.assertIn("could not be downloaded", result.output)

    def test_sync_exits_nonzero_when_content_is_missing(self):
        Path("photo.jpg").write_bytes(self.raw_bytes)
        self.runner.invoke(cli, ["track", "photo.jpg"])
        h = self._manifest_hash("photo.jpg")
        self.s3.delete_object(Bucket=TEST_BUCKET, Key=f"pfx/assets/{h}/photo.jpg")
        os.remove("photo.jpg")

        result = self.runner.invoke(cli, ["sync"])
        self.assertNotEqual(result.exit_code, 0, msg=result.output)
