#!/usr/bin/env python3
"""
Additional targeted tests to close coverage gaps in s3lfs/core.py.

Each test is annotated with the approximate line range in core.py it is
meant to exercise (as of the time this file was written).
"""

import os
import shutil
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import boto3
from botocore.exceptions import ClientError
from moto import mock_s3

from s3lfs.core import S3LFS


def _write_manifest(path, bucket="test-bucket", prefix="test-prefix", files=None):
    files = files or {}
    lines = [f"bucket_name: {bucket}", f"repo_prefix: {prefix}", "files:"]
    if not files:
        lines[-1] += " {}"
    else:
        for k, v in files.items():
            lines.append(f'  "{k}": "{v}"')
    path.write_text("\n".join(lines) + "\n")


@mock_s3
class TestCoreCoverageImprovements(unittest.TestCase):
    def setUp(self):
        self.s3_mock = mock_s3()
        self.s3_mock.start()

        self.bucket_name = "test-coverage-bucket-2"
        self.s3 = boto3.client("s3", region_name="us-east-1")
        self.s3.create_bucket(Bucket=self.bucket_name)

        self.test_dir = Path(tempfile.mkdtemp(prefix="s3lfs_cov_"))
        self.manifest_file = self.test_dir / ".s3_manifest.yaml"
        _write_manifest(self.manifest_file, bucket=self.bucket_name)

    def tearDown(self):
        self.s3_mock.stop()
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir, ignore_errors=True)

    def _make_s3lfs(self, **kwargs):
        kwargs.setdefault("bucket_name", self.bucket_name)
        kwargs.setdefault("manifest_file", str(self.manifest_file))
        kwargs.setdefault("temp_dir", str(self.test_dir / ".s3lfs_temp"))
        return S3LFS(**kwargs)

    # ------------------------------------------------------------------
    # lines 303-305: manifest outside git repo -> PathResolver(manifest_dir)
    # ------------------------------------------------------------------
    def test_init_manifest_outside_git_root(self):
        git_repo = self.test_dir / "repo"
        git_repo.mkdir()
        (git_repo / ".git").mkdir()

        # Manifest lives in a sibling directory, NOT inside git_repo.
        outside_dir = self.test_dir / "outside"
        outside_dir.mkdir()
        manifest_file = outside_dir / ".s3_manifest.yaml"
        _write_manifest(manifest_file, bucket=self.bucket_name)

        with patch("s3lfs.core.find_git_root", return_value=git_repo):
            s3lfs = S3LFS(
                bucket_name=self.bucket_name,
                manifest_file=str(manifest_file),
                temp_dir=str(outside_dir / ".s3lfs_temp"),
            )
        self.assertEqual(s3lfs.path_resolver.git_root.resolve(), outside_dir.resolve())

    # ------------------------------------------------------------------
    # lines 609-611: load_cache() when cache file absent
    # ------------------------------------------------------------------
    def test_load_cache_absent_resets_state(self):
        s3lfs = self._make_s3lfs()
        # Cache file does not exist yet.
        self.assertFalse(s3lfs.cache_file.exists())
        s3lfs._cache_mtime = 123.0
        s3lfs.hash_cache = {"stale": "data"}
        s3lfs.load_cache()
        self.assertEqual(s3lfs.hash_cache, {})
        self.assertIsNone(s3lfs._cache_mtime)
        self.assertFalse(s3lfs._cache_dirty)

    # ------------------------------------------------------------------
    # lines 616-617: OSError raised by cache_file.stat()
    # ------------------------------------------------------------------
    def test_load_cache_stat_oserror(self):
        s3lfs = self._make_s3lfs()
        s3lfs.hash_cache = {}
        s3lfs._cache_dirty = True
        s3lfs.save_cache()  # creates the cache file on disk

        # Hold the cache file's existence check True while its stat() fails,
        # so load_cache reaches the explicit stat() rather than the
        # file-absent branch. Counting stat() calls instead would depend on
        # whether Path.exists() is implemented in terms of Path.stat, which
        # varies by Python version.
        real_stat = Path.stat
        real_exists = Path.exists

        def failing_stat(self_path, *a, **kw):
            if self_path == s3lfs.cache_file:
                raise OSError("boom")
            return real_stat(self_path, *a, **kw)

        def always_exists(self_path, *a, **kw):
            if self_path == s3lfs.cache_file:
                return True
            return real_exists(self_path, *a, **kw)

        with (
            patch("s3lfs.core.Path.stat", failing_stat),
            patch("s3lfs.core.Path.exists", always_exists),
        ):
            s3lfs.load_cache(force=True)
        self.assertIsNone(s3lfs._cache_mtime)

    # ------------------------------------------------------------------
    # lines 667-668: OSError from stat() after save_cache() writes
    # ------------------------------------------------------------------
    def test_save_cache_stat_oserror_after_write(self):
        s3lfs = self._make_s3lfs()
        s3lfs.hash_cache = {"a": 1}
        s3lfs._cache_dirty = True

        real_stat = Path.stat
        call_count = {"n": 0}

        def flaky_stat(self_path, *a, **kw):
            call_count["n"] += 1
            if self_path == s3lfs.cache_file:
                raise OSError("cannot stat")
            return real_stat(self_path, *a, **kw)

        with patch("s3lfs.core.Path.stat", flaky_stat):
            s3lfs.save_cache()
        self.assertIsNone(s3lfs._cache_mtime)
        self.assertFalse(s3lfs._cache_dirty)

    # ------------------------------------------------------------------
    # line 705: hash_file "auto" selects mmap when sha256sum is unavailable
    # ------------------------------------------------------------------
    def test_hash_file_auto_selects_mmap_without_sha256sum(self):
        s3lfs = self._make_s3lfs()
        test_file = self.test_dir / "data.bin"
        test_file.write_text("some content for hashing")

        with patch("s3lfs.core.shutil.which", return_value=None):
            result = s3lfs.hash_file(test_file, method="auto")
        self.assertEqual(len(result), 64)

    # ------------------------------------------------------------------
    # lines 726-727: _changed_during_hashing OSError -> True
    # ------------------------------------------------------------------
    def test_changed_during_hashing_oserror(self):
        s3lfs = self._make_s3lfs()
        missing_file = self.test_dir / "missing.bin"
        result = s3lfs._changed_during_hashing(
            missing_file, {"size": 0, "mtime": 0.0, "inode": None}
        )
        self.assertTrue(result)

    # ------------------------------------------------------------------
    # line 750: _entry_is_racy -> True when timestamp/mtime missing
    # ------------------------------------------------------------------
    def test_entry_is_racy_missing_metadata(self):
        s3lfs = self._make_s3lfs()
        self.assertTrue(s3lfs._entry_is_racy({}))
        self.assertTrue(s3lfs._entry_is_racy({"timestamp": time.time()}))
        self.assertTrue(s3lfs._entry_is_racy({"metadata": {"mtime": time.time()}}))

    # ------------------------------------------------------------------
    # line 832: hash_file_cached finds cache populated by "another process"
    # between the pre-hash lock and the post-hash lock.
    # ------------------------------------------------------------------
    def test_hash_file_cached_concurrent_cache_hit(self):
        s3lfs = self._make_s3lfs()
        test_file = self.test_dir / "concurrent.bin"
        test_file.write_text("concurrent content")

        stat = test_file.stat()
        metadata = {
            "size": stat.st_size,
            "mtime": stat.st_mtime,
            "inode": getattr(stat, "st_ino", None),
        }
        file_path_str = str(test_file.as_posix())

        real_load_cache = s3lfs.load_cache
        call_count = {"n": 0}

        def fake_load_cache(force=False):
            call_count["n"] += 1
            real_load_cache(force=force)
            if call_count["n"] == 2:
                # Simulate another process having already computed and
                # cached the hash for this exact file while we were hashing.
                s3lfs.hash_cache[file_path_str] = {
                    "hash": "precomputed-hash-value",
                    "metadata": metadata,
                    "timestamp": time.time() + 10,
                }

        with patch.object(s3lfs, "load_cache", side_effect=fake_load_cache):
            result = s3lfs.hash_file_cached(test_file)

        self.assertEqual(result, "precomputed-hash-value")

    # ------------------------------------------------------------------
    # lines 992-996: track_modified_files_cached with empty manifest
    # ------------------------------------------------------------------
    def test_track_modified_files_cached_empty_manifest(self):
        s3lfs = self._make_s3lfs()
        s3lfs.load_manifest()
        # No files tracked - should print and return early.
        s3lfs.track_modified_files_cached()

    # ------------------------------------------------------------------
    # lines 1049-1052: track_modified_files_cached handles per-file exception
    # ------------------------------------------------------------------
    def test_track_modified_files_cached_handles_exception(self):
        s3lfs = self._make_s3lfs()
        test_file = self.test_dir / "tracked.bin"
        test_file.write_text("tracked content")
        s3lfs.load_manifest()
        manifest_key = test_file.name
        s3lfs.manifest["files"][manifest_key] = "somehash"
        s3lfs.save_manifest()

        with patch.object(
            s3lfs, "_check_cache_hit", side_effect=RuntimeError("kaboom")
        ):
            # Should not raise - error is caught, printed, and loop continues.
            s3lfs.track_modified_files_cached()

    # ------------------------------------------------------------------
    # line 1141: md5_file "auto" falls back to mmap on unsupported platform
    # ------------------------------------------------------------------
    def test_md5_file_auto_fallback_to_mmap(self):
        s3lfs = self._make_s3lfs()
        test_file = self.test_dir / "md5.bin"
        test_file.write_text("md5 content")

        with patch("s3lfs.core.sys.platform", "win32"):
            result = s3lfs.md5_file(test_file, method="auto")
        self.assertEqual(len(result), 32)

    # ------------------------------------------------------------------
    # lines 1276-1285: _compress_file_pigz
    # ------------------------------------------------------------------
    def test_compress_file_pigz_method(self):
        s3lfs = self._make_s3lfs()
        test_file = self.test_dir / "pigz.bin"
        test_file.write_text("pigz content" * 100)

        if shutil.which("pigz"):
            compressed = s3lfs.compress_file(test_file, method="pigz")
            self.assertTrue(compressed.exists())
        else:
            # No real pigz binary available; simulate the subprocess call
            # succeeding by faking subprocess.run to write gzip-compatible
            # bytes via the python gzip path instead.
            import gzip

            def fake_run(cmd, stdout=None, check=None):
                with open(test_file, "rb") as fin:
                    data = fin.read()
                stdout.write(gzip.compress(data, compresslevel=5))
                return subprocess.CompletedProcess(cmd, 0)

            with patch("s3lfs.core.subprocess.run", side_effect=fake_run):
                compressed = s3lfs.compress_file(test_file, method="pigz")
            self.assertTrue(compressed.exists())

    # ------------------------------------------------------------------
    # line 1317: decompress_file "auto" falls back to python when neither
    # pigz nor gzip CLI is available.
    # ------------------------------------------------------------------
    def test_decompress_file_auto_fallback_python(self):
        s3lfs = self._make_s3lfs()
        test_file = self.test_dir / "toDecompress.bin"
        test_file.write_text("decompress content")
        compressed = s3lfs.compress_file(test_file, method="python")

        with patch("s3lfs.core.shutil.which", return_value=None):
            output_path = self.test_dir / "decompressed_out.bin"
            result = s3lfs.decompress_file(compressed, output_path, method="auto")
        self.assertEqual(result, output_path)
        self.assertEqual(output_path.read_text(), "decompress content")

    # ------------------------------------------------------------------
    # lines 1367-1378: _decompress_file_pigz
    # ------------------------------------------------------------------
    def test_decompress_file_pigz_method(self):
        s3lfs = self._make_s3lfs()
        test_file = self.test_dir / "pigz_decompress.bin"
        test_file.write_text("pigz decompress content" * 50)
        compressed = s3lfs.compress_file(test_file, method="python")
        output_path = self.test_dir / "pigz_out.bin"

        if shutil.which("pigz"):
            result = s3lfs.decompress_file(compressed, output_path, method="pigz")
            self.assertEqual(result, output_path)
        else:
            import gzip

            def fake_run(cmd, stdout=None, check=None):
                with open(compressed, "rb") as fin:
                    data = fin.read()
                stdout.write(gzip.decompress(data))
                return subprocess.CompletedProcess(cmd, 0)

            with patch("s3lfs.core.subprocess.run", side_effect=fake_run):
                result = s3lfs.decompress_file(compressed, output_path, method="pigz")
            self.assertEqual(result, output_path)
        self.assertTrue(output_path.exists())

    # ------------------------------------------------------------------
    # line 1475: upload() re-raises ClientError with non-404 code
    # ------------------------------------------------------------------
    def test_upload_head_object_reraises_non_404_error(self):
        s3lfs = self._make_s3lfs()
        test_file = self.test_dir / "upload_err.bin"
        test_file.write_text("upload error content")

        error_response = {"Error": {"Code": "500", "Message": "Internal error"}}
        client_error = ClientError(error_response, "HeadObject")  # type: ignore[arg-type]

        with patch.object(s3lfs, "_get_s3_client") as mock_get_client:
            mock_client = Mock()
            mock_get_client.return_value = mock_client
            mock_client.head_object.side_effect = client_error
            with self.assertRaises(ClientError):
                s3lfs.upload(test_file, silence=True)

    # ------------------------------------------------------------------
    # lines 1586-1589: cleanup_s3 aborted when user declines confirmation
    # ------------------------------------------------------------------
    def test_cleanup_s3_user_declines(self):
        s3lfs = self._make_s3lfs()
        s3lfs.load_manifest()
        s3lfs.save_manifest()

        # Put an unreferenced object into S3 so we get past the "nothing to
        # clean up" early return.
        key = f"{s3lfs.repo_prefix}/assets/deadbeef/somefile.bin.gz"
        self.s3.put_object(Bucket=self.bucket_name, Key=key, Body=b"data")

        with patch("builtins.input", return_value="no"):
            s3lfs.cleanup_s3(force=False)

        # Object should still exist since cleanup was aborted.
        resp = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=key)
        self.assertEqual(resp.get("KeyCount", 0), 1)

    # ------------------------------------------------------------------
    # lines 1601-1602: cleanup_s3 skips keys that became referenced again
    # during the recheck window.
    # ------------------------------------------------------------------
    def test_cleanup_s3_skips_key_referenced_during_recheck(self):
        s3lfs = self._make_s3lfs()
        s3lfs.load_manifest()
        s3lfs.save_manifest()

        key = f"{s3lfs.repo_prefix}/assets/deadbeef/re_referenced.bin.gz"
        self.s3.put_object(Bucket=self.bucket_name, Key=key, Body=b"data")

        real_live_asset_keys = s3lfs._live_asset_keys
        call_count = {"n": 0}

        def fake_live_asset_keys():
            call_count["n"] += 1
            if call_count["n"] >= 2:
                # Simulate a concurrent upload publishing this asset just
                # before deletion actually happens.
                return {key}
            return real_live_asset_keys()

        with patch.object(s3lfs, "_live_asset_keys", side_effect=fake_live_asset_keys):
            s3lfs.cleanup_s3(force=True)

        resp = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=key)
        self.assertEqual(resp.get("KeyCount", 0), 1)

    # ------------------------------------------------------------------
    # lines 1631-1632: track_modified_files reports missing file
    # ------------------------------------------------------------------
    def test_track_modified_files_missing_file(self):
        s3lfs = self._make_s3lfs()
        s3lfs.load_manifest()
        s3lfs.manifest["files"]["ghost.bin"] = "somehash"
        s3lfs.save_manifest()

        with patch.object(s3lfs, "hash_file", return_value=None):
            s3lfs.track_modified_files(silence=True)

    # ------------------------------------------------------------------
    # lines 1671-1672: _prepare_file_for_upload removes compressed file
    # after splitting into chunks (OSError branch is best-effort; here we
    # exercise the actual chunked path).
    # ------------------------------------------------------------------
    def test_prepare_file_for_upload_chunked(self):
        s3lfs = self._make_s3lfs(chunk_size=10)
        test_file = self.test_dir / "chunked_upload.bin"
        test_file.write_text("x" * 1000)

        result = s3lfs._prepare_file_for_upload(test_file)
        self.assertIsNotNone(result)
        manifest_key, file_hash, chunks = result
        self.assertGreater(len(chunks), 1)
        for c in chunks:
            self.assertTrue(os.path.exists(c["path"]))
            os.remove(c["path"])

    # ------------------------------------------------------------------
    # lines 1671-1672: _prepare_file_for_upload tolerates OSError when
    # removing the pre-split compressed file.
    # ------------------------------------------------------------------
    def test_prepare_file_for_upload_chunked_remove_oserror(self):
        s3lfs = self._make_s3lfs(chunk_size=10)
        test_file = self.test_dir / "chunked_upload_err.bin"
        test_file.write_text("y" * 1000)

        with patch("s3lfs.core.os.remove", side_effect=OSError("cannot remove")):
            result = s3lfs._prepare_file_for_upload(test_file)
        self.assertIsNotNone(result)
        manifest_key, file_hash, chunks = result
        self.assertGreater(len(chunks), 1)
        for c in chunks:
            if os.path.exists(c["path"]):
                os.remove(c["path"])

    # ------------------------------------------------------------------
    # lines 1724-1725 / 1733-1734: _upload_chunk removes file when shutdown
    # requested, and after successful upload.
    # ------------------------------------------------------------------
    def test_upload_chunk_shutdown_requested_removes_file(self):
        s3lfs = self._make_s3lfs()
        chunk_file = self.test_dir / "chunk0.gz"
        chunk_file.write_bytes(b"chunk data")
        s3lfs._shutdown_requested = True

        from s3lfs.core import ShutdownRequested

        with self.assertRaises(ShutdownRequested):
            s3lfs._upload_chunk(
                {
                    "path": chunk_file,
                    "s3_key": f"{s3lfs.repo_prefix}/assets/x/y.gz",
                    "extra_args": {},
                }
            )
        self.assertFalse(chunk_file.exists())

    def test_upload_chunk_shutdown_requested_remove_oserror(self):
        s3lfs = self._make_s3lfs()
        chunk_file = self.test_dir / "chunk_shutdown_err.gz"
        chunk_file.write_bytes(b"chunk data")
        s3lfs._shutdown_requested = True

        from s3lfs.core import ShutdownRequested

        with patch("s3lfs.core.os.remove", side_effect=OSError("cannot remove")):
            with self.assertRaises(ShutdownRequested):
                s3lfs._upload_chunk(
                    {
                        "path": chunk_file,
                        "s3_key": f"{s3lfs.repo_prefix}/assets/x/y.gz",
                        "extra_args": {},
                    }
                )
        # os.remove was patched to always fail, so the file is still present.
        self.assertTrue(chunk_file.exists())
        chunk_file.unlink()

    def test_upload_chunk_finally_remove_oserror(self):
        s3lfs = self._make_s3lfs()
        chunk_file = self.test_dir / "chunk_finally_err.gz"
        chunk_file.write_bytes(b"chunk data")

        with patch.object(s3lfs, "_put_chunk", return_value=10):
            with patch("s3lfs.core.os.remove", side_effect=OSError("cannot remove")):
                result = s3lfs._upload_chunk(
                    {
                        "path": chunk_file,
                        "s3_key": f"{s3lfs.repo_prefix}/assets/x/y.gz",
                        "extra_args": {},
                    }
                )
        self.assertEqual(result[1], 10)
        chunk_file.unlink()

    # ------------------------------------------------------------------
    # lines 1778-1780: parallel_upload_chunked handles prep future exception
    # ------------------------------------------------------------------
    def test_parallel_upload_chunked_prep_exception(self):
        s3lfs = self._make_s3lfs()
        test_file = self.test_dir / "prepfail.bin"
        test_file.write_text("prepfail content")

        with patch.object(
            s3lfs, "_prepare_file_for_upload", side_effect=RuntimeError("prep fail")
        ):
            s3lfs.parallel_upload_chunked([str(test_file)], silence=True)

    # ------------------------------------------------------------------
    # line 1811: parallel_upload_chunked breaks phase-3 loop on shutdown
    # ------------------------------------------------------------------
    def test_parallel_upload_chunked_shutdown_during_phase3(self):
        s3lfs = self._make_s3lfs()
        test_file = self.test_dir / "shutdown_upload.bin"
        test_file.write_text("shutdown upload content")

        original_upload_chunk = s3lfs._upload_chunk

        def fake_upload_chunk(chunk_info):
            s3lfs._shutdown_requested = True
            return original_upload_chunk(chunk_info)

        with patch.object(s3lfs, "_upload_chunk", side_effect=fake_upload_chunk):
            s3lfs.parallel_upload_chunked([str(test_file)], silence=True)

    # ------------------------------------------------------------------
    # lines 1829-1830: parallel_upload_chunked catches KeyboardInterrupt
    # ------------------------------------------------------------------
    def test_parallel_upload_chunked_keyboard_interrupt(self):
        s3lfs = self._make_s3lfs()
        test_file = self.test_dir / "kbint_upload.bin"
        test_file.write_text("kbint upload content")

        # Raise from within the try/except block (not from the
        # test_s3_credentials precheck, which runs before the try) so the
        # KeyboardInterrupt is actually caught by parallel_upload_chunked's
        # own handler instead of propagating up and aborting the test run.
        with patch.object(
            s3lfs, "_prepare_file_for_upload", side_effect=KeyboardInterrupt
        ):
            s3lfs.parallel_upload_chunked([str(test_file)], silence=True)

    # ------------------------------------------------------------------
    # line 1861: parallel_upload_chunked reports >10 incomplete files
    # ------------------------------------------------------------------
    def test_parallel_upload_chunked_many_incomplete(self):
        s3lfs = self._make_s3lfs()
        files = []
        for i in range(12):
            f = self.test_dir / f"incomplete_{i}.bin"
            f.write_text(f"content {i}")
            files.append(str(f))

        with patch.object(s3lfs, "_upload_chunk", side_effect=RuntimeError("fail")):
            s3lfs.parallel_upload_chunked(files, silence=True)

    # ------------------------------------------------------------------
    # line 1887: parallel_download_all skips up-to-date files (verbose)
    # ------------------------------------------------------------------
    def test_parallel_download_all_skips_up_to_date_verbose(self):
        s3lfs = self._make_s3lfs()
        test_file = self.test_dir / "uptodate.bin"
        test_file.write_text("up to date content")
        file_hash = s3lfs.hash_file(test_file)

        s3lfs.load_manifest()
        manifest_key = test_file.name
        s3lfs.manifest["files"][manifest_key] = file_hash
        s3lfs.save_manifest()

        s3lfs.parallel_download_all(silence=False)

    # ------------------------------------------------------------------
    # lines 1972-1973, 1983-1984, 1992-1993: _finalize_file OSError
    # tolerances and checksum mismatch handling.
    # ------------------------------------------------------------------
    def test_finalize_file_checksum_mismatch_removes_output(self):
        s3lfs = self._make_s3lfs()
        manifest_key = "finalize_target.bin"
        content = b"some content for finalize"
        compressed_path = s3lfs.temp_dir / "chunk_source.gz"
        import gzip

        with open(compressed_path, "wb") as f:
            f.write(gzip.compress(content))

        with self.assertRaises(RuntimeError):
            s3lfs._finalize_file(
                manifest_key,
                [compressed_path],
                is_chunked=False,
                expected_hash="wronghash",
                silence=True,
            )
        filesystem_path = s3lfs.path_resolver.to_filesystem_path(manifest_key)
        self.assertFalse(filesystem_path.exists())

    def test_finalize_file_merges_chunks_and_removes_them(self):
        s3lfs = self._make_s3lfs()
        manifest_key = "finalize_merged.bin"
        content = b"merged chunk content here"
        import gzip

        compressed_data = gzip.compress(content)
        mid = len(compressed_data) // 2
        chunk1 = s3lfs.temp_dir / "c1.gz"
        chunk2 = s3lfs.temp_dir / "c2.gz"
        chunk1.write_bytes(compressed_data[:mid])
        chunk2.write_bytes(compressed_data[mid:])

        s3lfs._finalize_file(
            manifest_key,
            [chunk1, chunk2],
            is_chunked=True,
            expected_hash=None,
            silence=True,
        )
        self.assertFalse(chunk1.exists())
        self.assertFalse(chunk2.exists())
        filesystem_path = s3lfs.path_resolver.to_filesystem_path(manifest_key)
        self.assertEqual(filesystem_path.read_bytes(), content)

    def test_finalize_file_merge_chunk_removal_oserror(self):
        s3lfs = self._make_s3lfs()
        manifest_key = "finalize_merged_err.bin"
        content = b"merged chunk content for oserror test"
        import gzip

        compressed_data = gzip.compress(content)
        mid = len(compressed_data) // 2
        chunk1 = s3lfs.temp_dir / "c1err.gz"
        chunk2 = s3lfs.temp_dir / "c2err.gz"
        chunk1.write_bytes(compressed_data[:mid])
        chunk2.write_bytes(compressed_data[mid:])

        with patch("s3lfs.core.os.remove", side_effect=OSError("cannot remove")):
            s3lfs._finalize_file(
                manifest_key,
                [chunk1, chunk2],
                is_chunked=True,
                expected_hash=None,
                silence=True,
            )
        # Removal failed (patched), so both chunk files remain on disk.
        self.assertTrue(chunk1.exists())
        self.assertTrue(chunk2.exists())
        chunk1.unlink()
        chunk2.unlink()
        filesystem_path = s3lfs.path_resolver.to_filesystem_path(manifest_key)
        self.assertEqual(filesystem_path.read_bytes(), content)

    def test_finalize_file_compressed_removal_oserror(self):
        s3lfs = self._make_s3lfs()
        manifest_key = "finalize_compressed_err.bin"
        content = b"single chunk content for oserror test"
        import gzip

        compressed_path = s3lfs.temp_dir / "single_chunk_err.gz"
        compressed_path.write_bytes(gzip.compress(content))

        with patch("s3lfs.core.os.remove", side_effect=OSError("cannot remove")):
            s3lfs._finalize_file(
                manifest_key,
                [compressed_path],
                is_chunked=False,
                expected_hash=None,
                silence=True,
            )
        self.assertTrue(compressed_path.exists())
        compressed_path.unlink()

    def test_finalize_file_checksum_mismatch_removal_oserror(self):
        s3lfs = self._make_s3lfs()
        manifest_key = "finalize_mismatch_err.bin"
        content = b"content that will mismatch"
        import gzip

        compressed_path = s3lfs.temp_dir / "mismatch_chunk_err.gz"
        compressed_path.write_bytes(gzip.compress(content))

        with patch("s3lfs.core.os.remove", side_effect=OSError("cannot remove")):
            with self.assertRaises(RuntimeError):
                s3lfs._finalize_file(
                    manifest_key,
                    [compressed_path],
                    is_chunked=False,
                    expected_hash="wronghash2",
                    silence=True,
                )
        filesystem_path = s3lfs.path_resolver.to_filesystem_path(manifest_key)
        # Removal of the mismatched output failed (patched), so it remains.
        self.assertTrue(filesystem_path.exists())
        filesystem_path.unlink()

    # ------------------------------------------------------------------
    # lines 2035, 2038-2040: parallel_download_chunked shutdown/discovery
    # exception handling.
    # ------------------------------------------------------------------
    def test_parallel_download_chunked_discovery_exception(self):
        s3lfs = self._make_s3lfs()
        with patch.object(
            s3lfs,
            "_discover_chunks_for_file",
            side_effect=RuntimeError("discover fail"),
        ):
            s3lfs.parallel_download_chunked(
                [("some_key.bin", "somehash")], silence=True
            )

    def test_parallel_download_chunked_shutdown_phase2(self):
        s3lfs = self._make_s3lfs()

        def fake_discover(mk, fh):
            s3lfs._shutdown_requested = True
            return [
                {
                    "manifest_key": mk,
                    "file_hash": fh,
                    "s3_key": "somekey",
                    "chunk_index": 0,
                    "is_chunked": False,
                    "num_chunks": 1,
                }
            ]

        with patch.object(
            s3lfs, "_discover_chunks_for_file", side_effect=fake_discover
        ):
            s3lfs.parallel_download_chunked([("k1", "h1")], silence=True)

    # ------------------------------------------------------------------
    # lines 2061, 2098-2104, 2122-2125, 2139: phase 3 shutdown / finalize
    # error handling / cleanup of partial downloads / truncated listing.
    # ------------------------------------------------------------------
    def test_parallel_download_chunked_finalize_error_is_caught(self):
        s3lfs = self._make_s3lfs()
        with patch.object(
            s3lfs, "_finalize_file", side_effect=RuntimeError("finalize boom")
        ):
            with patch.object(
                s3lfs,
                "_discover_chunks_for_file",
                return_value=[
                    {
                        "manifest_key": "target.bin",
                        "file_hash": "abc",
                        "s3_key": "somekey",
                        "chunk_index": 0,
                        "is_chunked": False,
                        "num_chunks": 1,
                    }
                ],
            ):
                with patch.object(
                    s3lfs,
                    "_download_chunk",
                    return_value=(
                        "target.bin",
                        0,
                        s3lfs.temp_dir / "x.gz",
                        10,
                        False,
                        1,
                    ),
                ):
                    (s3lfs.temp_dir / "x.gz").write_bytes(b"data")
                    s3lfs.parallel_download_chunked(
                        [("target.bin", "abc")], silence=True
                    )

    def test_parallel_download_chunked_many_incomplete_files(self):
        # Each file is "discovered" with 2 expected chunks but only 1 ever
        # downloads, so file_tracker ends up with 12 genuinely incomplete
        # entries (as opposed to never being tracked at all), exercising the
        # ">10 more" truncated listing branch.
        s3lfs = self._make_s3lfs()
        file_items = [(f"file_{i}.bin", f"hash_{i}") for i in range(12)]

        def fake_discover(mk, fh):
            return [
                {
                    "manifest_key": mk,
                    "file_hash": fh,
                    "s3_key": f"{mk}.chunk{i}",
                    "chunk_index": i,
                    "is_chunked": True,
                    "num_chunks": 2,
                }
                for i in range(2)
            ]

        def fake_download(chunk_info, target_path):
            # Only ever "complete" chunk_index 0; chunk_index 1 fails.
            if chunk_info["chunk_index"] != 0:
                raise RuntimeError("simulated failure")
            target_path.write_bytes(b"data")
            return (
                chunk_info["manifest_key"],
                chunk_info["chunk_index"],
                target_path,
                4,
                chunk_info["is_chunked"],
                chunk_info["num_chunks"],
            )

        with patch.object(
            s3lfs, "_discover_chunks_for_file", side_effect=fake_discover
        ):
            with patch.object(s3lfs, "_download_chunk", side_effect=fake_download):
                s3lfs.parallel_download_chunked(file_items, silence=True)

    # ------------------------------------------------------------------
    # line 2061: parallel_download_chunked breaks phase-3 loop on shutdown
    # ------------------------------------------------------------------
    def test_parallel_download_chunked_shutdown_during_phase3(self):
        s3lfs = self._make_s3lfs()

        def fake_discover(mk, fh):
            return [
                {
                    "manifest_key": mk,
                    "file_hash": fh,
                    "s3_key": "somekey",
                    "chunk_index": 0,
                    "is_chunked": False,
                    "num_chunks": 1,
                }
            ]

        def fake_download(chunk_info, target_path):
            s3lfs._shutdown_requested = True
            target_path.write_bytes(b"data")
            return (
                chunk_info["manifest_key"],
                chunk_info["chunk_index"],
                target_path,
                4,
                chunk_info["is_chunked"],
                chunk_info["num_chunks"],
            )

        with patch.object(
            s3lfs, "_discover_chunks_for_file", side_effect=fake_discover
        ):
            with patch.object(s3lfs, "_download_chunk", side_effect=fake_download):
                s3lfs.parallel_download_chunked([("k1", "h1")], silence=True)

    # ------------------------------------------------------------------
    # line 2083: parallel_download_chunked skips a chunk result whose
    # manifest_key is missing from file_tracker.
    # ------------------------------------------------------------------
    def test_parallel_download_chunked_untracked_manifest_key(self):
        s3lfs = self._make_s3lfs()

        with patch.object(
            s3lfs,
            "_discover_chunks_for_file",
            return_value=[
                {
                    "manifest_key": "tracked.bin",
                    "file_hash": "abc",
                    "s3_key": "somekey",
                    "chunk_index": 0,
                    "is_chunked": False,
                    "num_chunks": 1,
                }
            ],
        ):
            # Return a manifest_key ("mystery.bin") that was never part of
            # file_tracker (built solely from discovery results).
            with patch.object(
                s3lfs,
                "_download_chunk",
                return_value=(
                    "mystery.bin",
                    0,
                    s3lfs.temp_dir / "mystery.gz",
                    4,
                    False,
                    1,
                ),
            ):
                (s3lfs.temp_dir / "mystery.gz").write_bytes(b"data")
                s3lfs.parallel_download_chunked([("tracked.bin", "abc")], silence=True)

    # ------------------------------------------------------------------
    # lines 2103-2104: parallel_download_chunked catches KeyboardInterrupt
    # ------------------------------------------------------------------
    def test_parallel_download_chunked_keyboard_interrupt(self):
        s3lfs = self._make_s3lfs()
        with patch.object(
            s3lfs, "_discover_chunks_for_file", side_effect=KeyboardInterrupt
        ):
            s3lfs.parallel_download_chunked([("k1", "h1")], silence=True)

    # ------------------------------------------------------------------
    # lines 2122-2125: parallel_download_chunked tolerates OSError when
    # removing partially-downloaded chunk files for incomplete entries.
    # ------------------------------------------------------------------
    def test_parallel_download_chunked_incomplete_cleanup_oserror(self):
        s3lfs = self._make_s3lfs()

        def fake_discover(mk, fh):
            return [
                {
                    "manifest_key": mk,
                    "file_hash": fh,
                    "s3_key": f"{mk}.chunk{i}",
                    "chunk_index": i,
                    "is_chunked": True,
                    "num_chunks": 2,
                }
                for i in range(2)
            ]

        def fake_download(chunk_info, target_path):
            if chunk_info["chunk_index"] != 0:
                raise RuntimeError("simulated failure")
            target_path.write_bytes(b"data")
            return (
                chunk_info["manifest_key"],
                chunk_info["chunk_index"],
                target_path,
                4,
                chunk_info["is_chunked"],
                chunk_info["num_chunks"],
            )

        with patch.object(
            s3lfs, "_discover_chunks_for_file", side_effect=fake_discover
        ):
            with patch.object(s3lfs, "_download_chunk", side_effect=fake_download):
                with patch(
                    "s3lfs.core.os.remove", side_effect=OSError("cannot remove")
                ):
                    s3lfs.parallel_download_chunked([("solo.bin", "h1")], silence=True)

    # ------------------------------------------------------------------
    # line 2191: remove_subtree skips assets with falsy hash
    # ------------------------------------------------------------------
    def test_remove_subtree_skips_falsy_hash(self):
        s3lfs = self._make_s3lfs()
        s3lfs.load_manifest()
        s3lfs.manifest["files"]["subtree/empty.bin"] = None
        s3lfs.manifest["files"]["subtree/real.bin"] = "realhash"
        s3lfs.save_manifest()

        with patch.object(s3lfs, "_delete_asset") as mock_delete:
            s3lfs.remove_subtree("subtree", keep_in_s3=False)
            mock_delete.assert_called_once()

    # ------------------------------------------------------------------
    # lines 2214, 2216, 2228: test_s3_credentials error translations
    # ------------------------------------------------------------------
    def test_s3_credentials_no_credentials_error(self):
        from botocore.exceptions import NoCredentialsError

        s3lfs = self._make_s3lfs()
        with patch.object(s3lfs, "_get_s3_client") as mock_get_client:
            mock_client = Mock()
            mock_get_client.return_value = mock_client
            mock_client.list_objects_v2.side_effect = NoCredentialsError()
            with self.assertRaises(RuntimeError):
                s3lfs.test_s3_credentials()

    def test_s3_credentials_partial_credentials_error(self):
        from botocore.exceptions import PartialCredentialsError

        s3lfs = self._make_s3lfs()
        with patch.object(s3lfs, "_get_s3_client") as mock_get_client:
            mock_client = Mock()
            mock_get_client.return_value = mock_client
            mock_client.list_objects_v2.side_effect = PartialCredentialsError(
                provider="env", cred_var="AWS_SECRET_ACCESS_KEY"
            )
            with self.assertRaises(RuntimeError):
                s3lfs.test_s3_credentials()

    def test_s3_credentials_generic_client_error(self):
        s3lfs = self._make_s3lfs()
        error_response = {"Error": {"Code": "SomeOtherError", "Message": "oops"}}
        client_error = ClientError(error_response, "ListObjectsV2")  # type: ignore[arg-type]
        with patch.object(s3lfs, "_get_s3_client") as mock_get_client:
            mock_client = Mock()
            mock_get_client.return_value = mock_client
            mock_client.list_objects_v2.side_effect = client_error
            with self.assertRaises(RuntimeError) as ctx:
                s3lfs.test_s3_credentials()
            self.assertIn("Error testing S3 credentials", str(ctx.exception))

    # ------------------------------------------------------------------
    # line 2266: _is_internal_path detects lock file suffix
    # ------------------------------------------------------------------
    def test_is_internal_path_lock_file(self):
        s3lfs = self._make_s3lfs()
        lock_path = s3lfs.path_resolver.git_root / "something.s3lfs.lock"
        self.assertTrue(s3lfs._is_internal_path(lock_path))

    # ------------------------------------------------------------------
    # lines 2475-2476: track() (non-interleaved) with no matching files
    # ------------------------------------------------------------------
    def test_track_no_files_found(self):
        s3lfs = self._make_s3lfs()
        s3lfs.track("nonexistent_path_xyz", silence=True, interleaved=False)

    # ------------------------------------------------------------------
    # lines 2509-2510: track() (non-interleaved) - all files up to date
    # ------------------------------------------------------------------
    def test_track_all_up_to_date_non_interleaved(self):
        s3lfs = self._make_s3lfs()
        test_file = self.test_dir / "already_tracked.bin"
        test_file.write_text("already tracked content")
        file_hash = s3lfs.hash_file(test_file)

        # track() (non-interleaved) phase-2 compares against
        # self.manifest["files"] keyed by the *absolute* filesystem path
        # (it only converts to a manifest-relative key in phase 4), so the
        # "up to date" branch requires the absolute path as key here.
        s3lfs.load_manifest()
        manifest_key = str(test_file.resolve().as_posix())
        s3lfs.manifest["files"][manifest_key] = file_hash
        s3lfs.save_manifest()

        s3lfs.track(str(test_file), silence=True, interleaved=False, use_cache=False)

    # ------------------------------------------------------------------
    # lines 2516, 2537-2538, 2542-2548: track() non-interleaved upload path
    # with credentials message, shutdown mid-upload, and error re-raise.
    # ------------------------------------------------------------------
    def test_track_non_interleaved_uploads_new_file(self):
        s3lfs = self._make_s3lfs()
        test_file = self.test_dir / "new_track.bin"
        test_file.write_text("new track content")

        s3lfs.track(str(test_file), silence=False, interleaved=False, use_cache=False)

        s3lfs.load_manifest()
        manifest_key = test_file.name
        self.assertIn(manifest_key, s3lfs.manifest["files"])

    def test_track_non_interleaved_shutdown_requested(self):
        s3lfs = self._make_s3lfs()
        test_file = self.test_dir / "shutdown_track.bin"
        test_file.write_text("shutdown track content")

        def fake_test_creds(silence=False):
            s3lfs._shutdown_requested = True

        with patch.object(s3lfs, "test_s3_credentials", side_effect=fake_test_creds):
            s3lfs.track(
                str(test_file), silence=True, interleaved=False, use_cache=False
            )

    def test_track_non_interleaved_upload_error_reraised(self):
        s3lfs = self._make_s3lfs()
        test_file = self.test_dir / "err_track.bin"
        test_file.write_text("err track content")

        with patch.object(s3lfs, "upload", side_effect=RuntimeError("upload broke")):
            with self.assertRaises(RuntimeError):
                s3lfs.track(
                    str(test_file), silence=True, interleaved=False, use_cache=False
                )

    # ------------------------------------------------------------------
    # lines 2547-2548: track() non-interleaved catches KeyboardInterrupt
    # raised from a worker thread during upload.
    # ------------------------------------------------------------------
    def test_track_non_interleaved_upload_keyboard_interrupt(self):
        s3lfs = self._make_s3lfs()
        test_file = self.test_dir / "kbint_track.bin"
        test_file.write_text("kbint track content")

        with patch.object(s3lfs, "upload", side_effect=KeyboardInterrupt):
            # Should be caught internally and NOT propagate.
            s3lfs.track(
                str(test_file), silence=True, interleaved=False, use_cache=False
            )

    # ------------------------------------------------------------------
    # lines 2594-2595: checkout() (non-interleaved) no files found
    # ------------------------------------------------------------------
    def test_checkout_no_files_found_non_interleaved(self):
        s3lfs = self._make_s3lfs()
        s3lfs.checkout("nonexistent_checkout_path", silence=True, interleaved=False)

    # ------------------------------------------------------------------
    # lines 2611, 2616, 2629-2633: checkout() non-interleaved hashing errors
    # ------------------------------------------------------------------
    def test_checkout_non_interleaved_hash_error_handled(self):
        s3lfs = self._make_s3lfs()
        test_file = self.test_dir / "checkout_hash_err.bin"
        test_file.write_text("checkout hash err content")

        s3lfs.load_manifest()
        manifest_key = test_file.name
        s3lfs.manifest["files"][manifest_key] = "somehash"
        s3lfs.save_manifest()

        with patch.object(s3lfs, "hash_file", side_effect=RuntimeError("hash fail")):
            with patch.object(s3lfs, "parallel_download_chunked") as mock_dl:
                s3lfs.checkout(
                    manifest_key, silence=True, interleaved=False, use_cache=False
                )
                mock_dl.assert_called_once()

    # ------------------------------------------------------------------
    # lines 2639-2640, 2643-2644: checkout() non-interleaved up-to-date path
    # ------------------------------------------------------------------
    def test_checkout_non_interleaved_all_up_to_date(self):
        s3lfs = self._make_s3lfs()
        test_file = self.test_dir / "checkout_uptodate.bin"
        test_file.write_text("checkout uptodate content")
        file_hash = s3lfs.hash_file(test_file)

        s3lfs.load_manifest()
        manifest_key = test_file.name
        s3lfs.manifest["files"][manifest_key] = file_hash
        s3lfs.save_manifest()

        s3lfs.checkout(manifest_key, silence=True, interleaved=False, use_cache=False)

    # ------------------------------------------------------------------
    # line 2611: checkout() non-interleaved hash_func closure using the
    # cached-hashing branch (use_cache=True).
    # ------------------------------------------------------------------
    def test_checkout_non_interleaved_use_cache_true(self):
        s3lfs = self._make_s3lfs()
        test_file = self.test_dir / "checkout_cache_true.bin"
        test_file.write_text("checkout cache true content")

        s3lfs.load_manifest()
        manifest_key = test_file.name
        # Different hash than actual content so the file is queued for
        # download, forcing hash_func (cached branch) to actually run.
        s3lfs.manifest["files"][manifest_key] = "different-hash"
        s3lfs.save_manifest()

        with patch.object(s3lfs, "parallel_download_chunked") as mock_dl:
            s3lfs.checkout(
                manifest_key, silence=True, interleaved=False, use_cache=True
            )
            mock_dl.assert_called_once()

    # ------------------------------------------------------------------
    # line 2774: _hash_and_download_worker without cache (use_cache=False)
    # ------------------------------------------------------------------
    def test_hash_and_download_worker_without_cache(self):
        s3lfs = self._make_s3lfs()
        test_file = self.test_dir / "worker_nocache.bin"
        test_file.write_text("worker nocache content")
        file_hash = s3lfs.hash_file(test_file)
        manifest_key = test_file.name

        result = s3lfs._hash_and_download_worker(
            (manifest_key, file_hash), use_cache=False
        )
        self.assertEqual(result, (manifest_key, False, 0))

    # ------------------------------------------------------------------
    # line 2814: track_interleaved with metrics enabled and no files found
    # ------------------------------------------------------------------
    def test_track_interleaved_metrics_enabled_no_files(self):
        from s3lfs import metrics

        s3lfs = self._make_s3lfs()
        metrics.enable_metrics()
        try:
            s3lfs.track_interleaved("nonexistent_interleaved_path", silence=True)
        finally:
            metrics.disable_metrics()

    # ------------------------------------------------------------------
    # lines 2956, 2979-2981 attempt: checkout_interleaved with metrics
    # enabled and no files found in manifest.
    # ------------------------------------------------------------------
    def test_checkout_interleaved_metrics_enabled_no_files(self):
        from s3lfs import metrics

        s3lfs = self._make_s3lfs()
        metrics.enable_metrics()
        try:
            s3lfs.checkout_interleaved(
                "nonexistent_checkout_interleaved_path", silence=True
            )
        finally:
            metrics.disable_metrics()

    # ------------------------------------------------------------------
    # lines 3202-3203, 3217-3219: download() error handling within the
    # per-chunk download loop and during final decompression.
    # ------------------------------------------------------------------
    def test_download_chunk_error_is_caught_and_logged(self):
        s3lfs = self._make_s3lfs()
        s3lfs.load_manifest()
        manifest_key = "download_err_target.bin"
        s3lfs.manifest["files"][manifest_key] = "somehash"
        s3lfs.save_manifest()

        s3_key = f"{s3lfs.repo_prefix}/assets/somehash/{manifest_key}.gz"
        self.s3.put_object(Bucket=self.bucket_name, Key=s3_key, Body=b"gzdata")

        with patch.object(s3lfs, "_get_s3_client") as mock_get_client:
            real_client = boto3.client("s3", region_name="us-east-1")
            mock_client = Mock(wraps=real_client)
            mock_get_client.return_value = mock_client
            mock_client.download_fileobj.side_effect = RuntimeError(
                "download chunk failed"
            )
            # merge_files/decompress_file will subsequently fail too since no
            # chunk was actually written, but the per-chunk error path itself
            # must be exercised and not propagate from the loop directly.
            with self.assertRaises(Exception):
                s3lfs.download(manifest_key, silence=True)

    def test_download_decompression_error_reraised(self):
        s3lfs = self._make_s3lfs()
        s3lfs.load_manifest()
        manifest_key = "decompress_err_target.bin"
        s3lfs.manifest["files"][manifest_key] = "somehash2"
        s3lfs.save_manifest()

        s3_key = f"{s3lfs.repo_prefix}/assets/somehash2/{manifest_key}.gz"
        self.s3.put_object(Bucket=self.bucket_name, Key=s3_key, Body=b"not-gzip-data")

        with patch.object(
            s3lfs, "decompress_file", side_effect=RuntimeError("decompress broke")
        ):
            with self.assertRaises(RuntimeError):
                s3lfs.download(manifest_key, silence=True)


if __name__ == "__main__":
    unittest.main()
