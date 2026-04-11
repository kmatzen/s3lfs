# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-04-11

### Added
- `--endpoint-url` flag for S3-compatible storage (MinIO, Cloudflare R2, Backblaze B2, Wasabi)
- `s3lfs install` / `s3lfs uninstall` commands for transparent git hook integration
- `.s3lfsconfig` per-repo configuration file for team-wide defaults
- `s3lfs migrate-from-lfs` command for one-step Git LFS migration
- GitHub Action (`kmatzen/s3lfs@main`) for CI/CD integration with selective checkout
- `--workers` flag to configure parallel worker count (auto-detected from CPU count by default)
- `--metrics` flag for parallelism performance metrics collection
- pigz support for parallel compression/decompression (auto-detected when available)
- `metrics.track()` context manager for cleaner metrics instrumentation

### Changed
- Downloads now use dynamic block-level parallelism: chunks across all files are downloaded in a single shared thread pool instead of sequentially per file
- Uploads now use the same dynamic parallel pattern: file preparation and chunk uploads share one pool
- `parallel_download_all` and `checkout_interleaved` use the new chunked download pipeline
- `parallel_upload` delegates to the new `parallel_upload_chunked` pipeline
- Worker count auto-detects from CPU count (`min(32, cpu_count + 4)`) instead of hardcoded 8
- boto3 `max_concurrency` now aligns with the worker count
- `track_modified_files_cached` loads manifest and cache once, checks files without locking, and batch-writes at the end
- Retry decorator uses exponential backoff (2s, 4s, 8s, capped at 30s) instead of immediate retries
- Compression auto-detection prefers pigz over gzip when available

### Fixed
- Upload MD5 check no longer loads entire chunks into memory (streams in 1MB blocks)
- `split_file` no longer loads entire chunks into memory (streams in 1MB blocks)
- Download no longer calls `head_object` twice per chunk (sizes from `list_objects_v2`)
- Cache `load_cache()` skips disk read when file mtime is unchanged
- Cache `save_cache()` skips write when nothing has changed (dirty flag tracking)
- Retry decorator now raises on final failure instead of silently retrying once more
- Fixed moto test failures from S3 region constraint (explicit us-east-1)
- Removed emoji characters from all output

## [0.1.0] - 2024-12-07

### Added
- Initial release of s3lfs
- Upload and track large files in S3 instead of Git
- SHA-256 content-based file deduplication
- AES256 server-side encryption for stored assets
- Parallel uploads/downloads with multi-threading
- Gzip compression before upload
- Flexible path resolution (files, directories, glob patterns)
- YAML-based manifest file (`.s3_manifest.yaml`)
- CLI commands: `init`, `track`, `checkout`, `ls`, `remove`, `cleanup`
- Subdirectory support - all commands work from any directory within git repo
- `--modified` flag to track only changed files
- `--no-sign-request` for public bucket access
- Pipe-friendly `ls` output in non-verbose mode
