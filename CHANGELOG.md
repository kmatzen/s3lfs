# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Sparse checkout support: s3lfs now applies the working copy's `git sparse-checkout` rules to tracked files, so a large repository can be checked out one slice at a time. Rules are matched by `git sparse-checkout check-rules` (git's own matcher, cone and non-cone), because tracked files are gitignored and therefore invisible to git's own sparse machinery. `sync` downloads only in-profile files and prunes ones that leave the profile, `checkout --all` means "everything this working copy materializes", `status` hides out-of-profile files behind a count, and the pre-commit hook walks only the slice so commit cost tracks the working copy rather than the repository. Requires git 2.42+; degrades to treating everything as in-profile (with a warning) otherwise, so it can only ever over-download.
- `s3lfs sparse` command: shows whether sparse checkout is active, the patterns in effect, and how many tracked files are materialized here
- `s3lfs sync [--from REV]` command: brings tracked files in line with the manifest by diffing against a revision's manifest, transferring only what changed and removing files the manifest no longer lists (only when their content still matches what it recorded). Replaces the blanket `checkout --all` in the post-checkout and post-merge hooks, which re-hashed every tracked file on every branch switch.
- `post-rewrite` git hook, so `git pull --rebase` -- which fires neither post-merge nor a branch post-checkout -- no longer leaves tracked files stale
- `s3lfs status` command: shows which tracked files are modified or missing, the view `git status` cannot give for gitignored files. Supports a path filter, `--all`, and `--porcelain`.
- `s3lfs merge-driver` and automatic registration by `s3lfs install`: merges concurrent changes to `.s3_manifest.yaml` key-wise and to the `.gitignore` s3lfs block as a set union, so two branches tracking different files no longer conflict. A conflict is still reported when both sides change the same path to different content.
- `s3lfs clone <url> [dir]` command: clone, install hooks, and download tracked files in one step, since git hooks are never cloned
- `s3lfs verify` command: checks that manifest entries reference content that exists in S3, with `--revision` to verify a committed manifest and `--base` to verify only the entries a push introduces
- `s3lfs pre-commit` command and pre-commit git hook: uploads modified tracked files and stages the updated manifest at commit time, so every commit's manifest matches the content in S3; blocks the commit if an s3lfs-tracked file is staged for commit in git itself
- `s3lfs track` now adds tracked paths to a marked block in `.gitignore` and removes already-committed tracked files from the git index, preventing large files from entering git history; `s3lfs remove` removes the `.gitignore` entry
- `S3LFS.compare_to_hashes()`: cached disk-state comparison shared by `sync` and `status`

### Changed
- Manifest and cache YAML now use the libyaml-backed loader and dumper when available, which parses roughly 8x faster (4.31s to 0.50s for a 100,000-entry manifest) and emits byte-identical output. Every command reads the whole manifest, so this is felt everywhere.
- The pre-push hook now verifies that pushed manifests reference uploaded content (`s3lfs verify`) instead of uploading at push time. Uploading during pre-push updated only the working-tree manifest, so the commits being pushed still referenced the old hashes; uploads now happen in the pre-commit hook where the manifest change lands in the commit itself.

### Fixed
- `test_load_cache_stat_oserror` assumed `Path.exists()` is implemented in terms of `Path.stat`, which is no longer true on Python 3.14; the test now patches both explicitly

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
