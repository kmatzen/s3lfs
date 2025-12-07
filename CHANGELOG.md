# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
