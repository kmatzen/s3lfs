# Changelog

## [0.6.3] - 2026-08-09

### Fixed

- **Manifest keys can no longer escape the repository root on Windows.**
  `to_filesystem_path` trusted `os.path.isabs`, but since Python 3.13 a
  rooted key like `/foo` is not "absolute" on Windows -- and pathlib's
  join replaces the base with it, resolving to `C:/foo`, outside the
  repo. Keys with embedded `..` segments got past the prefix check on
  every platform. Resolved paths are now verified to stay inside the
  repository.
- **Subdirectory context works from Windows 8.3 short paths.**
  `PathResolver` resolved an explicitly passed working directory but not
  the `Path.cwd()` default, so a shell sitting in a short path
  (`C:\Users\RUNNER~1\...`) never matched the resolved git root and
  subdirectory prefixing silently turned off.

### Added

- **Windows is now tested in CI** (`windows-latest`, Python 3.13). The
  remaining test-suite failures from #138 were POSIX assumptions in the
  tests themselves -- hardcoded `/bin/sh`, `:` as the PATH separator,
  exec-bit assertions, driveless absolute fixtures -- and are fixed or
  skipped where the concept does not exist on Windows.

## [0.6.2] - 2026-08-09

### Fixed

- **The CLI can now disable server-side encryption**, via `encryption: false`
  in `.s3lfsconfig`. s3lfs always sent the AES256 SSE header, and MinIO
  (and other S3-compatibles without KMS) rejects it, failing every upload
  with `NotImplemented` -- the Python API had an `encryption=False` escape
  hatch, the CLI had none. Found when the new benchmark CI job ran the CLI
  against MinIO for the first time.
- **`s3lfs doctor`'s write probe sends the same SSE header real uploads
  send.** A bare probe blessed endpoints where every actual `track` failed;
  now the probe fails the same way uploads would, and when the endpoint
  rejects the SSE header it points at `encryption: false` instead of IAM.

### Added

- **Benchmarks run in CI.** Every pull request runs `manifest_scaling.py
  --check`, which fails the build if sharded manifest reads stop beating a
  flat manifest by a wide ratio -- a machine-speed-independent guard against
  performance regressions in the lazy-loading machinery. The
  s5cmd transfer comparison also runs (against MinIO, informational only)
  and publishes its table to the CI job summary. README benchmark tables
  now state when they were measured.

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.1] - 2026-08-09

### Fixed
- `sha256sum`/`md5sum` output is now validated before use. GNU coreutils prefixes the whole line with a backslash when the filename needed escaping -- which every Windows path does, and any POSIX path containing a backslash or newline would too. That byte became part of the hash, the corrupt hash became an S3 key, and every downstream checksum comparison failed. Output that does not parse as a digest now falls back to Python hashing. Found by running the suite on Windows for the first time, where this one bug accounted for 53 of 71 failures.

### Added
- `s3lfs doctor`: one command that checks the whole integration -- manifest health, shard visibility under sparse checkouts, every hook, the merge driver, PATH visibility for hooks, and live S3 permissions probed operation-by-operation -- and prints the fix for each finding. This project's characteristic failure mode is a component that fails quietly; doctor makes those loud on demand.

## [0.6.0] - 2026-08-09

### Added
- **Adaptive compression.** Each file's content is sampled at upload; files that don't compress -- images, video, model weights, archives -- are stored **raw, under their natural key, byte-identical to the original**. Two effects: no more gzip time spent achieving nothing (cold upload of 200 MB of incompressible data: 2.34s to 1.49s even against a local server), and the stored object is directly usable by any S3 tool, addressing the "opaque format" objection to adopting s3lfs for long-term storage. `compression: auto|always|never` in `.s3lfsconfig`; compressible files still get gzip.
- The storage layout is now documented in the README as a contract, with a "getting your data out without s3lfs" recipe -- and a test that restores files using nothing but an S3 client and the manifest, so the recipe cannot silently rot.
- `benchmarks/transfer_comparison.py`: measures s3lfs against a raw transfer tool (s5cmd) across cold/no-op/incremental upload and download, per scenario rather than as one misleading number.

### Changed
- boto3 and package metadata are imported lazily, cutting CLI startup from ~200ms to ~135ms for anything that does not touch S3 -- `--help`, `--version`, and hooks with nothing to do. A test asserts importing the CLI does not pull in boto3, so the cost cannot creep back silently.
- The S3 transfer configuration requests boto3's CRT client in "auto" mode. With `pip install s3lfs[crt]`, transfers to standard AWS S3 endpoints use AWS's C-based engine; everything else (MinIO, R2, older boto3) falls back to the classic client unchanged.
- A missing stored object is now a loud error at download time instead of a fabricated key that 404s downstream. Non-contiguous chunk sets are also rejected with an explanation rather than reassembled short.
- Unchunked downloads no longer issue a `head_object` per file; sizes come from the discovery listing.

### Fixed
- **Downloads of files at or above the multipart threshold failed against S3-compatible backends** (moto, and the same breakage reported for MinIO and R2): boto3 >= 1.36 validates transport-level CRC checksums by default, and those backends return whole-object checksums for ranged GETs. Transport checksums are now requested only where S3 demands them; integrity is enforced end-to-end instead -- every download path now verifies the complete file's SHA-256 against the manifest, including the single-file path, which previously trusted the transport.
- **A failed download now fails the command.** `checkout --all` and `sync` exited 0 even when files could not be downloaded, reporting the problem only in scrollback; discovery errors were not counted at all. Download failures and never-completed files propagate to a non-zero exit code with a summary.

### Fixed
- `s3lfs track` crashed under write-only credentials: the upload path's ETag skip-check uses HeadObject, which requires `s3:GetObject`, and a 403 there aborted the upload. Upload-only policies are a legitimate CI shape; the check now degrades to uploading, whose worst case is re-sending bytes that already exist.

### Changed
- Raw files at or below the chunk size upload straight from the source instead of staging a temp copy first, halving the disk traffic of a raw upload. A stat snapshot taken before the transfer is compared after it; a file modified mid-upload is refused rather than published torn, and the pipeline never deletes the user's file.
- Downloads hash the bytes as they stream in, so a sequentially-written file is verified without being read back. boto3 downloads objects above its multipart threshold as out-of-order parallel ranges, where a streaming digest would be garbage -- the writer detects the seeks and falls back to hashing the finished file. Measured on real S3: incompressible cold download 3.7s to 3.2s, upload 3.1s to 3.0s.

### Compatibility
- Objects stored raw by this version are invisible to older s3lfs clients (they only look for `.gz` keys and will fail loudly at checkout). Teams with mixed versions can set `compression: always` until everyone upgrades. Buckets written by older versions are fully readable -- discovery handles both forms.

## [0.5.2] - 2026-08-09

### Changed
- The pre-commit hook is quiet when it has nothing to report. It printed a per-file progress bar and a running commentary on every commit -- noise the user could not turn off, which buried the messages that do matter. It still reports what it uploaded, files hidden under a tracked directory, and anything it blocks; `s3lfs track --modified --verbose` still shows progress.

## [0.5.1] - 2026-08-09

### Added
- `benchmarks/manifest_scaling.py`, which generates a synthetic manifest of a given size and times the operations every command depends on. The README's performance figures come from it, so they can be re-derived rather than taken on faith.

### Changed
- README gained a Performance section with measured numbers for manifest size, sharding and sparse reads, replacing a single inline claim.

### Fixed
- **`s3lfs track <dir>` made `git status` quadratic.** Since 0.3.0 it wrote one `.gitignore` entry per tracked file, and git matches every candidate path against every pattern with no pruning: at 100,000 tracked files `git status` took **69.6 seconds**, against 17 ms for a single directory pattern. A directory now becomes one `/dir/` pattern again -- 37 ms at the same scale.
- Re-tracking a directory replaces the old per-file entries rather than adding the directory pattern alongside them. Without that, upgrading fixed nothing for the repositories that needed it most: the slow block stayed in place.
- The precision that bought is recovered without the cost: `s3lfs status` and the pre-commit hook now report files under a tracked directory that s3lfs is not tracking, which would otherwise be invisible to git *and* absent from the manifest. On every commit the scan is bounded by directory mtime -- adding a file updates its directory -- so it costs ~70 ms over 100,000 files instead of ~1 s for a full walk.

## [0.5.0] - 2026-08-09

### Changed
- A sharded manifest is now read one shard at a time. Constructing `S3LFS` parses no shards at all; looking up or writing a path parses that path's shard only; iterating still parses everything, because that is what the caller asked for. On a 200,000-entry manifest across 100 shards (18.4 MB): construction went from parsing all of it to 0 shards, a single lookup reads 1 shard in 10 ms, and a three-shard slice costs 24 ms against 861 ms for the whole manifest.
- Sparse working copies read only the shards their profile can reach. `s3lfs status` in a checkout covering 1 of 100 directories went from 6.8 s to 0.32 s -- 21x -- because the other 99 shards are never opened.

### Fixed
- Enabling a git sparse checkout removed the entire sharded manifest from the working copy, and s3lfs then reported that nothing was tracked at all. Shards are git-tracked files under a directory, so any cone that does not name that directory excludes them. s3lfs now keeps the shard directory inside the cone, detecting exclusion with git's own matcher rather than by looking at the disk -- the files can still be present from before the rules changed, and git removes them the next time it applies them.

## [0.4.1] - 2026-08-08

### Fixed
- `git commit -a` with only s3lfs-tracked files changed aborted with git's "nothing to commit, working tree clean", which is misleading: the pre-commit hook had already uploaded the content and updated the manifest, and running the same command again committed it. `git commit -a` prepares its commit from a temporary index before hooks run, so it decides there is nothing to commit without seeing the manifest change. The hook now says so. Commits that also touch git-tracked files were never affected -- the manifest update lands in those normally.

## [0.4.0] - 2026-08-08

### Added
- `s3lfs shard` splits the manifest into one file per top-level directory under `.s3lfs_manifest/`, leaving the root manifest holding configuration only. A flat manifest is parsed in full by every command and rewritten in full by every `track`, which also lands a fresh copy of the whole thing in git history each time; sharding confines a change under `data/` to `data`'s shard. `--undo` merges it back. The merge driver covers shards and the pre-commit hook stages them.
- `s3lfs track` now records the hashes it computed into the hash cache, so the next modified-file scan is a cache hit rather than a re-read.
- `specs/check.sh` now covers **every** model in `specs/`, not just the two newest: chunked upload, garbage collection, manifest locking, namespace exclusion, working-copy safety and ownership. For each protocol it names the configuration matching the shipped design and asserts it holds, and asserts that the configurations modelling known defects still fail on their stated invariant. Which configuration is "current" now lives in the script, so a stale label fails the build instead of misleading a reader.

### Changed
- Deleting a tracked file now removes its manifest entry on the next `track --modified`, so the deletion reaches collaborators instead of their sync re-downloading the file forever. A file that was never downloaded here is left alone -- the hash cache distinguishes "this working copy had it" from "never materialized", so a fresh clone cannot wipe the manifest. `--no-prune-deleted` opts out.
- The `post-rewrite` hook takes its baseline from the first rewritten commit on stdin instead of `ORIG_HEAD`, which any reset, checkout or merge during an interactive rebase silently rewrites -- and a wrong baseline leaves files stale with no warning.
- `verify --base` now unions the manifests of every commit in the range, not just the endpoints. Content introduced by an intermediate commit and superseded before the tip is still needed by anyone who checks that commit out.
- The pre-commit and pre-push hooks warn when the repository uses s3lfs but `s3lfs` is not on `PATH` -- common when git is driven from an IDE. They previously did nothing at all, silently.

### Fixed
- Deletion detection could untrack files nobody deleted. `sync` hashes a file immediately before pruning it, which left it in the hash cache -- the very record used to tell a user deletion from a file that was never materialized here. A sparse working copy that narrowed its profile, synced, then ran `track --modified` untracked every out-of-profile file, removing it from the manifest for everyone. `sync` now forgets the hashes of files it prunes, `track --modified` applies the sparse profile as the pre-commit hook already did, and a bulk-absence guard refuses to untrack when most of the tracked files are missing -- a wiped working copy is not a set of deletions.
- `specs/check.sh` runs the configurations that expect a violation with a single TLC worker, and accepts any of the invariants a multi-defect configuration legitimately breaks. TLC stops at the first violation it finds, so with several workers the invariant reported depends on which state a machine reaches first -- the check passed locally and failed in CI on a different core count.
- `specs/README.md` described the chunked-upload defect as present in current code. It was fixed some releases ago; the code implements the verified `CommitAfter` design, and `check.sh` now enforces that.
- `specs/check.sh` gives each model check its own TLC metadata directory. TLC names it from the wall clock to the second, so back-to-back runs of one module collided and the second died before checking anything -- which looked exactly like a property having changed. Failures now also print TLC's output, so "did not run" is distinguishable from "no longer holds".

## [0.3.0] - 2026-08-08

### Added
- Sparse checkout support: s3lfs now applies the working copy's `git sparse-checkout` rules to tracked files, so a large repository can be checked out one slice at a time. Rules are matched by `git sparse-checkout check-rules` (git's own matcher, cone and non-cone), because tracked files are gitignored and therefore invisible to git's own sparse machinery. `sync` downloads only in-profile files and prunes ones that leave the profile, `checkout --all` means "everything this working copy materializes", `status` hides out-of-profile files behind a count, and the pre-commit hook walks only the slice so commit cost tracks the working copy rather than the repository. Requires git 2.42+; degrades to treating everything as in-profile (with a warning) otherwise, so it can only ever over-download.
- `specs/S3lfsOwnership.tla`: a TLA+ model of git/s3lfs file ownership, checking that no path is versioned by both systems at once and that no file on disk is hidden from git while absent from the manifest. `IGNORE_SCOPE = "directory"` reproduces the over-broad ignore defect.
- `specs/S3lfsWorkingCopy.tla`: a TLA+ model of the working-copy lifecycle (track, remove, commit, branch switch, sync, cleanup) checking that content existing only on disk is never destroyed, that every manifest entry has an object behind it, and that distinct paths never share a storage key. Constants isolate the two design decisions those properties depend on; TLC confirms each is load-bearing by violating an invariant when it is disabled.
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
- CI now tests Python 3.9 through 3.13 on Linux plus a macOS run. It previously tested whatever single interpreter the runner happened to provide, so the declared 3.9 floor was never exercised -- and a Python 3.14 pathlib change had already broken a test here.
- README no longer calls `cleanup` "experimental and untested": it has substantial test coverage and is the only subsystem with TLA+ models behind it. The warning was steering people away from the most rigorously verified code in the project.
- The TLA+ spec notes now cite functions instead of line numbers, which had drifted far enough to point at the wrong code.

### Fixed
- CI lint has been failing on `main` since before this release: flake8 was pinned at 6.0.0, whose pycodestyle mis-tokenizes PEP 701 f-strings on modern Python and reported phantom `E702` errors for lines containing no semicolon. The lint pins (black, isort, flake8, mypy) are now current, so the checks CI runs match the ones that pass locally.
- **`sync` no longer deletes the last copy of content that is no longer in S3.** The clobber guard added earlier asked only whether a file still matched the hash the manifest recorded -- but that object may since have been garbage-collected, in which case the copy on disk is the only one. TLC found the trace (`track`, commit, `remove`, `cleanup`, `sync`) against the new `S3lfsWorkingCopy` model, and the rule was strengthened to what the model requires: only take bytes off disk when those bytes can be fetched back.
- **`sync` no longer overwrites locally modified tracked files.** It compared disk content only against the *target* hash, so a file edited but not yet uploaded counted as "needs downloading" and was silently replaced -- no warning, no backup, from an automatic post-checkout hook. Tracked files are gitignored, so git could not warn either. It now also compares against the previous revision's hash: a file holding the content the old manifest recorded is safe to update, anything else is reported and kept. `--force` restores the old behaviour, and the no-baseline path (which cannot tell the two apart) keeps modified files too.
- **`s3lfs install` no longer installs dead code.** A block appended to an existing hook ending in `exit 0` -- git's own samples, husky, and many CI scaffolds do -- was never reached while install reported success, so nothing uploaded at commit and nothing verified at push. The block is now placed where it runs, and a hook whose interpreter is not a POSIX shell is refused with an explanation rather than corrupted.
- **Read-only commands no longer rewrite the manifest.** `S3LFS.__init__` saved unconditionally, so `status`, `sparse`, `ls`, `verify` and every sync hook dirtied the git-tracked manifest, breaking clean-tree checks and able to overwrite an unresolved merge. It now writes only when construction actually changed the stored configuration.
- **`s3lfs track <dir>` no longer hides later files from git.** It wrote `/<dir>/`, ignoring everything under that directory forever, so a source file added there afterwards was invisible to git (ignored) *and* to s3lfs (not in the manifest) -- present on one machine only. Literal specs now expand to one entry per tracked file, globs are kept as-is, and gitignore metacharacters are escaped so a directory like `runs[2024]` matches literally instead of as a character class.
- **`s3lfs track` reports when it tracks nothing.** A path outside the repository -- including a symlink pointing outside it -- was skipped with no output and exit 0, leaving the user believing a large file was safely in S3.
- `s3lfs install` no longer crashes in a linked worktree or submodule, where `.git` is a file rather than a directory. It asks git where the hooks live (`git rev-parse --git-path hooks`) instead of assuming.
- `s3lfs sync` on a branch from before s3lfs existed now says there is nothing to sync instead of erroring, so the post-checkout hook stops reporting a failure for an ordinary checkout.
- A manifest that cannot be parsed now produces an explanation rather than a bare YAML traceback, and names merge conflict markers when it finds them -- the state a teammate who has not run `s3lfs install` will hit.
- A revision whose manifest is unparseable or is not a manifest at all (a directory at that path, a commit with conflict markers) is treated as "no baseline" rather than raising out of a hook.
- The merge driver keeps keys whose value is legitimately null instead of reading them as deleted, detects `.gitignore` by content when git does not pass the path, and no longer claims git will "fall back" on failure -- git does not, so the message now says the file needs resolving by hand.
- `verify --revision` warns when that revision recorded a different bucket or prefix, which would otherwise make it check the wrong location and report false missing content.
- `pre-commit` stages `.gitignore` alongside the manifest and fails loudly if staging fails, instead of letting a commit record old hashes while the new content is already in S3.
- S3 listings used to derive chunk counts now follow continuation tokens; a truncated listing at 1000 keys would have silently rebuilt a short file. The loop continues only on a genuine continuation token, so an unexpected response shape cannot spin it forever.
- `git ls-files -z` output and `git rm` pathspecs are handled as bytes, so paths containing a carriage return are not mangled.
- `s3lfs sparse` reports a profile it could not apply even when the failure happens mid-listing.
- `sync --prune` keeps going and reports failures when a file cannot be removed, instead of aborting mid-loop with no summary.
- `test_load_cache_stat_oserror` assumed `Path.exists()` is implemented in terms of `Path.stat`, which is no longer true on Python 3.14; the test now patches both explicitly

## [0.2.0] - 2026-04-11 (never published)

This version was tagged in the changelog but no release was ever cut, so it
never reached PyPI. Everything below shipped in 0.3.0 instead.

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
