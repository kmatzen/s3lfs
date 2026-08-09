# s3lfs

A Python-based version control system for large assets using Amazon S3 and S3-compatible storage. This system is designed to work like Git LFS but utilizes S3 for better bandwidth and scalability. It supports file tracking, parallel operations, encryption, and any S3-compatible backend (MinIO, Cloudflare R2, Backblaze B2, Wasabi, DigitalOcean Spaces, etc.).

## Features

- Upload and track large files in S3 instead of Git
- Works with any S3-compatible storage (MinIO, Cloudflare R2, Backblaze B2, Wasabi)
- **Block-level parallel transfers**: Downloads and uploads flatten all chunks across all files into a single worker pool
- **Automatic parallel compression**: Uses pigz when available, falls back to gzip
- **Git hook integration**: `s3lfs install` sets up pre-commit, post-checkout, post-merge, post-rewrite, and pre-push hooks
- **Accident-proof tracking**: `s3lfs track` gitignores tracked paths and removes them from the git index, so large files can't sneak into git history
- **Sparse checkouts**: applies your `git sparse-checkout` rules to tracked files, so a working copy only materializes its slice of a large repository
- **Sharded manifest**: `s3lfs shard` splits the manifest by directory and reads only the shards you touch -- opening a 200,000-entry manifest drops from 1.1s to 20ms ([details](#performance))
- **Adaptive compression**: files that don't compress (images, video, model weights) are stored raw -- no gzip time wasted, and the stored object is directly usable by any S3 tool
- **Cheap branch switches**: `s3lfs sync` diffs the manifest between revisions and transfers only what changed
- **Conflict-free manifests**: a git merge driver unions concurrent changes to the manifest and `.gitignore`
- **One-command setup**: `s3lfs clone` clones, installs hooks, and downloads tracked files
- **Git LFS migration**: One-command migration with `s3lfs migrate-from-lfs`
- **GitHub Action**: Built-in CI/CD support with selective checkout
- **Per-repo config**: `.s3lfsconfig` file for team-wide defaults
- SHA-256 content-based file deduplication
- AES256 server-side encryption
- Configurable worker count (auto-detected from CPU count)
- Exponential backoff retries for transient S3 errors

## Installation

### From PyPI (Recommended)

```sh
pip install s3lfs
```

### From Source

```sh
pip install uv
uv sync
```

## Command Line Interface (CLI) Usage

The CLI tool provides a simplified set of commands for managing large files with S3. All commands automatically use the bucket and prefix configured during initialization.

**Subdirectory Support**: All s3lfs commands work from any subdirectory within the git repository. The tool automatically discovers the git repository root and resolves paths relative to it. For example, running `s3lfs track file.txt` from the `data/` directory will track `data/file.txt`.

### Initialize Repository
```sh
s3lfs init <bucket-name> <repo-prefix>
```
**Description**: Initializes the S3LFS system with the specified S3 bucket and repository prefix. This creates a `.s3_manifest.yaml` file that stores the configuration and file mappings.

**Example**:
```sh
s3lfs init my-bucket my-project
```

### Track Files
```sh
s3lfs track <path>
s3lfs track --modified
```
**Description**: Tracks and uploads files, directories, or glob patterns to S3.

**Options**:
- `--modified`: Track only files that have changed since last upload
- `--verbose`: Show detailed progress information
- `--no-sign-request`: Use unsigned S3 requests (for public buckets)
- `--workers N`: Number of parallel workers (default: auto-detected from CPU count)
- `--metrics`: Enable parallelism metrics collection

**Examples**:
```sh
s3lfs track data/large_file.zip          # Track a single file
s3lfs track data/                        # Track entire directory
s3lfs track "*.mp4"                      # Track all MP4 files
s3lfs track --modified                   # Track only changed files
```

**Git protection**: Tracking a path also adds it to a marked block in `.gitignore` (so `git add .` won't commit the large files to git) and, if any of the files were already committed to git, removes them from the git index (`git rm --cached`; the files stay on disk). `s3lfs remove` removes the corresponding `.gitignore` entries again.

A directory becomes a single `/dir/` pattern, a file becomes one entry, and a glob (`"*.mp4"`) is used as-is. One entry per tracked file would be more precise, but git matches every candidate path against every pattern with no pruning, so the cost is quadratic -- at 100,000 tracked files a per-file block took `git status` from 17ms to 70s.

The precision that buys is recovered elsewhere: because a directory pattern also hides anything put there later, `s3lfs status` and the pre-commit hook report files under a tracked directory that s3lfs is *not* tracking. Such a file would otherwise be invisible to git and absent from the manifest -- present on one machine and gone on the next clone. The check on every commit is bounded by directory mtime, so it costs ~70ms over 100,000 files rather than a full walk.

### Checkout Files
```sh
s3lfs checkout <path>
s3lfs checkout --all
```
**Description**: Downloads files, directories, or glob patterns from S3.

**Options**:
- `--all`: Download all files tracked in the manifest
- `--verbose`: Show detailed progress information
- `--no-sign-request`: Use unsigned S3 requests (for public buckets)
- `--workers N`: Number of parallel workers (default: auto-detected from CPU count)
- `--metrics`: Enable parallelism metrics collection

**Examples**:
```sh
s3lfs checkout data/large_file.zip       # Download a single file
s3lfs checkout data/                     # Download entire directory
s3lfs checkout "*.mp4"                   # Download all MP4 files
s3lfs checkout --all                     # Download all tracked files
```

### List Tracked Files
```sh
s3lfs ls [<path>]
s3lfs ls --all
```
**Description**: Lists files tracked by s3lfs. If no path is provided, all tracked files are listed by default. Supports files, directories, and glob patterns.

**Options**:
- `--all`: List all tracked files (default if no path is provided)
- `--verbose`: Show detailed information including file sizes and hashes
- `--no-sign-request`: Use unsigned S3 requests (for public buckets)

**Examples**:
```sh
s3lfs ls                          # List all tracked files
s3lfs ls data/                    # List files in the data directory
s3lfs ls "*.mp4"                  # List all MP4 files
s3lfs ls --all --verbose          # List all files with detailed info
```

**Pipe-friendly Output**: In non-verbose mode, the `ls` command outputs one file path per line without headers or formatting, making it easy to pipe into other commands. Paths are shown relative to your current directory:
```sh
s3lfs ls | grep "\.mp4"           # Filter for MP4 files in current directory
s3lfs ls | wc -l                  # Count tracked files in current directory
s3lfs ls data/ | xargs -I {} echo "Processing {}"  # Process each file in data/
```

### Remove Files from Tracking
```sh
s3lfs remove <path>
```
**Description**: Removes files or directories from tracking. Supports files, directories, and glob patterns.

**Options**:
- `--purge-from-s3`: Immediately delete files from S3 (default: keep for history)
- `--no-sign-request`: Use unsigned S3 requests

**Examples**:
```sh
s3lfs remove data/old_file.zip           # Remove single file
s3lfs remove data/temp/                  # Remove directory
s3lfs remove "*.tmp"                     # Remove all temp files
s3lfs remove data/ --purge-from-s3       # Remove and delete from S3
```

### Cleanup Unreferenced Files

```sh
s3lfs cleanup
```
**Description**: Removes files from S3 that are no longer referenced in the current manifest.

Reachability is computed on path *and* content hash, matching the storage layout, and uploads in flight are protected by a registry so a concurrent `track` cannot have its objects collected. Both properties are modelled in TLA+ under [`specs/`](specs/) and checked with TLC. Deletion is irreversible, so review what it reports before using `--force`.

**Options**:
- `--force`: Skip confirmation prompt
- `--no-sign-request`: Use unsigned S3 requests

**Example**:
```sh
s3lfs cleanup --force                    # Clean up without confirmation
```

### Diagnose the Setup
```sh
s3lfs doctor
```
**Description**: Checks that every part of the integration is actually wired up -- git repo, manifest (and shard visibility under a sparse checkout), each git hook, the merge driver, whether hooks can find `s3lfs` on PATH, and live S3 permissions (list/write/read/delete probed individually, so a write-only CI key is diagnosed precisely). Exits non-zero on blocking problems and prints the command that fixes each finding.

### Verify Uploaded Content
```sh
s3lfs verify
s3lfs verify --revision HEAD
s3lfs verify --revision HEAD --base origin/main
```
**Description**: Checks that every manifest entry references content that actually exists in S3, and exits non-zero listing any entries whose content was never uploaded. This is what the pre-push hook runs for each pushed ref.

**Options**:
- `--revision REV`: Verify the manifest as committed at a git revision (default: the working tree manifest)
- `--base REV`: Only verify entries added or changed relative to this revision's manifest (used by the pre-push hook to check just the entries the push introduces)
- `--no-sign-request`, `--endpoint-url`, `--workers`: As in other commands

### Show Status of Tracked Files
```sh
s3lfs status
s3lfs status data/
s3lfs status --porcelain
```
**Description**: Shows which tracked files are modified or missing. Tracked files are gitignored, so `git status` can't see them -- this is the equivalent view for s3lfs content. Uses the hash cache, so repeat runs are cheap.

**Options**:
- `--all`: Also list up-to-date files
- `--porcelain`: One `<code> <path>` line per file for scripting (`M` modified, `D` missing from disk)

### Sync Tracked Files
```sh
s3lfs sync
s3lfs sync --from HEAD~1
```
**Description**: Brings tracked files in line with the current manifest. With `--from`, only entries that differ from that revision's manifest are considered, which is what makes branch switches cheap: `checkout --all` re-hashes every tracked file, while a diff touches only what actually changed. Files that the manifest no longer lists are deleted, mirroring what git does for files absent from the branch you switch to -- but only when their content still matches what the old manifest recorded, so local modifications are never destroyed. This is what the post-checkout, post-merge, and post-rewrite hooks run.

**Safety**: `sync` never overwrites or deletes a tracked file whose content differs from what the manifest recorded -- that content exists nowhere else, and since tracked files are gitignored, git cannot warn you it is dirty. Such files are listed and left alone; upload them with `s3lfs track --modified`, or pass `--force` to discard them.

**Options**:
- `--from REV`: Diff against this revision's manifest (without it, checks every tracked file)
- `--no-prune`: Keep files the manifest no longer lists
- `--force`: Overwrite and delete locally modified files instead of keeping them
- `--verbose`, `--no-sign-request`, `--endpoint-url`, `--workers`: As in other commands

### Shard the Manifest
```sh
s3lfs shard
s3lfs shard --undo
```
**Description**: Splits the manifest into one file per top-level directory under `.s3lfs_manifest/`, leaving `.s3_manifest.yaml` holding configuration only. Every command parses the manifest in full and every `track` rewrites it in full, which also lands a fresh copy of the whole thing in git history each time. Sharding means a change under `data/` rewrites only `data`'s shard.

Commit the shards -- they *are* the manifest. `s3lfs` keeps `.s3lfs_manifest/` inside your sparse-checkout cone if you use one: a working copy that cannot read the manifest does not know what is tracked.

Shards are read on demand: looking up one path parses one shard, and a sparse working copy never opens the shards its profile cannot reach. See [Performance](#performance) for what that costs at scale. `s3lfs install` registers the merge driver for them, and the pre-commit hook stages them alongside the root file.

**Options**:
- `--undo`: Merge the shards back into a single manifest file
- `--force`: Skip the confirmation prompt

### Show Sparse Profile
```sh
s3lfs sparse
s3lfs sparse --porcelain
```
**Description**: Shows which tracked files this working copy materializes. See [Sparse Checkouts](#sparse-checkouts) below.

**Options**:
- `--porcelain`: One line per file, `+` in profile and `-` outside it

### Clone a Repository
```sh
s3lfs clone <url> [directory]
```
**Description**: Clones a repository, installs the s3lfs hooks, and downloads all tracked files. Git hooks live in `.git` and are never cloned, so a fresh clone otherwise has no s3lfs integration until someone remembers to run `s3lfs install`.

**Options**:
- `--no-checkout`: Install hooks but skip downloading tracked files
- `--no-sign-request`, `--endpoint-url`, `--workers`: As in other commands

### Install Git Hooks
```sh
s3lfs install
```
**Description**: Installs git hooks and the manifest merge driver for transparent s3lfs integration. After installation, modified tracked files are automatically uploaded and the manifest staged when you `git commit`, tracked files are automatically synced after `git checkout`, `git merge`, and `git pull --rebase`, and `git push` verifies that every pushed manifest references content that actually exists in S3.

**Installed hooks**:
- `pre-commit`: Uploads modified tracked files and stages the updated manifest, so each commit's manifest matches the content in S3. Also blocks the commit if an s3lfs-tracked file is staged for commit in git itself.
- `post-checkout`: Syncs tracked files after branch checkouts, using the previous HEAD's manifest as the diff baseline
- `post-merge`: Syncs tracked files after merges
- `post-rewrite`: Syncs tracked files after a rebase, so `git pull --rebase` doesn't leave them stale
- `pre-push`: Verifies the manifests being pushed reference uploaded content (runs `s3lfs verify` for each pushed ref); aborts the push if content is missing

**Merge driver**: `install` also registers a merge driver for `.s3_manifest.yaml` and `.gitignore`. Two branches that each track different files both rewrite those files, which git's line-based merge calls a conflict even though the change is a clean union. The driver merges the manifest key-wise and the s3lfs `.gitignore` block as a set union, and only reports a conflict when both sides really changed the same path to different content. The `.gitattributes` entry is committed so teammates inherit the rule; the driver itself is local config, and git falls back to its normal merge for anyone who hasn't run `s3lfs install`.

If an existing hook file cannot safely host the s3lfs block -- it is not a shell script, for instance -- `install` says so and skips that hook rather than writing something that would break or silently never run.

The post-* hooks are non-blocking -- if s3lfs fails or is not available, the git operation continues with a warning. The pre-commit and pre-push hooks abort their git operation on failure (bypass with `--no-verify`), because committing or pushing a manifest whose hashes have no objects behind them breaks every collaborator's checkout. Hooks are appended to existing hook files, preserving any other hooks you have.

### Uninstall Git Hooks
```sh
s3lfs uninstall
```
**Description**: Removes s3lfs git hooks and the merge driver registration. Other hooks in the same files, and other entries in `.gitattributes`, are preserved.

### Migrate from Git LFS
```sh
s3lfs migrate-from-lfs <bucket-name> <repo-prefix>
```
**Description**: Converts a Git LFS repository to s3lfs in one step. Detects LFS-tracked patterns from `.gitattributes`, verifies files contain real content (not pointer files), initializes s3lfs, and uploads all files to S3.

**Options**:
- `--dry-run`: Preview what would be migrated without making changes
- `--remove-lfs/--keep-lfs`: Remove LFS entries from `.gitattributes` after migration (default: keep)
- `--no-sign-request`: Use unsigned S3 requests
- `--use-acceleration`: Enable S3 Transfer Acceleration

**Examples**:
```sh
# Preview migration
s3lfs migrate-from-lfs my-bucket my-project --dry-run

# Migrate and keep LFS entries (safe, reversible)
s3lfs migrate-from-lfs my-bucket my-project

# Migrate and remove LFS tracking
s3lfs migrate-from-lfs my-bucket my-project --remove-lfs
```

**Prerequisites**: Run `git lfs pull` first to ensure all LFS files contain actual content (not pointer files). The command will error if any pointer files are detected.

## Git Workflow Integration

### 1. Initialize S3LFS
First, initialize S3LFS in your repository:
```sh
s3lfs init my-bucket my-project-name
```

This creates `.s3_manifest.yaml` which should be committed to Git, and automatically updates your `.gitignore` to exclude S3LFS cache files:
```sh
git add .s3_manifest.yaml .gitignore
git commit -m "Initialize S3LFS"
```

### 1b. (Optional) Install Hooks
For a Git LFS-like experience where files sync automatically:
```sh
s3lfs install
git add .gitattributes && git commit -m "Add s3lfs merge driver"
```
With hooks installed, `git commit` automatically uploads modified tracked files and stages the manifest, `git pull`/`git checkout`/`git pull --rebase` automatically sync tracked files, and `git push` verifies the pushed manifests reference uploaded content. The committed `.gitattributes` gives teammates the manifest merge driver once they run `s3lfs install` themselves.

### 2. Track Large Files
Instead of committing large files directly to Git, track them with S3LFS:
```sh
s3lfs track data/large_dataset.zip
s3lfs track models/
s3lfs track "*.mp4"
```
Tracking uploads the files to S3, adds the paths to `.gitignore` so they can't be committed to git by accident, and removes them from the git index if they were previously committed (the files stay on disk).

### 3. Commit Changes
After tracking files, commit the updated manifest and `.gitignore`:
```sh
git add .s3_manifest.yaml .gitignore
git commit -m "Track large files with S3LFS"
git push
```
With hooks installed, the pre-commit hook re-uploads any modified tracked files and stages the manifest for you, so `git commit` is enough for day-to-day changes.

### 4. Clone and Restore Files
Clone, install hooks, and download tracked files in one command:
```sh
s3lfs clone https://github.com/your-repo/my-repo.git
cd my-repo
```

Or, if you cloned with plain git:
```sh
git clone https://github.com/your-repo/my-repo.git
cd my-repo
s3lfs install          # hooks aren't cloned; set them up once per clone
s3lfs checkout --all
```

### 5. Update Workflow
For ongoing development with hooks installed, plain git commands are enough -- `git commit` uploads and stages, `git checkout`/`git pull` sync tracked files. To see or drive it by hand:
```sh
# What changed among tracked files? (git status can't see them)
s3lfs status

# Upload any modified large files
s3lfs track --modified

# Commit manifest changes
git add .s3_manifest.yaml
git commit -m "Update tracked files"

# Bring tracked files in line with the manifest
s3lfs sync
```

### 6. Selective Downloads
Download only specific files or directories:
```sh
s3lfs checkout data/                     # Only data directory
s3lfs checkout "models/*.pkl"            # Only pickle files in models
```

### 7. Working from Subdirectories
All commands work from any subdirectory within the git repository:
```sh
cd data/
s3lfs track large_file.zip               # Tracks data/large_file.zip
s3lfs ls                                 # Lists all tracked files (shows full paths from git root)
s3lfs checkout large_file.zip            # Downloads data/large_file.zip

cd ../models/
s3lfs track "*.pkl"                      # Tracks models/*.pkl files
s3lfs ls --verbose                       # Lists with detailed info (shows full paths)
```

**Note**: The `ls` command shows paths relative to your current directory when run from a subdirectory. For example, if you're in the `foo/` directory, `s3lfs ls` will show `file1.mp4` instead of `foo/file1.mp4`. This provides a local view of tracked files. In non-verbose mode, the output is pipe-friendly with one file path per line.

### 8. Cleanup (Experimental)
Periodically clean up unreferenced files (use with caution - this feature is untested):
```sh
s3lfs cleanup
```

## Sparse Checkouts

When a repository tracks more content than any one person needs on disk, a sparse checkout materializes only your slice of it. s3lfs supports this by applying **git's own `sparse-checkout` rules** to tracked files rather than carrying a second profile format of its own.

That choice matters for two reasons. One source of truth means a repository describes its slice once, and the same rule governs both source files and tracked assets — you don't maintain two configurations that can silently disagree. And it means s3lfs inherits git's pattern semantics, cone and non-cone alike, instead of reimplementing them. Matching is delegated to `git sparse-checkout check-rules`, which is git's own matcher.

The rules have to be applied by s3lfs rather than left to git, because tracked files are gitignored and therefore absent from git's index — git never considers them when it materializes a sparse working copy.

### Using it

```sh
# Narrow the working copy to the slice you need (plain git)
git sparse-checkout set --cone assets/textures models/production

# Make the tracked files match: downloads what's now in scope,
# removes what's now out of scope
s3lfs sync

# See what this working copy materializes
s3lfs sparse
```

Widening works the same way -- adjust the patterns with `git sparse-checkout`, then run `s3lfs sync`.

### What respects the profile

- `s3lfs sync` downloads only in-profile files, and removes materialized files that fall outside it. As always, it only deletes content that still matches the hash the manifest recorded, so local modifications are never destroyed -- it reports them and moves on.
- `s3lfs checkout --all` means "everything this working copy materializes," and says how many files it skipped.
- `s3lfs status` reports only in-profile files, with a count of the rest. Out-of-profile files are absent on purpose, so reporting them as missing would bury the real signal.
- The `pre-commit` hook walks only in-profile entries, so the cost of a commit tracks the size of your slice rather than the size of the repository.

An explicit `s3lfs checkout <path>` outside the profile is honored -- an explicit request is explicit intent -- but it says so, since a later `sync` will prune the file again unless you widen the profile.

`s3lfs verify` deliberately ignores the profile: it asks what exists in S3, not what is on disk.

### Requirements and fallback

Needs git 2.42 or newer (for `check-rules`). If sparse checkout is enabled but s3lfs can't apply the rules -- older git, or a half-configured working copy -- it warns and treats every tracked file as in-profile. A degraded match can only ever download too much, never too little, so it can't silently hide files from you.

### Scope

This governs s3lfs-tracked content only. If your working copy is large because of the sheer number of *source* files, `git sparse-checkout` is what shrinks that, and s3lfs simply follows the same rules. If it's large because of tracked binary assets, this is what shrinks it. Most large repositories have both problems, which is exactly why the two share one configuration here.

## CI/CD Integration

### GitHub Action

Use the built-in GitHub Action to install s3lfs and checkout tracked files in your workflows:

```yaml
steps:
  - uses: actions/checkout@v4

  - uses: aws-actions/configure-aws-credentials@v4
    with:
      aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
      aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
      aws-region: us-east-1

  - uses: kmatzen/s3lfs@main
    with:
      checkout: all
```

#### Selective Checkout

Only download the files your pipeline needs — no wasted bandwidth:

```yaml
  - uses: kmatzen/s3lfs@main
    with:
      checkout: "assets/textures/**"
```

#### Action Inputs

| Input | Default | Description |
|-------|---------|-------------|
| `version` | `latest` | s3lfs version to install |
| `checkout` | `none` | `all`, a glob pattern, or `none` (install only) |
| `no-sign-request` | `false` | Use unsigned S3 requests (public buckets) |
| `use-acceleration` | `false` | Enable S3 Transfer Acceleration |

See [`examples/`](examples/) for complete workflow files.

### Other CI Systems

For GitLab CI, Jenkins, or other systems, install s3lfs directly:

```sh
pip install s3lfs
s3lfs checkout --all           # or a selective glob
```

## Configuration

### AWS Credentials
Ensure your AWS credentials are configured:
```sh
aws configure
```

Or use environment variables:
```sh
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

### S3-Compatible Storage
Use the `--endpoint-url` flag to connect to any S3-compatible storage provider:
```sh
# MinIO
s3lfs init my-bucket my-project --endpoint-url http://localhost:9000

# Cloudflare R2
s3lfs init my-bucket my-project --endpoint-url https://<account-id>.r2.cloudflarestorage.com

# Backblaze B2
s3lfs init my-bucket my-project --endpoint-url https://s3.us-west-004.backblazeb2.com

# Wasabi
s3lfs init my-bucket my-project --endpoint-url https://s3.wasabisys.com
```

The endpoint URL is stored in the manifest, so subsequent commands pick it up automatically. You can override it per-command if needed.

### Per-Repo Config File
Create a `.s3lfsconfig` file at the git root to set defaults for the whole team:
```yaml
# .s3lfsconfig - commit this to version control
endpoint_url: https://minio.internal:9000
workers: 16
no_sign_request: true
use_acceleration: false
```

When `.s3lfsconfig` exists, its values are used as defaults for all commands. CLI flags still override config values - for example, `s3lfs track --no-sign-request` always uses unsigned requests regardless of the config.

**Supported keys**:
- `no_sign_request`: Use unsigned S3 requests (default: `false`)
- `use_acceleration`: Enable S3 Transfer Acceleration (default: `false`)
- `endpoint_url`: S3-compatible endpoint for the whole team (default: none, i.e. AWS)
- `workers`: Parallel worker count (default: auto-detected from CPU count)
- `compression`: `auto` (sample each file; store incompressible ones raw), `always`, or `never`

Unrecognised keys are reported rather than ignored -- a misspelled setting that changes where your data goes should not fail silently.

### Public Buckets
For public S3 buckets, use the `--no-sign-request` flag or set it in `.s3lfsconfig`:
```sh
s3lfs init public-bucket my-project --no-sign-request
s3lfs checkout --all --no-sign-request
```

### Manifest File
The `.s3_manifest.yaml` file contains:
- S3 bucket and prefix configuration
- File-to-hash mappings for tracked files
- Should be committed to Git for team collaboration

## Advanced Features

### Parallel Operations
Uploads and downloads use block-level parallelism: all chunks across all files are submitted to a single shared worker pool. This means a 20GB file split into 4 chunks downloads all 4 concurrently, alongside chunks from other files.

The worker count is auto-detected from your CPU count but can be overridden:
```sh
s3lfs track data/ --workers 32       # Use 32 parallel workers
s3lfs checkout --all --workers 16    # Limit to 16 workers
```

The default is `min(32, cpu_count + 4)`. Increase for high-bandwidth connections with many small files; decrease for memory-constrained environments.

### Compression
Files are automatically compressed with gzip before upload. When `pigz` is installed, s3lfs uses it for parallel compression across all CPU cores. The output format is identical to gzip, so existing tracked files work without changes.

To install pigz: `apt install pigz` (Debian/Ubuntu), `brew install pigz` (macOS).

### Performance Metrics
Use the `--metrics` flag to collect parallelism metrics during operations:
```sh
s3lfs track data/ --metrics
s3lfs checkout --all --metrics
```

This reports worker utilization, task durations, and stage-level parallelism for hashing, compression, upload, and download.

### Retry Behavior
Transient S3 errors (network timeouts, throttling) are retried automatically with exponential backoff (2s, 4s, 8s, capped at 30s). Each operation retries up to 3 times before failing.

### File Deduplication
Files with identical content (same hash) are stored only once in S3, regardless of path or filename.

### Multiple Hashing Algorithms
S3LFS supports both SHA-256 (default) and MD5 hashing:
- SHA-256: More secure, used for file integrity
- MD5: Available for compatibility with legacy systems

## Storage Format

The bucket layout is a documented contract, not an implementation detail:

```
<repo_prefix>/assets/<sha256-of-content>/<path-in-repo>[.gz][.chunkN]
```

- **Incompressible files are stored raw** -- exact original bytes under their
  natural name. A JPEG in the bucket is just a JPEG; fetch it with any S3
  tool and use it directly.
- Compressible files carry a `.gz` suffix and are standard gzip.
- Files larger than the chunk size are split into `.chunk0..N` pieces;
  concatenating them in order yields the (possibly gzipped) whole.

Whether a file compresses is decided by sampling its content at upload
(`compression: auto`). Set `compression: never` in `.s3lfsconfig` to store
everything raw, or `always` for the pre-0.6 behaviour.

### Getting your data out without s3lfs

Choosing a versioning tool should not mean your data is hostage to it. The
manifest is plain YAML mapping each path to its content hash, and the key
scheme above is all you need to restore files with generic tools:

```sh
# for each <path>: <hash> in .s3_manifest.yaml (or .s3lfs_manifest/*.yaml):
aws s3 cp "s3://BUCKET/PREFIX/assets/$hash/$path" "$path" \
  || { aws s3 cp "s3://BUCKET/PREFIX/assets/$hash/$path.gz" - | gunzip > "$path"; }
sha256sum "$path"   # must equal $hash
```

This recovery path is enforced by a test
(`TestEscapeHatch::test_restore_with_plain_boto3_and_the_manifest_only`)
that restores files using nothing but an S3 client and the manifest. If a
future change breaks the recipe, the build fails.

## Performance

The manifest is the one thing every command reads, so its size sets a floor on
how fast anything can be. Sharding splits it by top-level directory and shards
are read only when something touches a key in them.

Measured on a synthetic manifest of 200,000 entries spread over 100
directories (18.8 MB), Apple silicon, Python 3.14, PyYAML with libyaml:

| | single file | sharded |
|---|---|---|
| open the manifest | 1,103 ms | **20 ms** |
| look up one path | — (already loaded) | 8 ms |
| read 5 of 100 directories | — (not possible) | **43 ms** |
| read every entry | 1,182 ms | 901 ms |

Reproduce it with:

```sh
python benchmarks/manifest_scaling.py            # 200,000 entries, 100 shards
python benchmarks/manifest_scaling.py 50000 25   # or pick your own
```

A single file has to be parsed in full before any command can start, so
opening it is the floor for `status`, `ls`, `sync` and every hook. Sharding
removes that floor: you pay only for the directories you touch. Reading
*everything* is still roughly the same work either way, because it is the same
bytes -- sharding does not make a full scan cheaper, it makes a full scan
unnecessary.

End to end, in a working copy whose sparse profile covers 1 of those 100
directories:

```
s3lfs status  (no sparse profile, 200,000 entries)   6,787 ms
s3lfs status  (sparse, 2,000 entries in profile)       304 ms
```

Two related effects worth knowing:

- **Git history.** A flat manifest is rewritten in full by every `track`, so
  each commit that touches one asset adds another whole copy of it to history.
  With shards, only the affected directory's file changes -- about 190 KB
  instead of 18.8 MB in the example above.
- **The YAML parser.** s3lfs uses the libyaml-backed loader when PyYAML
  provides it, which is roughly 8x faster than the pure-Python one (4.31s vs
  0.50s to parse a 100,000-entry manifest). Nothing to configure; installing
  a PyYAML wheel built with libyaml is enough.
- **Startup.** boto3 and package metadata load only when a command actually
  touches S3, cutting the fixed cost of `--help`, `--version` and hooks with
  nothing to do from ~200ms to ~135ms; what remains is mostly the
  interpreter itself.
- **The transfer engine.** `pip install s3lfs[crt]` installs AWS's C-based
  transfer client (CRT). s3lfs requests it in "auto" mode: it is used where
  it applies (standard AWS S3 endpoints) and boto3 falls back to the classic
  client elsewhere -- MinIO, R2 and other S3-compatibles behave exactly as
  before. Measured against real AWS S3, CRT made no difference for this
  workload; the remaining gap to raw copy tools is s3lfs's own per-file
  work, not the HTTP engine.

### Against a raw copy tool, on real S3

Transferring 24 files (201 MB) to and from AWS S3 us-west-2, versus s5cmd
v2.3.0 (`benchmarks/transfer_comparison.py`):

| scenario | s3lfs | s5cmd |
|---|---|---|
| cold upload, incompressible | 3.0s | 2.2s |
| cold upload, compressible | **0.9s** | 2.0s |
| cold download, incompressible | 3.2s | 2.2s |
| cold download, compressible | **1.1s** | 2.3s |
| re-run, nothing changed (up or down) | 0.2--0.4s | ~0.1s |
| one file of 24 changed | 0.9s | 0.4s |

On incompressible data s5cmd is 1.4--1.5x faster: it moves bytes and does
nothing else, while s3lfs also hashes every file end-to-end (0.5ms/MB --
the price of content addressing and the reason a checkout can prove it
gave you the right bytes) and stages a snapshot copy. On compressible
data s3lfs is **over 2x faster in both directions**, because it moves
~1% of the bytes. Repeat operations are sub-second for both. Localhost
benchmarks overstate the gap several-fold: with a real network under the
transfer, the per-file costs mostly disappear into it.

## Correctness

The parts of s3lfs that can lose data are modelled in TLA+ and checked with TLC
on every pull request. The models live in [`specs/`](specs/); `specs/check.sh`
runs them.

What is checked:

| Property | Meaning |
|---|---|
| `NoDataLoss` | No automatic operation destroys content that exists only on disk |
| `NoDanglingReference` | Every manifest entry has an object behind it, so a checkout cannot 404 |
| `NoCollateralDeletion` | Distinct paths never share a storage key, so untracking one never destroys another's bytes |
| `NoDualOwnership` | No path is versioned by git and s3lfs at the same time |
| `NoOrphanedFile` | No file on disk is hidden from git while absent from the manifest |
| `NoSilentCorruption` | A checkout that reports success produced the whole file |
| `NoLostUpdate` | Concurrent manifest writers do not overwrite each other |

Each property is paired with a configuration that *disables* the design decision
it depends on, and CI asserts those still fail. A property that holds no matter
what the code does proves nothing, so both directions are checked.

This is model checking, not a proof about the Python. The models are hand-written
abstractions of the implementation over small bounded domains -- a few paths, a
few content values -- so they establish that the *design* is sound and that
specific defects are excluded, not that the code is free of bugs. Their practical
value is concrete: checking the working-copy model produced a counterexample
(`track` → commit → `remove` → `cleanup` → `sync`) that revealed `sync` would
delete the last copy of content whose object had been garbage-collected. That
trace is now a regression test.

## Troubleshooting

### Common Issues
1. **AWS Credentials**: Ensure credentials are properly configured
2. **Bucket Permissions**: Verify read/write access to the S3 bucket
3. **Network**: Check internet connectivity for S3 operations
4. **Disk Space**: Ensure sufficient local storage for file operations

### Verbose Output
Use `--verbose` flag for detailed operation information:
```sh
s3lfs track data/ --verbose
s3lfs checkout --all --verbose
```

## License
MIT License

## Contributing
Pull requests are welcome! Please submit issues and suggestions via GitHub.

## Development Setup

### Pre-commit Hooks

This project uses pre-commit hooks to ensure code quality. The hooks include:

- **Code Quality**: Trailing whitespace, end-of-file fixer, YAML validation, large file detection
- **Python Formatting**: Black code formatter with 88-character line length
- **Import Sorting**: isort with Black profile
- **Linting**: flake8 with extended ignore patterns
- **Type Checking**: mypy with boto3 type stubs
- **Unit Tests**: Automatic test execution on every commit

To set up pre-commit hooks:

```bash
# Install pre-commit
pip install pre-commit

# Install the git hook scripts
pre-commit install

# Run all hooks on all files
pre-commit run --all-files
```

The test hook will automatically run all unit tests before each commit, ensuring that code changes don't break existing functionality.
