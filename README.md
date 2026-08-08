# s3lfs

A Python-based version control system for large assets using Amazon S3 and S3-compatible storage. This system is designed to work like Git LFS but utilizes S3 for better bandwidth and scalability. It supports file tracking, parallel operations, encryption, and any S3-compatible backend (MinIO, Cloudflare R2, Backblaze B2, Wasabi, DigitalOcean Spaces, etc.).

## Features

- Upload and track large files in S3 instead of Git
- Works with any S3-compatible storage (MinIO, Cloudflare R2, Backblaze B2, Wasabi)
- **Block-level parallel transfers**: Downloads and uploads flatten all chunks across all files into a single worker pool
- **Automatic parallel compression**: Uses pigz when available, falls back to gzip
- **Git hook integration**: `s3lfs install` sets up pre-commit, post-checkout, post-merge, and pre-push hooks
- **Accident-proof tracking**: `s3lfs track` gitignores tracked paths and removes them from the git index, so large files can't sneak into git history
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

**Git protection**: Tracking a path also adds it to a marked block in `.gitignore` (so `git add .` won't commit the large files to git) and, if any of the files were already committed to git, removes them from the git index (`git rm --cached`; the files stay on disk). `s3lfs remove` removes the corresponding `.gitignore` entry again.

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

> ⚠️ **Work in Progress**: The cleanup command is experimental and untested. Use with caution.

```sh
s3lfs cleanup
```
**Description**: Removes files from S3 that are no longer referenced in the current manifest.

**Options**:
- `--force`: Skip confirmation prompt
- `--no-sign-request`: Use unsigned S3 requests

**Example**:
```sh
s3lfs cleanup --force                    # Clean up without confirmation
```

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

### Install Git Hooks
```sh
s3lfs install
```
**Description**: Installs git hooks for transparent s3lfs integration. After installation, modified tracked files are automatically uploaded and the manifest staged when you `git commit`, tracked files are automatically downloaded after `git checkout` and `git merge`, and `git push` verifies that every pushed manifest references content that actually exists in S3.

**Installed hooks**:
- `pre-commit`: Uploads modified tracked files and stages the updated manifest, so each commit's manifest matches the content in S3. Also blocks the commit if an s3lfs-tracked file is staged for commit in git itself.
- `post-checkout`: Downloads tracked files after branch checkouts
- `post-merge`: Downloads tracked files after merges
- `pre-push`: Verifies the manifests being pushed reference uploaded content (runs `s3lfs verify` for each pushed ref); aborts the push if content is missing

The post-* hooks are non-blocking -- if s3lfs fails or is not available, the git operation continues with a warning. The pre-commit and pre-push hooks abort their git operation on failure (bypass with `--no-verify`), because committing or pushing a manifest whose hashes have no objects behind them breaks every collaborator's checkout. Hooks are appended to existing hook files, preserving any other hooks you have.

### Uninstall Git Hooks
```sh
s3lfs uninstall
```
**Description**: Removes s3lfs git hooks. Other hooks in the same files are preserved.

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
```
With hooks installed, `git commit` automatically uploads modified tracked files and stages the manifest, `git pull` and `git checkout` automatically download tracked files, and `git push` verifies the pushed manifests reference uploaded content.

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
When cloning the repository, restore tracked files:
```sh
git clone https://github.com/your-repo/my-repo.git
cd my-repo
s3lfs checkout --all
```

### 5. Update Workflow
For ongoing development:
```sh
# Track any modified large files
s3lfs track --modified

# Commit manifest changes
git add .s3_manifest.yaml
git commit -m "Update tracked files"

# Download latest files
s3lfs checkout --all
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
no_sign_request: true
use_acceleration: false
```

When `.s3lfsconfig` exists, its values are used as defaults for all commands. CLI flags still override config values - for example, `s3lfs track --no-sign-request` always uses unsigned requests regardless of the config.

**Supported keys**:
- `no_sign_request`: Use unsigned S3 requests (default: `false`)
- `use_acceleration`: Enable S3 Transfer Acceleration (default: `false`)

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
