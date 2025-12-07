# s3lfs

A Python-based version control system for large assets using Amazon S3. This system is designed to work like Git LFS but utilizes S3 for better bandwidth and scalability. It supports file tracking, parallel operations, and encryption.

## Features

- Upload and track large files in S3 instead of Git
- Stores asset versions using SHA-256 hashes
- Encrypts stored assets with AES256 server-side encryption
- Cleanup of unreferenced files in S3 (experimental)
- **Parallel uploads/downloads**: Improves speed using multi-threading
- **Compression before upload**: Uses gzip to reduce storage and bandwidth usage
- **File deduplication**: Prevents redundant uploads using content hashing
- **Flexible path resolution**: Supports files, directories, and glob patterns
- **Multiple hashing algorithms**: SHA-256 (default) and MD5 support

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

**Examples**:
```sh
s3lfs track data/large_file.zip          # Track a single file
s3lfs track data/                        # Track entire directory
s3lfs track "*.mp4"                      # Track all MP4 files
s3lfs track --modified                   # Track only changed files
```

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

### 2. Track Large Files
Instead of committing large files directly to Git, track them with S3LFS:
```sh
s3lfs track data/large_dataset.zip
s3lfs track models/
s3lfs track "*.mp4"
```

### 3. Commit Changes
After tracking files, commit the updated manifest:
```sh
git add .s3_manifest.yaml
git commit -m "Track large files with S3LFS"
git push
```

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

### Public Buckets
For public S3 buckets, use the `--no-sign-request` flag:
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

### Multiple Hashing Algorithms
S3LFS supports both SHA-256 (default) and MD5 hashing:
- SHA-256: More secure, used for file integrity
- MD5: Available for compatibility with legacy systems

### Compression
All files are automatically compressed with gzip before upload, reducing storage costs and transfer time.

### Parallel Operations
Large file operations use multi-threading for improved performance on multiple files.

### File Deduplication
Files with identical content (same hash) are stored only once in S3, regardless of path or filename.

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
