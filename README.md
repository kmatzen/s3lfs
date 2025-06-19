# s3lfs

A Python-based version control system for large assets using Amazon S3. This system is designed to work like Git LFS but utilizes S3 for better bandwidth and scalability. It supports file tracking, parallel operations, encryption, and automatic cleanup of unused assets.

## Features
- Upload and track large files in S3 instead of Git
- Stores asset versions using SHA-256 hashes
- Encrypts stored assets with AES256 server-side encryption
- Cleans up unreferenced files in S3 after rebases or squashes
- **Parallel uploads/downloads**: Improves speed using multi-threading
- **Compression before upload**: Uses gzip to reduce storage and bandwidth usage
- **File deduplication**: Prevents redundant uploads using content hashing
- **Flexible path resolution**: Supports files, directories, and glob patterns
- **Multiple hashing algorithms**: SHA-256 (default) and MD5 support

## Installation

Install the required dependencies:
```sh
pip install poetry
poetry install
```

## Command Line Interface (CLI) Usage

The CLI tool provides a simplified set of commands for managing large files with S3. All commands automatically use the bucket and prefix configured during initialization.

### Initialize Repository
```sh
s3lfs init <bucket-name> <repo-prefix>
```
**Description**: Initializes the S3LFS system with the specified S3 bucket and repository prefix. This creates a `.s3_manifest.json` file that stores the configuration and file mappings.

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

This creates `.s3_manifest.json` which should be committed to Git:
```sh
git add .s3_manifest.json
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
git add .s3_manifest.json
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
git add .s3_manifest.json
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

### 7. Cleanup
Periodically clean up unreferenced files:
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
The `.s3_manifest.json` file contains:
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
