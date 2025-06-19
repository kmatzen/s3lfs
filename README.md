# s3lfs

A Python-based version control system for large assets using Amazon S3. This system is designed to work like Git LFS but utilizes S3 for better bandwidth and scalability. It integrates with Git and supports sparse checkouts, encryption, and automatic cleanup of unused assets.

## Features
- Upload and track large files in S3 instead of Git
- Stores asset versions using SHA-256 hashes
- Supports sparse checkouts to avoid downloading unnecessary files
- Encrypts stored assets with AES256
- Cleans up unreferenced files in S3 after rebases or squashes
- Direct Git integration for seamless workflow
- **Automatic tracking of modified files**: Detects and uploads changed files before committing.
- **Parallel uploads/downloads**: Improves speed using multi-threading.
- **Caching**: Prevents redundant downloads of recently accessed files.
- **Compression before upload**: Uses gzip to reduce storage and bandwidth usage.
- **File locking/conflict resolution**: Checks for existing files in S3 to prevent overwrites.

## Installation

Install the required dependencies:
```sh
pip install poetry
poetry install
```

## Command Line Interface (CLI) Usage

The CLI tool provides various commands for managing large files with S3. Below is a comprehensive list of available commands.  Almost all commands can take additional `--bucket` and `--repo-prefix` options to override values set in the manifest

### Initialize Repository
```sh
s3lfs init <bucket-name> <repo-prefix>
```
**Description**: Initializes the S3 asset versioning system with the specified S3 bucket and repository prefix.

### Upload a File
```sh
s3lfs upload <file-path>
```
**Description**: Uploads a single file to S3.

### Download a File
```sh
s3lfs download <file-path>
```
**Description**: Downloads a file from S3 by the hash stored in the manifest.

### Track and Upload Modified Files
```sh
s3lfs track-modified
```
**Description**: Detects modified files in a Git repository and uploads them to S3.

### Download All Files Listed in Manifest
```sh
s3lfs download-all
```
**Description**: Downloads all files listed in the manifest from S3.

### Sparse Checkout
```sh
s3lfs sparse-checkout <dir-path>
```
**Description**: Downloads all files under a given subtree (prefix) instead of fetching the entire repository.

### Set Up Git Filters for Automatic S3 Integration
```sh
s3lfs git-setup
```
**Description**: Configures Git filters for automatic asset tracking with S3.

### Recursively Track and Upload a Directory
```sh
s3lfs track-subtree <dir-path>
```
**Description**: Recursively tracks and uploads all files in a specified directory.

### Remove a File from Tracking
```sh
s3lfs remove <file-path> [--purge-from-s3]
```
**Description**: Removes a single file from tracking. Optionally, the file can be immediately deleted from S3.

### Remove a Tracked Directory
```sh
s3lfs remove-subtree <dir-path> [--purge-from-s3]
```
**Description**: Removes an entire directory from tracking. Optionally, the files can be immediately deleted from S3.

### Cleanup Unreferenced Files in S3
```sh
s3lfs cleanup [--force]
```
**Description**: Cleans up unreferenced files in S3 that are no longer tracked by Git.  This command prompts for confirmation unless `--force` is specified.

## Git Workflow Integration
To integrate S3 Asset Versioning into your Git workflow, follow these steps:

### 1. Initiailize s3lfs manifest
RUn the following command to create the manifest for s3lfs:
```sh
s3lfs init <bucket> <repo-prefix>
```

`.s3_manifest.json` will be created.

### 2. Initialize and Set Up Git Hooks
Run the following command to set up Git hooks for seamless asset tracking:
```sh
s3lfs git-setup
```
This configures Git to automatically upload tracked files on each commit.

### 3. Track Large Files
Instead of committing large binary files directly to Git, track them using:
```sh
s3lfs upload my_large_file.zip
```
This will:
- Upload the file to S3 (compressed and hashed)
- Store the mapping in `.s3_manifest.json`

### 4. Commit and Push
After tracking large files, commit and push your changes as usual:
```sh
git add .s3_manifest.json
```
```sh
git commit -m "Tracked large file using S3 Asset Versioning"
```
```sh
git push origin main
```

### 5. Clone and Retrieve Large Files
When another user or CI system clones the repository, they should run:
```sh
git clone https://github.com/your-repo/my-repo.git
cd my-repo
s3lfs download-all
```
This will restore all tracked large files from S3 to their appropriate locations.

### 6. Automatically Track Modified Files
To ensure modified assets are always synchronized with S3 before committing, run:
```sh
s3lfs track-modified
```
This detects changes in large files and uploads them automatically before committing.

### 7. Perform Sparse Checkouts
If you only need a specific file rather than downloading all assets, use:
```sh
s3lfs sparse-checkout <dir-name>
```
This will retrieve only the specified assets instead of downloading everything.

### 8. Clean Up Unused Files
After a rebase or squash, unreferenced files in S3 may remain. To clean them up, run:
```sh
s3lfs cleanup
```
This will remove all files from S3 that are no longer referenced in the Git history.

## Configuration
- Ensure your AWS credentials are set up using `aws configure`.
- The manifest file `.s3_manifest.json` should be committed to track asset mappings.

## License
MIT License

## Contributing
Pull requests are welcome! Please submit issues and suggestions via GitHub.
