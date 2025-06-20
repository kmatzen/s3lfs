# S3LFS Hash Cache System

## Overview

S3LFS uses a separate cache file to store hash computations for performance optimization. This cache is **separate from the manifest** and should **not be version controlled**.

## Automatic .gitignore Management

When you run `s3lfs init`, the system automatically:
- Creates or updates your `.gitignore` file
- Adds patterns to exclude S3LFS cache and temporary files
- Preserves existing `.gitignore` content
- Prevents duplicate entries on subsequent runs

The following patterns are automatically added:
```
# S3LFS cache and temporary files - should not be version controlled
*_cache.json
.s3lfs_temp/
*.s3lfs.lock
```

## File Structure

- **Manifest file**: `.s3_manifest.json` - Contains tracked files and their hashes (version controlled)
- **Cache file**: `.s3_manifest_cache.json` - Contains hash cache data (NOT version controlled)
- **Temp directory**: `.s3lfs_temp/` - Temporary files during operations (NOT version controlled)
- **Lock file**: `.s3lfs.lock` - Process synchronization (NOT version controlled)

## Cache File Format

```json
{
  "path/to/file.txt": {
    "hash": "sha256_hash_here",
    "metadata": {
      "size": 1024,
      "mtime": 1672531200.0,
      "inode": 12345
    },
    "timestamp": 1672531200.123
  }
}
```

## Multi-Process Safety

The cache system is multi-process safe using:
- **File-based locking** with `portalocker`
- **Atomic file operations** for cache updates
- **Manifest reloading** to get latest state from other processes
- **Double-check patterns** to prevent race conditions

## Cache Management Commands

- **Clear all cache**: `s3lfs.clear_hash_cache()`
- **Clear specific file**: `s3lfs.clear_hash_cache("path/to/file.txt")`
- **Cleanup stale entries**: `s3lfs.cleanup_stale_cache(max_age_days=30)`
- **Get file status**: `s3lfs.get_file_status("path/to/file.txt")`

## Performance Benefits

- **Cache Hits**: Skip expensive file hashing when metadata unchanged
- **Fast Change Detection**: Uses file system metadata comparison (size, mtime, inode)
- **Automatic Invalidation**: Cache invalidates when files change
- **Persistent**: Cache survives across sessions
- **Multi-Process Safe**: Works correctly with parallel operations

## Version Control

**Important**: Cache files should NOT be version controlled. They are automatically added to `.gitignore`:

```
# S3LFS hash cache files - should not be version controlled
*_cache.json
.s3_manifest_cache.json
```

## Cache Naming Convention

Cache files follow the pattern: `{manifest_name}_cache.json`

Examples:
- `.s3_manifest.json` → `.s3_manifest_cache.json`
- `project_manifest.json` → `project_manifest_cache.json`
