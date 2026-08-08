import json
import subprocess
from pathlib import Path

import click
import yaml

from s3lfs import metrics
from s3lfs.config import load_config
from s3lfs.core import S3LFS
from s3lfs.path_resolver import PathResolver
from s3lfs.utils import find_git_root


def _make_s3lfs(
    git_root, manifest_path, no_sign_request=False, use_acceleration=False, **extra
):
    """Create an S3LFS instance with .s3lfsconfig defaults applied.

    CLI flags (when True) override config-file values.
    """
    config = load_config(git_root)
    # CLI True overrides config; CLI False falls back to config
    effective_no_sign = no_sign_request or config.get("no_sign_request", False)
    effective_accel = use_acceleration or config.get("use_acceleration", False)
    return S3LFS(
        no_sign_request=effective_no_sign,
        manifest_file=str(manifest_path),
        use_acceleration=effective_accel,
        **extra,
    )


def _setup_s3lfs_command(cli_path=None, require_manifest=True):
    """
    Common setup for S3LFS CLI commands.

    Finds git root, checks manifest exists, creates PathResolver, and optionally
    resolves a CLI path argument to a manifest key.

    Args:
        cli_path: Optional path argument from CLI to resolve to manifest key
        require_manifest: If True, raises error if manifest doesn't exist

    Returns:
        If cli_path is None: (git_root, manifest_path, path_resolver)
        If cli_path is provided: (git_root, manifest_path, path_resolver, manifest_key)

    Raises:
        click.Abort: If git root not found or manifest doesn't exist
    """
    git_root = find_git_root()
    if not git_root:
        click.echo("Error: Not in a git repository")
        raise click.Abort()

    manifest_path = get_manifest_path(git_root)
    if require_manifest and not manifest_path.exists():
        click.echo("Error: S3LFS not initialized. Run 's3lfs init' first.")
        raise click.Abort()

    path_resolver = PathResolver(git_root)

    # If a CLI path is provided, resolve it to a manifest key
    if cli_path is not None:
        manifest_key = path_resolver.from_cli_input(cli_path, cwd=Path.cwd())
        return git_root, manifest_path, path_resolver, manifest_key

    return git_root, manifest_path, path_resolver


def get_manifest_path(git_root):
    """
    Get the manifest file path relative to the git repository root.
    Checks for YAML format first (preferred), then falls back to JSON for backward compatibility.

    Args:
        git_root: Path to the git repository root

    Returns:
        Path to the manifest file (YAML or JSON)
    """
    # Check for YAML format first (new default)
    yaml_manifest = git_root / ".s3_manifest.yaml"
    if yaml_manifest.exists():
        return yaml_manifest

    # Fall back to JSON for backward compatibility
    json_manifest = git_root / ".s3_manifest.json"
    if json_manifest.exists():
        return json_manifest

    # If neither exists, return YAML path for new repos
    return yaml_manifest


@click.group()
def cli():
    """S3-based asset versioning CLI tool."""
    pass


@click.command()
@click.argument("bucket", required=True)
@click.argument("prefix", required=True)
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
@click.option(
    "--use-acceleration", is_flag=True, help="Enable S3 Transfer Acceleration"
)
@click.option(
    "--endpoint-url",
    default=None,
    help="Custom S3 endpoint URL for S3-compatible storage (e.g. MinIO, R2, Wasabi)",
)
def init(bucket, prefix, no_sign_request, use_acceleration, endpoint_url):
    """Initialize S3LFS with a bucket and repo prefix"""
    # Find git root
    git_root = find_git_root()
    if not git_root:
        click.echo("Error: Not in a git repository")
        raise click.Abort()

    # Create manifest at git root
    manifest_path = get_manifest_path(git_root)
    if manifest_path.exists():
        print("Error: Repository already initialized")
        return

    try:
        s3lfs = S3LFS(
            bucket_name=bucket,
            repo_prefix=prefix,
            no_sign_request=no_sign_request,
            use_acceleration=use_acceleration,
            endpoint_url=endpoint_url,
        )
        s3lfs.initialize_repo()
        print(f"Repository initialized with bucket '{bucket}' and prefix '{prefix}'")
    except Exception as e:
        print(f"Error: {e}")
        return


@cli.command()
@click.argument("path", required=False)
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
@click.option(
    "--use-acceleration", is_flag=True, help="Enable S3 Transfer Acceleration"
)
@click.option(
    "--endpoint-url",
    default=None,
    help="Custom S3 endpoint URL for S3-compatible storage",
)
@click.option(
    "--verbose", is_flag=True, help="Show detailed progress and upload information"
)
@click.option(
    "--modified", is_flag=True, help="Track only modified files from manifest"
)
@click.option(
    "--metrics",
    "enable_metrics_flag",
    is_flag=True,
    help="Enable parallelism metrics collection",
)
@click.option(
    "--workers",
    type=int,
    default=None,
    help="Number of parallel workers (default: auto-detected from CPU count)",
)
def track(
    path,
    no_sign_request,
    use_acceleration,
    endpoint_url,
    verbose,
    modified,
    enable_metrics_flag,
    workers,
):
    """Track files, directories, or globs. Use --modified to track only changed files."""
    # Enable metrics if requested
    if enable_metrics_flag:
        metrics.enable_metrics()

    # Common setup: find git root, check manifest, resolve path
    if path:
        git_root, manifest_path, path_resolver, manifest_key = _setup_s3lfs_command(
            cli_path=path
        )
    else:
        git_root, manifest_path, path_resolver = _setup_s3lfs_command()
        manifest_key = None

    s3lfs = _make_s3lfs(
        git_root,
        manifest_path,
        no_sign_request=no_sign_request,
        use_acceleration=use_acceleration,
        endpoint_url=endpoint_url,
        workers=workers,
    )

    if modified:
        # Track only modified files using cached version for better performance
        s3lfs.track_modified_files_cached(silence=not verbose)
    elif manifest_key:
        # FILESYSTEM GLOB: Find files on disk and upload them
        # The manifest_key is converted to a filesystem path, then glob is applied
        s3lfs.track(
            manifest_key, silence=not verbose, interleaved=True, use_cache=False
        )
        _protect_tracked_path(git_root, s3lfs, manifest_key)
    else:
        click.echo("Error: Must provide either a path or use --modified flag")
        raise click.Abort()


@cli.command()
@click.argument("path", required=False)
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
@click.option(
    "--use-acceleration", is_flag=True, help="Enable S3 Transfer Acceleration"
)
@click.option(
    "--endpoint-url",
    default=None,
    help="Custom S3 endpoint URL for S3-compatible storage",
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Show detailed progress and download size information",
)
@click.option("--all", is_flag=True, help="Checkout all files from manifest")
@click.option(
    "--metrics",
    "enable_metrics_flag",
    is_flag=True,
    help="Enable parallelism metrics collection",
)
@click.option(
    "--workers",
    type=int,
    default=None,
    help="Number of parallel workers (default: auto-detected from CPU count)",
)
def checkout(
    path,
    no_sign_request,
    use_acceleration,
    endpoint_url,
    verbose,
    all,
    enable_metrics_flag,
    workers,
):
    """Checkout files, directories, or globs. Use --all to checkout all tracked files."""
    # Enable metrics if requested
    if enable_metrics_flag:
        metrics.enable_metrics()

    # Common setup: find git root, check manifest, resolve path
    if path:
        git_root, manifest_path, path_resolver, manifest_key = _setup_s3lfs_command(
            cli_path=path
        )
    else:
        git_root, manifest_path, path_resolver = _setup_s3lfs_command()
        manifest_key = None

    s3lfs = _make_s3lfs(
        git_root,
        manifest_path,
        no_sign_request=no_sign_request,
        use_acceleration=use_acceleration,
        endpoint_url=endpoint_url,
        workers=workers,
    )

    if all:
        # Download all files from manifest
        s3lfs.parallel_download_all(silence=not verbose)
    elif manifest_key:
        # MANIFEST GLOB: Find files in manifest and download them
        # The manifest_key is matched against manifest entries (files may not exist on disk)
        s3lfs.checkout(manifest_key, silence=not verbose)
    else:
        click.echo("Error: Must provide either a path or use --all flag")
        raise click.Abort()


@cli.command()
@click.argument("path", required=False)
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
@click.option(
    "--use-acceleration", is_flag=True, help="Enable S3 Transfer Acceleration"
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Show detailed information including file sizes and hashes",
)
@click.option("--all", is_flag=True, help="List all tracked files from manifest")
@click.option(
    "--endpoint-url",
    default=None,
    help="Custom S3 endpoint URL for S3-compatible storage",
)
def ls(
    path,
    no_sign_request,
    use_acceleration,
    verbose,
    all,
    endpoint_url,
    git_finder_func=None,
):
    """List tracked files, directories, or globs. If no path is provided, lists all tracked files."""
    # Common setup: find git root, check manifest, resolve path
    # Note: git_finder_func is for testing purposes only
    if git_finder_func:
        # Test mode: use custom git finder
        git_root = find_git_root(git_finder_func=git_finder_func)
        if not git_root:
            click.echo("Error: Not in a git repository")
            raise click.Abort()
        manifest_path = get_manifest_path(git_root)
        if not manifest_path.exists():
            click.echo("Error: S3LFS not initialized. Run 's3lfs init' first.")
            raise click.Abort()
        path_resolver = PathResolver(git_root)
        # Resolve path if provided
        manifest_key = (
            path_resolver.from_cli_input(path, cwd=Path.cwd()) if path else None
        )
    else:
        # Normal mode: use helper function
        if path:
            git_root, manifest_path, path_resolver, manifest_key = _setup_s3lfs_command(
                cli_path=path
            )
        else:
            git_root, manifest_path, path_resolver = _setup_s3lfs_command()
            manifest_key = None

    # Get current working directory relative to git root for output stripping
    cwd = Path.cwd()
    try:
        relative_cwd = cwd.relative_to(git_root)
    except ValueError:
        relative_cwd = Path(".")

    s3lfs = _make_s3lfs(
        git_root,
        manifest_path,
        no_sign_request=no_sign_request,
        use_acceleration=use_acceleration,
        endpoint_url=endpoint_url,
    )

    if all or not manifest_key:
        # List all files from manifest (default behavior when no path provided)
        s3lfs.list_all_files(
            verbose=verbose,
            strip_prefix=str(relative_cwd) if relative_cwd != Path(".") else None,
        )
    else:
        # MANIFEST GLOB: Find files in manifest and display them
        # The manifest_key is matched against manifest entries
        s3lfs.list_files(
            manifest_key,
            verbose=verbose,
            strip_prefix=str(relative_cwd) if relative_cwd != Path(".") else None,
        )


@click.command()
@click.argument("path", required=True)
@click.option("--purge-from-s3", is_flag=True, help="Purge file in S3 immediately")
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
@click.option(
    "--use-acceleration", is_flag=True, help="Enable S3 Transfer Acceleration"
)
@click.option(
    "--endpoint-url",
    default=None,
    help="Custom S3 endpoint URL for S3-compatible storage",
)
def remove(path, purge_from_s3, no_sign_request, use_acceleration, endpoint_url):
    """Remove files or directories from tracking. Supports glob patterns."""
    # Common setup: find git root, check manifest, resolve path
    git_root, manifest_path, path_resolver, manifest_key = _setup_s3lfs_command(
        cli_path=path
    )

    versioner = _make_s3lfs(
        git_root,
        manifest_path,
        no_sign_request=no_sign_request,
        use_acceleration=use_acceleration,
        endpoint_url=endpoint_url,
    )

    # Check if this is a single file (no glob, not a directory)
    has_glob = "*" in manifest_key or "?" in manifest_key or "[" in manifest_key
    filesystem_path = path_resolver.to_filesystem_path(manifest_key)
    is_single_file = not has_glob and filesystem_path.is_file()

    if is_single_file:
        # Optimize single file removal
        versioner.remove_file(manifest_key, keep_in_s3=not purge_from_s3)
    else:
        # MANIFEST GLOB: Find files in manifest and remove them
        # The manifest_key is matched against manifest entries
        # Note: This is manifest-only; files on disk are not affected
        versioner.remove_subtree(manifest_key, keep_in_s3=not purge_from_s3)

    if _remove_gitignore_entry(git_root, {f"/{manifest_key}", f"/{manifest_key}/"}):
        click.echo(f"Removed '{manifest_key}' from the s3lfs block in .gitignore")


@click.command()
@click.option("--force", is_flag=True, help="Skip confirmation for cleanup")
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
@click.option(
    "--use-acceleration", is_flag=True, help="Enable S3 Transfer Acceleration"
)
@click.option(
    "--endpoint-url",
    default=None,
    help="Custom S3 endpoint URL for S3-compatible storage",
)
@click.option(
    "--workers",
    type=int,
    default=None,
    help="Number of parallel workers (default: auto-detected from CPU count)",
)
def cleanup(force, no_sign_request, use_acceleration, endpoint_url, workers):
    """Clean up unreferenced files from S3."""
    # Find git root
    git_root = find_git_root()
    if not git_root:
        click.echo("Error: Not in a git repository")
        raise click.Abort()

    manifest_path = get_manifest_path(git_root)
    if not manifest_path.exists():
        click.echo("Error: S3LFS not initialized. Run 's3lfs init' first.")
        raise click.Abort()

    versioner = _make_s3lfs(
        git_root,
        manifest_path,
        no_sign_request=no_sign_request,
        use_acceleration=use_acceleration,
        endpoint_url=endpoint_url,
        workers=workers,
    )
    versioner.cleanup_s3(force=force)


@click.command()
@click.option("--force", is_flag=True, help="Skip confirmation and migrate immediately")
def migrate(force):
    """Migrate manifest from JSON to YAML format."""
    # Find git root
    git_root = find_git_root()
    if not git_root:
        click.echo("Error: Not in a git repository")
        raise click.Abort()

    json_manifest = git_root / ".s3_manifest.json"
    yaml_manifest = git_root / ".s3_manifest.yaml"

    # Check if JSON manifest exists
    if not json_manifest.exists():
        click.echo("Error: No JSON manifest found at .s3_manifest.json")
        click.echo("Nothing to migrate.")
        raise click.Abort()

    # Check if YAML manifest already exists
    if yaml_manifest.exists():
        click.echo("Error: YAML manifest already exists at .s3_manifest.yaml")
        click.echo("Aborting migration to avoid overwriting existing file.")
        raise click.Abort()

    # Load JSON manifest
    try:
        with open(json_manifest, "r") as f:
            manifest_data = json.load(f)
    except Exception as e:
        click.echo(f"Error: Failed to read JSON manifest: {e}")
        raise click.Abort()

    # Show migration plan
    file_count = len(manifest_data.get("files", {}))
    click.echo("Migration Plan:")
    click.echo(f"   Source: .s3_manifest.json ({file_count} tracked files)")
    click.echo("   Target: .s3_manifest.yaml")
    click.echo()

    if not force:
        click.echo("This will:")
        click.echo("  1. Create .s3_manifest.yaml with the same content")
        click.echo("  2. Keep .s3_manifest.json as backup (you can delete it later)")
        click.echo()
        confirm = click.confirm("Do you want to proceed?")
        if not confirm:
            click.echo("Migration cancelled.")
            return

    # Write YAML manifest
    try:
        with open(yaml_manifest, "w") as f:
            yaml.safe_dump(manifest_data, f, default_flow_style=False, sort_keys=True)
        click.echo(f"Successfully created {yaml_manifest.name}")
    except Exception as e:
        click.echo(f"Error: Failed to write YAML manifest: {e}")
        raise click.Abort()

    # Also migrate cache file if it exists
    json_cache = git_root / ".s3_manifest_cache.json"
    yaml_cache = git_root / ".s3_manifest_cache.yaml"

    if json_cache.exists() and not yaml_cache.exists():
        try:
            with open(json_cache, "r") as f:
                cache_data = json.load(f)
            with open(yaml_cache, "w") as f:
                yaml.safe_dump(cache_data, f, default_flow_style=False, sort_keys=True)
            click.echo(f"Successfully migrated cache file to {yaml_cache.name}")
        except Exception as e:
            click.echo(f"Warning: Failed to migrate cache file: {e}")
            click.echo("   (Cache will be rebuilt automatically)")

    click.echo()
    click.echo("Migration complete!")
    click.echo()
    click.echo("Next steps:")
    click.echo("  1. Test the YAML manifest: s3lfs ls")
    click.echo("  2. Commit .s3_manifest.yaml to version control")
    click.echo("  3. Delete .s3_manifest.json: rm .s3_manifest.json")
    click.echo("  4. Update .gitignore if needed")


def _is_lfs_pointer(file_path):
    """Check if a file is a Git LFS pointer file (not actual content)."""
    try:
        with open(file_path, "rb") as f:
            header = f.read(512)
        return header.startswith(b"version https://git-lfs.github.com/spec/v1")
    except (IOError, OSError):
        return False


def _parse_lfs_patterns(git_root):
    """Parse .gitattributes for LFS-tracked patterns.

    Returns a list of glob patterns that have the 'filter=lfs' attribute.
    """
    patterns: list = []
    gitattributes = Path(git_root) / ".gitattributes"
    if not gitattributes.exists():
        return patterns

    with open(gitattributes, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # .gitattributes format: <pattern> <attr1> <attr2> ...
            parts = line.split()
            if len(parts) >= 2 and "filter=lfs" in parts:
                patterns.append(parts[0])
    return patterns


def _find_lfs_files(git_root, patterns):
    """Find all files matching LFS patterns that exist on disk.

    Returns list of Path objects relative to git_root.
    """
    git_root = Path(git_root)
    matched_files = []

    for pattern in patterns:
        # Use glob to find matching files
        if "**" in pattern or "*" in pattern or "?" in pattern:
            for match in git_root.glob(pattern):
                if match.is_file() and not str(match.relative_to(git_root)).startswith(
                    ".git"
                ):
                    matched_files.append(match)
        else:
            # Exact path
            candidate = git_root / pattern
            if candidate.is_file():
                matched_files.append(candidate)

    # Deduplicate
    seen = set()
    unique = []
    for f in matched_files:
        if f not in seen:
            seen.add(f)
            unique.append(f)
    return unique


@click.command("migrate-from-lfs")
@click.argument("bucket", required=True)
@click.argument("prefix", required=True)
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
@click.option(
    "--use-acceleration", is_flag=True, help="Enable S3 Transfer Acceleration"
)
@click.option(
    "--dry-run", is_flag=True, help="Show what would be migrated without making changes"
)
@click.option(
    "--remove-lfs/--keep-lfs",
    default=False,
    help="Remove LFS tracking from .gitattributes after migration (default: keep)",
)
def migrate_from_lfs(
    bucket, prefix, no_sign_request, use_acceleration, dry_run, remove_lfs
):
    """Migrate a Git LFS repository to s3lfs.

    Detects LFS-tracked files from .gitattributes, verifies they contain
    real content (not pointer files), initializes s3lfs, and uploads all
    files to S3.

    Requires that LFS files have been fetched (run 'git lfs pull' first).
    """
    git_root = find_git_root()
    if not git_root:
        click.echo("Error: Not in a git repository")
        raise click.Abort()

    # Check if already initialized
    manifest_path = get_manifest_path(git_root)
    if manifest_path.exists():
        click.echo("Error: s3lfs is already initialized in this repository.")
        click.echo("Remove .s3_manifest.yaml first if you want to re-migrate.")
        raise click.Abort()

    # Step 1: Parse LFS patterns from .gitattributes
    gitattributes_path = git_root / ".gitattributes"
    if not gitattributes_path.exists():
        click.echo("Error: No .gitattributes file found.")
        click.echo("This repository doesn't appear to use Git LFS.")
        raise click.Abort()

    lfs_patterns = _parse_lfs_patterns(git_root)
    if not lfs_patterns:
        click.echo("Error: No Git LFS patterns found in .gitattributes.")
        click.echo("Expected entries like: '*.bin filter=lfs diff=lfs merge=lfs -text'")
        raise click.Abort()

    click.echo(f"Found {len(lfs_patterns)} LFS pattern(s) in .gitattributes:")
    for p in lfs_patterns:
        click.echo(f"  {p}")
    click.echo()

    # Step 2: Find matching files
    lfs_files = _find_lfs_files(git_root, lfs_patterns)
    if not lfs_files:
        click.echo("No files matching LFS patterns found on disk.")
        click.echo("Nothing to migrate.")
        return

    click.echo(f"Found {len(lfs_files)} file(s) to migrate:")
    total_size = 0
    for f in lfs_files:
        rel = f.relative_to(git_root)
        size = f.stat().st_size
        total_size += size
        click.echo(f"  {rel} ({_human_size(size)})")
    click.echo(f"  Total: {_human_size(total_size)}")
    click.echo()

    # Step 3: Check for un-smudged pointer files
    pointers = [f for f in lfs_files if _is_lfs_pointer(f)]
    if pointers:
        click.echo(
            f"Error: {len(pointers)} file(s) are LFS pointer files (not actual content):"
        )
        for f in pointers[:10]:
            click.echo(f"  {f.relative_to(git_root)}")
        if len(pointers) > 10:
            click.echo(f"  ... and {len(pointers) - 10} more")
        click.echo()
        click.echo(
            "Run 'git lfs pull' to download the actual file content, then retry."
        )
        raise click.Abort()

    if dry_run:
        click.echo("Dry run complete. No changes made.")
        return

    # Step 4: Initialize s3lfs
    click.echo("Initializing s3lfs...")
    try:
        s3lfs_obj = S3LFS(
            bucket_name=bucket,
            repo_prefix=prefix,
            no_sign_request=no_sign_request,
            use_acceleration=use_acceleration,
        )
        s3lfs_obj.initialize_repo()
    except Exception as e:
        click.echo(f"Error initializing s3lfs: {e}")
        raise click.Abort()
    click.echo()

    # Step 5: Track all LFS files
    click.echo("Uploading files to S3...")
    for lfs_pattern in lfs_patterns:
        s3lfs_obj.track(lfs_pattern, silence=False, interleaved=True, use_cache=False)
    click.echo()

    # Step 6: Optionally remove LFS tracking
    if remove_lfs:
        click.echo("Removing LFS tracking from .gitattributes...")
        _remove_lfs_from_gitattributes(gitattributes_path, lfs_patterns)
        click.echo("  Updated .gitattributes")
        click.echo()

    click.echo("Migration complete!")
    click.echo()
    click.echo("Next steps:")
    click.echo("  1. Review the changes: git diff")
    click.echo(
        "  2. Commit: git add .s3_manifest.yaml .gitignore .gitattributes && git commit -m 'Migrate from Git LFS to s3lfs'"
    )
    if not remove_lfs:
        click.echo(
            "  3. (Optional) Remove LFS: run again with --remove-lfs, or manually edit .gitattributes"
        )
    click.echo("  4. (Optional) Uninstall Git LFS: git lfs uninstall")


def _human_size(size_bytes):
    """Format a byte count as a human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


def _remove_lfs_from_gitattributes(gitattributes_path, lfs_patterns):
    """Remove LFS filter entries from .gitattributes.

    Removes lines whose pattern matches one of the LFS patterns.
    Leaves other lines intact.
    """
    lines = gitattributes_path.read_text().splitlines(keepends=True)
    kept = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            parts = stripped.split()
            if len(parts) >= 2 and "filter=lfs" in parts and parts[0] in lfs_patterns:
                continue  # Skip this LFS line
        kept.append(line)

    # Remove trailing blank lines
    text = "".join(kept).rstrip("\n") + "\n" if kept else ""
    gitattributes_path.write_text(text)


S3LFS_GITIGNORE_START = "# >>> s3lfs tracked files >>>"
S3LFS_GITIGNORE_END = "# <<< s3lfs tracked files <<<"


def _load_gitignore_block(git_root):
    """Split .gitignore into (lines_before, block_entries, lines_after).

    The s3lfs block is delimited by marker comments so entries can be
    added and removed without disturbing the rest of the file.
    """
    gitignore = git_root / ".gitignore"
    if not gitignore.exists():
        return [], [], []
    lines = gitignore.read_text().splitlines()
    if S3LFS_GITIGNORE_START not in lines:
        return lines, [], []
    start = lines.index(S3LFS_GITIGNORE_START)
    try:
        end = lines.index(S3LFS_GITIGNORE_END, start)
    except ValueError:
        # Malformed block (no end marker): drop only the start marker and
        # keep everything else out of the block rather than swallowing
        # user content into it.
        return lines[:start], [], lines[start + 1 :]
    entries = [line for line in lines[start + 1 : end] if line.strip()]
    return lines[:start], entries, lines[end + 1 :]


def _save_gitignore_block(git_root, before, entries, after):
    """Write .gitignore back with the s3lfs block holding these entries."""
    gitignore = git_root / ".gitignore"
    lines = list(before)
    if entries:
        if lines and lines[-1].strip():
            lines.append("")
        lines.append(S3LFS_GITIGNORE_START)
        lines.extend(entries)
        lines.append(S3LFS_GITIGNORE_END)
    lines.extend(after)
    if not lines and not gitignore.exists():
        return
    gitignore.write_text("\n".join(lines) + ("\n" if lines else ""))


def _add_gitignore_entry(git_root, entry):
    """Add an entry to the s3lfs block in .gitignore. Returns True if added."""
    before, entries, after = _load_gitignore_block(git_root)
    if entry in entries:
        return False
    entries.append(entry)
    _save_gitignore_block(git_root, before, entries, after)
    return True


def _remove_gitignore_entry(git_root, entry_variants):
    """Remove any of these entries from the s3lfs block. Returns True if removed."""
    before, entries, after = _load_gitignore_block(git_root)
    kept = [e for e in entries if e not in entry_variants]
    if kept == entries:
        return False
    _save_gitignore_block(git_root, before, kept, after)
    return True


def _gitignore_entry_for(git_root, manifest_key):
    """Root-anchored .gitignore pattern covering a tracked path spec.

    Manifest keys are relative to the git root, so anchoring with a
    leading slash keeps the pattern from matching same-named paths in
    other directories. Glob specs pass through: both glob.glob (used to
    resolve them at track time) and gitignore give an unanchored '*' the
    same single-level meaning.
    """
    if (git_root / manifest_key).is_dir():
        return f"/{manifest_key}/"
    return f"/{manifest_key}"


def _deindex_tracked_files(git_root, tracked_keys):
    """Drop s3lfs-tracked files from the git index; files stay on disk.

    .gitignore has no effect on files git already tracks, so anything
    committed before it was handed to s3lfs must be removed from the
    index or git will keep versioning it alongside S3.

    Returns the list of paths that were removed.
    """
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        capture_output=True,
        text=True,
        cwd=str(git_root),
    )
    if result.returncode != 0:
        return []
    indexed = {p for p in result.stdout.split("\0") if p}
    offenders = sorted(indexed & set(tracked_keys))
    if not offenders:
        return []
    result = subprocess.run(
        [
            "git",
            "rm",
            "--cached",
            # --force is index-only here: with --cached, git rm never touches
            # the working copy. Without it, git refuses files whose staged
            # content differs from HEAD.
            "--force",
            "--quiet",
            "--pathspec-from-file=-",
            "--pathspec-file-nul",
        ],
        input="\0".join(f":(literal){p}" for p in offenders),
        capture_output=True,
        text=True,
        cwd=str(git_root),
    )
    if result.returncode != 0:
        click.echo(
            "Warning: failed to remove tracked files from the git index:\n"
            f"{result.stderr.strip()}"
        )
        return []
    return offenders


def _protect_tracked_path(git_root, s3lfs, manifest_key):
    """Keep a newly tracked path spec out of git: ignore it and de-index it."""
    s3lfs.load_manifest()
    matched = s3lfs._resolve_manifest_paths(manifest_key)
    if not matched:
        # The spec tracked nothing; leave git configuration alone.
        return

    entry = _gitignore_entry_for(git_root, manifest_key)
    if _add_gitignore_entry(git_root, entry):
        click.echo(f"Added '{entry}' to .gitignore (s3lfs block)")

    removed = _deindex_tracked_files(git_root, matched.keys())
    if removed:
        click.echo(
            f"Removed {len(removed)} s3lfs-tracked file(s) from the git index "
            "(files remain on disk):"
        )
        for path in removed[:10]:
            click.echo(f"  {path}")
        if len(removed) > 10:
            click.echo(f"  ... and {len(removed) - 10} more")
        click.echo("Commit to finalize their removal from git.")


def _manifest_files_at_revision(git_root, revision):
    """Load the manifest's files mapping as of a git revision.

    Returns None when the revision has no manifest or is not available
    locally (e.g. a remote sha that was never fetched).
    """
    names = [get_manifest_path(git_root).name]
    for other in (".s3_manifest.yaml", ".s3_manifest.json"):
        if other not in names:
            names.append(other)
    for name in names:
        result = subprocess.run(
            ["git", "show", f"{revision}:{name}"],
            capture_output=True,
            text=True,
            cwd=str(git_root),
        )
        if result.returncode == 0:
            # yaml.safe_load handles both the YAML and JSON manifest formats
            data = yaml.safe_load(result.stdout) or {}
            return data.get("files") or {}
    return None


S3LFS_HOOK_START = "# >>> s3lfs hook >>>"
S3LFS_HOOK_END = "# <<< s3lfs hook <<<"

_POST_MERGE_BODY = """\
# Auto-checkout s3lfs files after merge
if command -v s3lfs >/dev/null 2>&1; then
    if ! s3lfs checkout --all 2>&1; then
        echo "s3lfs: ERROR: post-merge checkout failed" >&2
        echo "s3lfs: large files may be missing or stale; run 's3lfs checkout --all'" >&2
    fi
fi"""

_POST_CHECKOUT_BODY = """\
# Auto-checkout s3lfs files after checkout
# Only run on branch checkouts ($3 == 1), not file checkouts
if [ "$3" = "1" ] && command -v s3lfs >/dev/null 2>&1; then
    if ! s3lfs checkout --all 2>&1; then
        echo "s3lfs: ERROR: post-checkout checkout failed" >&2
        echo "s3lfs: large files may be missing or stale; run 's3lfs checkout --all'" >&2
    fi
fi"""

_PRE_COMMIT_BODY = """\
# Upload modified s3lfs files and stage the manifest before commit.
#
# Running at commit time (not push time) keeps every commit self-consistent:
# the commit that changes a tracked file is the commit whose manifest points
# at the new hash, and the content is already in S3 by the time that commit
# can be pushed. This hook also blocks committing s3lfs-tracked files into
# git itself.
if command -v s3lfs >/dev/null 2>&1; then
    if ! s3lfs pre-commit 2>&1; then
        echo "s3lfs: pre-commit failed; aborting commit" >&2
        echo "s3lfs: commit anyway with --no-verify if you are sure" >&2
        exit 1
    fi
fi"""

_PRE_PUSH_BODY = """\
# Verify the manifests being pushed reference content that exists in S3.
#
# Uploads happen at commit time (pre-commit hook); this is the last line of
# defense against publishing a manifest whose hashes have no objects behind
# them (commits made with --no-verify, or before hooks were installed).
# If the push proceeded anyway, every collaborator checkout would 404.
if command -v s3lfs >/dev/null 2>&1; then
    zero=0000000000000000000000000000000000000000
    while read local_ref local_sha remote_ref remote_sha; do
        [ "$local_sha" = "$zero" ] && continue
        if [ "$remote_sha" = "$zero" ]; then
            base_args=""
        else
            base_args="--base $remote_sha"
        fi
        if ! s3lfs verify --revision "$local_sha" $base_args 2>&1; then
            echo "s3lfs: content referenced by this push is missing from S3; aborting push" >&2
            echo "s3lfs: run 's3lfs track --modified', commit the manifest, and retry" >&2
            echo "s3lfs: or push with --no-verify to skip this check" >&2
            exit 1
        fi
    done
fi"""

HOOK_SCRIPTS = {
    "post-merge": S3LFS_HOOK_START + "\n" + _POST_MERGE_BODY + "\n" + S3LFS_HOOK_END,
    "post-checkout": (
        S3LFS_HOOK_START + "\n" + _POST_CHECKOUT_BODY + "\n" + S3LFS_HOOK_END
    ),
    "pre-commit": S3LFS_HOOK_START + "\n" + _PRE_COMMIT_BODY + "\n" + S3LFS_HOOK_END,
    "pre-push": S3LFS_HOOK_START + "\n" + _PRE_PUSH_BODY + "\n" + S3LFS_HOOK_END,
}


def _get_hooks_dir(git_root):
    """Get the git hooks directory, respecting core.hooksPath config."""
    import subprocess

    try:
        result = subprocess.run(
            ["git", "config", "core.hooksPath"],
            capture_output=True,
            text=True,
            cwd=str(git_root),
        )
        if result.returncode == 0 and result.stdout.strip():
            hooks_path = Path(result.stdout.strip())
            if not hooks_path.is_absolute():
                hooks_path = git_root / hooks_path
            return hooks_path
    except Exception:
        pass
    return git_root / ".git" / "hooks"


def _install_hook(hooks_dir, hook_name, hook_block):
    """Install an s3lfs hook block into a git hook file."""
    hook_path = hooks_dir / hook_name
    shebang = "#!/bin/sh\n"

    if hook_path.exists():
        content = hook_path.read_text()
        if S3LFS_HOOK_START in content:
            import re

            pattern = re.escape(S3LFS_HOOK_START) + r".*?" + re.escape(S3LFS_HOOK_END)
            content = re.sub(pattern, hook_block, content, flags=re.DOTALL)
        else:
            content = content.rstrip("\n") + "\n\n" + hook_block + "\n"
    else:
        content = shebang + "\n" + hook_block + "\n"

    _write_hook_atomically(hook_path, content)
    return hook_path


def _write_hook_atomically(hook_path, content):
    """Replace a hook file in one step.

    write_text truncates before writing, so an interrupt part-way through
    leaves a user's pre-existing hook truncated and executable. Write to a
    sibling temp file and rename, which is atomic within a directory.
    """
    from uuid import uuid4

    temp_path = hook_path.with_name(f"{hook_path.name}.{uuid4().hex}.tmp")
    try:
        temp_path.write_text(content)
        temp_path.chmod(0o755)
        temp_path.replace(hook_path)
    except Exception:
        if temp_path.exists():
            temp_path.unlink()
        raise


def _uninstall_hook(hooks_dir, hook_name):
    """Remove the s3lfs hook block from a git hook file."""
    import re

    hook_path = hooks_dir / hook_name
    if not hook_path.exists():
        return False

    content = hook_path.read_text()
    if S3LFS_HOOK_START not in content:
        return False

    pattern = (
        r"\n*"
        + re.escape(S3LFS_HOOK_START)
        + r".*?"
        + re.escape(S3LFS_HOOK_END)
        + r"\n*"
    )
    new_content = re.sub(pattern, "\n", content, flags=re.DOTALL)

    stripped = new_content.strip()
    if stripped == "" or stripped == "#!/bin/sh":
        hook_path.unlink()
    else:
        _write_hook_atomically(hook_path, new_content)

    return True


@click.command()
def install():
    """Install git hooks for automatic s3lfs checkout and track."""
    git_root = find_git_root()
    if not git_root:
        click.echo("Error: Not in a git repository")
        raise click.Abort()

    manifest_path = get_manifest_path(git_root)
    if not manifest_path.exists():
        click.echo("Error: S3LFS not initialized. Run 's3lfs init' first.")
        raise click.Abort()

    hooks_dir = _get_hooks_dir(git_root)
    hooks_dir.mkdir(parents=True, exist_ok=True)

    for hook_name, hook_block in HOOK_SCRIPTS.items():
        hook_path = _install_hook(hooks_dir, hook_name, hook_block)
        click.echo(f"  Installed {hook_name} hook -> {hook_path}")

    click.echo()
    click.echo("s3lfs hooks installed. Your git workflow now automatically:")
    click.echo(
        "  - Uploads modified files and stages the manifest on commit (pre-commit)"
    )
    click.echo("  - Blocks committing s3lfs-tracked files into git (pre-commit)")
    click.echo(
        "  - Downloads tracked files after checkout/merge (post-checkout, post-merge)"
    )
    click.echo("  - Verifies pushed manifests reference uploaded content (pre-push)")
    click.echo()
    click.echo("Run 's3lfs uninstall' to remove hooks.")


@click.command()
def uninstall():
    """Remove s3lfs git hooks."""
    git_root = find_git_root()
    if not git_root:
        click.echo("Error: Not in a git repository")
        raise click.Abort()

    hooks_dir = _get_hooks_dir(git_root)
    removed = False
    for hook_name in HOOK_SCRIPTS:
        if _uninstall_hook(hooks_dir, hook_name):
            click.echo(f"  Removed {hook_name} hook")
            removed = True

    if removed:
        click.echo()
        click.echo("s3lfs hooks removed.")
    else:
        click.echo("No s3lfs hooks found.")


@click.command()
@click.option(
    "--revision",
    default=None,
    help="Git revision whose manifest to verify (default: working tree manifest)",
)
@click.option(
    "--base",
    default=None,
    help="Only verify entries added or changed relative to this revision's manifest",
)
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
@click.option(
    "--use-acceleration", is_flag=True, help="Enable S3 Transfer Acceleration"
)
@click.option(
    "--endpoint-url",
    default=None,
    help="Custom S3 endpoint URL for S3-compatible storage",
)
@click.option(
    "--workers",
    type=int,
    default=None,
    help="Number of parallel workers (default: auto-detected from CPU count)",
)
def verify(revision, base, no_sign_request, use_acceleration, endpoint_url, workers):
    """Verify that manifest entries have content behind them in S3.

    Exits non-zero if any entry references content that was never uploaded.
    Used by the pre-push hook to stop a push that would publish a manifest
    whose hashes have no objects behind them.
    """
    git_root, manifest_path, path_resolver = _setup_s3lfs_command()

    s3lfs = _make_s3lfs(
        git_root,
        manifest_path,
        no_sign_request=no_sign_request,
        use_acceleration=use_acceleration,
        endpoint_url=endpoint_url,
        workers=workers,
    )

    if revision:
        files = _manifest_files_at_revision(git_root, revision)
        if files is None:
            click.echo(f"No manifest at revision {revision}; nothing to verify.")
            return
    else:
        files = dict(s3lfs.manifest.get("files", {}))

    if base:
        base_files = _manifest_files_at_revision(git_root, base) or {}
        files = {k: h for k, h in files.items() if base_files.get(k) != h}

    if not files:
        click.echo("No manifest entries to verify.")
        return

    noun = "entry" if len(files) == 1 else "entries"
    click.echo(f"Verifying {len(files)} manifest {noun} against S3...")
    missing = s3lfs.find_missing_assets(files)
    if missing:
        click.echo("Content missing from S3:")
        for key, file_hash in sorted(missing):
            click.echo(f"  {key} ({file_hash[:12]})")
        click.echo(
            f"Error: {len(missing)} of {len(files)} {noun} have no content in S3."
        )
        raise SystemExit(1)
    click.echo(f"All {len(files)} {noun} verified present in S3.")


@click.command("pre-commit")
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
@click.option(
    "--use-acceleration", is_flag=True, help="Enable S3 Transfer Acceleration"
)
@click.option(
    "--endpoint-url",
    default=None,
    help="Custom S3 endpoint URL for S3-compatible storage",
)
def pre_commit(no_sign_request, use_acceleration, endpoint_url):
    """Prepare a commit (run by the pre-commit git hook).

    Blocks the commit if any staged file is tracked by s3lfs, uploads
    modified tracked content, and stages the updated manifest so the
    commit's manifest matches what is in S3.
    """
    git_root, manifest_path, path_resolver = _setup_s3lfs_command()

    s3lfs = _make_s3lfs(
        git_root,
        manifest_path,
        no_sign_request=no_sign_request,
        use_acceleration=use_acceleration,
        endpoint_url=endpoint_url,
    )
    tracked = set(s3lfs.manifest.get("files", {}))

    result = subprocess.run(
        ["git", "diff", "--cached", "--name-only", "--diff-filter=d", "-z"],
        capture_output=True,
        text=True,
        cwd=str(git_root),
    )
    staged = {p for p in result.stdout.split("\0") if p}
    offenders = sorted(staged & tracked)
    if offenders:
        click.echo(
            "Error: these files are tracked by s3lfs but staged for commit in git:"
        )
        for path in offenders:
            click.echo(f"  {path}")
        click.echo("Unstage them (the files stay on disk):")
        for path in offenders:
            click.echo(f"  git rm --cached '{path}'")
        raise SystemExit(1)

    if tracked:
        s3lfs.track_modified_files_cached(silence=True)

    subprocess.run(
        ["git", "add", "--", manifest_path.name],
        cwd=str(git_root),
    )


cli.add_command(init)
cli.add_command(track)
cli.add_command(checkout)
cli.add_command(ls)
cli.add_command(remove)
cli.add_command(cleanup)
cli.add_command(migrate)
cli.add_command(migrate_from_lfs)
cli.add_command(install)
cli.add_command(uninstall)
cli.add_command(verify)
cli.add_command(pre_commit)


def main():
    cli()


if __name__ == "__main__":
    main()
