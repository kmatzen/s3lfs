import json
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
        print(f"✅ Repository initialized with bucket '{bucket}' and prefix '{prefix}'")
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
def track(
    path,
    no_sign_request,
    use_acceleration,
    endpoint_url,
    verbose,
    modified,
    enable_metrics_flag,
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
def checkout(
    path,
    no_sign_request,
    use_acceleration,
    endpoint_url,
    verbose,
    all,
    enable_metrics_flag,
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
def cleanup(force, no_sign_request, use_acceleration, endpoint_url):
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
    click.echo("📋 Migration Plan:")
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
            click.echo("❌ Migration cancelled.")
            return

    # Write YAML manifest
    try:
        with open(yaml_manifest, "w") as f:
            yaml.safe_dump(manifest_data, f, default_flow_style=False, sort_keys=True)
        click.echo(f"✅ Successfully created {yaml_manifest.name}")
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
            click.echo(f"✅ Successfully migrated cache file to {yaml_cache.name}")
        except Exception as e:
            click.echo(f"⚠️  Warning: Failed to migrate cache file: {e}")
            click.echo("   (Cache will be rebuilt automatically)")

    click.echo()
    click.echo("🎉 Migration complete!")
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


cli.add_command(init)
cli.add_command(track)
cli.add_command(checkout)
cli.add_command(ls)
cli.add_command(remove)
cli.add_command(cleanup)
cli.add_command(migrate)
cli.add_command(migrate_from_lfs)


def main():
    cli()


if __name__ == "__main__":
    main()
