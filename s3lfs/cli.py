import json
from pathlib import Path

import click
import yaml

from s3lfs import metrics
from s3lfs.core import S3LFS
from s3lfs.path_resolver import PathResolver
from s3lfs.utils import find_git_root


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
def init(bucket, prefix, no_sign_request, use_acceleration):
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
    path, no_sign_request, use_acceleration, verbose, modified, enable_metrics_flag
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

    s3lfs = S3LFS(
        no_sign_request=no_sign_request,
        manifest_file=str(manifest_path),
        use_acceleration=use_acceleration,
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
    path, no_sign_request, use_acceleration, verbose, all, enable_metrics_flag
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

    s3lfs = S3LFS(
        no_sign_request=no_sign_request,
        manifest_file=str(manifest_path),
        use_acceleration=use_acceleration,
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
def ls(path, no_sign_request, use_acceleration, verbose, all, git_finder_func=None):
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

    s3lfs = S3LFS(
        no_sign_request=no_sign_request,
        manifest_file=str(manifest_path),
        use_acceleration=use_acceleration,
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
def remove(path, purge_from_s3, no_sign_request, use_acceleration):
    """Remove files or directories from tracking. Supports glob patterns."""
    # Common setup: find git root, check manifest, resolve path
    git_root, manifest_path, path_resolver, manifest_key = _setup_s3lfs_command(
        cli_path=path
    )

    versioner = S3LFS(
        no_sign_request=no_sign_request,
        manifest_file=str(manifest_path),
        use_acceleration=use_acceleration,
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
def cleanup(force, no_sign_request, use_acceleration):
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

    versioner = S3LFS(
        no_sign_request=no_sign_request,
        manifest_file=str(manifest_path),
        use_acceleration=use_acceleration,
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


cli.add_command(init)
cli.add_command(track)
cli.add_command(checkout)
cli.add_command(ls)
cli.add_command(remove)
cli.add_command(cleanup)
cli.add_command(migrate)


def main():
    cli()


if __name__ == "__main__":
    main()
