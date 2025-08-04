import os
from pathlib import Path

import click

from s3lfs.core import S3LFS


def find_git_root(start_path=None):
    """
    Find the git repository root by walking up the directory tree.

    Args:
        start_path: Starting path to search from (defaults to current directory)

    Returns:
        Path object pointing to the git repository root, or None if not found
    """
    if start_path is None:
        start_path = Path.cwd()
    else:
        start_path = Path(start_path)

    current = start_path.resolve()

    while current != current.parent:
        if (current / ".git").exists():
            return current
        current = current.parent

    return None


def resolve_path_from_git_root(path_arg, git_root):
    """
    Resolve a path argument relative to the git repository root.

    Args:
        path_arg: The path argument from the CLI
        git_root: Path to the git repository root

    Returns:
        Resolved path relative to git root
    """
    if not path_arg:
        return path_arg

    # If path is absolute, return as-is
    if os.path.isabs(path_arg):
        return path_arg

    # Get current working directory relative to git root
    cwd = Path.cwd()
    try:
        relative_cwd = cwd.relative_to(git_root)
    except ValueError:
        # If we're not in the git repository, return the original path
        return path_arg

    # If we're at the git root, just return the path
    if relative_cwd == Path("."):
        return path_arg

    # Prepend the relative path from git root to current directory
    resolved_path = relative_cwd / path_arg

    # Normalize the path (handle . and ..)
    resolved_path = resolved_path.resolve()

    # Convert back to relative path from git root
    try:
        return str(resolved_path.relative_to(git_root))
    except ValueError:
        # If the resolved path is outside the git root, return the original path
        return path_arg


def get_manifest_path(git_root):
    """
    Get the manifest file path relative to the git repository root.

    Args:
        git_root: Path to the git repository root

    Returns:
        Path to the manifest file
    """
    return git_root / ".s3_manifest.json"


@click.group()
def cli():
    """S3-based asset versioning CLI tool."""
    pass


@click.command()
@click.argument("bucket", required=True)
@click.argument("prefix", required=True)
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
def init(bucket, prefix, no_sign_request):
    """Initialize S3LFS with a bucket and repo prefix"""
    # Find git root
    git_root = find_git_root()
    if not git_root:
        click.echo("Error: Not in a git repository")
        raise click.Abort()

    # Create manifest at git root
    manifest_path = get_manifest_path(git_root)
    versioner = S3LFS(
        bucket_name=bucket,
        repo_prefix=prefix,
        no_sign_request=no_sign_request,
        manifest_file=str(manifest_path),
    )
    versioner.initialize_repo()


@cli.command()
@click.argument("path", required=False)
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
@click.option(
    "--verbose", is_flag=True, help="Show detailed progress and upload information"
)
@click.option(
    "--modified", is_flag=True, help="Track only modified files from manifest"
)
def track(path, no_sign_request, verbose, modified):
    """Track files, directories, or globs. Use --modified to track only changed files."""
    # Find git root and resolve path
    git_root = find_git_root()
    if not git_root:
        click.echo("Error: Not in a git repository")
        raise click.Abort()

    manifest_path = get_manifest_path(git_root)
    if not manifest_path.exists():
        click.echo("Error: S3LFS not initialized. Run 's3lfs init' first.")
        raise click.Abort()

    # Resolve path if provided
    resolved_path = resolve_path_from_git_root(path, git_root) if path else None

    s3lfs = S3LFS(no_sign_request=no_sign_request, manifest_file=str(manifest_path))

    if modified:
        # Track only modified files using cached version for better performance
        s3lfs.track_modified_files_cached(silence=not verbose)
    elif resolved_path:
        # Track specific path
        s3lfs.track(resolved_path, silence=not verbose)
    else:
        click.echo("Error: Must provide either a path or use --modified flag")
        raise click.Abort()


@cli.command()
@click.argument("path", required=False)
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
@click.option(
    "--verbose",
    is_flag=True,
    help="Show detailed progress and download size information",
)
@click.option("--all", is_flag=True, help="Checkout all files from manifest")
def checkout(path, no_sign_request, verbose, all):
    """Checkout files, directories, or globs. Use --all to checkout all tracked files."""
    # Find git root and resolve path
    git_root = find_git_root()
    if not git_root:
        click.echo("Error: Not in a git repository")
        raise click.Abort()

    manifest_path = get_manifest_path(git_root)
    if not manifest_path.exists():
        click.echo("Error: S3LFS not initialized. Run 's3lfs init' first.")
        raise click.Abort()

    # Resolve path if provided
    resolved_path = resolve_path_from_git_root(path, git_root) if path else None

    s3lfs = S3LFS(no_sign_request=no_sign_request, manifest_file=str(manifest_path))

    if all:
        # Download all files from manifest
        s3lfs.parallel_download_all(silence=not verbose)
    elif resolved_path:
        # Checkout specific path
        s3lfs.checkout(resolved_path, silence=not verbose)
    else:
        click.echo("Error: Must provide either a path or use --all flag")
        raise click.Abort()


@cli.command()
@click.argument("path", required=False)
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
@click.option(
    "--verbose",
    is_flag=True,
    help="Show detailed information including file sizes and hashes",
)
@click.option("--all", is_flag=True, help="List all tracked files from manifest")
def ls(path, no_sign_request, verbose, all):
    """List tracked files, directories, or globs. If no path is provided, lists all tracked files."""
    # Find git root and resolve path
    git_root = find_git_root()
    if not git_root:
        click.echo("Error: Not in a git repository")
        raise click.Abort()

    manifest_path = get_manifest_path(git_root)
    if not manifest_path.exists():
        click.echo("Error: S3LFS not initialized. Run 's3lfs init' first.")
        raise click.Abort()

    # Get current working directory relative to git root
    cwd = Path.cwd()
    try:
        relative_cwd = cwd.relative_to(git_root)
    except ValueError:
        relative_cwd = Path(".")

    # For ls command, don't resolve the path - use it as-is for filtering
    # But we'll strip the current directory prefix from output
    resolved_path = path

    s3lfs = S3LFS(no_sign_request=no_sign_request, manifest_file=str(manifest_path))

    if all or not resolved_path:
        # List all files from manifest (default behavior when no path provided)
        s3lfs.list_all_files(
            verbose=verbose,
            strip_prefix=str(relative_cwd) if relative_cwd != Path(".") else None,
        )
    else:
        # List specific path (using original path argument, not resolved)
        s3lfs.list_files(
            resolved_path,
            verbose=verbose,
            strip_prefix=str(relative_cwd) if relative_cwd != Path(".") else None,
        )


@click.command()
@click.argument("path", required=True)
@click.option("--purge-from-s3", is_flag=True, help="Purge file in S3 immediately")
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
def remove(path, purge_from_s3, no_sign_request):
    """Remove files or directories from tracking. Supports glob patterns."""
    # Find git root and resolve path
    git_root = find_git_root()
    if not git_root:
        click.echo("Error: Not in a git repository")
        raise click.Abort()

    manifest_path = get_manifest_path(git_root)
    if not manifest_path.exists():
        click.echo("Error: S3LFS not initialized. Run 's3lfs init' first.")
        raise click.Abort()

    # Resolve path
    resolved_path = resolve_path_from_git_root(path, git_root)

    versioner = S3LFS(no_sign_request=no_sign_request, manifest_file=str(manifest_path))

    # Check if path is a directory pattern or single file
    if Path(resolved_path).is_dir() or "*" in resolved_path or "?" in resolved_path:
        # Handle as directory/pattern - use remove_subtree logic
        versioner.remove_subtree(resolved_path, keep_in_s3=not purge_from_s3)
    else:
        # Handle as single file
        versioner.remove_file(resolved_path, keep_in_s3=not purge_from_s3)


@click.command()
@click.option("--force", is_flag=True, help="Skip confirmation for cleanup")
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
def cleanup(force, no_sign_request):
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

    versioner = S3LFS(no_sign_request=no_sign_request, manifest_file=str(manifest_path))
    versioner.cleanup_s3(force=force)


cli.add_command(init)
cli.add_command(track)
cli.add_command(checkout)
cli.add_command(ls)
cli.add_command(remove)
cli.add_command(cleanup)


def main():
    cli()


if __name__ == "__main__":
    main()
