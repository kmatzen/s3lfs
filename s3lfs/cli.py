import click

from s3lfs.core import S3LFS


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
    versioner = S3LFS(
        bucket_name=bucket, repo_prefix=prefix, no_sign_request=no_sign_request
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
    s3lfs = S3LFS(no_sign_request=no_sign_request)

    if modified:
        # Track only modified files
        s3lfs.track_modified_files(silence=not verbose)
    elif path:
        # Track specific path
        s3lfs.track(path, silence=not verbose)
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
    s3lfs = S3LFS(no_sign_request=no_sign_request)

    if all:
        # Download all files from manifest
        s3lfs.parallel_download_all(silence=not verbose)
    elif path:
        # Checkout specific path
        s3lfs.checkout(path, silence=not verbose)
    else:
        click.echo("Error: Must provide either a path or use --all flag")
        raise click.Abort()


@click.command()
@click.argument("path", required=True)
@click.option("--purge-from-s3", is_flag=True, help="Purge file in S3 immediately")
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
def remove(path, purge_from_s3, no_sign_request):
    """Remove files or directories from tracking. Supports glob patterns."""
    versioner = S3LFS(no_sign_request=no_sign_request)

    # Check if path is a directory pattern or single file
    from pathlib import Path

    if Path(path).is_dir() or "*" in path or "?" in path:
        # Handle as directory/pattern - use remove_subtree logic
        versioner.remove_subtree(path, keep_in_s3=not purge_from_s3)
    else:
        # Handle as single file
        versioner.remove_file(path, keep_in_s3=not purge_from_s3)


@click.command()
@click.option("--force", is_flag=True, help="Skip confirmation for cleanup")
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
def cleanup(force, no_sign_request):
    """Cleanup unreferenced files in S3."""
    versioner = S3LFS(no_sign_request=no_sign_request)
    versioner.cleanup_s3(force=force)


cli.add_command(init)
cli.add_command(track)
cli.add_command(checkout)
cli.add_command(remove)
cli.add_command(cleanup)


def main():
    cli()


if __name__ == "__main__":
    main()
