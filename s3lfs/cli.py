import click

from s3lfs.core import S3LFS


@click.group()
def cli():
    """S3-based asset versioning CLI tool."""
    pass


@click.command()
@click.argument("file", required=True)
@click.option("--bucket", help="S3 bucket name (optional if stored in manifest)")
@click.option("--prefix", help="Repo-specific prefix (optional if stored in manifest)")
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
def upload(file, bucket, prefix, no_sign_request):
    """Upload a file to S3 with optional repo-specific prefix"""
    versioner = S3LFS(
        bucket_name=bucket, repo_prefix=prefix, no_sign_request=no_sign_request
    )
    versioner.upload(file)


@click.command()
@click.argument("file", required=True)
@click.option("--bucket", help="S3 bucket name (optional if stored in manifest)")
@click.option("--prefix", help="Repo-specific prefix (optional if stored in manifest)")
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
def download(file, bucket, prefix, no_sign_request):
    """Download a file from S3 by hash"""
    versioner = S3LFS(
        bucket_name=bucket, repo_prefix=prefix, no_sign_request=no_sign_request
    )
    versioner.download(file)


@click.command()
@click.option("--bucket", help="S3 bucket name")
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
def track_modified(bucket, no_sign_request):
    """Track and upload modified files detected in Git"""
    versioner = S3LFS(bucket_name=bucket, no_sign_request=no_sign_request)
    versioner.track_modified_files()


@click.command()
@click.option("--bucket", help="S3 bucket name")
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
@click.option("--verbose", is_flag=True, help="Print download progress")
def download_all(bucket, no_sign_request, verbose):
    """Download all files listed in the manifest"""
    versioner = S3LFS(bucket_name=bucket, no_sign_request=no_sign_request)
    versioner.parallel_download_all(silence=not verbose)


@click.command()
@click.argument("directory", required=True)
@click.option("--bucket", help="S3 bucket name (optional if stored in manifest)")
@click.option("--prefix", help="Repo-specific prefix (optional if stored in manifest)")
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
def sparse_checkout(directory, prefix, bucket, no_sign_request):
    """Download all files under a given subtree (prefix)"""
    versioner = S3LFS(
        bucket_name=bucket, repo_prefix=prefix, no_sign_request=no_sign_request
    )
    versioner.sparse_checkout(directory)


@click.command()
@click.option("--bucket", help="S3 bucket name")
@click.option("--prefix", help="Repo-specific prefix")
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
def git_setup(bucket, prefix, no_sign_request):
    """Set up Git filters for automatic S3 integration"""
    versioner = S3LFS(
        bucket_name=bucket, repo_prefix=prefix, no_sign_request=no_sign_request
    )
    versioner.integrate_with_git()


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


@click.command()
@click.argument("directory", required=True)
@click.option("--bucket", help="S3 bucket name (optional if stored in manifest)")
@click.option("--prefix", help="Repo-specific prefix (optional if stored in manifest)")
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
def track_subtree(directory, bucket, prefix, no_sign_request):
    """Recursively track and upload all files in a directory"""
    versioner = S3LFS(
        bucket_name=bucket, repo_prefix=prefix, no_sign_request=no_sign_request
    )
    versioner.track_subtree(directory)


@click.command()
@click.argument("file", required=True)
@click.option("--purge-from-s3", is_flag=True, help="Purge file in S3 immediately")
@click.option("--bucket", help="S3 bucket name (optional if stored in manifest)")
@click.option("--prefix", help="Repo-specific prefix (optional if stored in manifest)")
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
def remove(file, purge_from_s3, bucket, prefix, no_sign_request):
    """Remove a file from tracking."""
    versioner = S3LFS(
        bucket_name=bucket, repo_prefix=prefix, no_sign_request=no_sign_request
    )
    versioner.remove_file(file, keep_in_s3=not purge_from_s3)


@click.command()
@click.option("--force", is_flag=True, help="Skip confirmation for cleanup")
@click.option("--bucket", help="S3 bucket name (optional if stored in manifest)")
@click.option("--prefix", help="Repo-specific prefix (optional if stored in manifest)")
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
def cleanup(force, bucket, prefix, no_sign_request):
    """Cleanup unreferenced files in S3."""
    versioner = S3LFS(
        bucket_name=bucket, repo_prefix=prefix, no_sign_request=no_sign_request
    )
    versioner.cleanup_s3(force=force)


@click.command()
@click.argument("directory", required=True)
@click.option("--purge-from-s3", is_flag=True, help="Purge file in S3 immediately")
@click.option("--bucket", help="S3 bucket name (optional if stored in manifest)")
@click.option("--prefix", help="Repo-specific prefix (optional if stored in manifest)")
@click.option("--no-sign-request", is_flag=True, help="Use unsigned S3 requests")
def remove_subtree(directory, purge_from_s3, bucket, prefix, no_sign_request):
    """Remove a tracked subtree from tracking."""
    versioner = S3LFS(
        bucket_name=bucket, repo_prefix=prefix, no_sign_request=no_sign_request
    )
    versioner.remove_subtree(directory, keep_in_s3=not purge_from_s3)


cli.add_command(remove_subtree)
cli.add_command(remove)
cli.add_command(cleanup)
cli.add_command(track_subtree)
cli.add_command(init)
cli.add_command(upload)
cli.add_command(download)
cli.add_command(cleanup)
cli.add_command(track_modified)
cli.add_command(download_all)
cli.add_command(sparse_checkout)
cli.add_command(git_setup)


def main():
    cli()


if __name__ == "__main__":
    main()
