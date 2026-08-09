# s3lfs/__init__.py
"""
s3lfs - A Python-based version control system for large assets using Amazon S3.

This package provides Git LFS-like functionality using S3 for storage,
with support for file tracking, parallel operations, encryption, and
automatic cleanup of unused assets.
"""

__all__ = ["S3LFS", "metrics", "__version__"]


def __getattr__(name):
    # Resolved lazily (PEP 562): importing the package must stay cheap.
    # Eagerly importing core pulls hashlib/yaml/tqdm, and reading package
    # metadata costs ~37ms -- costs every `s3lfs --help` would pay.
    if name == "S3LFS":
        from .core import S3LFS

        return S3LFS
    if name == "metrics":
        # importlib, not `from . import`: the from-import form consults
        # this very __getattr__ while the submodule is mid-import and
        # recurses forever.
        import importlib

        return importlib.import_module("s3lfs.metrics")
    if name == "__version__":
        try:
            from importlib.metadata import PackageNotFoundError, version

            return version("s3lfs")
        except PackageNotFoundError:  # running from a source tree
            return "0.0.0+unknown"
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
