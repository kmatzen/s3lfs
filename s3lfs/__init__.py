# s3lfs/__init__.py
"""
s3lfs - A Python-based version control system for large assets using Amazon S3.

This package provides Git LFS-like functionality using S3 for storage,
with support for file tracking, parallel operations, encryption, and
automatic cleanup of unused assets.
"""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _installed_version

from . import metrics
from .core import S3LFS

try:
    # Read the version from installed package metadata so it cannot drift
    # from pyproject.toml -- there is only one place to bump.
    __version__ = _installed_version("s3lfs")
except PackageNotFoundError:  # pragma: no cover - running from a source tree
    __version__ = "0.0.0+unknown"

__all__ = ["S3LFS", "metrics", "__version__"]
