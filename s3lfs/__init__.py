# s3lfs/__init__.py
"""
s3lfs - A Python-based version control system for large assets using Amazon S3.

This package provides Git LFS-like functionality using S3 for storage,
with support for file tracking, parallel operations, encryption, and
automatic cleanup of unused assets.
"""

from . import metrics
from .core import S3LFS

__version__ = "0.1.0"
__all__ = ["S3LFS", "metrics", "__version__"]
