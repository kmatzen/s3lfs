"""
Path resolution abstraction layer for s3lfs.

This module provides a single source of truth for all path operations,
eliminating the complexity and bugs that arise from mixing relative and
absolute paths across different contexts (CLI, manifest, filesystem).

Design principles:
1. Manifest keys are ALWAYS relative to git root, using POSIX format
2. Filesystem operations use absolute paths
3. CLI input is resolved considering current working directory
4. All path conversions go through this single class
"""

import os
from pathlib import Path
from typing import Optional, Union


class PathResolver:
    """
    Single source of truth for all path operations in s3lfs.

    This class handles conversions between three path representations:
    1. CLI input: What the user types (may be relative to CWD)
    2. Manifest key: Relative to git root, POSIX format (storage format)
    3. Filesystem path: Absolute path for actual file operations
    """

    def __init__(self, git_root: Path):
        """
        Initialize the path resolver.

        :param git_root: Absolute path to the git repository root
        """
        git_root = Path(git_root)
        if not git_root.is_absolute():
            # Check before resolve() to catch relative paths
            raise ValueError(f"git_root must be absolute: {git_root}")
        self.git_root = git_root.resolve()

    def to_manifest_key(self, path: Union[str, Path]) -> str:
        """
        Convert any path to a manifest key (relative to git root, POSIX format).

        This is the canonical storage format for paths in the manifest.

        :param path: Any path (relative, absolute, or Path object)
        :return: Path relative to git root as POSIX string
        :raises ValueError: If path is outside git repository

        Examples:
            >>> resolver = PathResolver(Path("/repo"))
            >>> resolver.to_manifest_key("/repo/data/file.txt")
            'data/file.txt'
            >>> resolver.to_manifest_key("data/file.txt")  # from /repo
            'data/file.txt'
        """
        path = Path(path)

        # Convert to absolute path if not already
        if not path.is_absolute():
            # Relative paths are resolved relative to git root
            # This is important: we assume paths are relative to git root by default
            path = (self.git_root / path).resolve()
        else:
            path = path.resolve()

        # Make relative to git root
        try:
            rel_path = path.relative_to(self.git_root)
            return str(rel_path.as_posix())
        except ValueError:
            # Path is outside git root - this is an error condition
            raise ValueError(
                f"Path '{path}' is outside git repository '{self.git_root}'"
            )

    def to_filesystem_path(self, manifest_key: str) -> Path:
        """
        Convert a manifest key to an absolute filesystem path.

        :param manifest_key: Path relative to git root (POSIX format)
        :return: Absolute Path object for filesystem operations

        Examples:
            >>> resolver = PathResolver(Path("/repo"))
            >>> resolver.to_filesystem_path("data/file.txt")
            Path('/repo/data/file.txt')
        """
        if os.path.isabs(manifest_key):
            raise ValueError(
                f"Manifest key must be relative, got absolute path: {manifest_key}"
            )

        if manifest_key.startswith(".."):
            raise ValueError(f"Manifest key cannot escape git root: {manifest_key}")

        # Convert POSIX path to platform-specific path and make absolute
        return (self.git_root / manifest_key).resolve()

    def from_cli_input(
        self, cli_path: str, cwd: Optional[Path] = None, allow_absolute: bool = False
    ) -> str:
        """
        Convert CLI input to a manifest key, considering current working directory.

        This handles the complex logic of:
        - User is in a subdirectory
        - User provides relative path (relative to their CWD)
        - User provides path that already includes subdirectory prefix
        - User provides absolute path (if allowed)

        :param cli_path: Path as provided by user on command line
        :param cwd: Current working directory (defaults to Path.cwd())
        :param allow_absolute: Whether to allow absolute paths
        :return: Manifest key (relative to git root)

        Examples:
            # User in /repo/subdir, types "file.txt"
            >>> resolver.from_cli_input("file.txt", Path("/repo/subdir"))
            'subdir/file.txt'

            # User in /repo/subdir, types "subdir/file.txt" (full path from root)
            >>> resolver.from_cli_input("subdir/file.txt", Path("/repo/subdir"))
            'subdir/file.txt'

            # User at /repo, types "subdir/file.txt"
            >>> resolver.from_cli_input("subdir/file.txt", Path("/repo"))
            'subdir/file.txt'
        """
        if cwd is None:
            cwd = Path.cwd()
        else:
            cwd = Path(cwd).resolve()

        cli_path_obj = Path(cli_path)

        # Handle absolute paths
        if cli_path_obj.is_absolute():
            if not allow_absolute:
                raise ValueError(f"Absolute paths not allowed: {cli_path}")
            return self.to_manifest_key(cli_path_obj)

        # Get CWD relative to git root
        try:
            cwd_relative = cwd.relative_to(self.git_root)
        except ValueError:
            # CWD is outside git root - treat cli_path as relative to git root
            return self.to_manifest_key(cli_path)

        # If we're at git root, path is already correct
        if cwd_relative == Path("."):
            return self.to_manifest_key(cli_path)

        # We're in a subdirectory
        # Check if cli_path already includes the subdirectory prefix
        cwd_relative_str = str(cwd_relative.as_posix())

        if cli_path.startswith(cwd_relative_str + "/") or cli_path == cwd_relative_str:
            # Path already includes subdirectory prefix, use as-is
            return self.to_manifest_key(cli_path)

        # Path is relative to CWD, prepend the subdirectory
        full_path = cwd / cli_path
        return self.to_manifest_key(full_path)

    def validate_manifest_key(self, key: str) -> bool:
        """
        Validate that a string is a valid manifest key.

        :param key: String to validate
        :return: True if valid, False otherwise

        A valid manifest key:
        - Is a relative path (not absolute)
        - Does not escape the git root (no ..)
        - Uses POSIX separators (forward slashes)
        - Does not start or end with /
        """
        if not key:
            return False

        if os.path.isabs(key):
            return False

        if key.startswith("..") or "/.." in key:
            return False

        if "\\" in key:
            return False

        if key.startswith("/") or key.endswith("/"):
            return False

        return True

    def is_within_repo(self, path: Union[str, Path]) -> bool:
        """
        Check if a path is within the git repository.

        :param path: Path to check (can be relative or absolute)
        :return: True if path is within repo, False otherwise
        """
        try:
            path = Path(path)
            if not path.is_absolute():
                path = (self.git_root / path).resolve()
            else:
                path = path.resolve()

            path.relative_to(self.git_root)
            return True
        except (ValueError, OSError):
            return False

    def get_relative_cwd(self, cwd: Optional[Path] = None) -> Path:
        """
        Get current working directory relative to git root.

        :param cwd: Current working directory (defaults to Path.cwd())
        :return: CWD relative to git root, or Path(".") if outside repo
        """
        if cwd is None:
            cwd = Path.cwd()
        else:
            cwd = Path(cwd).resolve()

        try:
            return cwd.relative_to(self.git_root)
        except ValueError:
            return Path(".")

    def __repr__(self) -> str:
        return f"PathResolver(git_root={self.git_root})"
