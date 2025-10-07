"""
Utility functions for s3lfs.
"""

from pathlib import Path


def find_git_root(start_path=None, git_finder_func=None):
    """
    Find the git repository root by walking up the directory tree.

    Args:
        start_path: Starting path to search from (defaults to current directory)
        git_finder_func: Custom function to find git root (for testing)

    Returns:
        Path object pointing to the git repository root, or None if not found
    """
    if git_finder_func:
        return git_finder_func(start_path)

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
