"""
Load per-repo configuration from .s3lfsconfig (YAML).

The config file lives at the git repository root and is intended to be
committed so every team member gets the same defaults.  CLI flags
always override values from the config file.
"""

from pathlib import Path

import yaml

CONFIG_FILENAME = ".s3lfsconfig"

# Keys recognised in .s3lfsconfig and their default values.
DEFAULTS = {
    "no_sign_request": False,
    "use_acceleration": False,
}


def find_config(git_root):
    """Return the Path to .s3lfsconfig if it exists, else None."""
    path = Path(git_root) / CONFIG_FILENAME
    return path if path.is_file() else None


def load_config(git_root):
    """Load .s3lfsconfig from *git_root* and return a dict.

    Missing file → empty dict.  Unknown keys are silently ignored so the
    file stays forward-compatible.
    """
    path = find_config(git_root)
    if path is None:
        return {}
    with open(path, "r") as f:
        data = yaml.safe_load(f) or {}
    # Only keep recognised keys
    return {k: data[k] for k in DEFAULTS if k in data}


def apply_config(config, cli_kwargs):
    """Merge *config* (from file) with *cli_kwargs* (from CLI flags).

    CLI flags take precedence.  For boolean flags Click always supplies a
    value (True/False), so we treat ``False`` the same as "not supplied"
    — only an explicit ``True`` from the CLI overrides the config.

    Returns a new dict suitable for passing to ``S3LFS()``.
    """
    merged = dict(DEFAULTS)
    merged.update(config)
    for key in DEFAULTS:
        cli_val = cli_kwargs.get(key)
        if cli_val:
            merged[key] = cli_val
    return merged
