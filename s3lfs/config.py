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
#
# endpoint_url and workers matter most for teams: without them here, every
# developer on MinIO/R2 has to remember --endpoint-url on every command,
# and putting it in this file used to be dropped silently, sending requests
# to AWS instead.
DEFAULTS = {
    "no_sign_request": False,
    "use_acceleration": False,
    "endpoint_url": None,
    "workers": None,
    "compression": None,
    # AES256 server-side encryption on uploads. MinIO and some other
    # S3-compatibles reject the SSE header unless KMS is configured, which
    # makes every upload fail with NotImplemented; those repos need
    # "encryption: false" here since encryption is otherwise on.
    "encryption": None,
}


def find_config(git_root):
    """Return the Path to .s3lfsconfig if it exists, else None."""
    path = Path(git_root) / CONFIG_FILENAME
    return path if path.is_file() else None


def load_config(git_root):
    """Load .s3lfsconfig from *git_root* and return a dict.

    Missing file → empty dict.

    Unrecognised keys are reported rather than dropped in silence: a
    misspelled or unsupported setting that changes where data goes is
    exactly the kind of thing you want to hear about, not discover later.
    """
    path = find_config(git_root)
    if path is None:
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    unknown = sorted(set(data) - set(DEFAULTS))
    if unknown:
        print(
            f"Warning: ignoring unrecognised key(s) in {CONFIG_FILENAME}: "
            + ", ".join(unknown)
        )

    return {k: data[k] for k in DEFAULTS if k in data}
