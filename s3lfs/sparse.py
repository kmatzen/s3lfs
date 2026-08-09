"""Which tracked paths this working copy materializes.

s3lfs derives sparseness from git's own sparse-checkout rules rather than
carrying a second profile format. That keeps one source of truth, lets a
repository express "my slice of the tree" once for both source files and
tracked assets, and inherits git's pattern semantics (cone and non-cone)
instead of reimplementing them.

The rules have to be applied here rather than left to git: s3lfs-tracked
files are gitignored, so they are absent from git's index and git never
considers them when it materializes a sparse working copy.

Matching is delegated to `git sparse-checkout check-rules`, which is git's
own matcher fed a list of paths. When it is unavailable (git older than
2.42) or fails, every path is treated as in-profile -- the behaviour s3lfs
had before sparse support, so a degraded match can only ever download too
much, never too little.
"""

import subprocess
from pathlib import Path

from s3lfs.core import MANIFEST_ROOT_SHARD

# Paths are fed to check-rules in batches so a very large manifest does not
# have to be held in a pipe buffer all at once.
CHECK_RULES_BATCH = 5000


def _git(git_root, *args, **kwargs):
    return subprocess.run(
        ["git", *args],
        capture_output=True,
        cwd=str(git_root),
        **kwargs,
    )


class SparseProfile:
    """The set of manifest paths this working copy wants on disk.

    An inactive profile contains everything, which is the non-sparse case
    and the default.
    """

    def __init__(self, git_root, active=False, degraded_reason=None):
        self.git_root = Path(git_root)
        self.active = active
        # Set when sparse checkout is on but s3lfs could not apply it, so
        # callers can say so once instead of silently ignoring the rules.
        self.degraded_reason = degraded_reason

    @classmethod
    def detect(cls, git_root):
        """Read the working copy's sparse-checkout state."""
        result = _git(git_root, "config", "--bool", "core.sparseCheckout", text=True)
        if result.returncode != 0 or result.stdout.strip() != "true":
            return cls(git_root, active=False)

        # Probe the matcher once so a git too old to have check-rules is
        # reported clearly rather than failing on every later call.
        probe = _git(
            git_root,
            "sparse-checkout",
            "check-rules",
            "-z",
            input=b"",
        )
        if probe.returncode != 0:
            return cls(
                git_root,
                active=False,
                degraded_reason=(
                    "sparse checkout is enabled but 'git sparse-checkout "
                    "check-rules' is unavailable (needs git 2.42+); s3lfs is "
                    "treating every tracked file as in-profile"
                ),
            )
        return cls(git_root, active=True)

    def select(self, keys):
        """Return the subset of *keys* this working copy materializes.

        Order is not preserved; callers work from manifest dicts.
        """
        keys = list(keys)
        if not self.active or not keys:
            return set(keys)

        selected: set = set()
        for start in range(0, len(keys), CHECK_RULES_BATCH):
            batch = keys[start : start + CHECK_RULES_BATCH]
            result = _git(
                self.git_root,
                "sparse-checkout",
                "check-rules",
                "-z",
                input=("\0".join(batch) + "\0").encode(),
            )
            if result.returncode != 0:
                # Mid-flight failure: fall back to everything rather than
                # silently dropping paths the user expects to have.
                self.degraded_reason = (
                    "git sparse-checkout check-rules failed; s3lfs is treating "
                    "every tracked file as in-profile"
                )
                self.active = False
                return set(keys)
            selected.update(part for part in result.stdout.decode().split("\0") if part)
        return selected

    def partition(self, files):
        """Split a manifest mapping into (in_profile, out_of_profile) dicts."""
        if not self.active:
            return dict(files), {}
        inside = self.select(files.keys())
        return (
            {k: v for k, v in files.items() if k in inside},
            {k: v for k, v in files.items() if k not in inside},
        )

    def contains(self, key):
        """Is a single path in the profile?"""
        return not self.active or key in self.select([key])

    def shards(self):
        """Top-level directories this profile can materialize, or None.

        Manifest shards are named for the first path component, so this is
        the set of shards a sparse working copy could possibly need. None
        means "cannot tell" -- an inactive profile, or non-cone patterns
        whose reach is not a simple prefix -- and the caller must then
        consider every shard.
        """
        if not self.active:
            return None
        result = _git(self.git_root, "sparse-checkout", "list", text=True)
        if result.returncode != 0:
            return None
        # Cone mode lists directories. Anything containing a glob or a
        # negation is not a plain prefix, so bail out rather than guess.
        shards = {MANIFEST_ROOT_SHARD}
        for line in result.stdout.splitlines():
            pattern = line.strip()
            if not pattern:
                continue
            if any(ch in pattern for ch in "*?[!"):
                return None
            shards.add(pattern.strip("/").split("/", 1)[0])
        return shards

    def patterns(self):
        """The configured sparse patterns, for display."""
        result = _git(self.git_root, "sparse-checkout", "list", text=True)
        if result.returncode != 0:
            return []
        return [line for line in result.stdout.splitlines() if line.strip()]
