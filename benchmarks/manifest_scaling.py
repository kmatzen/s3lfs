"""Measure what manifest size costs, and what sharding buys.

Generates a synthetic manifest of a given size and times the operations
every s3lfs command depends on. Reproduces the table in the README:

    python benchmarks/manifest_scaling.py            # 200,000 entries
    python benchmarks/manifest_scaling.py 50000 25   # entries, shards

With --check, exits non-zero unless sharding still pays for itself. The
assertions compare sharded against single-file timings from the same run,
so they hold on any machine speed; only the ratio matters. CI runs this on
every pull request.
"""

import hashlib
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from s3lfs.core import S3LFS, USING_LIBYAML, yaml_dump  # noqa: E402


def timed(fn):
    start = time.perf_counter()
    result = fn()
    return time.perf_counter() - start, result


def build(root, entries, shards, sharded):
    """Write a manifest of *entries* keys spread over *shards* directories."""
    per = entries // shards
    files: dict = {}
    grouped: dict = {}
    for s in range(shards):
        name = f"dir{s:03d}"
        group = {
            f"{name}/sub{i % 20}/file_{i}.bin": hashlib.sha256(
                f"{s}-{i}".encode()
            ).hexdigest()
            for i in range(per)
        }
        grouped[name] = group
        files.update(group)

    config: dict = {"bucket_name": "b", "repo_prefix": "p"}
    if sharded:
        config["manifest_format"] = "sharded"
        shard_dir = root / ".s3lfs_manifest"
        shard_dir.mkdir(parents=True, exist_ok=True)
        for name, group in grouped.items():
            with open(shard_dir / f"{name}.yaml", "w") as f:
                yaml_dump(group, f, default_flow_style=False, sort_keys=True)
    else:
        config["files"] = files
    with open(root / ".s3_manifest.yaml", "w") as f:
        yaml_dump(config, f, default_flow_style=False, sort_keys=True)
    return files


def bytes_on_disk(root):
    total = (root / ".s3_manifest.yaml").stat().st_size
    shard_dir = root / ".s3lfs_manifest"
    if shard_dir.is_dir():
        total += sum(p.stat().st_size for p in shard_dir.glob("*.yaml"))
    return total


def measure(entries, shards):
    print(
        f"{platform.system()} {platform.machine()}, "
        f"Python {platform.python_version()}, libyaml={USING_LIBYAML}"
    )
    print(f"{entries:,} entries over {shards} directories\n")

    results = {}
    for sharded in (False, True):
        root = Path(tempfile.mkdtemp())
        try:
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            files = build(root, entries, shards, sharded)
            probe = sorted(files)[len(files) // 2]
            label = "sharded" if sharded else "single file"
            size = bytes_on_disk(root) / 1e6

            def make():
                return S3LFS(
                    bucket_name="b",
                    manifest_file=str(root / ".s3_manifest.yaml"),
                    temp_dir=str(root / ".s3lfs_temp"),
                    s3_factory=lambda no_sign: None,
                )

            open_time, s3lfs = timed(make)
            lookup, _ = timed(lambda: s3lfs.manifest["files"][probe])
            full, _ = timed(lambda: len(make().manifest["files"]))

            slice_time = None
            if sharded:
                subset = [f"dir{i:03d}" for i in range(max(1, shards // 20))]
                slice_time, _ = timed(lambda: make().manifest["files"].preload(subset))
            results[label] = {
                "open": open_time,
                "lookup": lookup,
                "full": full,
                "slice": slice_time,
            }

            print(f"  {label:12}  {size:6.1f} MB on disk")
            print(f"    open manifest        {open_time * 1000:8.1f} ms")
            print(f"    look up one path     {lookup * 1000:8.1f} ms")
            if slice_time is not None:
                print(
                    f"    read {len(subset):>3} of {shards} shards "
                    f"{slice_time * 1000:8.1f} ms"
                )
            print(f"    read every entry     {full * 1000:8.1f} ms\n")
        finally:
            shutil.rmtree(root, ignore_errors=True)
    return results


def check(results):
    """Ratio-based regression guard: sharding must still pay for itself.

    Thresholds sit far below the measured ratios (~55x open, ~25x slice)
    so runner noise cannot trip them; tripping means the lazy-loading
    machinery actually broke.
    """
    single, sharded = results["single file"], results["sharded"]
    checks = [
        ("sharded open vs full parse", single["open"] / sharded["open"], 5.0),
        ("partial read vs full read", single["full"] / sharded["slice"], 3.0),
    ]
    failed = False
    for name, ratio, floor in checks:
        verdict = "ok" if ratio >= floor else "REGRESSION"
        failed |= ratio < floor
        print(f"  {name:28} {ratio:6.1f}x (floor {floor}x)  {verdict}")
    return not failed


if __name__ == "__main__":
    argv = [a for a in sys.argv[1:] if a != "--check"]
    n = int(argv[0]) if argv else 200_000
    k = int(argv[1]) if len(argv) > 1 else 100
    results = measure(n, k)
    if "--check" in sys.argv and not check(results):
        raise SystemExit(1)
