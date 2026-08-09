"""Compare s3lfs against a raw S3 transfer tool on the same data and bucket.

s3lfs and s5cmd do different amounts of work, so a single "which is
faster" number would be misleading. This measures the scenarios that
actually occur in a workflow and reports each separately:

  cold upload        first upload of a fresh dataset
  no-op upload       running it again with nothing changed
  incremental upload one file of many changed
  cold download      fetching into an empty directory
  no-op download     fetching when the files are already correct

s3lfs hashes every file, compresses it, and maintains a manifest; s5cmd
moves bytes. Expect s5cmd to win the cold cases on incompressible data
and s3lfs to win the repeat cases, where knowing what is already correct
beats transferring it again. Both numbers are worth publishing.

Usage:
  python benchmarks/transfer_comparison.py \
      --bucket my-bucket [--endpoint-url URL] [--files 200] [--size-kb 512] \
      [--compressible] [--s5cmd ./s5cmd]
"""

import argparse
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def human(seconds):
    return f"{seconds:8.2f}s"


def run(cmd, cwd=None, env=None):
    return subprocess.run(
        cmd,
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        shell=isinstance(cmd, str),
    )


def timed(label, fn):
    start = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - start
    return label, elapsed, result


def make_dataset(root, count, size_kb, compressible):
    """Write *count* files of *size_kb* KB each."""
    data_dir = root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    payload_size = size_kb * 1024
    for i in range(count):
        directory = data_dir / f"part{i % 10:02d}"
        directory.mkdir(exist_ok=True)
        if compressible:
            # Text-like: compresses well, which is where s3lfs can send
            # fewer bytes than a raw copy.
            chunk = (f"row {i} " + "lorem ipsum dolor sit amet " * 8 + "\n").encode()
            body = (chunk * (payload_size // len(chunk) + 1))[:payload_size]
        else:
            body = random.randbytes(payload_size)
        (directory / f"file_{i:05d}.bin").write_bytes(body)
    total = sum(p.stat().st_size for p in data_dir.rglob("*") if p.is_file())
    return data_dir, total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--endpoint-url", default=None)
    ap.add_argument("--files", type=int, default=200)
    ap.add_argument("--size-kb", type=int, default=512)
    ap.add_argument("--compressible", action="store_true")
    ap.add_argument("--s5cmd", default="s5cmd")
    ap.add_argument("--prefix", default="bench")
    args = ap.parse_args()

    env = dict(os.environ)
    endpoint = ["--endpoint-url", args.endpoint_url] if args.endpoint_url else []
    s5_endpoint = ["--endpoint-url", args.endpoint_url] if args.endpoint_url else []

    root = Path(tempfile.mkdtemp(prefix="s3lfs-bench-"))
    results = []
    try:
        run(["git", "init", "-q", str(root)])
        run(["git", "-C", str(root), "config", "user.email", "b@b"])
        run(["git", "-C", str(root), "config", "user.name", "b"])
        data_dir, total = make_dataset(
            root, args.files, args.size_kb, args.compressible
        )
        kind = "compressible" if args.compressible else "incompressible"
        print(
            f"{args.files} files x {args.size_kb} KB = {total / 1e6:.1f} MB "
            f"({kind})\n"
        )

        s3lfs = [sys.executable, "-m", "s3lfs.cli"]
        init = run(
            s3lfs + ["init", args.bucket, args.prefix] + endpoint, cwd=root, env=env
        )
        if init.returncode != 0:
            print("s3lfs init failed:\n" + init.stdout + init.stderr)
            return 1

        # --- upload -----------------------------------------------------
        results.append(
            timed(
                "s3lfs   cold upload",
                lambda: run(s3lfs + ["track", "data"], cwd=root, env=env),
            )
        )
        results.append(
            timed(
                "s3lfs   no-op upload",
                lambda: run(s3lfs + ["track", "--modified"], cwd=root, env=env),
            )
        )
        dest = f"s3://{args.bucket}/{args.prefix}-s5cmd/"
        results.append(
            timed(
                "s5cmd   cold upload",
                lambda: run(
                    [args.s5cmd] + s5_endpoint + ["cp", "data/*", dest],
                    cwd=root,
                    env=env,
                ),
            )
        )
        results.append(
            timed(
                "s5cmd   no-op upload (sync)",
                lambda: run(
                    [args.s5cmd] + s5_endpoint + ["sync", "data/", dest],
                    cwd=root,
                    env=env,
                ),
            )
        )

        # --- one file changed -------------------------------------------
        victim = next(data_dir.rglob("*.bin"))
        victim.write_bytes(victim.read_bytes() + b"changed")
        results.append(
            timed(
                "s3lfs   incremental upload",
                lambda: run(s3lfs + ["track", "--modified"], cwd=root, env=env),
            )
        )
        results.append(
            timed(
                "s5cmd   incremental upload (sync)",
                lambda: run(
                    [args.s5cmd] + s5_endpoint + ["sync", "data/", dest],
                    cwd=root,
                    env=env,
                ),
            )
        )

        # --- download ---------------------------------------------------
        shutil.rmtree(data_dir)
        results.append(
            timed(
                "s3lfs   cold download",
                lambda: run(s3lfs + ["checkout", "--all"], cwd=root, env=env),
            )
        )
        results.append(
            timed(
                "s3lfs   no-op download",
                lambda: run(s3lfs + ["checkout", "--all"], cwd=root, env=env),
            )
        )
        out = root / "s5out"
        out.mkdir(exist_ok=True)
        results.append(
            timed(
                "s5cmd   cold download",
                lambda: run(
                    [args.s5cmd] + s5_endpoint + ["cp", dest + "*", str(out)],
                    cwd=root,
                    env=env,
                ),
            )
        )
        results.append(
            timed(
                "s5cmd   no-op download (sync)",
                lambda: run(
                    [args.s5cmd] + s5_endpoint + ["sync", dest + "*", str(out) + "/"],
                    cwd=root,
                    env=env,
                ),
            )
        )

        print(f"{'scenario':38} {'wall clock':>10}   status")
        for label, elapsed, proc in results:
            status = "ok" if proc.returncode == 0 else f"FAILED rc={proc.returncode}"
            print(f"{label:38} {human(elapsed)}   {status}")
            if proc.returncode != 0:
                tail = (proc.stdout + proc.stderr).strip().splitlines()[-2:]
                for line in tail:
                    print(f"    | {line[:110]}")
    finally:
        shutil.rmtree(root, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
