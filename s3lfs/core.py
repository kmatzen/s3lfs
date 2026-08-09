import contextlib
import fnmatch
import functools
import glob
import gzip
import hashlib
import json
import mmap
import os
import random
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections.abc import MutableMapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Optional, Union
from uuid import uuid4

import portalocker
import yaml
from tqdm import tqdm

from s3lfs import metrics
from s3lfs.path_resolver import PathResolver
from s3lfs.utils import find_git_root

# boto3 and botocore are imported lazily: they cost ~140ms to import, which
# would be paid by every invocation including --help, --version, and the
# git hooks that usually have nothing to do. Anything needing an exception
# class or client resolves it at call time via the helpers below.


# Constants
DEFAULT_CHUNK_SIZE = 5 * 1024 * 1024 * 1024  # 5 GB
DEFAULT_BUFFER_SIZE = 1024 * 1024  # 1 MB
DEFAULT_THREAD_POOL_SIZE = 8  # Fallback when os.cpu_count() is unavailable
DEFAULT_MULTIPART_THRESHOLD = 5 * 1024 * 1024 * 1024  # 5 GB
DEFAULT_MAX_CONCURRENCY = 15  # Balanced for bandwidth-limited downloads

# Sharded manifests keep entries in per-directory files under this directory,
# beside the root manifest.
MANIFEST_SHARD_DIR = ".s3lfs_manifest"
MANIFEST_ROOT_SHARD = "_root"

# Bulk-absence guard for deletion detection: more than this many missing
# files, and more than this fraction of those checked, reads as a wiped
# working copy rather than a set of deliberate deletions.
BULK_DELETION_FLOOR = 5

# Adaptive compression: gzip a sample from the head of each file and store
# the object raw unless compressing saves at least this fraction.
COMPRESSION_SAMPLE_BYTES = 256 * 1024
COMPRESSION_SAMPLE_LEVEL = 1
COMPRESSION_MIN_SAVINGS_RATIO = 0.9
BULK_DELETION_FRACTION = 0.5

try:
    # The libyaml-backed loader parses and emits roughly an order of
    # magnitude faster than the pure-Python one. Every command reads the
    # whole manifest, so on a large repository this is the difference
    # between a responsive tool and a stalled one. Output is identical.
    from yaml import CSafeDumper as _YamlDumper
    from yaml import CSafeLoader as _YamlLoader
except ImportError:  # pragma: no cover - depends on the PyYAML build installed
    from yaml import SafeDumper as _YamlDumper  # type: ignore[assignment]
    from yaml import SafeLoader as _YamlLoader  # type: ignore[assignment]

USING_LIBYAML = _YamlLoader.__name__.startswith("C")


def yaml_load(stream):
    """Parse YAML with the fastest safe loader available."""
    return yaml.load(stream, Loader=_YamlLoader)


def yaml_dump(data, stream=None, **kwargs):
    """Emit YAML with the fastest safe dumper available."""
    return yaml.dump(data, stream, Dumper=_YamlDumper, **kwargs)


class ShardedFiles(MutableMapping):
    """The manifest's files mapping, backed by per-directory shard files.

    Behaves like a dict, but reads a shard only when something actually
    touches a key in it. Looking up or writing one path parses one small
    file; iterating the whole mapping still parses everything, because
    that is what the caller asked for.

    The point is the sparse case: a working copy that wants `assets/` need
    never read the shards for the rest of the repository.
    """

    def __init__(self, owner):
        self._owner = owner
        self._loaded: dict = {}  # shard -> {key: hash}
        self._dirty: set = set()
        self._all_loaded = False

    # -- loading ---------------------------------------------------------
    def _ensure(self, shard):
        if shard not in self._loaded:
            self._loaded[shard] = dict(self._owner._read_shard(shard))
        return self._loaded[shard]

    def _ensure_all(self):
        if self._all_loaded:
            return
        for shard in self._owner.shard_names():
            self._ensure(shard)
        self._all_loaded = True

    def preload(self, shards):
        """Read these shards now and treat the rest as absent.

        Used when the caller knows its slice -- a sparse profile -- so
        iteration does not pull in the whole repository.
        """
        for shard in shards:
            self._ensure(shard)
        self._all_loaded = True

    @property
    def loaded_shards(self):
        return set(self._loaded)

    # -- mapping protocol ------------------------------------------------
    def __getitem__(self, key):
        return self._ensure(self._owner.shard_for(key))[key]

    def __setitem__(self, key, value):
        shard = self._owner.shard_for(key)
        entries = self._ensure(shard)
        if entries.get(key) != value:
            entries[key] = value
            self._dirty.add(shard)

    def __delitem__(self, key):
        shard = self._owner.shard_for(key)
        entries = self._ensure(shard)
        del entries[key]
        self._dirty.add(shard)

    def __iter__(self):
        self._ensure_all()
        for entries in self._loaded.values():
            yield from entries

    def __len__(self):
        self._ensure_all()
        return sum(len(e) for e in self._loaded.values())

    def __contains__(self, key):
        try:
            return key in self._ensure(self._owner.shard_for(key))
        except Exception:
            return False

    # -- persistence -----------------------------------------------------
    def save(self):
        """Write the shards that changed, and remove any left empty."""
        for shard in sorted(self._dirty):
            entries = self._loaded.get(shard, {})
            path = self._owner._shard_path(shard)
            if entries:
                self._owner._write_atomic(path, entries)
            else:
                path.unlink(missing_ok=True)
        self._dirty.clear()

    def as_dict(self):
        self._ensure_all()
        merged: dict = {}
        for entries in self._loaded.values():
            merged.update(entries)
        return merged


class _HashingWriter:
    """File-object wrapper that folds every written byte into a SHA-256.

    Hashing during the write means a downloaded file never has to be read
    back just to verify it -- on a 200MB download that second read was
    pure overhead.

    Only valid for sequential writes. boto3 downloads objects above its
    multipart threshold as parallel ranges, seeking and writing out of
    order; hashing those writes in arrival order produces garbage. Any
    out-of-order seek therefore invalidates the digest, and hexdigest()
    returns None so the caller falls back to hashing the finished file.
    """

    def __init__(self, fileobj):
        self._file = fileobj
        self._hasher = hashlib.sha256()
        self._pos = 0
        self._sequential = True

    def write(self, data):
        if self._sequential:
            self._hasher.update(data)
            self._pos += len(data)
        return self._file.write(data)

    def seek(self, offset, whence=0):
        if not (whence == 0 and offset == self._pos):
            self._sequential = False
        return self._file.seek(offset, whence)

    def hexdigest(self):
        return self._hasher.hexdigest() if self._sequential else None

    def __getattr__(self, name):
        return getattr(self._file, name)


def _default_workers():
    """Compute a sensible default worker count based on available CPUs.

    Uses the same heuristic as Python's ThreadPoolExecutor default:
    min(32, cpu_count + 4).  Falls back to DEFAULT_THREAD_POOL_SIZE if
    cpu_count is unavailable.
    """
    cpu = os.cpu_count()
    if cpu is None:
        return DEFAULT_THREAD_POOL_SIZE
    return min(32, cpu + 4)


# Common error messages
ERROR_MESSAGES = {
    "no_credentials": "AWS credentials are missing. Please configure them or use --no-sign-request.",
    "partial_credentials": "Incomplete AWS credentials. Check your AWS configuration.",
    "invalid_credentials": "Invalid AWS credentials. Please verify your access key and secret key.",
    "s3_access_denied": "Invalid or insufficient AWS credentials for bucket '{bucket_name}'.",
    "acceleration_not_supported": "Transfer acceleration is not supported for unsigned requests.",
}


# Errors that will not succeed on a second attempt. Retrying them wastes the
# caller's time and buries the real cause behind a delay.
NON_RETRYABLE_S3_CODES = frozenset(
    {
        "AccessDenied",
        "AllAccessDisabled",
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
        "NoSuchBucket",
        "InvalidBucketName",
        "AccountProblem",
        "InvalidObjectState",
        "EntityTooLarge",
    }
)


def _transient_network_errors():
    """The exception classes retried on, resolved lazily.

    Referencing these at class-definition time would force the boto3
    import back onto the startup path that lazy loading just removed.
    """
    from botocore.exceptions import BotoCoreError, ClientError
    from urllib3.exceptions import SSLError

    return (BotoCoreError, ClientError, SSLError)


def _is_retryable(exc):
    """Is this exception worth another attempt?"""
    from botocore.exceptions import ClientError

    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code", "")
        if code in NON_RETRYABLE_S3_CODES:
            return False
        # 4xx other than throttling and request timeout are client errors.
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if isinstance(status, int) and 400 <= status < 500:
            return code in {
                "RequestTimeout",
                "SlowDown",
                "Throttling",
                "ThrottlingException",
            }
    return True


def retry(times, exceptions, max_delay=30):
    """Retry decorator with exponential backoff and full jitter.

    :param times: Maximum number of retry attempts.
    :param exceptions: Exception types that trigger a retry -- a tuple, or
        a zero-arg callable returning one so decorators need not import
        the classes eagerly.
    :param max_delay: Cap on the backoff delay in seconds.
    """

    def decorator(func):
        @functools.wraps(func)
        def newfn(*args, **kwargs):
            for attempt in range(times):
                try:
                    return func(*args, **kwargs)
                except exceptions() if callable(exceptions) else exceptions as exc:
                    if attempt >= times - 1 or not _is_retryable(exc):
                        raise
                    # Full jitter. Without it, every worker that failed at the
                    # same moment retries at the same moment, so a transient
                    # blip becomes a synchronised stampede against the
                    # endpoint that just failed.
                    ceiling = min(2 ** (attempt + 1), max_delay)
                    delay = random.uniform(0, ceiling)
                    print(
                        f"Retry {attempt + 1}/{times} for {func.__name__} "
                        f"in {delay:.1f}s: {exc}"
                    )
                    time.sleep(delay)

        return newfn

    return decorator


class ShutdownRequested(Exception):
    """Raised by a worker that declined to start because of an interrupt.

    Distinguishes cancelled work from work that genuinely failed, so the
    drain loops can stay quiet about it rather than printing an error per
    queued task.
    """


class S3LFS:
    def __init__(
        self,
        bucket_name=None,
        manifest_file=".s3_manifest.yaml",
        repo_prefix=None,
        encryption=True,
        no_sign_request=False,
        temp_dir=None,
        chunk_size=DEFAULT_CHUNK_SIZE,
        s3_factory=None,
        use_acceleration=False,
        endpoint_url=None,
        workers=None,
        compression="auto",
    ):
        """
        :param bucket_name: Name of the S3 bucket (can be stored in manifest)
        :param manifest_file: Path to the local manifest file (YAML or JSON)
        :param repo_prefix: A unique prefix to isolate this repository's files
        :param encryption: If True, use AES256 server-side encryption
        :param no_sign_request: If True, use unsigned requests
        :param temp_dir: Path to the temporary directory for compression/decompression
        :param chunk_size: Size of chunks for multipart uploads (default: 5 GB)
        :param s3_factory: Custom S3 client factory function (for testing)
        :param use_acceleration: If True, enable S3 Transfer Acceleration
        :param endpoint_url: Custom S3 endpoint URL for S3-compatible storage (e.g. MinIO, R2, Wasabi)
        :param workers: Number of parallel workers for uploads/downloads (default: auto-detected)
        """
        self.chunk_size = chunk_size
        self.use_acceleration = use_acceleration
        self.endpoint_url = endpoint_url
        self.workers = workers if workers is not None else _default_workers()
        if compression not in ("auto", "always", "never"):
            raise ValueError(
                f"compression must be 'auto', 'always' or 'never', got {compression!r}"
            )
        self.compression = compression

        def default_s3_factory(no_sign_request):
            """Default S3 client factory with proper boto3 usage."""
            import boto3
            from botocore import UNSIGNED
            from botocore.config import Config

            # Transport-level (CRC) checksums only where S3 demands them.
            # boto3 >= 1.36 attaches and validates them by default, which
            # breaks ranged downloads against S3-compatible backends that
            # return whole-object checksums for a range (moto, and the same
            # class of breakage reported for MinIO and R2). s3lfs already
            # verifies the complete file's SHA-256 against the manifest,
            # which is end-to-end and strictly stronger than per-request
            # CRCs. Older botocore without these options lands in except.
            try:
                base = Config(
                    request_checksum_calculation="when_required",
                    response_checksum_validation="when_required",
                )
            except TypeError:
                base = Config()

            kwargs = {}
            if self.endpoint_url:
                kwargs["endpoint_url"] = self.endpoint_url
            if no_sign_request:
                if self.use_acceleration:
                    raise RuntimeError(ERROR_MESSAGES["acceleration_not_supported"])
                config = base.merge(Config(signature_version=UNSIGNED))
                return boto3.client("s3", config=config, **kwargs)
            else:
                if self.use_acceleration:
                    # Use transfer acceleration endpoint
                    return boto3.client(
                        "s3",
                        config=base.merge(Config(s3={"use_accelerate_endpoint": True})),
                        **kwargs,
                    )
                else:
                    return boto3.client("s3", config=base, **kwargs)

        self.s3_factory = s3_factory if s3_factory is not None else default_s3_factory

        # Set the temporary directory to the base of the repository if not provided
        self.temp_dir = Path(temp_dir or ".s3lfs_temp")
        self.temp_dir.mkdir(parents=True, exist_ok=True)  # Ensure the directory exists

        # Note: boto3 spawns max_concurrency threads per transfer and s3lfs
        # runs self.workers transfers at once, so in the worst case these
        # multiply. That is deliberate: a single large asset is one transfer,
        # and dividing the budget across the pool would leave it with no
        # multipart parallelism at all, which is the case s3lfs exists for.
        from boto3.s3.transfer import TransferConfig

        max_concurrency = max(self.workers, DEFAULT_MAX_CONCURRENCY)
        transfer_kwargs = {"max_concurrency": max_concurrency}
        if no_sign_request:
            # If we're not signing, we can't use multipart. Set the threshold to the max.
            transfer_kwargs["multipart_threshold"] = DEFAULT_MULTIPART_THRESHOLD
        try:
            # Prefer AWS's C-based transfer client (CRT) when the awscrt
            # package is installed -- install with `pip install s3lfs[crt]`.
            # "auto" uses it only where it applies (standard AWS S3
            # endpoints) and falls back to the classic client elsewhere,
            # e.g. MinIO or R2, so behaviour is unchanged when it cannot
            # help. Older boto3 without the kwarg lands in the except.
            self.config = TransferConfig(
                preferred_transfer_client="auto", **transfer_kwargs
            )
        except TypeError:
            self.config = TransferConfig(**transfer_kwargs)
        self.thread_local = threading.local()
        self.manifest_file = Path(manifest_file)

        # Separate cache file - should NOT be version controlled
        # Use same format as manifest (YAML or JSON)
        cache_suffix = (
            ".yaml" if self.manifest_file.suffix in [".yaml", ".yml"] else ".json"
        )
        cache_file_name = self.manifest_file.stem + "_cache" + cache_suffix
        self.cache_file = self.manifest_file.parent / cache_file_name

        # Use a file-based lock for cross-process synchronization. The lock is
        # anchored to the manifest it guards, not to the current working
        # directory: a CWD-relative path gives processes started from different
        # directories different lock files for the same manifest, silently
        # removing mutual exclusion between them. It is kept inside
        # .s3lfs_temp/ rather than beside the manifest so that file enumeration
        # (rglob) does not pick it up as a trackable file.
        lock_dir = self.manifest_file.parent.resolve() / ".s3lfs_temp"
        lock_dir.mkdir(parents=True, exist_ok=True)
        self._lock_file = lock_dir / ".s3lfs.lock"

        # Registry of hashes an uploader has claimed but not yet published in
        # the manifest. Garbage collection treats these as live; see
        # _inflight_claim.
        self._inflight_file = lock_dir / ".s3lfs_inflight.yaml"

        self.no_sign_request = no_sign_request
        self._cache_mtime: Optional[float] = None
        self._cache_dirty = False
        self.hash_cache: dict = {}
        self.manifest: dict = {"files": {}}
        self._loaded_shards: dict = {}
        self.load_manifest()
        self.load_cache()

        def stored_config():
            return (
                self.manifest.get("bucket_name"),
                self.manifest.get("repo_prefix"),
                self.manifest.get("endpoint_url"),
            )

        config_before = stored_config()

        # Use the stored bucket name if none is provided
        with self._lock_context():
            if bucket_name:
                self.bucket_name = bucket_name
                self.manifest["bucket_name"] = bucket_name
            else:
                self.bucket_name = self.manifest.get("bucket_name")

        if not self.bucket_name:
            raise ValueError(
                "Bucket name must be provided either as a parameter or stored in the manifest. "
                "Use 'initialize_repo()' to set up the repository configuration."
            )

        with self._lock_context():
            if repo_prefix:
                self.repo_prefix = repo_prefix
                self.manifest["repo_prefix"] = repo_prefix
            else:
                self.repo_prefix = self.manifest.get("repo_prefix", "s3lfs")

            # Load endpoint_url from manifest if not provided as parameter
            if self.endpoint_url:
                self.manifest["endpoint_url"] = self.endpoint_url
            else:
                self.endpoint_url = self.manifest.get("endpoint_url")

            # Only write when construction actually changed the stored
            # configuration. The manifest is a git-tracked file, and an
            # unconditional write here dirties the working tree on every
            # read-only command and every sync hook -- which breaks clean-tree
            # checks in CI and can overwrite an unresolved merge.
            if stored_config() != config_before:
                self.save_manifest()

        self.encryption = encryption

        # Initialize PathResolver for consistent path handling
        # Find git root for path resolution
        manifest_dir = Path(self.manifest_file).parent.resolve()
        git_root = find_git_root(start_path=manifest_dir)

        # Determine the base directory for PathResolver
        if git_root:
            # Check if manifest is within git repo
            try:
                manifest_dir.relative_to(git_root)
                # Manifest is within git repo, use git root
                self.path_resolver = PathResolver(git_root)
            except ValueError:
                # Manifest is outside git repo, use manifest directory
                self.path_resolver = PathResolver(manifest_dir)
        else:
            # If not in a git repo, use manifest directory
            self.path_resolver = PathResolver(manifest_dir)

        self._shutdown_requested = False
        # signal.signal only works on the main thread of the main interpreter,
        # and raises ValueError elsewhere. Constructing an S3LFS from a worker
        # thread is legitimate for library callers, so treat the handler as
        # best-effort rather than a hard requirement.
        try:
            signal.signal(signal.SIGINT, self._handle_sigint)
        except ValueError:
            pass

    def _handle_sigint(self, signum, frame):
        """
        Handle SIGINT (Ctrl+C) to gracefully shut down parallel operations.
        """
        print("\nInterrupt received. Shutting down...")
        self._shutdown_requested = True
        sys.exit(1)  # Exit the program

    @contextmanager
    def _lock_context(self):
        """
        Context manager for acquiring and releasing the file-based lock using portalocker.
        """
        lock = open(self._lock_file, "w")  # Open the lock file in write mode
        try:
            portalocker.lock(lock, portalocker.LOCK_EX)  # Acquire an exclusive lock
            yield lock  # Provide the lock to the context
        finally:
            portalocker.unlock(lock)  # Release the lock
            lock.close()  # Close the file handle

    # A claim older than this is treated as abandoned by a crashed process.
    # This bounds the leak from a crash; it plays no part in closing the race
    # itself, which the registry handles outright.
    INFLIGHT_TTL_SECONDS = 24 * 60 * 60

    def _load_inflight(self):
        """Read the in-flight claim registry. Caller must hold the lock."""
        if not self._inflight_file.exists():
            return {}
        try:
            with open(self._inflight_file, "r") as f:
                data = yaml_load(f) or {}
            claims = data.get("claims", {})
            return claims if isinstance(claims, dict) else {}
        except Exception:
            # A corrupt registry must not block uploads. Treating it as empty
            # is the conservative direction: GC may delete an object an
            # in-flight upload is about to reference, which is the behaviour
            # without a registry at all, rather than leaking forever.
            return {}

    def _save_inflight(self, claims):
        """Write the in-flight claim registry. Caller must hold the lock."""
        # Unique temp name: a shared one lets concurrent writers interleave
        # into a single file before the rename.
        temp_file = self._inflight_file.with_name(
            f"{self._inflight_file.name}.{uuid4().hex}.tmp"
        )
        with open(temp_file, "w") as f:
            yaml_dump({"claims": claims}, f)
        temp_file.replace(self._inflight_file)

    def _live_inflight_keys(self):
        """Claims that have not aged out. Caller must hold the lock."""
        cutoff = time.time() - self.INFLIGHT_TTL_SECONDS
        return {k for k, ts in self._load_inflight().items() if ts >= cutoff}

    def _claim_inflight(self, base_key):
        """Register an asset key as in-flight, before any bytes reach S3."""
        with self._lock_context():
            claims = self._load_inflight()
            claims[base_key] = time.time()
            self._save_inflight(claims)

    def _release_inflight(self, base_keys):
        """Drop claims. Must run only after the manifest entry is published."""
        if not base_keys:
            return
        with self._lock_context():
            claims = self._load_inflight()
            cutoff = time.time() - self.INFLIGHT_TTL_SECONDS
            claims = {
                k: ts
                for k, ts in claims.items()
                if k not in base_keys and ts >= cutoff  # also prune aged-out claims
            }
            self._save_inflight(claims)

    def _asset_base_key(self, manifest_key, file_hash):
        """The stem of the S3 key a file's content is stored under.

        The stored object is either <stem>.gz (compressed) or <stem>
        itself (raw), each optionally chunked as <object>.chunkN. Raw
        objects keep the file's natural name and exact bytes, so anything
        that speaks S3 can fetch and use them without s3lfs.
        """
        return f"{self.repo_prefix}/assets/{file_hash}/{manifest_key}"

    def _list_objects(self, prefix):
        """Every key under a prefix with its size, following pagination.

        A single list_objects_v2 call stops at 1000 keys. Deriving a chunk
        count from a truncated listing rebuilds a short file and calls it a
        success, which is worse than failing outright.
        """
        client = self._get_s3_client()
        objects: dict = {}
        token = None
        while True:
            kwargs = {"Bucket": self.bucket_name, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = client.list_objects_v2(**kwargs)
            for obj in resp.get("Contents") or []:
                objects[obj["Key"]] = obj.get("Size", 0)

            # Continue only on a genuine continuation token. Testing
            # IsTruncated for truthiness is not enough: any object that is
            # not a bool -- a stub client, an unexpected response shape --
            # reads as "more pages" and spins forever.
            token = resp.get("NextContinuationToken")
            if resp.get("IsTruncated") is not True or not isinstance(token, str):
                return objects

    def _list_keys(self, prefix):
        """Every key under a prefix, following pagination."""
        return list(self._list_objects(prefix))

    def _locate_asset(self, manifest_key, file_hash):
        """Find the stored object(s) for an entry.

        Returns (keys, sizes, is_chunked, compressed) with keys in chunk
        order. Which form exists -- raw or .gz, single or chunked -- is
        discovered from the bucket rather than assumed, so a mix of writer
        versions in one repository works.
        """
        stem = self._asset_base_key(manifest_key, file_hash)
        objects = {
            k: size
            for k, size in self._list_objects(stem).items()
            if self._key_covered_by(k, {stem})
        }

        def chunks_of(base):
            found = {}
            for k in objects:
                head, sep, tail = k.rpartition(".chunk")
                if sep and head == base and tail.isdigit():
                    found[int(tail)] = k
            return found

        for base, compressed in ((stem, False), (stem + ".gz", True)):
            if base in objects:
                return [base], objects, False, compressed
            chunks = chunks_of(base)
            if chunks:
                if set(chunks) != set(range(len(chunks))):
                    raise RuntimeError(
                        f"Stored chunks for {manifest_key} are not contiguous: "
                        f"have indices {sorted(chunks)}. The object is "
                        "incomplete; re-upload with 's3lfs track'."
                    )
                keys = [chunks[i] for i in range(len(chunks))]
                return keys, objects, True, compressed

        raise RuntimeError(
            f"No stored object for {manifest_key} ({file_hash[:12]}). "
            "The content may never have been uploaded, or was removed."
        )

    def _delete_asset(self, base_key):
        """Delete an asset and every chunk belonging to it.

        A large file is stored as base_key.chunk0..N rather than at base_key
        itself, so deleting only the base key leaves the whole file behind.
        """
        client = self._get_s3_client()
        keys = self._list_keys(base_key)
        # Guard against a prefix match on an unrelated, longer key.
        keys = [k for k in keys if self._key_covered_by(k, {base_key})]
        if not keys:
            keys = [base_key]

        for key in keys:
            client.delete_object(Bucket=self.bucket_name, Key=key)
            print(f"File removed from S3: s3://{self.bucket_name}/{key}")

    def _live_asset_keys(self):
        """Base keys reachable from the manifest. Caller must hold the lock."""
        return {
            self._asset_base_key(manifest_key, file_hash)
            for manifest_key, file_hash in self.manifest.get("files", {}).items()
        }

    def _is_asset_key(self, key):
        """Does this key have the shape of a stored asset?

        Anything else under the prefix is left alone: s3lfs did not put it
        there in a form it recognises, so it is not ours to delete.
        """
        prefix = f"{self.repo_prefix}/assets/"
        if not key.startswith(prefix):
            return False
        rest = key[len(prefix) :]
        head, sep, tail = rest.partition("/")
        return bool(head and sep and tail)

    @staticmethod
    def _key_covered_by(key, base_keys):
        """Is this key one of these assets, or a chunk belonging to one?

        Reachability has to be judged on hash *and* path, because that is what
        the storage layout keys on. Judging on the hash alone means an object
        stays reachable as long as any path shares its content, so a removed
        or renamed path leaks its object permanently.

        Base keys are stems; each asset may be stored as <stem>.gz or as
        <stem> raw, so both spellings (and their chunks) are covered.
        """
        variants = set(base_keys) | {b + ".gz" for b in base_keys}
        if key in variants:
            return True
        head, sep, tail = key.rpartition(".chunk")
        return bool(sep) and tail.isdigit() and head in variants

    def find_missing_assets(self, files):
        """Return the manifest entries whose content is absent from S3.

        :param files: dict of manifest_key -> file_hash to check
        :return: list of (manifest_key, file_hash) tuples with no object behind them

        An entry is present if its base key exists, or at least one of its
        chunks does (a large file is stored only as base_key.chunk0..N).
        Chunk completeness is not verified; this answers "was this content
        ever uploaded", which is what push-time verification needs.
        """

        @retry(3, _transient_network_errors)
        def check(item):
            manifest_key, file_hash = item
            base_key = self._asset_base_key(manifest_key, file_hash)
            resp = self._get_s3_client().list_objects_v2(
                Bucket=self.bucket_name, Prefix=base_key
            )
            keys = [obj["Key"] for obj in resp.get("Contents", [])]
            # A prefix match can hit an unrelated, longer key.
            if any(self._key_covered_by(k, {base_key}) for k in keys):
                return None
            return item

        # Accepts a mapping or an iterable of (manifest_key, hash) pairs.
        # A verification over a range of commits needs the latter: the same
        # path can carry different content at different commits, and each
        # one is a separate object that has to exist.
        items = list(files.items()) if hasattr(files, "items") else list(files)
        if not items:
            return []

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            results = list(pool.map(check, items))
        return [item for item in results if item is not None]

    def _get_s3_client(self):
        """Ensures each thread gets its own instance of the S3 client with appropriate authentication handling."""
        if not hasattr(self.thread_local, "s3"):
            from botocore.exceptions import (
                ClientError,
                NoCredentialsError,
                PartialCredentialsError,
            )

            try:
                self.thread_local.s3 = self.s3_factory(self.no_sign_request)
            except NoCredentialsError:
                raise RuntimeError(ERROR_MESSAGES["no_credentials"])
            except PartialCredentialsError:
                raise RuntimeError(ERROR_MESSAGES["partial_credentials"])
            except ClientError as e:
                if e.response["Error"]["Code"] in [
                    "InvalidAccessKeyId",
                    "SignatureDoesNotMatch",
                ]:
                    raise RuntimeError(ERROR_MESSAGES["invalid_credentials"])
                raise RuntimeError(f"Error initializing S3 client: {e}")

        return self.thread_local.s3

    def initialize_repo(self):
        """
        Initialize the repository with a bucket name and a repo-specific prefix.
        Also updates .gitignore to exclude S3LFS cache files.

        :param bucket_name: Name of the S3 bucket to use
        :param repo_prefix: A unique prefix for this repository in the bucket
        """
        with self._lock_context():
            # Store configuration in manifest
            if self.bucket_name is not None:
                self.manifest["bucket_name"] = str(self.bucket_name)
            if self.repo_prefix is not None:
                self.manifest["repo_prefix"] = str(self.repo_prefix)
            if self.endpoint_url is not None:
                self.manifest["endpoint_url"] = str(self.endpoint_url)
            self.save_manifest()

        # Update .gitignore to exclude cache files
        self._update_gitignore()

        print("Successfully initialized S3LFS with:")
        print(f"   Bucket Name: {self.bucket_name}")
        print(f"   Repo Prefix: {self.repo_prefix}")
        if self.endpoint_url:
            print(f"   Endpoint URL: {self.endpoint_url}")
        print(f"Manifest file saved as {self.manifest_file.name}")

    def _update_gitignore(self):
        """
        Update .gitignore to exclude S3LFS cache files and temporary directories.
        Creates .gitignore if it doesn't exist, or appends to existing one.
        """
        gitignore_path = Path(".gitignore")

        # S3LFS patterns to add
        s3lfs_patterns = [
            "",  # Empty line for separation
            "# S3LFS cache and temporary files - should not be version controlled",
            "*_cache.json",
            "*_cache.yaml",
            ".s3lfs_temp/",
            "*.s3lfs.lock",
        ]

        # Check if .gitignore exists and read current content
        existing_content = []
        if gitignore_path.exists():
            with open(gitignore_path, "r") as f:
                existing_content = [line.rstrip() for line in f.readlines()]

        # Check which patterns are already present
        patterns_to_add = []
        for pattern in s3lfs_patterns:
            if pattern.startswith("#") or pattern == "":
                # Always add comments and empty lines for structure
                patterns_to_add.append(pattern)
            elif pattern not in existing_content:
                patterns_to_add.append(pattern)

        # Only update if we have patterns to add
        if any(p for p in patterns_to_add if not p.startswith("#") and p != ""):
            # Check if we already have S3LFS section
            has_s3lfs_section = any("S3LFS" in line for line in existing_content)

            if not has_s3lfs_section:
                # Add all patterns including header
                with open(gitignore_path, "a") as f:
                    for pattern in patterns_to_add:
                        f.write(f"{pattern}\n")
                print("Updated .gitignore to exclude S3LFS cache files")
            else:
                # Only add missing patterns (without header)
                missing_patterns = [
                    p for p in patterns_to_add if not p.startswith("#") and p != ""
                ]
                if missing_patterns:
                    with open(gitignore_path, "a") as f:
                        for pattern in missing_patterns:
                            f.write(f"{pattern}\n")
                    print(
                        f"Added {len(missing_patterns)} missing S3LFS patterns to .gitignore"
                    )
        else:
            print(".gitignore already contains S3LFS cache exclusions")

    @property
    def shard_dir(self):
        """Directory holding manifest shards, when the manifest is sharded."""
        return self.manifest_file.parent / MANIFEST_SHARD_DIR

    @staticmethod
    def shard_for(manifest_key):
        """Which shard a manifest key belongs to.

        The first path component, so a change under one top-level directory
        rewrites one small file instead of the whole manifest. That matters
        twice over: every command parses what it loads, and every rewrite
        becomes a new blob in git history.
        """
        head, sep, _ = manifest_key.partition("/")
        return head if sep and head else MANIFEST_ROOT_SHARD

    def _shard_path(self, shard):
        # Shard names come from path components, so they can contain
        # anything a directory name can. Percent-encode everything outside
        # a conservative set rather than trusting them as filenames.
        safe = "".join(
            ch if ch.isalnum() or ch in "-_." else f"%{ord(ch):02x}" for ch in shard
        )
        return self.shard_dir / f"{safe}.yaml"

    def _load_shards(self):
        """Merge every shard into one files mapping (eager; tests use it)."""
        files: dict = {}
        for shard in self.shard_names():
            files.update(self._read_shard(shard))
        return files

    def _read_shard(self, shard):
        """Parse one shard file. Missing reads as empty."""
        path = self._shard_path(shard)
        if not path.is_file():
            return {}
        try:
            with open(path, "r") as f:
                data = yaml_load(f) or {}
        except yaml.YAMLError as e:
            hint = ""
            if "<<<<<<<" in path.read_text(errors="replace"):
                hint = (
                    " It contains merge conflict markers -- resolve the "
                    "conflict, or run 's3lfs install' to register the "
                    "merge driver that prevents it."
                )
            raise RuntimeError(f"Cannot read manifest shard {path}: {e}.{hint}")
        return data if isinstance(data, dict) else {}

    def shard_names(self):
        """Shard names present on disk, from the directory listing alone.

        Cheap: no shard is parsed. This is what lets a working copy decide
        which shards it needs before paying to read any of them.
        """
        if not self.shard_dir.is_dir():
            return []
        names = []
        for path in sorted(self.shard_dir.glob("*.yaml")):
            stem = path.stem
            # Reverse the percent-encoding applied by _shard_path.
            out, i = [], 0
            while i < len(stem):
                if stem[i] == "%" and i + 2 < len(stem) + 1:
                    try:
                        out.append(chr(int(stem[i + 1 : i + 3], 16)))
                        i += 3
                        continue
                    except ValueError:
                        pass
                out.append(stem[i])
                i += 1
            names.append("".join(out))
        return names

    def files_in_shards(self, shards):
        """Entries from just these shards, leaving the rest unread."""
        files = {}
        for shard in shards:
            files.update(self._read_shard(shard))
        return files

    def _write_atomic(self, path, data):
        temp_file = path.with_name(f"{path.name}.{uuid4().hex}.tmp")
        try:
            with open(temp_file, "w") as f:
                yaml_dump(data, f, default_flow_style=False, sort_keys=True)
            temp_file.replace(path)
        except Exception as e:
            print(f"Failed to write {path}: {e}")
            if temp_file.exists():
                temp_file.unlink()

    @property
    def is_sharded(self):
        return self.manifest.get("manifest_format") == "sharded"

    def load_manifest(self):
        """Load the local manifest (YAML or JSON format)."""
        self._loaded_shards = {}
        if self.manifest_file.exists():
            try:
                with open(self.manifest_file, "r") as f:
                    # Detect format based on extension
                    if self.manifest_file.suffix in [".yaml", ".yml"]:
                        self.manifest = yaml_load(f) or {"files": {}}
                    else:
                        self.manifest = json.load(f)
            except (yaml.YAMLError, json.JSONDecodeError) as e:
                # The likeliest cause by far is an unresolved merge, which a
                # teammate who has not run 's3lfs install' will hit. A bare
                # parser traceback does not tell them that.
                hint = ""
                if "<<<<<<<" in self.manifest_file.read_text(errors="replace"):
                    hint = (
                        " It contains merge conflict markers -- resolve the "
                        "conflict, or run 's3lfs install' to register the "
                        "merge driver that prevents it."
                    )
                raise RuntimeError(
                    f"Cannot read manifest {self.manifest_file}: {e}.{hint}"
                ) from e
            if not isinstance(self.manifest, dict):
                raise RuntimeError(
                    f"Manifest {self.manifest_file} is not a mapping; "
                    "it may have been overwritten."
                )
            self.manifest.setdefault("files", {})
            if self.is_sharded:
                # The root file carries configuration only; entries live in
                # per-directory shards beside it, read on demand.
                self.manifest["files"] = ShardedFiles(self)
        else:
            self.manifest = {"files": {}}  # Use file paths as keys

    def save_manifest(self):
        """Save the manifest back to disk atomically (YAML or JSON format)."""
        if self.is_sharded:
            files = self.manifest.get("files")
            if isinstance(files, ShardedFiles):
                files.save()
            else:
                # A plain dict here means the manifest was just converted to
                # the sharded format; write every shard once.
                grouped: dict = {}
                for key, file_hash in (files or {}).items():
                    grouped.setdefault(self.shard_for(key), {})[key] = file_hash
                self.shard_dir.mkdir(parents=True, exist_ok=True)
                for shard, entries in grouped.items():
                    self._write_atomic(self._shard_path(shard), entries)
            root = {k: v for k, v in self.manifest.items() if k != "files"}
            self._write_atomic(self.manifest_file, root)
            return

        # Unique temp name. A shared one lets two writers interleave into the
        # same file before either renames: the rename is atomic, the content
        # is not.
        temp_file = self.manifest_file.with_name(
            f"{self.manifest_file.name}.{uuid4().hex}.tmp"
        )
        try:
            # Write the manifest to a temporary file
            with open(temp_file, "w") as f:
                # Detect format based on extension
                if self.manifest_file.suffix in [".yaml", ".yml"]:
                    yaml_dump(
                        self.manifest, f, default_flow_style=False, sort_keys=True
                    )
                else:
                    json.dump(self.manifest, f, indent=4, sort_keys=True)

            # Atomically move the temporary file to the target location
            temp_file.replace(self.manifest_file)
        except Exception as e:
            print(f"Failed to save manifest: {e}")
            if temp_file.exists():
                temp_file.unlink()  # Clean up the temporary file

    def load_cache(self, force=False):
        """Load the hash cache from a separate cache file.

        Skips the disk read when the file's mtime has not changed since
        the last load, unless *force* is True.
        """
        if not self.cache_file.exists():
            # Only reset if we haven't already established that the
            # file is absent (avoids clearing in-memory mutations
            # between load-save cycles when the file doesn't exist yet).
            if self._cache_mtime is not None or not hasattr(self, "hash_cache"):
                self.hash_cache = {}
                self._cache_mtime = None
                self._cache_dirty = False
            return

        try:
            current_mtime = self.cache_file.stat().st_mtime
        except OSError:
            current_mtime = None

        # Skip re-read if on-disk file hasn't changed
        if (
            not force
            and self._cache_mtime is not None
            and current_mtime == self._cache_mtime
        ):
            return

        try:
            with open(self.cache_file, "r") as f:
                if self.cache_file.suffix in [".yaml", ".yml"]:
                    self.hash_cache = yaml_load(f) or {}
                else:
                    self.hash_cache = json.load(f)
        except (json.JSONDecodeError, yaml.YAMLError, IOError) as e:
            print(f"Warning: Failed to load cache file, starting with empty cache: {e}")
            self.hash_cache = {}

        self._cache_mtime = current_mtime
        self._cache_dirty = False

    def save_cache(self):
        """Save the hash cache back to disk atomically.

        Skips the write when nothing has changed since the last
        load or save.
        """
        if hasattr(self, "_cache_dirty") and not self._cache_dirty:
            return

        # Unique temp name; see save_manifest.
        temp_file = self.cache_file.with_name(
            f"{self.cache_file.name}.{uuid4().hex}.tmp"
        )
        try:
            with open(temp_file, "w") as f:
                if self.cache_file.suffix in [".yaml", ".yml"]:
                    yaml_dump(
                        self.hash_cache, f, default_flow_style=False, sort_keys=True
                    )
                else:
                    json.dump(self.hash_cache, f, indent=4, sort_keys=True)

            temp_file.replace(self.cache_file)

            # Update mtime so subsequent load_cache() calls are no-ops
            try:
                self._cache_mtime = self.cache_file.stat().st_mtime
            except OSError:
                self._cache_mtime = None
            self._cache_dirty = False
        except Exception as e:
            print(f"Failed to save cache: {e}")
            if temp_file.exists():
                temp_file.unlink()

    def hash_file(self, file_path: Union[str, Path], method: str = "auto") -> str:
        """
        Compute a SHA-256 hash of the file's contents.

        The hash covers content only: the same bytes at two different paths
        produce the same digest. Supports multiple hashing methods for
        performance optimization.

        :param file_path: Path to the file to hash.
        :param method: Hashing method to use. Options are:
                    - "auto": Automatically select the best method.
                    - "mmap": Use memory-mapped files (default for non-empty files).
                    - "iter": Use an iterative read approach (fallback for empty files).
                    - "cli": Use the `sha256sum` CLI utility (POSIX only).
        :return: The computed SHA-256 hash as a hexadecimal string.
        """
        file_path = Path(file_path)

        # Ensure the file exists
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Automatically select the best method if "auto" is specified
        if method == "auto":
            if file_path.stat().st_size == 0:  # Empty file
                method = "iter"
            elif shutil.which("sha256sum"):
                # Prefer CLI - no GIL contention, better parallelism
                method = "cli"
            else:
                method = "mmap"

        # Use the selected hashing method
        if method == "mmap":
            return self._hash_file_mmap(file_path)
        elif method == "iter":
            return self._hash_file_iter(file_path)
        elif method == "cli":
            return self._hash_file_cli(file_path)
        else:
            raise ValueError(f"Unsupported hashing method: {method}")

    # Filesystem mtime is frequently stored at 1-second resolution. A file
    # modified within this window of being hashed cannot be distinguished from
    # one that was not, so such entries are not cached at all.
    MTIME_GRANULARITY_SECONDS = 1.0

    def _changed_during_hashing(self, file_path, metadata):
        """Did the file change between the pre-hash stat and now?"""
        try:
            stat = Path(file_path).stat()
        except OSError:
            return True

        return (
            stat.st_size != metadata["size"]
            or stat.st_mtime != metadata["mtime"]
            or getattr(stat, "st_ino", None) != metadata["inode"]
        )

    def _entry_is_racy(self, cached_data):
        """Was this entry written too soon after the file was modified?

        If the gap between the file's mtime and the moment we recorded the
        hash is below mtime granularity, a further modification in that same
        tick would leave (size, mtime, inode) unchanged and go unnoticed. Such
        an entry cannot be trusted on the strength of its metadata alone.

        The entry is still kept: recomputing once refreshes it with a
        timestamp comfortably after the mtime, and it is trusted from then on.
        This mirrors git's "racily clean" handling.
        """
        written_at = cached_data.get("timestamp")
        mtime = cached_data.get("metadata", {}).get("mtime")
        if written_at is None or mtime is None:
            return True
        return (written_at - mtime) < self.MTIME_GRANULARITY_SECONDS

    def hash_file_cached(
        self, file_path: Union[str, Path], method: str = "auto"
    ) -> str:
        """
        Compute SHA-256 hash with caching based on file metadata (mtime, size, inode).
        Returns cached hash if file hasn't changed, otherwise computes and caches new hash.
        This method is multi-process safe using file-based locking.

        :param file_path: Path to the file to hash.
        :param method: Hashing method to use if computation is needed.
        :return: The computed SHA-256 hash as a hexadecimal string.
        """
        file_path = Path(file_path)
        file_path_str = str(file_path.as_posix())

        # Ensure the file exists
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Get current file metadata
        stat = file_path.stat()
        current_metadata = {
            "size": stat.st_size,
            "mtime": stat.st_mtime,
            "inode": getattr(
                stat, "st_ino", None
            ),  # inode may not exist on all platforms
        }

        # Use file lock for multi-process safety
        with self._lock_context():
            # Reload cache to get latest state from other processes
            self.load_cache()

            # Check if we have cached data for this file
            cached_data = self.hash_cache.get(file_path_str)
            if cached_data:
                cached_metadata = cached_data.get("metadata", {})

                # Compare metadata to see if file has changed
                if (
                    cached_metadata.get("size") == current_metadata["size"]
                    and cached_metadata.get("mtime") == current_metadata["mtime"]
                    and cached_metadata.get("inode") == current_metadata["inode"]
                    and not self._entry_is_racy(cached_data)
                ):
                    # File hasn't changed, return cached hash
                    return cached_data["hash"]

            # File has changed or no cache exists, compute new hash
            # Release lock while computing hash (can be expensive)
            pass

        # Compute hash outside of lock to avoid blocking other processes
        new_hash = self.hash_file(file_path, method)

        # The metadata above was read before hashing. If the file changed while
        # we were reading it, that hash belongs to no single version of the
        # file, and storing it against the pre-hash metadata would leave an
        # entry that is wrong whenever those metadata recur.
        if self._changed_during_hashing(file_path, current_metadata):
            return new_hash

        # Acquire lock again to update cache
        with self._lock_context():
            # Reload cache again in case it changed while we were computing hash
            self.load_cache()

            # Double-check if another process already computed this hash
            cached_data = self.hash_cache.get(file_path_str)
            if cached_data:
                cached_metadata = cached_data.get("metadata", {})
                if (
                    cached_metadata.get("size") == current_metadata["size"]
                    and cached_metadata.get("mtime") == current_metadata["mtime"]
                    and cached_metadata.get("inode") == current_metadata["inode"]
                    and not self._entry_is_racy(cached_data)
                ):
                    # Another process computed it while we were working
                    return cached_data["hash"]

            # Cache the new hash with metadata
            self.hash_cache[file_path_str] = {
                "hash": new_hash,
                "metadata": current_metadata,
                "timestamp": time.time(),
            }
            self._cache_dirty = True

            # Save cache with updated data
            self.save_cache()

        return new_hash

    def get_file_status(self, file_path: Union[str, Path]) -> dict:
        """
        Get comprehensive status information about a file including cache status.

        :param file_path: Path to the file to check.
        :return: Dictionary with file status information.
        """
        file_path = Path(file_path)
        file_path_str = str(file_path.as_posix())

        if not file_path.exists():
            return {"exists": False, "cached": False}

        stat = file_path.stat()
        current_metadata = {
            "size": stat.st_size,
            "mtime": stat.st_mtime,
            "inode": getattr(stat, "st_ino", None),
        }

        # Check cache status - reload cache to get latest state
        with self._lock_context():
            self.load_cache()
            cached_data = self.hash_cache.get(file_path_str)

        is_cached = False
        cache_valid = False

        if cached_data:
            is_cached = True
            cached_metadata = cached_data.get("metadata", {})
            cache_valid = (
                cached_metadata.get("size") == current_metadata["size"]
                and cached_metadata.get("mtime") == current_metadata["mtime"]
                and cached_metadata.get("inode") == current_metadata["inode"]
            )

        return {
            "exists": True,
            "size": current_metadata["size"],
            "mtime": current_metadata["mtime"],
            "cached": is_cached,
            "cache_valid": cache_valid,
            "cached_hash": cached_data.get("hash") if cached_data else None,
            "cache_timestamp": cached_data.get("timestamp") if cached_data else None,
        }

    def clear_hash_cache(self, file_path: Union[str, Path, None] = None):
        """
        Clear hash cache for a specific file or all files.
        This method is multi-process safe using file-based locking.

        :param file_path: If provided, clear cache only for this file. If None, clear all cache.
        """
        with self._lock_context():
            self.load_cache()  # Get latest state

            if file_path is None:
                # Clear all cache
                self.hash_cache = {}
                self._cache_dirty = True
                print("Cleared all hash cache entries.")
            else:
                # Clear cache for specific file
                file_path_str = str(Path(file_path).as_posix())
                if file_path_str in self.hash_cache:
                    del self.hash_cache[file_path_str]
                    self._cache_dirty = True
                    print(f"Cleared hash cache for '{file_path}'.")

            self.save_cache()

    def cleanup_stale_cache(self, max_age_days: int = 30):
        """
        Remove cache entries for files that no longer exist or are very old.
        This method is multi-process safe using file-based locking.

        :param max_age_days: Remove cache entries older than this many days.
        """
        with self._lock_context():
            self.load_cache()  # Get latest state

            current_time = time.time()
            max_age_seconds = max_age_days * 24 * 60 * 60

            stale_entries = []

            for file_path_str, cached_data in self.hash_cache.items():
                # Check if file still exists
                if not Path(file_path_str).exists():
                    stale_entries.append(file_path_str)
                    continue

                # Check if cache entry is too old
                cache_timestamp = cached_data.get("timestamp", 0)
                if current_time - cache_timestamp > max_age_seconds:
                    stale_entries.append(file_path_str)

            # Remove stale entries
            for file_path_str in stale_entries:
                del self.hash_cache[file_path_str]
                self._cache_dirty = True

            if stale_entries:
                print(f"Cleaned up {len(stale_entries)} stale cache entries.")
                self.save_cache()

    def _check_cache_hit(self, file_path_str, current_metadata):
        """Check if the in-memory cache has a valid entry for this file.

        Does NOT acquire the lock or reload cache from disk.
        Returns the cached hash if valid, or None.
        """
        cached_data = self.hash_cache.get(file_path_str)
        if not cached_data:
            return None
        cached_meta = cached_data.get("metadata", {})
        if (
            cached_meta.get("size") == current_metadata["size"]
            and cached_meta.get("mtime") == current_metadata["mtime"]
            and cached_meta.get("inode") == current_metadata["inode"]
        ):
            return cached_data["hash"]
        return None

    def compare_to_hashes(self, expected, progress=False):
        """Compare files on disk against the hashes they are expected to have.

        :param expected: dict of manifest_key -> expected hash
        :param progress: show a progress bar while hashing
        :return: dict of manifest_key -> "up_to_date" | "modified" | "missing"
        """
        return {
            key: (
                "missing"
                if h is None
                else "up_to_date" if h == expected[key] else "modified"
            )
            for key, h in self.disk_hashes(expected, progress=progress).items()
        }

    def disk_hashes(self, keys, progress=False):
        """Hash what is currently on disk for each manifest key.

        :return: dict of manifest_key -> hash, or None where no file exists

        Uses the same load-cache-once, hash-on-miss strategy as
        track_modified_files_cached, so repeat calls over unchanged files
        cost a stat() each rather than a full re-read.
        """
        expected = keys
        if not expected:
            return {}

        with self._lock_context():
            self.load_cache()

        hashes: dict = {}
        cache_updates = {}

        with tqdm(
            total=len(expected),
            desc="Checking files",
            unit="file",
            disable=not progress,
        ) as pbar:
            for manifest_key in expected:
                pbar.update(1)
                filesystem_path = self.path_resolver.to_filesystem_path(manifest_key)
                if not filesystem_path.exists():
                    hashes[manifest_key] = None
                    continue

                stat = filesystem_path.stat()
                metadata = {
                    "size": stat.st_size,
                    "mtime": stat.st_mtime,
                    "inode": getattr(stat, "st_ino", None),
                }
                cache_key = str(Path(manifest_key).as_posix())

                current_hash = self._check_cache_hit(cache_key, metadata)
                if current_hash is None:
                    current_hash = self.hash_file(filesystem_path)
                    cache_updates[cache_key] = {
                        "hash": current_hash,
                        "metadata": metadata,
                        "timestamp": time.time(),
                    }

                hashes[manifest_key] = current_hash

        if cache_updates:
            with self._lock_context():
                self.hash_cache.update(cache_updates)
                self._cache_dirty = True
                self.save_cache()

        return hashes

    def forget_hashes(self, keys):
        """Drop cache entries for files s3lfs itself removed from disk.

        The cache doubles as the record of what this working copy has held,
        which is how a file the user deleted is told apart from one that was
        never materialized here. A file s3lfs prunes -- because it left the
        sparse profile, say -- is neither: leaving its entry behind would
        make the next scan read it as a user deletion and untrack it for
        everyone.
        """
        keys = [str(Path(k).as_posix()) for k in keys]
        if not keys:
            return
        with self._lock_context():
            self.load_cache()
            changed = False
            for key in keys:
                if self.hash_cache.pop(key, None) is not None:
                    changed = True
            if changed:
                self._cache_dirty = True
                self.save_cache()

    def record_hashes(self, entries):
        """Record known hashes for files currently on disk.

        Called after an upload, where the hash was just computed from the
        bytes on disk. Two reasons to keep it: the next modified-file scan
        gets a cache hit instead of re-reading the file, and the cache
        becomes a reliable record of what this working copy has actually
        held -- which is how a file the user deleted is told apart from one
        that was never downloaded here.
        """
        updates = {}
        for manifest_key, file_hash in entries.items():
            filesystem_path = self.path_resolver.to_filesystem_path(manifest_key)
            try:
                stat = filesystem_path.stat()
            except OSError:
                continue
            updates[str(Path(manifest_key).as_posix())] = {
                "hash": file_hash,
                "metadata": {
                    "size": stat.st_size,
                    "mtime": stat.st_mtime,
                    "inode": getattr(stat, "st_ino", None),
                },
                "timestamp": time.time(),
            }
        if not updates:
            return
        with self._lock_context():
            self.load_cache()
            self.hash_cache.update(updates)
            self._cache_dirty = True
            self.save_cache()

    def track_modified_files_cached(self, silence=True, keys=None, prune_deleted=True):
        """
        Check manifest for outdated hashes using cached hashing and upload
        changed files in parallel.

        Loads the manifest and cache once at the start, checks all files
        against the in-memory snapshot without holding the lock, and
        batch-writes cache updates at the end.

        :param keys: optional collection of manifest keys to check instead
            of the whole manifest. A sparse working copy passes its
            profile here so the per-commit cost tracks the slice it has
            on disk rather than the size of the repository.
        :param prune_deleted: drop manifest entries for files this working
            copy had and the user deleted. Without it a deletion never
            reaches collaborators: their next sync downloads the file again,
            and keeps doing so forever.
        """
        files_to_upload = []
        deleted = []
        cache_hits = 0
        cache_misses = 0

        # Load manifest and cache once, snapshot stored hashes
        with self._lock_context():
            files_to_check = list(self.manifest["files"].keys())
            stored_hashes = dict(self.manifest["files"])
            self.load_cache()

        if keys is not None:
            allowed = set(keys)
            files_to_check = [key for key in files_to_check if key in allowed]

        if not files_to_check:
            print(
                "No files found in manifest. "
                "Use 's3lfs track <path>' to track files first."
            )
            return

        if not silence:
            print(f"Checking {len(files_to_check)} tracked files for modifications...")

        cache_updates = {}

        # Quiet when the caller asked for quiet. This runs from the
        # pre-commit hook on every commit, where a progress bar for a scan
        # that usually finds nothing is just noise across the terminal.
        with tqdm(
            total=len(files_to_check),
            desc="Checking files",
            unit="file",
            disable=silence,
        ) as pbar:
            for file_path in files_to_check:
                try:
                    fp = Path(file_path)
                    filesystem_path = self.path_resolver.to_filesystem_path(file_path)

                    if not filesystem_path.exists():
                        # Absent for one of two very different reasons. If
                        # this working copy has hashed the file before, it
                        # was here and the user deleted it -- a real change
                        # that should reach collaborators. If it has never
                        # been hashed here, it was simply never downloaded
                        # (a fresh clone, or outside a sparse profile), and
                        # untracking it would delete other people's data.
                        cache_key = str(fp.as_posix())
                        if cache_key in self.hash_cache:
                            deleted.append(file_path)
                        else:
                            print(f"Warning: File {file_path} is missing. Skipping.")
                        pbar.update(1)
                        continue

                    stat = filesystem_path.stat()
                    metadata = {
                        "size": stat.st_size,
                        "mtime": stat.st_mtime,
                        "inode": getattr(stat, "st_ino", None),
                    }
                    file_path_str = str(fp.as_posix())

                    cached_hash = self._check_cache_hit(file_path_str, metadata)
                    if cached_hash is not None:
                        current_hash = cached_hash
                        cache_hits += 1
                    else:
                        current_hash = self.hash_file(filesystem_path)
                        cache_misses += 1
                        cache_updates[file_path_str] = {
                            "hash": current_hash,
                            "metadata": metadata,
                            "timestamp": time.time(),
                        }

                    if current_hash != stored_hashes.get(file_path):
                        print(f"File {file_path} has changed. " f"Marking for upload.")
                        files_to_upload.append(file_path)

                    pbar.set_postfix(
                        {
                            "changed": len(files_to_upload),
                            "cache_hits": cache_hits,
                            "cache_misses": cache_misses,
                        }
                    )
                    pbar.update(1)

                except Exception as e:
                    print(f"Error processing {file_path}: {e}")
                    pbar.update(1)
                    continue

        # Batch-write cache updates in a single lock acquisition
        if cache_updates:
            with self._lock_context():
                self.hash_cache.update(cache_updates)
                self._cache_dirty = True
                self.save_cache()

        if not silence:
            print(
                f"Hash cache performance: " f"{cache_hits} hits, {cache_misses} misses"
            )

        # A wiped working copy is not a deletion. `git clean -xfd` removes
        # every gitignored file, which is every tracked file; so does a
        # stale cache after a manual rm -rf. Untracking in bulk is almost
        # never what someone meant and takes the entries away from everyone,
        # so past a threshold this refuses and asks for an explicit removal.
        bulk = len(deleted) > BULK_DELETION_FLOOR and len(deleted) > (
            len(files_to_check) * BULK_DELETION_FRACTION
        )
        if deleted and prune_deleted and bulk:
            print(
                f"WARNING: {len(deleted)} of {len(files_to_check)} tracked "
                "file(s) are missing -- that looks like a wiped working copy, "
                "not a deletion."
            )
            print(
                "Nothing was untracked. Restore them with 's3lfs checkout "
                "--all', or if you really meant to stop tracking them, use "
                "'s3lfs remove <path>'."
            )
        elif deleted and prune_deleted:
            with self._lock_context():
                self.load_manifest()
                for file_path in deleted:
                    self.manifest["files"].pop(file_path, None)
                self.save_manifest()
            noun = "entry" if len(deleted) == 1 else "entries"
            print(f"Removed {len(deleted)} manifest {noun} for deleted file(s):")
            for file_path in sorted(deleted)[:10]:
                print(f"  {file_path}")
            if len(deleted) > 10:
                print(f"  ... and {len(deleted) - 10} more")
            print("The objects stay in S3 so earlier commits still check out.")
        elif deleted:
            print(
                f"{len(deleted)} tracked file(s) were deleted here but left "
                "in the manifest."
            )

        # Upload files in parallel if needed
        if files_to_upload:
            print(
                f"Uploading {len(files_to_upload)} modified file(s) " f"in parallel..."
            )
            # parallel_upload_chunked commits the manifest itself, reloading
            # under the lock and merging only the keys it uploaded. Saving
            # again here would write this process's older in-memory copy back
            # over anything another process committed in between.
            self.parallel_upload(files_to_upload, silence=silence)
        elif not silence:
            print("No modified files needing upload.")

    def _hash_file_mmap(self, file_path):
        """
        Compute the SHA-256 hash using memory-mapped files.
        """
        with metrics.track("hashing", str(file_path)):
            hasher = hashlib.sha256()
            with open(file_path, "rb") as f:
                with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                    hasher.update(mm)
            return hasher.hexdigest()

    def _hash_file_iter(self, file_path, chunk_size=DEFAULT_BUFFER_SIZE):
        """
        Compute the SHA-256 hash by iteratively reading the file in chunks.
        """
        with metrics.track("hashing", str(file_path)):
            hasher = hashlib.sha256()
            with open(file_path, "rb") as f:
                while chunk := f.read(chunk_size):
                    hasher.update(chunk)
            return hasher.hexdigest()

    def _hash_file_cli(self, file_path):
        """
        Compute the SHA-256 hash using the `sha256sum` CLI utility (POSIX only).
        """
        result = subprocess.run(
            ["sha256sum", str(file_path)],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.split()[0]  # Extract the hash from the output

    def md5_file(self, file_path: Union[str, Path], method: str = "auto") -> str:
        """
        Compute an MD5 hash of the file using its content.
        Supports multiple hashing methods for performance optimization.

        :param file_path: Path to the file to hash.
        :param method: Hashing method to use. Options are:
                    - "auto": Automatically select the best method.
                    - "mmap": Use memory-mapped files (default for non-empty files).
                    - "iter": Use an iterative read approach (fallback for empty files).
                    - "cli": Use the `md5sum` CLI utility (POSIX only).
        :return: The computed MD5 hash as a hexadecimal string.
        """
        file_path = Path(file_path)

        # Ensure the file exists
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Automatically select the best method if "auto" is specified
        if method == "auto":
            if file_path.stat().st_size == 0:  # Empty file
                method = "iter"
            elif sys.platform.startswith("linux") and shutil.which("md5sum"):
                method = "cli"
            elif sys.platform.startswith("darwin") and shutil.which("md5"):
                method = "cli"
            else:
                method = "mmap"

        # Use the selected hashing method
        if method == "mmap":
            return self._md5_file_mmap(file_path)
        elif method == "iter":
            return self._md5_file_iter(file_path)
        elif method == "cli":
            return self._md5_file_cli(file_path)
        else:
            raise ValueError(f"Unsupported MD5 hashing method: {method}")

    def _md5_file_mmap(self, file_path):
        """
        Compute the MD5 hash using memory-mapped files.
        """
        hasher = hashlib.md5()
        with open(file_path, "rb") as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                hasher.update(mm)
        return hasher.hexdigest()

    def _md5_file_iter(self, file_path, chunk_size=DEFAULT_BUFFER_SIZE):
        """
        Compute the MD5 hash by iteratively reading the file in chunks.
        """
        hasher = hashlib.md5()
        with open(file_path, "rb") as f:
            while chunk := f.read(chunk_size):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _md5_file_cli(self, file_path):
        """
        Compute the MD5 hash using the appropriate CLI utility (md5sum on Linux, md5 on macOS).
        """
        if sys.platform.startswith("linux") and shutil.which("md5sum"):
            # Linux: use md5sum
            result = subprocess.run(
                ["md5sum", str(file_path)],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.split()[0]  # Extract the hash from the output
        elif sys.platform.startswith("darwin") and shutil.which("md5"):
            # macOS: use md5 -r (for raw output similar to md5sum)
            result = subprocess.run(
                ["md5", "-r", str(file_path)],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.split()[0]  # Extract the hash from the output
        else:
            raise RuntimeError("No suitable MD5 CLI utility found (md5sum or md5)")

    def compress_file(self, file_path, method="auto"):
        """
        Compress the file using gzip and return the path of the compressed file in the temp directory.

        :param file_path: Path to the file to compress.
        :param method: Compression method to use. Options are:
                    - "auto": Automatically select the best method.
                    - "python": Use Python's gzip module (default).
                    - "cli": Use the `gzip` CLI utility (POSIX only).
        :return: The path to the compressed file.
        """
        file_path = Path(file_path)

        # Ensure the file exists
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Automatically select the best method if "auto" is specified
        if method == "auto":
            if shutil.which("pigz"):
                method = "pigz"
            elif shutil.which("gzip"):
                method = "cli"
            else:
                method = "python"

        # Use the selected compression method
        if method == "python":
            return self._compress_file_python(file_path)
        elif method == "pigz":
            return self._compress_file_pigz(file_path)
        elif method == "cli":
            return self._compress_file_cli(file_path)
        else:
            raise ValueError(f"Unsupported compression method: {method}")

    def _compress_file_python(self, file_path):
        """
        Compress the file deterministically using Python's gzip module.
        """
        with metrics.track("compression", str(file_path)):
            compressed_path = self.temp_dir / f"{uuid4()}.gz"
            buffer_size = DEFAULT_BUFFER_SIZE

            with open(file_path, "rb") as f_in, open(compressed_path, "wb") as f_out:
                with gzip.GzipFile(
                    filename="",  # avoid embedding filename
                    mode="wb",
                    fileobj=f_out,
                    compresslevel=5,
                    mtime=0,  # fixed mtime for determinism
                ) as gz_out:
                    shutil.copyfileobj(f_in, gz_out, length=buffer_size)

            return compressed_path

    def _compress_file_cli(self, file_path):
        """
        Compress the file deterministically using the `gzip` CLI utility.
        """
        compressed_path = self.temp_dir / f"{uuid4()}.gz"

        with open(compressed_path, "wb") as f_out:
            subprocess.run(
                ["gzip", "-n", "-c", "-5", str(file_path)],  # -n = no name/timestamp
                stdout=f_out,
                check=True,
            )

        return compressed_path

    def _compress_file_pigz(self, file_path):
        """
        Compress the file deterministically using pigz (parallel gzip).

        pigz uses all available CPU cores and produces gzip-compatible
        output, so existing stored files remain readable.
        """
        compressed_path = self.temp_dir / f"{uuid4()}.gz"

        with open(compressed_path, "wb") as f_out:
            subprocess.run(
                ["pigz", "-n", "-c", "-5", str(file_path)],
                stdout=f_out,
                check=True,
            )

        return compressed_path

    def decompress_file(self, compressed_path, output_path=None, method="auto"):
        """
        Decompress a file using gzip and return the path of the decompressed file.

        :param compressed_path: Path to the compressed file.
        :param output_path: Path to save the decompressed file. If None, use the same name without the `.gz` extension.
        :param method: Decompression method to use. Options are:
                    - "auto": Automatically select the best method.
                    - "python": Use Python's gzip module (default).
                    - "cli": Use the `gzip` CLI utility (POSIX only).
        :return: The path to the decompressed file.
        """
        compressed_path = Path(compressed_path)

        # Ensure the compressed file exists
        if not compressed_path.exists():
            raise FileNotFoundError(f"Compressed file not found: {compressed_path}")

        # Determine the output path
        if output_path is None:
            output_path = compressed_path.with_suffix("")  # Remove the `.gz` extension
        output_path = Path(output_path)

        # Automatically select the best method if "auto" is specified
        if method == "auto":
            if shutil.which("pigz"):
                method = "pigz"
            elif shutil.which("gzip"):
                method = "cli"
            else:
                method = "python"

        # Use the selected decompression method
        if method == "python":
            return self._decompress_file_python(compressed_path, output_path)
        elif method == "pigz":
            return self._decompress_file_pigz(compressed_path, output_path)
        elif method == "cli":
            return self._decompress_file_cli(compressed_path, output_path)
        else:
            raise ValueError(f"Unsupported decompression method: {method}")

    def _decompress_file_python(self, compressed_path, output_path):
        """
        Decompress the file using Python's gzip module and save it to the output path.
        """
        with metrics.track("decompression", str(output_path)):
            with gzip.open(compressed_path, "rb") as f_in:
                with open(output_path, "wb") as f_out:
                    while True:
                        chunk = f_in.read(DEFAULT_BUFFER_SIZE)
                        if not chunk:
                            break
                        if isinstance(chunk, str):
                            chunk = chunk.encode("utf-8")
                        f_out.write(chunk)

            return output_path

    def _decompress_file_cli(self, compressed_path, output_path):
        """
        Decompress the file using the `gzip` CLI utility and save it to the output path.
        """
        result = subprocess.run(
            ["gzip", "-d", "-c", str(compressed_path)],
            stdout=open(output_path, "wb"),
            check=True,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to decompress file using gzip CLI: {compressed_path}"
            )

        return output_path

    def _decompress_file_pigz(self, compressed_path, output_path):
        """
        Decompress the file using pigz (parallel gzip) and save it to the output path.
        """
        result = subprocess.run(
            ["pigz", "-d", "-c", str(compressed_path)],
            stdout=open(output_path, "wb"),
            check=True,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to decompress file using pigz: {compressed_path}"
            )

        return output_path

    @retry(3, _transient_network_errors)
    def upload(
        self,
        file_path: Union[str, Path],
        silence: bool = False,
        needs_immediate_update: bool = True,
        progress_callback: Optional[Callable[[int], None]] = None,
    ) -> None:
        """
        Upload a file to S3 and update the manifest using the file path as the key.
        """
        file_path = Path(file_path)
        if not file_path.exists():
            print(f"Error: {file_path} does not exist.")
            return

        from botocore.exceptions import ClientError

        file_hash = self.hash_file(file_path)
        # Use manifest key (relative to git root) for S3 key
        manifest_key = self._get_manifest_key(file_path)
        stem = self._asset_base_key(manifest_key, file_hash)

        extra_args = {"ServerSideEncryption": "AES256"} if self.encryption else {}
        source_is_user_file = False
        snapshot = None
        if self._should_compress(file_path):
            s3_key = stem + ".gz"
            compressed_path = self.compress_file(file_path)
        else:
            # Raw storage: the object keeps the file's natural name and
            # exact bytes. Small enough files upload straight from the
            # source with a stat-snapshot guard against concurrent edits;
            # larger ones stage through split_file's temps as before.
            s3_key = stem
            if file_path.stat().st_size <= self.chunk_size:
                snapshot = self._stat_snapshot(file_path)
                compressed_path = file_path
                source_is_user_file = True
            else:
                compressed_path = self.temp_dir / f"{uuid4()}.raw"
                shutil.copyfile(file_path, compressed_path)

        chunked = False
        if compressed_path.stat().st_size > self.chunk_size:
            paths = self.split_file(compressed_path)
            chunked = True
        else:
            paths = [compressed_path]

        for chunk_idx, path in enumerate(paths):
            try:
                if not silence:
                    print(f"Uploading {path}")
                file_size = path.stat().st_size
                # Set up progress callback and context manager
                if progress_callback:
                    # Use the provided callback for progress updates
                    def upload_callback(bytes_transferred):
                        progress_callback(bytes_transferred)

                    context_manager = contextlib.nullcontext()
                elif not silence:
                    # Create individual progress bar only if not silenced
                    progress_bar = tqdm(
                        total=file_size,
                        unit="B",
                        unit_scale=True,
                        desc=f"Uploading {path.name}",
                        leave=False,
                    )

                    def upload_callback(bytes_transferred):
                        progress_bar.update(bytes_transferred)

                    context_manager = progress_bar
                else:
                    # No progress display
                    def upload_callback(bytes_transferred):
                        pass

                    context_manager = contextlib.nullcontext()

                with context_manager:
                    upload_key = s3_key if not chunked else f"{s3_key}.chunk{chunk_idx}"

                    # Compute the local MD5 checksum (streaming)
                    md5_hash = hashlib.md5()
                    with open(path, "rb") as f:
                        while True:
                            block = f.read(DEFAULT_BUFFER_SIZE)
                            if not block:
                                break
                            md5_hash.update(block)
                    local_md5 = md5_hash.hexdigest()

                    # Check if the file already exists in S3
                    try:
                        s3_object = self._get_s3_client().head_object(
                            Bucket=self.bucket_name,
                            Key=upload_key,
                        )
                        s3_etag = s3_object["ETag"].strip('"')
                        if local_md5 == s3_etag:
                            if not silence:
                                print(
                                    f"Skipping upload for {path}, "
                                    f"already exists in S3."
                                )
                            if progress_callback:
                                progress_callback(file_size)
                            continue
                    except ClientError as e:
                        # 404 means not uploaded yet. Anything else -- most
                        # commonly 403 from a write-only policy, since
                        # HeadObject requires s3:GetObject -- means we cannot
                        # check, and the safe response is to upload anyway:
                        # worst case is re-sending bytes that were already
                        # there. Raising here made `track` unusable under
                        # upload-only credentials, a legitimate CI setup.
                        if e.response["Error"]["Code"] not in (
                            "404",
                            "403",
                            "AccessDenied",
                        ):
                            raise
                    with metrics.track("s3_upload", str(path)):
                        with open(path, "rb") as f:
                            self._get_s3_client().upload_fileobj(
                                f,
                                self.bucket_name,
                                upload_key,
                                ExtraArgs=extra_args,
                                Config=self.config,
                                Callback=upload_callback,
                            )
                if not silence:
                    print(f"{path} uploaded")
            finally:
                if not source_is_user_file:
                    try:
                        os.remove(path)
                    except OSError:
                        pass

        if not source_is_user_file:
            try:
                os.remove(compressed_path)  # Ensure temp file is deleted
            except OSError:
                pass

        if snapshot is not None and self._stat_snapshot(file_path) != snapshot:
            # The file changed while streaming out; the stored object may be
            # torn. Refuse to record it so nothing ever references it.
            raise RuntimeError(
                f"{file_path} was modified during upload; run 's3lfs track' "
                "again to upload the current content."
            )

        # Store file path as key, hash as value
        if needs_immediate_update:
            with self._lock_context():
                self.load_manifest()
                manifest_key = self._get_manifest_key(file_path)
                self.manifest["files"][manifest_key] = file_hash
                self.save_manifest()
        if not silence:
            print(f"Uploaded {file_path} -> s3://{self.bucket_name}/{s3_key}")

    def remove_file(self, file_path, keep_in_s3=True):
        """
        Remove a file from tracking.
        If `keep_in_s3` is True, the file will remain in S3 to support previous git commits.
        Otherwise, it will be scheduled for garbage collection.

        :param file_path: The local file path to remove from tracking.
        :param keep_in_s3: If False, schedule the file for deletion in future GC.
        """
        file_path = Path(file_path)
        # Normalise the same way every other call site does. Using the raw
        # argument meant "./data/x.bin" or an absolute path reported the file
        # as untracked when it was tracked under "data/x.bin".
        file_path_str = self._get_manifest_key(file_path)

        with self._lock_context():
            # Re-read under the lock: this process's copy may predate another
            # process's commit, and saving without reloading would erase it.
            self.load_manifest()

            if file_path_str not in self.manifest["files"]:
                print(f"File '{file_path}' is not currently tracked.")
                return

            # Retrieve the file hash before removal
            file_hash = self.manifest["files"].pop(file_path_str, None)
            self.save_manifest()

        print(f"Removed tracking for '{file_path}'.")

        if not keep_in_s3:
            # Objects are stored under the manifest key, so deletion must use
            # it too; the raw argument would miss the object entirely.
            self._delete_asset(self._asset_base_key(file_path_str, file_hash))
        else:
            print(
                f"File remains in S3: s3://{self.bucket_name}/{file_hash}/{file_path_str}"
            )

    def cleanup_s3(self, force=False):
        """
        Remove unreferenced assets from S3 that are not in the current manifest.

        :param force: If True, bypass confirmation (for automated tests).
        """
        with self._lock_context():
            self.load_manifest()
            live_keys = self._live_asset_keys() | self._live_inflight_keys()

        paginator = self._get_s3_client().get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=self.bucket_name, Prefix=f"{self.repo_prefix}/assets/"
        )

        unreferenced_files = []

        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    key = obj["Key"]
                    if not self._is_asset_key(key):
                        continue

                    # Collect unreferenced files
                    if not self._key_covered_by(key, live_keys):
                        unreferenced_files.append(key)

        if not unreferenced_files:
            print("No unreferenced files found in S3.")
            return

        print(f"Found {len(unreferenced_files)} unreferenced files in S3.")

        # If not in test mode, ask for confirmation
        if not force:
            confirm = input("Do you want to delete them? (yes/no): ").strip().lower()
            if confirm != "yes":
                print("Cleanup aborted. No files were deleted.")
                return

        # Re-check under the lock immediately before deleting. Listing S3 and
        # waiting for confirmation both take unbounded time, during which an
        # upload may have claimed or published any of these keys.
        with self._lock_context():
            self.load_manifest()
            live_now = self._live_asset_keys() | self._live_inflight_keys()

        to_delete = []
        for key in unreferenced_files:
            if self._key_covered_by(key, live_now):
                print(f"Skipping {key} (became referenced during cleanup)")
                continue
            to_delete.append(key)

        # Proceed with deletion
        for key in to_delete:
            self._get_s3_client().delete_object(Bucket=self.bucket_name, Key=key)
            print(f"Deleted {key}")

        print("S3 cleanup completed.")

    def track_modified_files(self, silence=True):
        """Check manifest for outdated hashes and upload changed files in parallel."""

        files_to_upload = []
        with self._lock_context():
            files_to_check = list(
                self.manifest["files"].keys()
            )  # Files listed in the manifest

        # Compute hashes in parallel
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            results = zip(files_to_check, executor.map(self.hash_file, files_to_check))

        # Process results
        for file, current_hash in results:
            with self._lock_context():
                stored_hash = self.manifest["files"].get(file)

            if current_hash is None:
                print(f"Warning: File {file} is missing. Skipping.")
                continue

            if current_hash != stored_hash:
                print(f"File {file} has changed. Marking for upload.")
                files_to_upload.append(file)

        # Upload files in parallel if needed
        if files_to_upload:
            print(f"Uploading {len(files_to_upload)} modified file(s) in parallel...")
            # parallel_upload_chunked commits the manifest itself; see the
            # note in track_modified_files_cached.
            self.parallel_upload(files_to_upload, silence=silence)
        else:
            print("No modified files needing upload.")

    def parallel_upload(self, files, silence=True):
        """Upload multiple files with block-level parallelism."""
        self.parallel_upload_chunked(files, silence=silence)

    @staticmethod
    def _stat_snapshot(path):
        """(size, mtime_ns, inode) -- cheap identity for change detection."""
        st = os.stat(path)
        return (st.st_size, st.st_mtime_ns, getattr(st, "st_ino", None))

    def _should_compress(self, file_path):
        """Decide whether compressing this file is worth anything.

        Most large assets -- images, video, model weights, archives -- are
        already compressed, and gzip on such data costs ~20ms/MB to save
        nothing. Worse, it makes the stored object unusable without s3lfs.
        A file stored raw sits in the bucket under its natural name with
        its exact bytes, fetchable by any S3 tool.

        The decision samples the head of the file rather than reading all
        of it: compressibility is a property of the encoding, which does
        not change partway through for the formats that matter.
        """
        if self.compression == "always":
            return True
        if self.compression == "never":
            return False
        try:
            with open(file_path, "rb") as f:
                sample = f.read(COMPRESSION_SAMPLE_BYTES)
        except OSError:
            return True
        if not sample:
            return False
        compressed = gzip.compress(sample, COMPRESSION_SAMPLE_LEVEL)
        return len(compressed) < len(sample) * COMPRESSION_MIN_SAVINGS_RATIO

    def _prepare_file_for_upload(self, file_path):
        """Hash, compress, and split a file into uploadable chunks."""
        file_path = Path(file_path)
        file_hash = self.hash_file(file_path)
        manifest_key = self._get_manifest_key(file_path)

        with self._lock_context():
            stored_hash = self.manifest["files"].get(manifest_key)
        if file_hash == stored_hash:
            return None

        extra_args = {"ServerSideEncryption": "AES256"} if self.encryption else {}

        stem = self._asset_base_key(manifest_key, file_hash)
        if self._should_compress(file_path):
            s3_key = stem + ".gz"
            compressed_path = self.compress_file(file_path)
        else:
            s3_key = stem
            if file_path.stat().st_size <= self.chunk_size:
                # Upload straight from the source file. Copying 200MB to a
                # temp file just to read it back doubles the disk traffic of
                # every raw upload; instead, detect a concurrent edit by
                # comparing a stat snapshot before and after the transfer
                # and refuse to publish a torn object.
                return (
                    manifest_key,
                    file_hash,
                    [
                        {
                            "path": file_path,
                            "s3_key": s3_key,
                            "chunk_index": 0,
                            "extra_args": extra_args,
                            "ephemeral": False,
                            "snapshot": self._stat_snapshot(file_path),
                        }
                    ],
                )
            # Chunked raw: split_file writes chunk temps anyway, which are
            # themselves the snapshot.
            compressed_path = self.temp_dir / f"{uuid4()}.raw"
            shutil.copyfile(file_path, compressed_path)

        if compressed_path.stat().st_size > self.chunk_size:
            chunk_paths = self.split_file(compressed_path)
            try:
                os.remove(compressed_path)
            except OSError:
                pass
            chunks = [
                {
                    "path": p,
                    "s3_key": f"{s3_key}.chunk{i}",
                    "chunk_index": i,
                    "extra_args": extra_args,
                }
                for i, p in enumerate(chunk_paths)
            ]
        else:
            chunks = [
                {
                    "path": compressed_path,
                    "s3_key": s3_key,
                    "chunk_index": 0,
                    "extra_args": extra_args,
                }
            ]

        return (manifest_key, file_hash, chunks)

    @retry(3, _transient_network_errors)
    def _put_chunk(self, path, s3_key, extra_args):
        """PUT one chunk. Retried, so it must not consume its input."""
        with open(path, "rb") as f:
            self._get_s3_client().upload_fileobj(
                f,
                self.bucket_name,
                s3_key,
                ExtraArgs=extra_args,
                Config=self.config,
            )
        return path.stat().st_size

    def _upload_chunk(self, chunk_info):
        """Upload a single compressed chunk to S3.

        The chunk file is removed once, after all retry attempts. Deleting it
        inside the retried call would leave nothing for the next attempt to
        read, so the retry could only ever fail.
        """
        path = chunk_info["path"]
        s3_key = chunk_info["s3_key"]
        extra_args = chunk_info["extra_args"]
        # Ephemeral paths are temp files this pipeline created and owns.
        # A non-ephemeral path is the user's own file, uploaded in place --
        # deleting it would destroy their data.
        ephemeral = chunk_info.get("ephemeral", True)

        # Queued work should not start after an interrupt. Only the drain
        # loops checked this, so every task already submitted to the pool ran
        # to completion and Ctrl-C appeared to hang on large transfers.
        if self._shutdown_requested:
            if ephemeral:
                try:
                    os.remove(path)
                except OSError:
                    pass
            raise ShutdownRequested(f"Upload cancelled: {s3_key}")

        try:
            bytes_uploaded = self._put_chunk(path, s3_key, extra_args)
        finally:
            if ephemeral:
                try:
                    os.remove(path)
                except OSError:
                    pass

        snapshot = chunk_info.get("snapshot")
        if snapshot is not None and self._stat_snapshot(path) != snapshot:
            # The file changed while its bytes were streaming out: the
            # object under this hash may be torn. Refusing here keeps the
            # entry out of the manifest, so nothing ever references it.
            raise RuntimeError(
                f"{path} was modified during upload; run 's3lfs track' "
                "again to upload the current content."
            )

        return (s3_key, bytes_uploaded)

    def parallel_upload_chunked(self, files, silence=True):
        """Upload files with block-level parallelism.

        Preparation futures (hash + compress + split) are collected via
        as_completed; as each resolves, chunk upload futures are
        submitted into the same pool.
        """
        if not files:
            print("Nothing to upload.")
            return

        self.test_s3_credentials(silence=silence)

        manifest_updates = {}
        # manifest_key -> {hash, expected, done}. A file earns its manifest
        # entry only once every one of its chunks has landed in S3.
        pending = {}
        # Hashes claimed in the in-flight registry, released once the manifest
        # has been written.
        claimed = set()
        total_bytes = 0
        total_chunks = 0
        chunks_done = 0

        try:
            with tqdm(desc="Uploading", unit="chunk") as pbar:
                with ThreadPoolExecutor(max_workers=self.workers) as executor:
                    # Phase 1: submit all prep tasks
                    prep_futures = {
                        executor.submit(self._prepare_file_for_upload, f): f
                        for f in files
                    }

                    # Phase 2: as preps complete, submit uploads
                    ul_futures = {}
                    for prep_future in as_completed(prep_futures):
                        if self._shutdown_requested:
                            break
                        try:
                            result = prep_future.result()
                        except Exception as e:
                            print(f"Error preparing file: {e}")
                            continue

                        if result is None:
                            continue

                        manifest_key, file_hash, chunks = result

                        # Claim the asset before submitting any chunk upload.
                        # Between a chunk landing in S3 and the manifest entry
                        # being published, the object is unreferenced and a
                        # concurrent cleanup_s3 would otherwise delete it.
                        base_key = self._asset_base_key(manifest_key, file_hash)
                        self._claim_inflight(base_key)
                        claimed.add(base_key)

                        pending[manifest_key] = {
                            "hash": file_hash,
                            "expected": len(chunks),
                            "done": 0,
                        }
                        total_chunks += len(chunks)
                        pbar.total = total_chunks
                        pbar.refresh()

                        for chunk in chunks:
                            f = executor.submit(self._upload_chunk, chunk)
                            ul_futures[f] = (manifest_key, chunk)

                    # Phase 3: collect upload results
                    for ul_future in as_completed(ul_futures):
                        if self._shutdown_requested:
                            break
                        manifest_key, chunk = ul_futures[ul_future]
                        try:
                            _, bytes_uploaded = ul_future.result()
                        except Exception as e:
                            if not isinstance(e, ShutdownRequested):
                                print(f"Error uploading " f"{chunk['s3_key']}: {e}")
                            continue

                        entry = pending[manifest_key]
                        entry["done"] += 1
                        if entry["done"] == entry["expected"]:
                            manifest_updates[manifest_key] = entry["hash"]

                        total_bytes += bytes_uploaded
                        chunks_done += 1
                        pbar.update(1)

        except KeyboardInterrupt:
            print("\nUpload interrupted by user.")
        finally:
            if manifest_updates:
                with self._lock_context():
                    self.load_manifest()
                    self.manifest["files"].update(manifest_updates)
                    self.save_manifest()

            # Strictly after the manifest is published. Releasing first would
            # reopen the window the registry exists to close: the hash would be
            # in neither the manifest nor the registry, and a sweep running in
            # between would delete the objects.
            self._release_inflight(claimed)

            # Files whose chunks did not all land are deliberately absent from
            # the manifest. Recording them would leave an entry pointing at an
            # incomplete object, and checkout infers the chunk count from the
            # objects that exist -- so a missing tail reassembles into a
            # truncated file with no error raised anywhere.
            incomplete = sorted(
                key for key, v in pending.items() if v["done"] != v["expected"]
            )
            if incomplete:
                print(
                    f"WARNING: {len(incomplete)} file(s) did not upload "
                    f"completely and were NOT added to the manifest:"
                )
                for key in incomplete[:10]:
                    v = pending[key]
                    print(f"  {key} ({v['done']}/{v['expected']} chunks)")
                if len(incomplete) > 10:
                    print(f"  ... and {len(incomplete) - 10} more")

            print(
                f"Uploaded {len(manifest_updates)} file(s) "
                f"({chunks_done} chunks, "
                f"{total_bytes / (1024 * 1024):.1f} MB compressed)."
            )

    def parallel_download_all(self, silence=True, only=None, preserve_modified=False):
        """Download all files using block-level parallelism.

        :param only: optional collection of manifest keys to limit the
            download to, used to honour a sparse profile.
        :param preserve_modified: if True, never overwrite a file whose
            content differs from the manifest. Tracked files are gitignored,
            so git cannot warn about local edits to them; an automatic
            caller (a hook) must not destroy work the user has not uploaded.
        """
        with self._lock_context():
            items = list(self.manifest["files"].items())

        if only is not None:
            only = set(only)
            items = [(key, file_hash) for key, file_hash in items if key in only]

        if not items:
            print("Manifest is empty. Nothing to download.")
            return

        print("Starting block-level parallel download of all tracked files...")

        files_to_download = []
        modified = []
        for manifest_key, file_hash in items:
            filesystem_path = self.path_resolver.to_filesystem_path(manifest_key)
            if filesystem_path.exists():
                current_hash = self.hash_file(filesystem_path)
                if current_hash == file_hash:
                    if not silence:
                        print(f"  Skipping {manifest_key} (up-to-date)")
                    continue
                if preserve_modified:
                    modified.append(manifest_key)
                    continue
            files_to_download.append((manifest_key, file_hash))

        if modified:
            print(
                f"Keeping {len(modified)} locally modified file(s); "
                "upload with 's3lfs track --modified' or overwrite with "
                "'s3lfs checkout --all':"
            )
            for manifest_key in sorted(modified)[:10]:
                print(f"  {manifest_key}")
            if len(modified) > 10:
                print(f"  ... and {len(modified) - 10} more")

        if not files_to_download:
            print("All files are up-to-date.")
            return 0

        return self.parallel_download_chunked(files_to_download, silence=silence)

    @retry(3, _transient_network_errors)
    def _discover_chunks_for_file(self, manifest_key, file_hash):
        """Discover the stored object(s) for a file, raw or compressed."""
        keys, _sizes, is_chunked, compressed = self._locate_asset(
            manifest_key, file_hash
        )
        return [
            {
                "manifest_key": manifest_key,
                "file_hash": file_hash,
                "s3_key": key,
                "chunk_index": i,
                "is_chunked": is_chunked,
                "num_chunks": len(keys),
                "compressed": compressed,
            }
            for i, key in enumerate(keys)
        ]

    @retry(3, _transient_network_errors)
    def _download_chunk(self, chunk_info, target_path):
        """Download a single S3 chunk to a target path."""
        # See _upload_chunk: queued work must not start after an interrupt.
        if self._shutdown_requested:
            raise ShutdownRequested(f"Download cancelled: {chunk_info['s3_key']}")

        with open(target_path, "wb") as f:
            writer = _HashingWriter(f)
            self._get_s3_client().download_fileobj(
                Bucket=self.bucket_name,
                Key=chunk_info["s3_key"],
                Fileobj=writer,
            )
        return (
            chunk_info["manifest_key"],
            chunk_info["chunk_index"],
            target_path,
            target_path.stat().st_size,
            chunk_info["is_chunked"],
            chunk_info["num_chunks"],
            writer.hexdigest(),
        )

    def _finalize_file(
        self,
        manifest_key,
        chunk_paths,
        is_chunked,
        expected_hash=None,
        silence=True,
        compressed=True,
        stream_hash=None,
    ):
        """Merge chunks (if needed) and decompress to final location.

        If expected_hash is given, the reassembled file is hashed and compared
        against it. Chunk discovery infers the chunk count from the objects
        that happen to exist, so a partial upload whose tail is missing
        reassembles into a shorter file that is otherwise well-formed; without
        this check nothing downstream would notice.
        """
        filesystem_path = self.path_resolver.to_filesystem_path(manifest_key)

        if is_chunked and len(chunk_paths) > 1:
            compressed_path = self.temp_dir / f"{uuid4()}.gz"
            self.merge_files(compressed_path, chunk_paths)
            for p in chunk_paths:
                try:
                    os.remove(p)
                except OSError:
                    pass
        else:
            compressed_path = chunk_paths[0]

        os.makedirs(os.path.dirname(filesystem_path), exist_ok=True)
        try:
            if compressed:
                self.decompress_file(compressed_path, filesystem_path)
            else:
                shutil.move(str(compressed_path), str(filesystem_path))
        finally:
            try:
                os.remove(compressed_path)
            except OSError:
                pass

        if expected_hash is not None:
            # stream_hash was computed while the bytes were written; a
            # re-read of the finished file tells us nothing it does not.
            actual_hash = (
                stream_hash
                if stream_hash is not None
                else self.hash_file(filesystem_path)
            )
            if actual_hash != expected_hash:
                # Remove the bad output rather than leave it looking valid.
                try:
                    os.remove(filesystem_path)
                except OSError:
                    pass
                raise RuntimeError(
                    f"Checksum mismatch for {manifest_key}: "
                    f"expected {expected_hash}, got {actual_hash}. "
                    f"The stored object is incomplete or corrupt."
                )

    def parallel_download_chunked(self, file_items, silence=True):
        """Download files with block-level parallelism.

        Discovery and download share a single thread pool.  Discovery
        futures are collected via as_completed; as each resolves, its
        chunk download futures are submitted into the same pool.  A
        second as_completed pass collects download results and triggers
        per-file finalization when all chunks land.
        """
        if not file_items:
            print("Nothing to download.")
            return

        self.test_s3_credentials(silence=silence)

        file_tracker = {}
        files_finalized = 0
        files_failed = 0
        total_bytes = 0
        total_chunks = 0
        chunks_done = 0

        try:
            with tqdm(desc="Downloading", unit="chunk") as pbar:
                with ThreadPoolExecutor(max_workers=self.workers) as executor:
                    # Phase 1: submit all discovery tasks
                    disc_futures = {
                        executor.submit(self._discover_chunks_for_file, mk, fh): mk
                        for mk, fh in file_items
                    }

                    # Phase 2: as discoveries complete, submit downloads
                    dl_futures = {}
                    for disc_future in as_completed(disc_futures):
                        if self._shutdown_requested:
                            break
                        try:
                            chunks = disc_future.result()
                        except Exception as e:
                            print(f"Error discovering chunks: {e}")
                            files_failed += 1
                            continue

                        mk = chunks[0]["manifest_key"]
                        file_tracker[mk] = {
                            "expected": len(chunks),
                            "received": [],
                            "is_chunked": chunks[0]["is_chunked"],
                            "file_hash": chunks[0]["file_hash"],
                            "compressed": chunks[0]["compressed"],
                        }
                        total_chunks += len(chunks)
                        pbar.total = total_chunks
                        pbar.refresh()

                        for chunk in chunks:
                            target = self.temp_dir / f"{uuid4()}.gz"
                            f = executor.submit(self._download_chunk, chunk, target)
                            dl_futures[f] = chunk

                    # Phase 3: collect download results, finalize files
                    for dl_future in as_completed(dl_futures):
                        if self._shutdown_requested:
                            break
                        try:
                            (
                                manifest_key,
                                chunk_index,
                                target_path,
                                bytes_downloaded,
                                is_chunked,
                                num_chunks,
                                stream_digest,
                            ) = dl_future.result()
                        except Exception as e:
                            chunk = dl_futures[dl_future]
                            if not isinstance(e, ShutdownRequested):
                                print(f"Error downloading " f"{chunk['s3_key']}: {e}")
                            continue

                        total_bytes += bytes_downloaded
                        chunks_done += 1
                        pbar.update(1)

                        entry = file_tracker.get(manifest_key)
                        if entry is None:
                            continue
                        entry["received"].append(
                            (chunk_index, target_path, stream_digest)
                        )

                        if len(entry["received"]) == entry["expected"]:
                            entry["received"].sort(key=lambda x: x[0])
                            chunk_paths = [p for _, p, _ in entry["received"]]
                            # Raw unchunked: the stream digest IS the file
                            # hash, so finalize can verify without re-reading.
                            stream_hash = (
                                entry["received"][0][2]
                                if not entry["compressed"] and not entry["is_chunked"]
                                else None
                            )
                            try:
                                self._finalize_file(
                                    manifest_key,
                                    chunk_paths,
                                    entry["is_chunked"],
                                    expected_hash=entry["file_hash"],
                                    silence=silence,
                                    compressed=entry["compressed"],
                                    stream_hash=stream_hash,
                                )
                                files_finalized += 1
                            except Exception as e:
                                # One corrupt file must not abandon the rest.
                                print(f"ERROR: {manifest_key}: {e}")
                                files_failed += 1

        except KeyboardInterrupt:
            print("\nDownload interrupted by user.")
        finally:
            print(
                f"Downloaded {files_finalized} file(s) "
                f"({chunks_done} chunks, "
                f"{total_bytes / (1024 * 1024):.1f} MB compressed)."
            )

            # A file whose chunks did not all arrive is never finalized. Say so
            # and drop its partial chunks, rather than leaving the caller to
            # infer it from the count and the temp files behind.
            incomplete = sorted(
                mk
                for mk, e in file_tracker.items()
                if len(e["received"]) != e["expected"]
            )
            for mk in incomplete:
                for _, path, _ in file_tracker[mk]["received"]:
                    try:
                        os.remove(path)
                    except OSError:
                        pass

            if incomplete or files_failed:
                print(
                    f"WARNING: {len(incomplete) + files_failed} file(s) were "
                    f"not written:"
                )
                for mk in incomplete[:10]:
                    tracked = file_tracker[mk]
                    print(
                        f"  {mk} (received {len(tracked['received'])}/"
                        f"{tracked['expected']} chunks)"
                    )
                if len(incomplete) > 10:
                    print(f"  ... and {len(incomplete) - 10} more")

        # Incomplete files were never written; that is a failure too.
        return files_failed + len(incomplete)

    def remove_subtree(self, directory, keep_in_s3=True):
        """
        Remove files matching a pattern from tracking.
        Handles single files, directories, and glob patterns uniformly.
        Optionally keep the files in S3 for historical reference.

        :param directory: The path, directory, or glob pattern to remove from tracking.
        :param keep_in_s3: If False, delete the files from S3 as well.
        """
        directory = Path(directory)
        pattern = str(directory.as_posix())

        with self._lock_context():
            # Match, mutate, and save in a single critical section against a
            # fresh read. Matching under one lock and saving under a later one
            # lets another process commit in between, and the save would then
            # write this process's older copy back over it.
            self.load_manifest()

            # Try matching with the pattern as-is (handles files and glob patterns)
            files_to_remove = [
                path
                for path in self.manifest["files"]
                if fnmatch.fnmatch(path, pattern)
            ]

            # If no matches, try as directory by appending /*
            # This handles cases like "dir" -> "dir/*" or "dir*" -> "dir*/*"
            if not files_to_remove:
                dir_pattern = pattern.rstrip("/") + "/*"
                files_to_remove = [
                    path
                    for path in self.manifest["files"]
                    if fnmatch.fnmatch(path, dir_pattern)
                ]

            if not files_to_remove:
                print(f"No tracked files found matching '{directory}'.")
                return

            removed_hashes = {
                file_path: self.manifest["files"].pop(file_path, None)
                for file_path in files_to_remove
            }
            self.save_manifest()

        # S3 deletion is network I/O and must not hold the manifest lock.
        if not keep_in_s3:
            for file_path, file_hash in removed_hashes.items():
                if not file_hash:
                    continue
                self._delete_asset(self._asset_base_key(file_path, file_hash))

        count = len(files_to_remove)
        print(
            f"Removed tracking for {count} file{'s' if count != 1 else ''} matching '{directory}'."
        )

    def test_s3_credentials(self, silence=False):
        """
        Test the S3 credentials to ensure they are valid for the target bucket.
        This prevents repeated failures during bulk operations.

        :param silence: If True, suppress success messages.
        """
        from botocore.exceptions import (
            ClientError,
            NoCredentialsError,
            PartialCredentialsError,
        )

        try:
            # Attempt to list objects in the target bucket with a minimal prefix
            self._get_s3_client().list_objects_v2(
                Bucket=self.bucket_name, MaxKeys=1, Prefix=""
            )
            if not silence:
                print(f"S3 credentials are valid for bucket '{self.bucket_name}'.")
        except NoCredentialsError:
            raise RuntimeError(ERROR_MESSAGES["no_credentials"])
        except PartialCredentialsError:
            raise RuntimeError(ERROR_MESSAGES["partial_credentials"])
        except ClientError as e:
            if e.response["Error"]["Code"] in [
                "InvalidAccessKeyId",
                "SignatureDoesNotMatch",
                "AccessDenied",
            ]:
                raise RuntimeError(
                    ERROR_MESSAGES["s3_access_denied"].format(
                        bucket_name=self.bucket_name
                    )
                )
            raise RuntimeError(f"Error testing S3 credentials: {e}")

    def _get_manifest_key(self, file_path: Union[str, Path]) -> str:
        """
        Convert a file path to a manifest key (relative to git root).

        :param file_path: Absolute or relative file path
        :return: Path relative to git root as string (POSIX format)
        """
        # Use PathResolver for consistent path handling
        return self.path_resolver.to_manifest_key(file_path)

    def _is_internal_path(self, path: Union[str, Path]) -> bool:
        """
        Is this a file s3lfs must never track?

        Covers git's own metadata and s3lfs's own bookkeeping. Filesystem
        enumeration uses rglob("*"), which matches dotfiles, so without this
        `track .` walks into .git/ and also picks up the manifest, the hash
        cache, and the lock file.

        :param path: Absolute or relative file path
        :return: True if the path is internal and must be skipped
        """
        resolved = Path(path).resolve()
        try:
            parts = resolved.relative_to(self.path_resolver.git_root).parts
        except ValueError:
            # Outside the repository; fall back to the whole path.
            parts = resolved.parts

        if ".git" in parts:
            return True
        if ".s3lfs_temp" in parts or self.temp_dir.name in parts:
            return True
        # Manifest shards are the manifest. Tracking them would upload the
        # index of what is tracked into the store it indexes.
        if MANIFEST_SHARD_DIR in parts:
            return True
        if resolved.name in {self.manifest_file.name, self.cache_file.name}:
            return True
        if resolved.name.endswith(".s3lfs.lock"):
            return True
        return False

    def _resolve_filesystem_paths(self, path):
        """
        FILESYSTEM GLOB: Find files on disk matching a pattern.

        This is used for TRACKING operations where we need to find actual files
        on the filesystem (which may not be in the manifest yet).

        The glob pattern is applied against the filesystem, not the manifest.

        :param path: Either a manifest key (relative to git root) or an absolute path.
                     Could be:
                     - A file: "subdir/file.txt" or "/repo/subdir/file.txt"
                     - A directory: "subdir/" or "/repo/subdir/"
                     - A glob pattern: "subdir/*.txt" or "/repo/subdir/*.txt"
        :return: List of Path objects for files found on disk (as absolute paths)

        Example:
            User in /repo/subdir types: "*.txt"
            CLI converts to manifest key: "subdir/*.txt"
            This method converts to filesystem path: "/repo/subdir/*.txt"
            Glob finds actual files: ["/repo/subdir/a.txt", "/repo/subdir/b.txt"]
        """
        # Handle both manifest keys and absolute paths
        path_obj = Path(path)
        if path_obj.is_absolute():
            # Already an absolute path, use as-is
            filesystem_path = path_obj
        else:
            # Convert manifest key to filesystem path (prepends git_root)
            # For example: "subdir/file.txt" -> "/repo/subdir/file.txt"
            # For globs: "subdir/*.txt" -> "/repo/subdir/*.txt"
            filesystem_path = self.path_resolver.to_filesystem_path(path)

        # If it's an existing file, return it directly
        if filesystem_path.is_file():
            resolved_files = [filesystem_path]
        # If it's an existing directory, get all files recursively
        elif filesystem_path.is_dir():
            resolved_files = [f for f in filesystem_path.rglob("*") if f.is_file()]
        else:
            # Otherwise treat as a glob pattern against the filesystem
            matched_paths = glob.glob(str(filesystem_path), recursive=True)

            # Handle both files and directories that match the pattern
            resolved_files = []
            for p in matched_paths:
                path_obj = Path(p)
                if path_obj.is_file():
                    resolved_files.append(path_obj)
                elif path_obj.is_dir():
                    # For directories, find all files recursively
                    resolved_files.extend(
                        [f for f in path_obj.rglob("*") if f.is_file()]
                    )

        # Return absolute paths, less anything internal to git or s3lfs
        return [p.resolve() for p in resolved_files if not self._is_internal_path(p)]

    def _resolve_manifest_paths(self, path):
        """
        MANIFEST GLOB: Find files in the manifest matching a pattern.

        This is used for CHECKOUT, REMOVE, and LS operations where we need to find
        files that are already tracked in the manifest.

        The glob pattern is applied against manifest keys, not the filesystem.

        :param path: Manifest key (relative to git root) that could be:
                     - A file: "subdir/file.txt"
                     - A directory: "subdir/"
                     - A glob pattern: "subdir/*.txt" or "dir*/file*"
        :return: Dictionary of manifest entries {manifest_key: hash}

        Example:
            User in /repo/subdir types: "*.txt"
            CLI converts to manifest key: "subdir/*.txt"
            This method matches against manifest keys: {"subdir/a.txt": "hash1", "subdir/b.txt": "hash2"}
            Files may or may not exist on disk - we're just finding tracked files.
        """
        # Convert absolute paths to manifest keys (relative to git root)
        path_obj = Path(path)
        if path_obj.is_absolute():
            path_str = self.path_resolver.to_manifest_key(path_obj)
        else:
            path_str = str(path_obj.as_posix())

        with self._lock_context():
            manifest_files = self.manifest["files"]

            # Try matching with the pattern as-is (handles files and glob patterns)
            matched_files = {}
            for file_path, file_hash in manifest_files.items():
                if self._glob_match(file_path, path_str):
                    matched_files[file_path] = file_hash

            # If no matches, try as directory by appending /**
            # This handles cases like "dir" -> "dir/**" (recursive)
            # This matches filesystem behavior where specifying a directory
            # returns all files recursively within it
            if not matched_files:
                dir_pattern = path_str.rstrip("/") + "/**"
                for file_path, file_hash in manifest_files.items():
                    if self._glob_match(file_path, dir_pattern):
                        matched_files[file_path] = file_hash

            return matched_files

    def _glob_match(self, file_path, pattern):
        """
        Glob matching that behaves like filesystem glob (glob.glob semantics).

        Follows glob.glob rules:
        - * matches within a directory level (doesn't cross /)
        - ** matches recursively across directories (zero or more levels)
        - ? matches a single character (not /)

        This ensures MANIFEST GLOB and FILESYSTEM GLOB are consistent.

        :param file_path: The file path to test (manifest key)
        :param pattern: The glob pattern
        :return: True if the file path matches the pattern
        """
        # Handle ** recursive patterns
        if "**" in pattern:
            # Convert pattern to regex for matching
            # ** can match zero or more directory levels
            # Examples:
            #   "**/file.txt" -> matches "file.txt" and "a/b/file.txt"
            #   "a/**" -> matches "a/b" and "a/b/c"
            #   "a/**/file.txt" -> matches "a/file.txt" and "a/b/c/file.txt"

            regex_pattern = pattern

            # Replace **/ with marker (zero or more directories with trailing /)
            regex_pattern = regex_pattern.replace("**/", "\x00DOUBLESTAR_SLASH\x00")

            # Replace /** with marker (/ followed by zero or more directories)
            regex_pattern = regex_pattern.replace("/**", "\x00SLASH_DOUBLESTAR\x00")

            # Replace remaining ** (standalone) with marker
            regex_pattern = regex_pattern.replace("**", "\x00DOUBLESTAR\x00")

            # Escape regex special chars
            regex_pattern = re.escape(regex_pattern)

            # Replace * with [^/]* (match anything except /)
            regex_pattern = regex_pattern.replace(r"\*", "[^/]*")

            # Replace ? with [^/] (match single char except /)
            regex_pattern = regex_pattern.replace(r"\?", "[^/]")

            # Replace markers with appropriate regex
            # **/ -> (?:.*/)?  (zero or more dirs with trailing /, optional)
            regex_pattern = regex_pattern.replace(
                "\x00DOUBLESTAR_SLASH\x00", "(?:.*/)?"
            )

            # /** -> (?:/.*)?  (optional / with zero or more dirs)
            regex_pattern = regex_pattern.replace(
                "\x00SLASH_DOUBLESTAR\x00", "(?:/.*)?"
            )

            # ** standalone -> .*  (match anything)
            regex_pattern = regex_pattern.replace("\x00DOUBLESTAR\x00", ".*")

            # Anchor the pattern
            regex_pattern = f"^{regex_pattern}$"

            return bool(re.match(regex_pattern, file_path))
        else:
            # For non-** patterns, match segment by segment
            # This ensures * doesn't cross directory boundaries
            pattern_parts = pattern.split("/")
            file_parts = file_path.split("/")

            # Pattern and file must have the same number of segments for exact match
            # (No prefix matching - that's handled by the caller appending /*)
            if len(pattern_parts) != len(file_parts):
                return False

            # Match each pattern segment against corresponding file segment
            for pattern_part, file_part in zip(pattern_parts, file_parts):
                if not fnmatch.fnmatch(file_part, pattern_part):
                    return False

            # All segments matched
            return True

    def track(self, path, silence=True, interleaved=True, use_cache=True):
        """
        Track and upload files, directories, or globs.

        :param path: A file, directory, or glob pattern to track.
        :param silence: Silences verbose logging.
        :param interleaved: If True, use interleaved hashing and uploading for better performance.
        :param use_cache: If True, use cached hashing for better performance on repeated operations.
        """
        if interleaved:
            return self.track_interleaved(path, silence=silence, use_cache=use_cache)

        # Original two-stage implementation
        # Phase 1: Resolve filesystem paths and compute hashes
        print("Resolving filesystem paths and computing hashes...")
        files_to_track = self._resolve_filesystem_paths(path)

        if not files_to_track:
            print(f"No files found to track for '{path}'.")
            return

        # Compute hashes in parallel with a progress bar
        with tqdm(total=len(files_to_track), desc="Hashing files", unit="file") as pbar:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                if use_cache:

                    def hash_func(f):
                        return self._hash_with_progress_cached(f, pbar)

                else:

                    def hash_func(f):
                        return self._hash_with_progress(f, pbar)

                file_hashes = {
                    str(file.as_posix()): hash_result
                    for file, hash_result in zip(
                        files_to_track,
                        executor.map(hash_func, files_to_track),
                    )
                }

        # Phase 2: Lock the manifest and determine which files need updates
        print("Locking manifest to determine files needing updates...")
        with self._lock_context():
            files_to_upload = []
            for file_path, current_hash in file_hashes.items():
                stored_hash = self.manifest["files"].get(file_path)
                if current_hash != stored_hash:
                    files_to_upload.append((file_path, current_hash))

        if not files_to_upload:
            print("All files are up-to-date. No uploads needed.")
            return

        print(f"{len(files_to_upload)} files need to be uploaded.")

        # Test S3 credentials once before starting parallel operations
        if not silence:
            print("Testing S3 credentials...")
        self.test_s3_credentials(silence=silence)

        # Phase 3: Upload files needing updates
        print("Uploading files...")
        try:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                futures = [
                    executor.submit(
                        self.upload,
                        file_path,
                        silence=silence,
                        needs_immediate_update=False,
                    )
                    for file_path, _ in files_to_upload
                ]

                for future in tqdm(
                    as_completed(futures), total=len(futures), desc="Uploading files"
                ):
                    if self._shutdown_requested:
                        print("Shutdown requested. Cancelling remaining uploads...")
                        return

                    try:
                        future.result()  # Will re-raise exceptions from the worker thread
                    except Exception as e:
                        print(f"An error occurred during upload: {e}")
                        raise

        except KeyboardInterrupt:
            print("\nUpload interrupted by user.")
            return

        with self._lock_context():
            self.load_manifest()
            # Phase 4: Lock the manifest and update it
            for file_path, file_hash in files_to_upload:
                manifest_key = self._get_manifest_key(file_path)
                self.manifest["files"][manifest_key] = file_hash
            self.save_manifest()

        print(f"Successfully tracked and uploaded files for '{path}'.")

    def _hash_with_progress_cached(self, file_path, progress_bar):
        """
        Helper function to compute the cached hash of a file and update the progress bar.
        """
        result = self.hash_file_cached(file_path)
        progress_bar.update(1)
        return result

    def _hash_with_progress(self, file_path, progress_bar):
        """
        Helper function to compute the hash of a file and update the progress bar.
        """
        result = self.hash_file(file_path)
        progress_bar.update(1)
        return result

    def checkout(self, path, silence=True, interleaved=True, use_cache=True):
        """
        Checkout files, directories, or globs from the manifest.

        :param path: A file, directory, or glob pattern to checkout.
        :param silence: Silences verbose logging.
        :param interleaved: If True, use interleaved hashing and downloading for better performance.
        :param use_cache: If True, use cached hashing for better performance on repeated operations.
        """
        if interleaved:
            return self.checkout_interleaved(path, silence=silence, use_cache=use_cache)

        # Original two-stage implementation
        # Phase 1: Resolve manifest paths using improved globbing
        print("Resolving paths from manifest...")
        files_to_checkout = self._resolve_manifest_paths(path)

        if not files_to_checkout:
            print(f"No files found in the manifest for '{path}'.")
            return

        print(f"Found {len(files_to_checkout)} files to check out.")

        # Phase 2: Hash files to determine which need to be downloaded
        print("Hashing files to determine which need to be downloaded...")
        files_to_download = []
        file_hashes = {}

        with tqdm(
            total=len(files_to_checkout), desc="Hashing files", unit="file"
        ) as pbar:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                if use_cache:

                    def hash_func(f):
                        return self._hash_with_progress_cached(f, pbar)

                else:

                    def hash_func(f):
                        return self._hash_with_progress(f, pbar)

                future_to_file = {
                    executor.submit(
                        hash_func, self.path_resolver.to_filesystem_path(file)
                    ): file
                    for file in files_to_checkout.keys()
                    if self.path_resolver.to_filesystem_path(
                        file
                    ).exists()  # Only hash files that exist on disk
                }

                for future in as_completed(future_to_file):
                    file = future_to_file[future]
                    try:
                        file_hashes[file] = future.result()
                    except Exception as exc:
                        print(f"Error hashing file {file}: {exc}")

        # Add files that don't exist on disk to the download list
        for file in files_to_checkout.keys():
            if not self.path_resolver.to_filesystem_path(file).exists():
                files_to_download.append(file)
            elif file_hashes.get(file) != files_to_checkout[file]:
                files_to_download.append(file)

        if not files_to_download:
            print("All files are up-to-date. No downloads needed.")
            return

        print(f"{len(files_to_download)} files need to be downloaded.")

        file_items = [(f, files_to_checkout[f]) for f in files_to_download]
        self.parallel_download_chunked(file_items, silence=silence)

    def merge_files(self, output_path, chunk_paths):
        """
        Merge multiple chunk files into a single file.

        :param output_path: Path to the output file.
        :param chunk_paths: List of chunk file paths to merge.
        :return: Path to the merged file.
        """
        with open(output_path, "wb") as output_file:
            for chunk_path in chunk_paths:
                with open(chunk_path, "rb") as chunk_file:
                    shutil.copyfileobj(chunk_file, output_file)

        return output_path

    def split_file(self, file_path):
        """
        Split a file into smaller chunks.

        :param file_path: Path to the file to split.
        :param chunk_size: Size of each chunk in bytes (default: 5 GB).
        :return: List of chunk file paths.
        """
        file_path = Path(file_path)
        chunk_paths = []

        max_chunk_bytes = self.chunk_size - 1
        with open(file_path, "rb") as f:
            chunk_index = 0
            while True:
                chunk_path = Path(f"{file_path}.chunk{chunk_index}")
                bytes_written = 0
                wrote_any = False

                with open(chunk_path, "wb") as chunk_file:
                    while bytes_written < max_chunk_bytes:
                        to_read = min(
                            DEFAULT_BUFFER_SIZE,
                            max_chunk_bytes - bytes_written,
                        )
                        block = f.read(to_read)
                        if not block:
                            break
                        chunk_file.write(block)
                        bytes_written += len(block)
                        wrote_any = True

                if not wrote_any:
                    # Nothing left to read; remove the empty file
                    chunk_path.unlink(missing_ok=True)
                    break

                chunk_paths.append(chunk_path)
                chunk_index += 1

        return chunk_paths

    def _hash_and_upload_worker(
        self, file_path, silence=True, progress_callback=None, use_cache=True
    ):
        """
        Worker function that hashes a file and uploads it if needed.
        Returns (file_path, hash, uploaded, bytes_transferred) tuple.

        :param file_path: Path to the file to process
        :param silence: Whether to suppress individual file progress bars
        :param progress_callback: Optional callback function for progress updates
        :param use_cache: Whether to use cached hashing for performance
        """
        try:
            if use_cache:
                current_hash = self.hash_file_cached(file_path)
            else:
                current_hash = self.hash_file(file_path)

            # Check if upload is needed
            manifest_key = self._get_manifest_key(file_path)
            with self._lock_context():
                stored_hash = self.manifest["files"].get(manifest_key)

            if current_hash == stored_hash:
                return (file_path, current_hash, False, 0)  # No upload needed

            # Get file size for progress tracking
            file_size = Path(file_path).stat().st_size

            # Upload the file with progress callback
            self.upload(
                file_path,
                silence=True,
                needs_immediate_update=False,
                progress_callback=progress_callback,
            )
            return (file_path, current_hash, True, file_size)  # Upload completed

        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            raise

    def _hash_and_download_worker(
        self, file_info, silence=True, progress_callback=None, use_cache=True
    ):
        """
        Worker function that checks if a file needs download and downloads it if needed.
        file_info is (file_path, expected_hash) tuple where file_path is a manifest key.
        Returns (file_path, downloaded, bytes_transferred) tuple.

        :param file_info: Tuple of (manifest_key, expected_hash)
        :param silence: Whether to suppress individual file progress bars
        :param progress_callback: Optional callback function for progress updates
        :param use_cache: Whether to use cached hashing for performance
        """
        file_path, expected_hash = file_info
        try:
            # Convert manifest key to filesystem path for checking existence
            filesystem_path = self.path_resolver.to_filesystem_path(file_path)

            # Check if file exists and has correct hash
            if filesystem_path.exists():
                with metrics.track("hashing", str(filesystem_path)):
                    if use_cache:
                        current_hash = self.hash_file_cached(filesystem_path)
                    else:
                        current_hash = self.hash_file(filesystem_path)

                if current_hash == expected_hash:
                    # File is up-to-date, don't add to download total since no download is needed
                    return (file_path, False, 0)  # No download needed

            # Download the file with progress callback that supports size discovery
            # Pass expected_hash to avoid lock contention
            bytes_transferred = self.download(
                file_path,
                silence=True,
                progress_callback=progress_callback,
                expected_hash=expected_hash,
            )
            return (file_path, True, bytes_transferred or 0)  # Download completed

        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            raise

    def track_interleaved(self, path, silence=True, use_cache=True):
        """
        Track and upload files with interleaved hashing and uploading for better performance.

        :param path: A file, directory, or glob pattern to track.
        :param silence: Silences verbose logging.
        :param use_cache: If True, use cached hashing for better performance on repeated operations.
        """
        # Start pipeline metrics if enabled
        if metrics.is_enabled():
            tracker = metrics.get_tracker()
            tracker.start_pipeline()

        # Phase 1: Resolve filesystem paths
        print("Resolving filesystem paths...")
        files_to_track = self._resolve_filesystem_paths(path)

        if not files_to_track:
            print(f"No files found to track for '{path}'.")
            if metrics.is_enabled():
                tracker.end_pipeline()
            return

        # Test S3 credentials once before starting parallel operations
        if not silence:
            print("Testing S3 credentials...")
        self.test_s3_credentials(silence=silence)

        print(
            f"Processing {len(files_to_track)} files with interleaved hashing and uploading..."
        )

        # Start tracking stages
        if metrics.is_enabled():
            tracker.start_stage("hashing", max_workers=self.workers)
            tracker.start_stage("compression", max_workers=self.workers)
            tracker.start_stage("s3_upload", max_workers=self.workers)

        # Phase 2: Process files with interleaved hashing and uploading
        files_uploaded = []
        files_processed = 0
        total_bytes_transferred = 0

        try:
            # Create unified progress bars
            with (
                tqdm(
                    total=len(files_to_track),
                    desc="Files processed",
                    unit="file",
                    position=0,
                ) as file_pbar,
                tqdm(
                    total=0,
                    desc="Data transferred",
                    unit="B",
                    unit_scale=True,
                    position=1,
                ) as bytes_pbar,
            ):

                def progress_callback(bytes_chunk):
                    """Callback to update the bytes progress bar"""
                    bytes_pbar.update(bytes_chunk)

                with ThreadPoolExecutor(max_workers=self.workers) as executor:
                    # Submit all hash-and-upload tasks
                    future_to_file = {
                        executor.submit(
                            self._hash_and_upload_worker,
                            str(file.as_posix()),
                            True,
                            progress_callback,
                            use_cache,
                        ): file
                        for file in files_to_track
                    }

                    # Process results as they complete
                    for future in as_completed(future_to_file):
                        if self._shutdown_requested:
                            print(
                                "Shutdown requested. Cancelling remaining operations..."
                            )
                            return

                        try:
                            (
                                file_path,
                                file_hash,
                                uploaded,
                                bytes_transferred,
                            ) = future.result()
                            files_processed += 1
                            total_bytes_transferred += bytes_transferred

                            if uploaded:
                                files_uploaded.append((file_path, file_hash))
                                # Update the bytes progress bar total for uploaded files
                                bytes_pbar.total = (
                                    bytes_pbar.total or 0
                                ) + bytes_transferred
                                bytes_pbar.refresh()

                            file_pbar.update(1)
                            file_pbar.set_postfix(
                                {
                                    "uploaded": len(files_uploaded),
                                    "skipped": files_processed - len(files_uploaded),
                                }
                            )

                        except Exception as e:
                            print(f"An error occurred during processing: {e}")
                            raise

        except KeyboardInterrupt:
            print("\nProcessing interrupted by user.")
            return

        # Phase 3: Update manifest with all changes
        if files_uploaded:
            print(f"Updating manifest with {len(files_uploaded)} uploaded files...")
            with self._lock_context():
                self.load_manifest()
                for file_path, file_hash in files_uploaded:
                    manifest_key = self._get_manifest_key(file_path)
                    self.manifest["files"][manifest_key] = file_hash
                self.save_manifest()

        print(
            f"Successfully processed {files_processed} files ({len(files_uploaded)} uploaded) for '{path}'."
        )

        # End metrics tracking
        if metrics.is_enabled():
            tracker.end_stage("hashing")
            tracker.end_stage("compression")
            tracker.end_stage("s3_upload")
            tracker.end_pipeline()
            tracker.print_summary(verbose=not silence)

    def checkout_interleaved(self, path, silence=True, use_cache=True):
        """
        Checkout files with interleaved hashing and downloading for better performance.

        :param path: A file, directory, or glob pattern to checkout.
        :param silence: Silences verbose logging.
        :param use_cache: If True, use cached hashing for better performance on repeated operations.
        """
        # Start pipeline metrics if enabled
        if metrics.is_enabled():
            tracker = metrics.get_tracker()
            tracker.start_pipeline()

        # Phase 1: Resolve manifest paths
        print("Resolving paths from manifest...")
        files_to_checkout = self._resolve_manifest_paths(path)

        if not files_to_checkout:
            print(f"No files found in the manifest for '{path}'.")
            if metrics.is_enabled():
                tracker.end_pipeline()
            return

        # Test S3 credentials once before starting parallel operations
        if not silence:
            print("Testing S3 credentials...")
        self.test_s3_credentials(silence=silence)

        print(
            f"Processing {len(files_to_checkout)} files with interleaved hashing and downloading..."
        )

        # Start tracking stages
        if metrics.is_enabled():
            tracker.start_stage("hashing", max_workers=self.workers)
            tracker.start_stage("s3_download", max_workers=self.workers)
            tracker.start_stage("decompression", max_workers=self.workers)

        # Phase 2: Start processing immediately - discover sizes during download
        # We'll process ALL files to ensure proper progress tracking, even for up-to-date ones
        files_to_process = files_to_checkout

        if not files_to_process:
            if not silence:
                print("No files to process.")
            return

        if not silence:
            print(
                f"Processing {len(files_to_process)} files (calculating sizes during processing...)",
                flush=True,
            )

        # Phase 3: Process files with interleaved hashing and downloading
        files_downloaded = 0
        files_processed = 0
        total_bytes_transferred = 0

        try:
            # Create unified progress bars with dynamic total for bytes
            with (
                tqdm(
                    total=len(files_to_process),
                    desc="Files processed",
                    unit="file",
                    position=0,
                ) as file_pbar,
                tqdm(
                    total=0,
                    desc="Data downloaded",
                    unit="B",
                    unit_scale=True,
                    position=1,
                ) as bytes_pbar,
            ):

                def progress_callback(bytes_chunk, file_size=None):
                    """Callback to update the bytes progress bar and optionally set total"""
                    if file_size is not None:
                        # Update total when we discover a new file size
                        bytes_pbar.total = (bytes_pbar.total or 0) + file_size
                        bytes_pbar.refresh()
                    bytes_pbar.update(bytes_chunk)

                with ThreadPoolExecutor(max_workers=self.workers) as executor:
                    # Submit hash-and-download tasks for all files (including up-to-date ones for progress tracking)
                    future_to_file = {
                        executor.submit(
                            self._hash_and_download_worker,
                            (file_path, expected_hash),
                            True,
                            progress_callback,
                            use_cache,
                        ): file_path
                        for file_path, expected_hash in files_to_process.items()
                    }

                    # Process results as they complete
                    for future in as_completed(future_to_file):
                        if self._shutdown_requested:
                            print(
                                "Shutdown requested. Cancelling remaining operations..."
                            )
                            break

                        try:
                            file_path, downloaded, bytes_transferred = future.result()
                            files_processed += 1
                            total_bytes_transferred += bytes_transferred

                            if downloaded:
                                files_downloaded += 1

                            file_pbar.update(1)
                            file_pbar.set_postfix(
                                {
                                    "downloaded": files_downloaded,
                                    "skipped": files_processed - files_downloaded,
                                }
                            )

                        except Exception as e:
                            print(f"An error occurred during processing: {e}")
                            raise

        except KeyboardInterrupt:
            print("\nProcessing interrupted by user.")
        finally:
            print(
                f"Successfully processed {files_processed} files ({files_downloaded} downloaded) for '{path}'."
            )

            # End metrics tracking
            if metrics.is_enabled():
                tracker.end_stage("hashing")
                tracker.end_stage("s3_download")
                tracker.end_stage("decompression")
                tracker.end_pipeline()
                tracker.print_summary(verbose=not silence)

    @retry(3, _transient_network_errors)
    def download(
        self,
        file_path: Union[str, Path],
        silence: bool = False,
        progress_callback: Optional[Callable[[int], None]] = None,
        expected_hash: Optional[str] = None,
    ) -> Optional[int]:
        """
        Download a file from S3 by its recorded hash, but skip if it already exists and matches.

        :param file_path: Manifest key (relative to git root)
        :param expected_hash: Optional pre-fetched hash to avoid lock contention in parallel downloads
        """
        # file_path is always a manifest key from _resolve_manifest_paths()
        manifest_key = str(file_path)

        # Convert manifest key to absolute filesystem path for operations
        filesystem_path = self.path_resolver.to_filesystem_path(manifest_key)

        # Get the expected hash for the file (use provided hash if available to avoid lock)
        if expected_hash is None:
            with self._lock_context():
                expected_hash = self.manifest["files"].get(manifest_key)
        if not expected_hash:
            print(f"File '{file_path}' is not in the manifest.")
            return None

        # If the file exists, check its hash
        if not silence:
            print(f"file_path exists?: {filesystem_path.exists()}")
        if filesystem_path.exists():
            current_hash = self.hash_file(filesystem_path)
            if not silence:
                print(f"current_hash: {current_hash}")
                print(f"expected_hash: {expected_hash}")
            if current_hash == expected_hash:
                if not silence:
                    print(
                        f"Skipping download: '{filesystem_path}' is already up-to-date."
                    )
                return 0  # Skip download if hashes match

        # Proceed with download if file is missing or different
        compressed_path = self.temp_dir / f"{uuid4()}.part"

        keys, key_sizes, is_chunked, compressed = self._locate_asset(
            manifest_key, expected_hash
        )

        base_directory = os.path.dirname(compressed_path)
        os.makedirs(base_directory, exist_ok=True)

        target_paths = []
        stream_digests: list = []
        total_file_size = sum(key_sizes.values())

        if progress_callback:
            try:
                progress_callback(0, **{"file_size": total_file_size})
            except TypeError:
                pass

        for idx, key in enumerate(keys):
            try:
                target_path = self.temp_dir / f"{uuid4()}.part"
                target_paths.append(target_path)
                file_size = key_sizes[key]

                # Set up progress callback and context manager
                if progress_callback:
                    # Use the provided callback for unified progress tracking
                    def download_callback(bytes_transferred):
                        progress_callback(bytes_transferred)

                    context_manager = contextlib.nullcontext()
                elif not silence:
                    # Create individual progress bar only if not silenced and no unified callback
                    progress_bar = tqdm(
                        total=file_size,
                        unit="B",
                        unit_scale=True,
                        desc=f"Downloading {os.path.basename(key)}",
                        leave=False,
                    )

                    def download_callback(bytes_transferred):
                        progress_bar.update(bytes_transferred)

                    context_manager = progress_bar
                else:
                    # No progress display
                    def download_callback(bytes_transferred):
                        pass

                    context_manager = contextlib.nullcontext()

                with context_manager:
                    if not silence:
                        print(f"Downloading {key} to {target_path}")
                    with metrics.track("s3_download", key):
                        with open(target_path, "wb") as f:
                            writer = _HashingWriter(f)
                            self._get_s3_client().download_fileobj(
                                Bucket=self.bucket_name,
                                Key=key,
                                Fileobj=writer,
                                Callback=download_callback,
                            )
                            stream_digests.append(writer.hexdigest())
            except Exception as e:
                print(f"Error downloading {key}: {e}")

        if is_chunked and len(target_paths) > 1:
            compressed_path = self.merge_files(compressed_path, target_paths)
            for path in target_paths:
                os.remove(path)
        else:
            compressed_path = target_paths[0]

        if os.path.dirname(filesystem_path):
            os.makedirs(os.path.dirname(filesystem_path), exist_ok=True)
        try:
            if compressed:
                with metrics.track("decompression", str(filesystem_path)):
                    self.decompress_file(compressed_path, filesystem_path)
            else:
                shutil.move(str(compressed_path), str(filesystem_path))
        except Exception as e:
            print(f"Error finalizing {compressed_path} for key {keys}: {e}")
            raise
        # The raw path moves the temp file into place, so it is already gone.
        Path(compressed_path).unlink(missing_ok=True)

        # End-to-end verification, same as the parallel path: transport
        # checksums are off, so this is the check that the bytes on disk
        # are the bytes the manifest promised. For a raw single object the
        # digest was computed as the bytes streamed in, so the file need
        # not be read back.
        if (
            not compressed
            and not is_chunked
            and len(stream_digests) == 1
            and stream_digests[0] is not None
        ):
            actual_hash = stream_digests[0]
        else:
            actual_hash = self.hash_file(filesystem_path)
        if actual_hash != expected_hash:
            try:
                os.remove(filesystem_path)
            except OSError:
                pass
            raise RuntimeError(
                f"Checksum mismatch for {manifest_key}: expected "
                f"{expected_hash}, got {actual_hash}. The stored object is "
                "incomplete or corrupt."
            )
        if not silence:
            print(
                f"Downloaded {filesystem_path} from "
                f"s3://{self.bucket_name}/{keys[0]}"
            )

        # Return bytes transferred for progress tracking
        return filesystem_path.stat().st_size if filesystem_path.exists() else 0

    def list_files(self, path, verbose=False, strip_prefix=None):
        """
        List tracked files matching a path pattern.

        :param path: A file, directory, or glob pattern to list.
        :param verbose: If True, show detailed information including file sizes and hashes.
        :param strip_prefix: If provided, strip this prefix from displayed paths.
        """
        # Resolve manifest paths using the same logic as checkout
        files_to_list = self._resolve_manifest_paths(path)

        if not files_to_list:
            if verbose:
                print(f"No tracked files found for '{path}'.")
            return

        if verbose:
            print(f"Found {len(files_to_list)} tracked file(s) for '{path}':")
            print()

        # Sort files for consistent output
        sorted_files = sorted(files_to_list.items())

        for file_path, file_hash in sorted_files:
            # Strip prefix if provided
            display_path = file_path
            if strip_prefix and file_path.startswith(strip_prefix + "/"):
                display_path = file_path[len(strip_prefix + "/") :]

            if verbose:
                # Get file status if it exists locally
                file_status = self.get_file_status(file_path)
                if file_status["exists"]:
                    size_str = f"{file_status['size']:,} bytes"
                    status = "" if file_status["cache_valid"] else ""
                else:
                    size_str = "missing"
                    status = ""

                print(f"{status} {display_path}")
                print(f"    Hash: {file_hash}")
                print(f"    Size: {size_str}")
                print()
            else:
                print(display_path)

    def list_all_files(self, verbose=False, strip_prefix=None):
        """
        List all tracked files from the manifest.

        :param verbose: If True, show detailed information including file sizes and hashes.
        :param strip_prefix: If provided, strip this prefix from displayed paths.
        """
        with self._lock_context():
            all_files = dict(self.manifest["files"])

        if not all_files:
            if verbose:
                print("No files are currently tracked.")
            return

        if verbose:
            print(f"All tracked files ({len(all_files)} total):")
            print()

        # Sort files for consistent output
        sorted_files = sorted(all_files.items())

        for file_path, file_hash in sorted_files:
            # Strip prefix if provided
            display_path = file_path
            if strip_prefix and file_path.startswith(strip_prefix + "/"):
                display_path = file_path[len(strip_prefix + "/") :]

            if verbose:
                # Get file status if it exists locally
                file_status = self.get_file_status(file_path)
                if file_status["exists"]:
                    size_str = f"{file_status['size']:,} bytes"
                    status = "" if file_status["cache_valid"] else ""
                else:
                    size_str = "missing"
                    status = ""

                print(f"{status} {display_path}")
                print(f"    Hash: {file_hash}")
                print(f"    Size: {size_str}")
                print()
            else:
                print(display_path)
