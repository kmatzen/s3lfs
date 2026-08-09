# TLA+ specifications

Formal models of s3lfs's concurrency-sensitive protocols and of the operations
that can lose data, checked with TLC.

`specs/check.sh` runs every configuration whose expected outcome is known and
fails if any disagrees. CI runs it on every pull request. It checks both
directions: configurations modelling the shipped design must hold, and
configurations modelling a defect the code no longer has must still violate
their invariant -- otherwise the corresponding property is vacuous and proves
nothing about the code.

Which configuration corresponds to the shipped design is recorded in
`check.sh`, not only in prose here, so a stale label fails the build rather
than misleading a reader.

References below name functions rather than line numbers, which go stale as
the code moves.

## Setup

```sh
curl -sSL -o tla2tools.jar \
  https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar
```

`tla2tools.jar` is not committed — add it to `.gitignore` if you keep it here.

## S3lfsWorkingCopy — clobbering, aliasing, and dangling references

Models the working-copy lifecycle -- `track`, `remove`, commit, branch switch,
`sync` (download and prune), and `cleanup` -- against a user who edits tracked
files without uploading them. The other specs model the storage layer; this one
models the layer that decides which bytes on disk get replaced or deleted.

Three invariants:

- `NoDataLoss` — no automatic operation destroys content that exists only on
  disk. A history variable records any content removed while stored nowhere.
- `NoDanglingReference` — every manifest entry has an object behind it, so a
  checkout cannot 404.
- `NoCollateralDeletion` — distinct live paths occupy distinct storage keys, so
  untracking or collecting one path never destroys bytes another path refers to.

Two constants isolate the design decisions those properties rest on:

```sh
java -jar tla2tools.jar -config S3lfsWorkingCopy_Fixed.cfg            S3lfsWorkingCopy.tla  # current code
java -jar tla2tools.jar -config S3lfsWorkingCopy_NoGuard.cfg          S3lfsWorkingCopy.tla  # violates NoDataLoss
java -jar tla2tools.jar -config S3lfsWorkingCopy_ContentAddressed.cfg S3lfsWorkingCopy.tla  # violates NoCollateralDeletion
java -jar tla2tools.jar -config S3lfsWorkingCopy_Large.cfg            S3lfsWorkingCopy.tla  # three content values
```

`CLOBBER_GUARD = FALSE` reproduces the defect fixed in #108: `sync` chose what to
download from manifest hashes alone, so a file edited but not uploaded was
replaced silently.

`PATH_AWARE_KEYS = FALSE` reproduces content-only addressing, where two paths
holding identical bytes share one object and untracking either destroys both.
The real key function (`_asset_base_key`) includes the path, which is what makes
the invariant hold.

**This spec changed the code.** Checking the first version -- where the guard
asked only whether a file still matched the hash the manifest recorded -- TLC
produced this trace:

```
track p2  ->  commit  ->  remove p2  ->  cleanup  ->  sync
```

`remove` drops the manifest entry, `cleanup` collects the now-unreferenced
object, and `sync` then prunes the local file because it still matches the
recorded hash -- deleting the last copy. The guard was strengthened to the
condition the model actually requires: **only take bytes off disk when those
bytes can be fetched back from S3**. That is `_recoverable()` in `cli.py`, and
`TestSyncNeverRemovesUnrecoverableContent` pins the trace as a regression test.

## S3lfsOwnership — which system owns each file

s3lfs keeps its files out of git by writing entries into a marked `.gitignore`
block and removing them from the index, which makes ownership a shared, mutable
thing between two systems. Two ways to get it wrong:

- `NoDualOwnership` — a path is never tracked by git and s3lfs at once, which
  would put the large file into git history anyway.
- `NoOrphanedFile` — a file on disk that git ignores is tracked by s3lfs.
  Otherwise it is invisible to both: git will not stage it, s3lfs will not
  upload it, and it survives only on the machine that created it.

```sh
java -jar tla2tools.jar -config S3lfsOwnership_PerFile.cfg   S3lfsOwnership.tla  # current code
java -jar tla2tools.jar -config S3lfsOwnership_Directory.cfg S3lfsOwnership.tla  # violates NoOrphanedFile
```

`IGNORE_SCOPE = "directory"` reproduces the defect fixed in #108, where tracking
a directory wrote a single `/dir/` pattern: a source file added under that
directory afterwards is hidden from git and absent from the manifest. The
per-file expansion (`_gitignore_entries_for`) ignores exactly what s3lfs stores
and nothing else.

## S3lfsGC — garbage collection vs. concurrent upload

Models `S3LFS.cleanup_s3` racing `S3LFS.parallel_upload_chunked`.

The invariant `NoDanglingReference` states that every hash referenced by the
manifest has a corresponding S3 object. Violating it means a `checkout` 404s on
a file the manifest claims is tracked.

Three configurations, selected by the `REVALIDATE` and `INFLIGHT` constants:

```sh
java -jar tla2tools.jar -config S3lfsGC.cfg         S3lfsGC.tla  # original defect
java -jar tla2tools.jar -config S3lfsGCFixed.cfg    S3lfsGC.tla  # revalidate under lock
java -jar tla2tools.jar -config S3lfsGCInflight.cfg S3lfsGC.tla  # in-flight registry
```

Results as of the last run (TLC 2.19):

| Config | Outcome |
| --- | --- |
| `S3lfsGC.cfg` (baseline) | Violated at depth 7 |
| `S3lfsGCFixed.cfg` (revalidate) | Violated at depth 7 |
| `S3lfsGCInflight.cfg` (registry) | No error, 87 distinct states, exhaustive |

### Why revalidation is not enough

Re-reading the manifest under the lock immediately before deleting looks like
the natural fix, but TLC rejects it. The counterexample is:

```
WStart   uploader picks hash h1
WUpload  h1 lands in S3, still unreferenced
GMark    GC snapshots the manifest -- h1 absent, correctly so
GList    GC marks h1 unreferenced
GSweep   GC revalidates: h1 still absent from the manifest -> deletes it
WCommit  uploader publishes manifest -> h1, now dangling
```

The whole GC cycle fits inside the uploader's upload-then-commit window. During
that window the manifest genuinely does not reference the object, so no amount
of re-reading it helps. Revalidation only closes the narrower case where the
uploader commits partway through a sweep.

### What does work

The uploader claims the hash under the lock *before* any bytes reach S3, and
releases the claim only after the manifest reference is published. The GC treats
`manifest ∪ inflight` as the live set. This is the `INFLIGHT` configuration and
it checks clean over the full state space.

**This is now implemented.** `parallel_upload_chunked` claims each hash as its
prep completes and releases the set in its `finally`, strictly after the manifest
write; `cleanup_s3` unions the registry into its live set at mark time and
re-checks under the lock immediately before deleting. The registry lives at
`.s3lfs_temp/.s3lfs_inflight.yaml`.

The release ordering is the load-bearing part. Releasing before the manifest
write would reopen exactly the window the registry exists to close — the hash
would be in neither the manifest nor the registry, and a sweep landing in
between would delete the objects.

One residual: a crashed uploader leaves its claims behind, so claims age out
after `INFLIGHT_TTL_SECONDS` (24h). That timeout bounds the leak from a crash;
it plays no part in closing the race, which the registry closes outright. This
is deliberately narrower than the alternative of an object-age grace period,
where the timeout *is* the correctness argument.

An object-age grace period (skip objects younger than the longest plausible
upload) is a weaker alternative — simpler to implement, no shared registry, but
it trades correctness for a timeout guess and is not modeled here.

## S3lfsManifest — concurrent read-modify-write of the manifest

Models N processes loading, mutating, and saving `.s3_manifest.yaml`. Two
independent knobs:

- `RELOAD` — whether the process re-reads the manifest under the lock before
  saving. TRUE models `parallel_upload_chunked:1525`, `upload:1258`,
  `track_interleaved:2480`; FALSE models `remove_file:1284`,
  `remove_subtree:1776`, `track_modified_files_cached:830`,
  `S3LFS.track_modified_files`.
- `SHARED_LOCK` — whether all processes agree on one lock file. FALSE models the
  CWD-relative `temp_dir` defect in `S3LFS.__init__`.

`NoLostUpdate` compares the manifest against a ghost variable holding the result
of an equivalent serial execution. A violation is one process erasing another's
committed work, orphaning its S3 objects.

```sh
for c in NoReload_NoLock NoReload_Lock Reload_NoLock Reload_Lock; do
  java -jar tla2tools.jar -config S3lfsManifest_$c.cfg S3lfsManifest.tla
done
```

|  | `SHARED_LOCK = FALSE` | `SHARED_LOCK = TRUE` |
| --- | --- | --- |
| `RELOAD = FALSE` | Violated, depth 9 | Violated, depth 9 |
| `RELOAD = TRUE` | Violated, depth 9 | **No error**, 269 states, exhaustive |

Both fixes are individually necessary and only jointly sufficient. Neither one
alone moves the needle.

With a working lock but no reload (`NoReload_Lock`), the processes serialize
correctly and it still loses the update, because the second process writes back
a snapshot it took before the first one committed:

```
PLoad(q1) PAcquire(q1) PReload(q1) PLoad(q2) PSave(q1) PAcquire(q2) PReload(q2) PSave(q2)
```

With correct reload discipline but no shared lock (`Reload_NoLock`), both
processes hold their own lock, both reload, and both then save — the reload is
correct but no longer excludes anything:

```
PLoad(q1) PAcquire(q1) PReload(q1) PLoad(q2) PAcquire(q2) PReload(q2) PSave(q1) PSave(q2)
```

`PReload` is deliberately a separate step from `PSave` rather than folded into
it. Folding them would make each save atomic and would hide the `Reload_NoLock`
counterexample entirely — the spec would report a clean result for a broken
system.

## S3lfsChunks — partial chunked upload and silent truncation

Models `S3LFS.parallel_upload_chunked` followed by the checkout that
reassembles the file (`S3LFS._discover_chunks_for_file`,
`S3LFS._finalize_file`).

**The code now implements `CommitAfter`.** `parallel_upload_chunked` records a
manifest entry only once every chunk of that file has landed, and prints a
warning naming any file whose chunks did not all upload. `specs/check.sh`
asserts `CommitAfter` holds and that `Baseline` still violates
`NoSilentCorruption`, so a regression toward the original behaviour fails CI.

The three defects the `Baseline` configuration models, kept for the record:

1. The manifest entry is recorded at *prep* time, in `_prepare_file_for_upload`,
   before any chunk is PUT.
2. Per-chunk upload failures are caught, printed, and skipped; the `finally`
   in `parallel_upload_chunked` writes the manifest anyway.
3. Checkout infers the chunk count from `len(chunk_keys)` in
   `_discover_chunks_for_file` and reads indices `0..n-1`, assuming the
   surviving chunks form a contiguous prefix.

`NoSilentCorruption` states that a checkout reporting success produced the whole
file. `ManifestImpliesChunks` is the stronger upload-side property: a manifest
entry implies all of its chunks exist.

```sh
for c in Baseline StoreCount VerifyHash CommitAfter CommitAndVerify; do
  java -jar tla2tools.jar -config S3lfsChunks_$c.cfg S3lfsChunks.tla
done
```

| Config | `NoSilentCorruption` | `ManifestImpliesChunks` |
| --- | --- | --- |
| `Baseline` (the original defect, since fixed) | Violated | Violated |
| `StoreCount` | No error | Violated |
| `VerifyHash` | No error | Violated |
| `CommitAfter` | No error | **No error** |

### Trailing gaps are the dangerous ones

With `NumChunks = 3`, chunk 0 uploads and chunks 1 and 2 fail:

```
UStart  UUpload   uploaded = {0}
UCommit           manifest entry written anyway
DCheckout         len(chunk_keys) = 1 -> read indices 0..0 -> all present
                  outcome = "ok", content = {0}
```

The user gets a file truncated to one third of its length and **no error is
raised anywhere**. Nothing downstream detects it: there is no hash check in
`_finalize_file`.

An *interior* gap is much less dangerous. With `uploaded = {0,1,2,4}` of 5 the
inferred count is 4, index 3 is missing, and the download fails loudly on a 404.
The bug's severity depends entirely on which chunks fail, which is why testing
is unlikely to surface it — a test that kills one chunk mid-file gets a clean
error and looks like it passed.

### Choosing a fix

All three candidates independently eliminate silent corruption, but they are not
equivalent:

- `STORE_COUNT` and `VERIFY_HASH` only convert corruption into a *loud* failure
  at checkout time. The manifest still references an incomplete object, so the
  repository stays broken and every later checkout keeps failing.
- `COMMIT_AFTER_UPLOAD` is the only one that satisfies `ManifestImpliesChunks`.
  It prevents the bad state from being recorded at all.

Recommended: `COMMIT_AFTER_UPLOAD` as the actual fix, with `VERIFY_HASH` as
defence in depth against chunks lost after a correct commit (lifecycle
expiration, out-of-band deletion, the GC race modeled in `S3lfsGC`). Those
combined are the `CommitAndVerify` configuration.

## S3lfsNamespace — derived lock identity and the file namespace

This supersedes the `SHARED_LOCK` knob in `S3lfsManifest`, and exists because
that knob was a modeling error worth recording.

`S3lfsManifest` takes mutual exclusion as an input (`SHARED_LOCK \in BOOLEAN`).
A model told whether the lock works can only score fixes someone already thought
of; it cannot evaluate a placement, because placement is exactly the thing it
abstracts away. Here the lock is a `(base, name)` pair resolved the way the code
resolves it, and mutual exclusion is *derived* from whether two processes land on
the same file.

The model also carries the directory tree, because s3lfs's own metadata lives in
the tree s3lfs enumerates. `_resolve_filesystem_paths` uses `rglob("*")` with no
exclusion list.

```
R                 repository root, holds .s3_manifest.yaml
|-- R_temp        R/.s3lfs_temp
+-- S             a subdirectory a process may be started from
    +-- S_temp    S/.s3lfs_temp
```

`LOCK_POLICY` selects among the three placements actually considered:

| Policy | Resolves to | Status |
| --- | --- | --- |
| `cwd_temp` | `<cwd>/.s3lfs_temp/.s3lfs.lock` | the original defect |
| `manifest_root` | `<manifest dir>/.s3lfs.lock` | first attempt |
| `manifest_temp` | `<manifest dir>/.s3lfs_temp/.s3lfs.lock` | shipped |

### Results

`NoLostUpdate`, with `RELOAD = TRUE` throughout:

| Policy | Result |
| --- | --- |
| `cwd_temp` | Violated |
| `manifest_root` | No error |
| `manifest_temp` | No error |

The derived model reproduces `S3lfsManifest`'s `SHARED_LOCK` result without being
handed it, and adds the finding that `manifest_root` and `manifest_temp` are
**equivalent** for correctness. The choice between them is organizational.

`NoInternalFileTracked` — no internal file inside the enumerated subtree:

| Policy | `TRACK_TARGET = R` | `TRACK_TARGET = S` |
| --- | --- | --- |
| `cwd_temp` | Violated | Violated |
| `manifest_root` | Violated | No error |
| `manifest_temp` | Violated | No error |

Tracking the repository root violates the invariant under **every** policy,
because the manifest itself sits in the enumerated subtree. That is not a
consequence of the lock fix; it is a standing defect. Confirmed against the real
code: enumerating from the repository root returns `.s3_manifest.yaml`, the lock
file, and all of `.git/**`.

### Why this spec exists

The lock fix was first written with the lock beside the manifest
(`manifest_root`) and justified on the grounds that `.s3lfs_temp/` would keep it
out of file enumeration. That justification is false — both placements are
equally enumerated — and no spec at the time could contradict it, because none of
them modeled the namespace. A boolean knob cannot reject a bad reason for a
correct change.

## S3lfsCombined — the manifest and GC protocols together

The other specs are per-protocol silos, and each assumed the others' properties
held. `S3lfsGC` assumed mutual exclusion, which was false until the lock path was
fixed. `S3lfsManifest` assumed paths did not matter, which hid the namespace
defect. Cross-cutting defects live in exactly those seams.

This model puts an uploader, a remover and a collector against one manifest, so
both safety properties are checked against the **same** behaviours rather than
against two separate idealisations. Reachability is keyed on hash *and* path,
matching the storage layout and the path-aware GC the code now implements.

```sh
for c in Broken ReloadOnly RevalOnly Fixed; do
  java -jar tla2tools.jar -config S3lfsCombined_$c.cfg S3lfsCombined.tla
done
```

| Config | `RELOAD` | `INFLIGHT` | `GC_REVALIDATE` | Result |
| --- | --- | --- | --- | --- |
| `Broken` | ✗ | ✗ | ✗ | `NoLostUpdate` violated |
| `ReloadOnly` | ✓ | ✗ | ✗ | `NoDanglingReference` violated |
| `RevalOnly` | ✓ | ✗ | ✓ | `NoDanglingReference` **still** violated |
| `Fixed` | ✓ | ✓ | ✓ | No error |

`RevalOnly` is the interesting row: it confirms in the composed model what
`S3lfsGC` found in isolation — re-validating the manifest before deleting does
not close the race, because the whole mark/sweep cycle fits inside the
uploader's store-then-publish window.

`NoOrphanedObjectSurvivesGC` is the property neither single-protocol spec could
state, because it needs the remover and the collector in one behaviour: a lost
update orphans an object, and the collector then deletes it. A manifest bug
becomes data loss only when GC is also in play.

## Termination, and why the deadlock check is back on

Every configuration previously set `CHECK_DEADLOCK FALSE`. That was expedient
and wrong: processes finishing looks like a deadlock to TLC, so the check was
switched off — which also switched off detection of *real* deadlocks. That
mattered, because `_lock_context` is not reentrant and widening a critical
section risks a self-deadlock; the question ended up being answered by
instrumentation instead (see below).

Each spec now has an explicit `Terminating` stuttering step, so a finished
behaviour is a legitimate self-loop rather than a stuck state, and
**`CHECK_DEADLOCK TRUE` is set everywhere**. All clean configurations pass with
it enabled.

## Larger configurations

The counterexamples all appear at 2 processes / 2 paths / 3 chunks, but a clean
result at that size establishes very little. The verified-good configurations
were re-run larger:

| Config | Size | Result |
| --- | --- | --- |
| `S3lfsCombined_FixedLarge` | 3 paths, 3 hashes | No error |
| `S3lfsManifest_Reload_Lock_Large` | 3 processes, 3 paths | No error |
| `S3lfsChunks_CommitAndVerifyLarge` | 6 chunks | No error |

Nothing new appears at greater scale. This still is not a proof — TLC checks the
model it is given — but it removes the "only ever checked at the smallest
interesting size" caveat.

## Verification notes

### Lock reentrancy — checked by instrumentation, not by TLC

`_lock_context` is not reentrant. `portalocker` takes `LOCK_EX` on a freshly
opened descriptor, so a second acquisition on the same thread blocks against the
first and the process hangs. Widening a critical section therefore risks a
self-deadlock.

This is a property of the actual call graph, and a spec would only re-encode
whatever assumptions were made when writing it. It was instead checked directly:
`_lock_context` was instrumented with a per-instance, per-thread depth counter
that reports any nested acquisition, and the full test suite was run against it.

Result: **0 reentrant acquisitions**, with the instrumented run reproducing the
uninstrumented pass/fail counts exactly. The suite also completed without
hanging, which separately rules out cross-instance nesting (two S3LFS objects
sharing one lock file on one thread), since that blocks on flock rather than
tripping the counter.

This covers exercised paths only. 61 tests in this environment fail early for an
unrelated reason (they shell out to `python`, absent here), so their paths are
unverified.

### A measurement error worth recording

Earlier commit messages in this branch claimed the lock-path fix resolved 28
`test_cli_integration.py::TestS3LFSCLIInProcess` failures. **That claim is
false.** The baseline had been extracted with `git archive`, which does not
include `.git`, and those 28 tests require a git repository. They were failing
for that reason alone.

Measured correctly, with `.git` present in both trees:

| Tree | Result |
| --- | --- |
| `385fe1b` (before all three code fixes) | 61 failed / 531 passed |
| `1c355de` (after all three) | 61 failed / 538 passed |

The delta is exactly the seven tests added alongside the fixes. **All three code
fixes have zero test-suite delta.** Each is verified by a targeted probe or a
purpose-written regression test instead:

- lock path — two processes at different working directories, critical sections
  interleave before and serialize after;
- enumeration — `track .` on a fresh repo, 22 manifest entries before and 2
  after;
- read-modify-write — a concurrent commit erased before and preserved after,
  with two of the three new tests failing on the parent commit.

The general lesson: a baseline built by copying a tree is not the same tree. Diff
the failure sets, and confirm any claimed movement against a control that differs
only in the change under test.

## Scope and limitations

The model abstracts the manifest to a set of content hashes, dropping the path
dimension. It therefore cannot see the mismatch between GC's reachability unit
(hash) and storage's unit (hash + path), which is a separate defect.

It also assumes manifest reads and writes are mutually exclusive — that the
`portalocker` lock actually works. `S3lfsManifest` discharges exactly that
assumption, and shows it does not currently hold: the lock is only real when
`SHARED_LOCK` is TRUE, which the CWD-relative `temp_dir` in `S3LFS.__init__`
does not guarantee. **The `INFLIGHT` result therefore depends on fixing the lock
path first.** Resolve the lock file relative to the git root, as the manifest
path already is.

Both specs use two processes and two paths. That is enough to exhibit every
counterexample here, but it does not establish correctness for larger
configurations — TLC checks the model it is given, not the code.

`S3lfsManifest` also models each process as performing a single mutation. Real
`track` runs mutate many entries across a long upload, which widens every window
modeled here without changing their shape.

`S3lfsChunks` models a single file and treats an upload as one atomic choice of
which chunks survive, rather than as concurrent per-chunk tasks. That is sound
for the properties checked — every surviving-subset is reachable either way —
but it means the spec says nothing about the worker-pool behaviour itself,
including the shared-pool submission pattern or the download-side tracker.

`S3lfsCombined` models one uploader, one remover and one collector. Multiple
concurrent uploaders are not modelled, so it cannot speak to two uploads racing
on the same path.

The hash cache is not modelled at all. Its lost-write mechanism has the same
shape as the manifest read-modify-write bug and would be straightforward to add.
