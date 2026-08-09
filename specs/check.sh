#!/bin/sh
# Run every TLA+ model whose expected outcome is known, and fail if any of
# them disagrees.
#
# Both outcomes matter. A configuration modelling the shipped design must
# hold. A configuration modelling a defect the code does not have must
# still violate its invariant -- otherwise the property is vacuous and
# proves nothing about the code.
#
# The "holds" list is the load-bearing part: it names, for each protocol,
# the configuration that corresponds to what the code actually does. If a
# change regresses the code back toward a modelled defect, the matching
# entry here starts failing.
#
# Usage: specs/check.sh   (downloads tla2tools.jar if absent)

set -eu

cd "$(dirname "$0")"

if [ ! -f tla2tools.jar ]; then
    echo "Downloading tla2tools.jar..."
    curl -sSL -o tla2tools.jar \
        https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar
fi

module_for() {
    case "$1" in
        S3lfsGC|S3lfsGCFixed|S3lfsGCInflight) echo S3lfsGC ;;
        *) echo "$1" | sed 's/_.*//' ;;
    esac
}

run() {
    # Each run gets its own metadir. TLC names it from the wall clock to the
    # second by default, so back-to-back runs of the same module collide and
    # the second one dies before it checks anything -- which looks exactly
    # like the property having changed.
    meta="${TMPDIR:-/tmp}/tlc-meta-$1"
    rm -rf "$meta"
    java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC \
        -metadir "$meta" -config "$1.cfg" -workers auto \
        "$(module_for "$1").tla" > "${TMPDIR:-/tmp}/tlc_$1.out" 2>&1 || true
    rm -rf "$meta"
    cat "${TMPDIR:-/tmp}/tlc_$1.out"
}

# A model check can also fail to run at all -- out of memory, a parse error,
# a missing config. That is a different problem from a property changing,
# and saying so saves the reader from chasing the wrong one.
explain() {
    echo "   TLC output (last lines):"
    tail -5 "${TMPDIR:-/tmp}/tlc_$1.out" | sed "s/^/   | /"
}

failures=0

# Configurations modelling the shipped design: these must hold.
#
#   S3lfsChunks_CommitAfter        a manifest entry is written only once every
#                                  chunk has landed (parallel_upload_chunked)
#   S3lfsGCInflight                uploads in flight are registered so a
#                                  concurrent sweep cannot collect them
#   S3lfsCombined_Fixed            the two above interacting
#   S3lfsManifest_Reload_Lock      the manifest is re-read under the lock
#                                  before every read-modify-write
#   S3lfsNamespace_*_trackS        s3lfs's own metadata is excluded from the
#                                  tree it enumerates (_is_internal_path)
#   S3lfsWorkingCopy_Fixed         sync only removes bytes it can fetch back
#   S3lfsOwnership_PerFile         .gitignore covers exactly what is tracked
for cfg in \
    S3lfsChunks_CommitAfter \
    S3lfsChunks_CommitAndVerify \
    S3lfsChunks_CommitAndVerifyLarge \
    S3lfsGCInflight \
    S3lfsCombined_Fixed \
    S3lfsCombined_FixedLarge \
    S3lfsManifest_Reload_Lock \
    S3lfsManifest_Reload_Lock_Large \
    S3lfsNamespace_manifest_root_trackS \
    S3lfsNamespace_manifest_temp_trackS \
    S3lfsWorkingCopy_Fixed \
    S3lfsWorkingCopy_Large \
    S3lfsOwnership_PerFile
do
    printf '%-38s ' "$cfg (holds)"
    if run "$cfg" | grep -q "No error has been found"; then
        echo "ok"
    else
        echo "FAILED -- an invariant the shipped design relies on does not hold"
        explain "$cfg"
        failures=$((failures + 1))
    fi
done

# Configurations modelling a defect: these must still fail, on the stated
# invariant. If one starts passing, the model has stopped discriminating
# and the matching "holds" entry no longer proves anything.
for pair in \
    "S3lfsChunks_Baseline                NoSilentCorruption" \
    "S3lfsGC                             NoDanglingReference" \
    "S3lfsGCFixed                        NoDanglingReference" \
    "S3lfsCombined_Broken                NoLostUpdate" \
    "S3lfsCombined_ReloadOnly            NoDanglingReference" \
    "S3lfsCombined_RevalOnly             NoDanglingReference" \
    "S3lfsManifest_NoReload_Lock         NoLostUpdate" \
    "S3lfsManifest_NoReload_NoLock       NoLostUpdate" \
    "S3lfsManifest_Reload_NoLock         NoLostUpdate" \
    "S3lfsNamespace_cwd_temp_trackR      NoInternalFileTracked" \
    "S3lfsNamespace_cwd_temp_trackS      NoInternalFileTracked" \
    "S3lfsNamespace_manifest_root_trackR NoInternalFileTracked" \
    "S3lfsNamespace_manifest_temp_trackR NoInternalFileTracked" \
    "S3lfsWorkingCopy_NoGuard            NoDataLoss" \
    "S3lfsWorkingCopy_ContentAddressed   NoCollateralDeletion" \
    "S3lfsOwnership_Directory            NoOrphanedFile"
do
    # shellcheck disable=SC2086
    set -- $pair
    printf '%-38s ' "$1 ($2 fails)"
    if run "$1" | grep -q "Error: Invariant $2 is violated"; then
        echo "ok"
    else
        echo "FAILED -- the modelled defect no longer violates $2, so the"
        echo "   corresponding property is vacuous and proves nothing"
        explain "$1"
        failures=$((failures + 1))
    fi
done

if [ "$failures" -ne 0 ]; then
    echo "$failures model check(s) disagreed with expectations"
    exit 1
fi

echo "All model checks agree with expectations."
