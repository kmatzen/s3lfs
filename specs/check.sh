#!/bin/sh
# Run the TLA+ models whose expected outcome is known, and fail if any of
# them disagrees.
#
# Both outcomes matter. A model that should hold must hold; a model of a
# known defect must still violate its invariant, otherwise the property is
# vacuous and proves nothing about the code.
#
# Usage: specs/check.sh   (downloads tla2tools.jar if absent)

set -eu

cd "$(dirname "$0")"

if [ ! -f tla2tools.jar ]; then
    echo "Downloading tla2tools.jar..."
    curl -sSL -o tla2tools.jar \
        https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar
fi

run() {
    java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC \
        -config "$2.cfg" -workers auto "$1.tla" 2>&1
}

failures=0

# Configurations that must pass: module, config
for pair in \
    "S3lfsWorkingCopy S3lfsWorkingCopy_Fixed" \
    "S3lfsWorkingCopy S3lfsWorkingCopy_Large" \
    "S3lfsOwnership   S3lfsOwnership_PerFile"
do
    # shellcheck disable=SC2086
    set -- $pair
    printf '%-34s ' "$2 (expect: holds)"
    if run "$1" "$2" | grep -q "No error has been found"; then
        echo "ok"
    else
        echo "FAILED -- an invariant that should hold does not"
        failures=$((failures + 1))
    fi
done

# Configurations modelling a known defect: module, config, invariant
for triple in \
    "S3lfsWorkingCopy S3lfsWorkingCopy_NoGuard          NoDataLoss" \
    "S3lfsWorkingCopy S3lfsWorkingCopy_ContentAddressed NoCollateralDeletion" \
    "S3lfsOwnership   S3lfsOwnership_Directory          NoOrphanedFile"
do
    # shellcheck disable=SC2086
    set -- $triple
    printf '%-34s ' "$2 (expect: $3 fails)"
    if run "$1" "$2" | grep -q "Error: Invariant $3 is violated"; then
        echo "ok"
    else
        echo "FAILED -- the modelled defect no longer violates $3, so the"
        echo "   property is vacuous and proves nothing about the code"
        failures=$((failures + 1))
    fi
done

if [ "$failures" -ne 0 ]; then
    echo "$failures model check(s) disagreed with expectations"
    exit 1
fi

echo "All model checks agree with expectations."
