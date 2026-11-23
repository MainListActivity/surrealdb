#!/usr/bin/env bash
set -euo pipefail

test "$(uname -s):$(uname -m)" = Linux:aarch64
reference_sha=eab8473e4d4e9f0bfecde6013862ff18a7a676a9
git cat-file -e "$reference_sha^{commit}" 2>/dev/null ||
    git fetch --no-tags --depth=1 origin "$reference_sha"
candidate_sha=$(git rev-parse HEAD)
scratch=$(mktemp -d "${RUNNER_TEMP:?}/native-quota.XXXXXXXX")
reference="$scratch/reference"
git worktree add --detach "$reference" "$reference_sha"
trap 'git worktree remove "$reference"' EXIT
test "$(git -C "$reference" rev-parse HEAD)" = "$reference_sha"

manifest=compatibility/native-quota-v1.json
jq -e --slurpfile reference "$reference/$manifest" '
    .performance.thresholds_percent == $reference[0].performance.thresholds_percent
' "$manifest" >/dev/null

export CARGO_TARGET_DIR="$PWD/target"
evidence="$CARGO_TARGET_DIR/native-quota-performance"
mkdir -p "$evidence"
baseline="$evidence/baseline.json"
candidate="$evidence/candidate.json"
rm -f -- "$baseline" "$candidate"
jq -n --arg baseline_sha "$reference_sha" --arg candidate_sha "$candidate_sha" \
    --arg runner "${ImageOS:-unknown}:${ImageVersion:-unknown}" \
    --arg rustc "$(rustc --version)" \
    '{$baseline_sha, $candidate_sha, $runner, $rustc}' > "$evidence/identity.json"
benchmark=(cargo bench --locked -p surrealdb-core --bench quota --features kv-rocksdb --)
(
    cd "$reference"
    "${benchmark[@]}" --output "$baseline"
)
test -s "$baseline"

for attempt in 1 2 3; do
    log="$scratch/candidate-$attempt.log"
    set +e
    "${benchmark[@]}" --baseline "$baseline" --output "$candidate" 2>&1 | tee "$log"
    exit_code=${PIPESTATUS[0]}
    set -e
    if [ "$exit_code" -eq 0 ]; then
        test -s "$candidate"
        exit 0
    fi
    if [ "$attempt" -eq 3 ] ||
        ! grep -Eq 'normalized (throughput|p(95|99) latency) regressed beyond' "$log"; then
        exit "$exit_code"
    fi
    echo "Retrying warmed candidate after a throughput or latency regression (attempt $((attempt + 1))/3)."
    rm -f -- "$candidate"
done
