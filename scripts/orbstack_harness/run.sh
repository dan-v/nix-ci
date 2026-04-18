#!/usr/bin/env bash
# L3.5 OrbStack harness — orchestrates coordinator + mock-worker
# containers against the existing nix-ci-pg container.
#
# Usage: run.sh <profile|teardown>

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKTREE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="$SCRIPT_DIR/out"
mkdir -p "$OUT_DIR"

NET=nix-ci-harness-net
COORDINATOR_C=nix-ci-harness-coordinator
DRIVER_C=nix-ci-harness-driver
DB_NAME=nix_ci_harness
PG_CONTAINER=nix-ci-pg
RUNTIME_IMG=rust:1

# Path INSIDE nix-ci-rust + new containers. The target volume is
# mounted at /cargo-target (same as nix-ci-rust's volume).
TARGET_VOL=nix-ci-cargo-target

cleanup() {
    docker rm -f "$COORDINATOR_C" >/dev/null 2>&1 || true
    docker rm -f "$DRIVER_C" >/dev/null 2>&1 || true
    docker ps -a --format '{{.Names}}' \
        | grep '^nix-ci-harness-worker-' \
        | xargs -r docker rm -f >/dev/null 2>&1 || true
    docker network disconnect "$NET" "$PG_CONTAINER" >/dev/null 2>&1 || true
    docker network rm "$NET" >/dev/null 2>&1 || true
}

if [[ "${1:-}" == "teardown" ]]; then
    cleanup
    echo "teardown complete"
    exit 0
fi

PROFILE=${1:?usage: run.sh <profile|teardown>}
PROFILE_FILE="$SCRIPT_DIR/profiles/$PROFILE.sh"
if [[ ! -f "$PROFILE_FILE" ]]; then
    echo "unknown profile: $PROFILE" >&2
    echo "available:" >&2
    ls "$SCRIPT_DIR/profiles" >&2
    exit 1
fi
# shellcheck source=/dev/null
source "$PROFILE_FILE"

trap cleanup EXIT

echo "[harness] profile=$PROFILE"
cleanup

# Sanity check: binaries exist in the shared volume.
for bin in nix-ci nix-ci-harness-driver nix-ci-mock-worker; do
    if ! docker exec nix-ci-rust test -x "/cargo-target/release/$bin"; then
        echo "missing release binary: $bin" >&2
        echo "build with:  docker exec -w /work/.claude/worktrees/$(basename "$WORKTREE_ROOT")/rust nix-ci-rust cargo build --release -p nix-ci -p nix-ci-harness" >&2
        exit 1
    fi
done

# Fresh DB per run.
echo "[harness] resetting database $DB_NAME"
docker exec "$PG_CONTAINER" psql -U postgres -tc \
    "SELECT 1 FROM pg_database WHERE datname='$DB_NAME'" | grep -q 1 && \
    docker exec "$PG_CONTAINER" psql -U postgres -c "DROP DATABASE $DB_NAME" >/dev/null
docker exec "$PG_CONTAINER" psql -U postgres -c "CREATE DATABASE $DB_NAME" >/dev/null

# Network + pg bridge.
echo "[harness] creating network $NET"
docker network create "$NET" >/dev/null
docker network connect "$NET" "$PG_CONTAINER" >/dev/null

# Coordinator.
CONFIG_PATH="$SCRIPT_DIR/coord-config/$COORD_CONFIG"
if [[ ! -f "$CONFIG_PATH" ]]; then
    echo "missing coord-config: $CONFIG_PATH" >&2
    exit 1
fi
echo "[harness] starting coordinator"
docker run -d --rm \
    --name "$COORDINATOR_C" \
    --network "$NET" \
    -v "$TARGET_VOL:/cargo-target:ro" \
    -v "$CONFIG_PATH:/etc/nix-ci.json:ro" \
    -e "DATABASE_URL=postgres://postgres:postgres@${PG_CONTAINER}:5432/${DB_NAME}" \
    -e "RUST_LOG=${COORD_LOG:-warn}" \
    "$RUNTIME_IMG" \
    /cargo-target/release/nix-ci server \
    --config /etc/nix-ci.json >/dev/null

# Wait for coordinator /healthz (via a one-shot from inside the net).
echo -n "[harness] waiting for coordinator health"
for _ in $(seq 1 60); do
    if docker run --rm --network "$NET" "$RUNTIME_IMG" \
        sh -c "curl -fsS http://${COORDINATOR_C}:8080/healthz" \
        >/dev/null 2>&1; then
        echo " — ready"
        break
    fi
    echo -n "."
    sleep 1
done

# Mock workers. Profile-specific count + mode.
start_worker_container() {
    local idx=$1 mode=$2 shared=${3:-false}
    local shared_flag=()
    [[ "$shared" == "true" ]] && shared_flag=(--shared-worker-id)

    # Deliberately NO --rm: workers emit their final WorkerReport on
    # stdout during SIGTERM shutdown, and we need `docker logs` after
    # stop to collect it. Cleanup trap removes them.
    docker run -d \
        --name "nix-ci-harness-worker-${mode}-${idx}" \
        --network "$NET" \
        -v "$TARGET_VOL:/cargo-target:ro" \
        -e "HARNESS_COORDINATOR=http://${COORDINATOR_C}:8080" \
        ${HARNESS_AUTH_BEARER:+-e "HARNESS_AUTH_BEARER=${HARNESS_AUTH_BEARER}"} \
        "$RUNTIME_IMG" \
        /cargo-target/release/nix-ci-mock-worker \
        --workers "$WORKERS_PER_CONTAINER" \
        --worker-id-prefix "mock-${mode}-${idx}" \
        --mode "$mode" \
        --wait-secs 20 \
        "${shared_flag[@]}" >/dev/null
}

case "$SCENARIO" in
wide-fleet)
    echo "[harness] starting $NUM_WORKER_CONTAINERS healthy worker containers ($WORKERS_PER_CONTAINER workers each)"
    for i in $(seq 1 "$NUM_WORKER_CONTAINERS"); do
        start_worker_container "$i" healthy
    done
    ;;
sick-worker-contained)
    echo "[harness] starting $NUM_HEALTHY_CONTAINERS healthy + $NUM_SICK_CONTAINERS sick worker containers"
    for i in $(seq 1 "$NUM_HEALTHY_CONTAINERS"); do
        # Healthy containers model multi-slot hosts with a shared
        # identity so the quarantine-state cardinality doesn't
        # explode; it also more realistically matches operators'
        # deploy patterns (one worker_id per machine).
        start_worker_container "$i" healthy true
    done
    for i in $(seq 1 "$NUM_SICK_CONTAINERS"); do
        # Sick: shared worker_id so the 10 slots aggregate their
        # failure count — 3 total failures within the window trips
        # the threshold, matching "a sick host is quarantined."
        start_worker_container "$i" sick true
    done
    ;;
*)
    echo "unknown SCENARIO in profile: $SCENARIO" >&2
    exit 1
    ;;
esac

# Driver. Blocks until scenario terminal, emits report JSON.
echo "[harness] running driver"
DRIVER_EXIT=0
docker run --rm \
    --name "$DRIVER_C" \
    --network "$NET" \
    -v "$TARGET_VOL:/cargo-target:ro" \
    -v "$OUT_DIR:/out" \
    -e "HARNESS_COORDINATOR=http://${COORDINATOR_C}:8080" \
    ${ADMIN_BEARER:+-e "HARNESS_ADMIN_BEARER=${ADMIN_BEARER}"} \
    "$RUNTIME_IMG" \
    /cargo-target/release/nix-ci-harness-driver \
    --scenario "$SCENARIO" \
    --layers "$LAYERS" \
    --width "$WIDTH" \
    --fan-in "$FAN_IN" \
    --max-wait-secs "$MAX_WAIT_SECS" \
    --output /out/report.json || DRIVER_EXIT=$?

# Signal workers SIGTERM so they drain their claim loops and emit the
# final WorkerReport on stdout. `docker stop --time` sends SIGTERM,
# waits, then SIGKILL if still running. Grace period ≥ wait_secs + 5s
# covers the worst-case long-poll in flight.
echo "[harness] stopping worker containers (graceful)"
WORKERS_RUNNING=$(docker ps --format '{{.Names}}' | grep '^nix-ci-harness-worker-' || true)
if [[ -n "$WORKERS_RUNNING" ]]; then
    # shellcheck disable=SC2086
    docker stop --time 30 $WORKERS_RUNNING >/dev/null
fi

echo "[harness] collecting worker reports"
: >"$OUT_DIR/workers.jsonl"
for c in $(docker ps -a --format '{{.Names}}' | grep '^nix-ci-harness-worker-' || true); do
    # The final stdout line is the JSON WorkerReport; earlier lines
    # may be startup banners. Filter to lines starting with `{`.
    docker logs "$c" 2>/dev/null | awk '/^\{/' | tail -n 1 >>"$OUT_DIR/workers.jsonl" || true
done

echo "[harness] report: $OUT_DIR/report.json"
echo "[harness] workers: $OUT_DIR/workers.jsonl"
echo "[harness] driver exit: $DRIVER_EXIT"
exit "$DRIVER_EXIT"
