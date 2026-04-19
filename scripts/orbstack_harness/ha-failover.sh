#!/usr/bin/env bash
# L3.5 HA failover scenario. Two coordinator containers, same PG,
# same advisory-lock key; proves the primary/standby contract under
# real Docker TCP + a real `docker kill` (SIGKILL, not SIGTERM — the
# hardest case).
#
# Sibling to run.sh rather than a profile because the topology is
# fundamentally different (two coordinators, no mock workers) and
# re-using run.sh's worker-launch + driver-dispatch scaffolding
# would be more code than a standalone script.
#
# Usage:
#   scripts/orbstack_harness/ha-failover.sh       # run the scenario
#   scripts/orbstack_harness/ha-failover.sh teardown  # clean up
#
# Requires:
#   - nix-ci release binary built into the nix-ci-cargo-target volume
#     (docker exec -w /work/.../rust nix-ci-rust cargo build --release -p nix-ci)
#   - nix-ci-pg container running (docker ps)
#
# Emits scripts/orbstack_harness/out/ha-failover.json with per-phase
# pass/fail + timing.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKTREE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="$SCRIPT_DIR/out"
mkdir -p "$OUT_DIR"

NET=nix-ci-harness-ha-net
COORD_A=nix-ci-harness-ha-coord-a
COORD_B=nix-ci-harness-ha-coord-b
DB_NAME=nix_ci_ha
PG_CONTAINER=nix-ci-pg
RUNTIME_IMG=rust:1
TARGET_VOL=nix-ci-cargo-target

REPORT="$OUT_DIR/ha-failover.json"

cleanup() {
    docker rm -f "$COORD_A" >/dev/null 2>&1 || true
    docker rm -f "$COORD_B" >/dev/null 2>&1 || true
    docker network disconnect "$NET" "$PG_CONTAINER" >/dev/null 2>&1 || true
    docker network rm "$NET" >/dev/null 2>&1 || true
}

if [[ "${1:-}" == "teardown" ]]; then
    cleanup
    echo "teardown complete"
    exit 0
fi

trap cleanup EXIT

echo "[ha] setting up"
cleanup

# Binary sanity.
if ! docker exec nix-ci-rust test -x /cargo-target/release/nix-ci; then
    echo "missing release binary; build with:" >&2
    echo "  docker exec -w /work/.claude/worktrees/$(basename "$WORKTREE_ROOT")/rust nix-ci-rust cargo build --release -p nix-ci" >&2
    exit 1
fi

# Fresh DB.
echo "[ha] resetting $DB_NAME"
docker exec "$PG_CONTAINER" psql -U postgres -tc \
    "SELECT 1 FROM pg_database WHERE datname='$DB_NAME'" | grep -q 1 && \
    docker exec "$PG_CONTAINER" psql -U postgres -c "DROP DATABASE $DB_NAME" >/dev/null
docker exec "$PG_CONTAINER" psql -U postgres -c "CREATE DATABASE $DB_NAME" >/dev/null

# Network.
docker network create "$NET" >/dev/null
docker network connect "$NET" "$PG_CONTAINER" >/dev/null

# Minimal config for both coords. Same lock_key by construction (the
# default) — that's what makes them a failover pair.
cat >"$OUT_DIR/ha-coord.json" <<EOF
{
  "database_url": "postgres://postgres:postgres@${PG_CONTAINER}:5432/${DB_NAME}",
  "listen": "0.0.0.0:8080",
  "graceful_shutdown_secs": 5
}
EOF

start_coord() {
    local name=$1
    docker run -d --rm \
        --name "$name" \
        --network "$NET" \
        -v "$TARGET_VOL:/cargo-target:ro" \
        -v "$OUT_DIR/ha-coord.json:/etc/nix-ci.json:ro" \
        -e "RUST_LOG=${COORD_LOG:-warn}" \
        "$RUNTIME_IMG" \
        /cargo-target/release/nix-ci server \
        --config /etc/nix-ci.json >/dev/null
}

is_readyz_ok() {
    local name=$1
    docker run --rm --network "$NET" "$RUNTIME_IMG" \
        sh -c "curl -fsS --max-time 2 http://${name}:8080/readyz" \
        >/dev/null 2>&1
}

is_readyz_refused() {
    local name=$1
    # Inverse: curl must FAIL quickly (connection refused means port
    # not bound, which is the advisory-lock-blocked state).
    ! docker run --rm --network "$NET" "$RUNTIME_IMG" \
        sh -c "curl -fsS --max-time 2 http://${name}:8080/readyz" \
        >/dev/null 2>&1
}

# ── Phase 1: bring up primary ─────────────────────────────────────
echo "[ha] starting coord-A"
start_coord "$COORD_A"
echo -n "[ha] waiting for coord-A /readyz"
for _ in $(seq 1 60); do
    if is_readyz_ok "$COORD_A"; then
        echo " — primary up"
        break
    fi
    echo -n "."
    sleep 1
done
if ! is_readyz_ok "$COORD_A"; then
    echo "[ha] FAIL: coord-A never became ready" >&2
    exit 1
fi

# ── Phase 2: coord-B boots as standby, blocks on advisory lock ────
echo "[ha] starting coord-B (expected to block on advisory lock)"
start_coord "$COORD_B"
# Give coord-B 5s to reach CoordinatorLock::acquire.
sleep 5
if is_readyz_ok "$COORD_B"; then
    echo "[ha] FAIL: coord-B became primary while coord-A holds the lock" >&2
    exit 1
fi
if ! is_readyz_refused "$COORD_B"; then
    echo "[ha] FAIL: coord-B /readyz did neither 200 nor refused" >&2
    exit 1
fi
# coord-B's process must still be running (blocked, not crashed).
if ! docker ps --format '{{.Names}}' | grep -q "^${COORD_B}\$"; then
    echo "[ha] FAIL: coord-B container exited unexpectedly" >&2
    docker logs "$COORD_B" 2>&1 | tail -20 >&2 || true
    exit 1
fi
echo "[ha] coord-B is standby (port refused) — OK"

# ── Phase 3: submit a pre-failover job to coord-A ─────────────────
echo "[ha] creating pre-failover job on coord-A"
PRE_JOB=$(docker run --rm --network "$NET" "$RUNTIME_IMG" \
    sh -c "curl -fsS -X POST http://${COORD_A}:8080/jobs \
          -H 'content-type: application/json' \
          -d '{\"external_ref\":\"ha-pre\"}'" | grep -oE '"id":"[^"]*"' | cut -d'"' -f4)
if [[ -z "$PRE_JOB" ]]; then
    echo "[ha] FAIL: could not create pre-failover job" >&2
    exit 1
fi
echo "[ha] pre-failover job id: $PRE_JOB"

# Ingest a drv (unsealed — so clear_busy has a live non-terminal job
# to flip).
docker run --rm --network "$NET" "$RUNTIME_IMG" \
    sh -c "curl -fsS -X POST http://${COORD_A}:8080/jobs/${PRE_JOB}/drvs/batch \
          -H 'content-type: application/json' \
          -d '{\"drvs\":[{\"drv_path\":\"/nix/store/cccccccccccccccccccccccccccccccc-ha.drv\",\"drv_name\":\"ha\",\"system\":\"x86_64-linux\",\"is_root\":true,\"required_features\":[],\"input_drvs\":[]}],\"eval_errors\":[]}'" \
    >/dev/null

# ── Phase 4: docker kill coord-A (SIGKILL — hardest path) ──────────
# Portable second-resolution timing. `date +%s%N` isn't portable
# (macOS BSD date lacks %N) and shelling into a Linux container
# per-sample would itself take ~200ms and bias the measurement. One-
# second granularity is enough here — the assertion is "< 10s," not
# "sub-second p50."
echo "[ha] docker kill coord-A (SIGKILL)"
KILL_START=$(date +%s)
docker kill --signal=KILL "$COORD_A" >/dev/null

# ── Phase 5: standby unblocks, becomes primary ────────────────────
echo -n "[ha] waiting for coord-B to take over"
FAILOVER_SECS=0
for i in $(seq 1 60); do
    if is_readyz_ok "$COORD_B"; then
        FAILOVER_SECS=$(( $(date +%s) - KILL_START ))
        echo " — took over in ${FAILOVER_SECS}s"
        break
    fi
    echo -n "."
    sleep 1
done
if ! is_readyz_ok "$COORD_B"; then
    echo "[ha] FAIL: coord-B never became primary after coord-A kill" >&2
    docker logs "$COORD_B" 2>&1 | tail -30 >&2 || true
    exit 1
fi

# ── Phase 6: pre-failover job is now cancelled (clear_busy fired) ─
PRE_STATUS_JSON=$(docker run --rm --network "$NET" "$RUNTIME_IMG" \
    sh -c "curl -fsS http://${COORD_B}:8080/jobs/${PRE_JOB}")
PRE_STATUS=$(echo "$PRE_STATUS_JSON" | grep -oE '"status":"[^"]*"' | head -1 | cut -d'"' -f4)
if [[ "$PRE_STATUS" != "cancelled" ]]; then
    echo "[ha] FAIL: pre-failover job status != cancelled (got: $PRE_STATUS)" >&2
    echo "$PRE_STATUS_JSON" >&2
    exit 1
fi
echo "[ha] pre-failover job correctly cancelled by clear_busy"

# ── Phase 7: new job on coord-B works end-to-end ──────────────────
NEW_JOB=$(docker run --rm --network "$NET" "$RUNTIME_IMG" \
    sh -c "curl -fsS -X POST http://${COORD_B}:8080/jobs \
          -H 'content-type: application/json' \
          -d '{\"external_ref\":\"ha-post\"}'" | grep -oE '"id":"[^"]*"' | cut -d'"' -f4)
docker run --rm --network "$NET" "$RUNTIME_IMG" \
    sh -c "curl -fsS -X POST http://${COORD_B}:8080/jobs/${NEW_JOB}/seal" >/dev/null
# Empty-toplevels seal → immediate Done.
for _ in $(seq 1 20); do
    NEW_STATUS_JSON=$(docker run --rm --network "$NET" "$RUNTIME_IMG" \
        sh -c "curl -fsS http://${COORD_B}:8080/jobs/${NEW_JOB}")
    NEW_STATUS=$(echo "$NEW_STATUS_JSON" | grep -oE '"status":"[^"]*"' | head -1 | cut -d'"' -f4)
    if [[ "$NEW_STATUS" == "done" ]]; then
        break
    fi
    sleep 0.5
done
if [[ "$NEW_STATUS" != "done" ]]; then
    echo "[ha] FAIL: post-failover new job didn't reach done (got: $NEW_STATUS)" >&2
    exit 1
fi
echo "[ha] post-failover new job reached Done"

# ── Emit JSON report ──────────────────────────────────────────────
cat >"$REPORT" <<EOF
{
  "scenario": "ha-failover",
  "failover_secs": $FAILOVER_SECS,
  "pre_failover_job_id": "$PRE_JOB",
  "pre_failover_status": "$PRE_STATUS",
  "post_failover_job_id": "$NEW_JOB",
  "post_failover_status": "$NEW_STATUS",
  "assertions": [
    {"name": "coord_a_primary_at_start", "passed": true},
    {"name": "coord_b_standby_blocked", "passed": true},
    {"name": "failover_under_10s", "passed": $([[ $FAILOVER_SECS -lt 10 ]] && echo true || echo false)},
    {"name": "pre_failover_job_cancelled", "passed": true},
    {"name": "post_failover_job_works", "passed": true}
  ]
}
EOF
echo "[ha] report: $REPORT"
cat "$REPORT"
echo
echo "[ha] PASS (failover in ${FAILOVER_SECS}s)"
