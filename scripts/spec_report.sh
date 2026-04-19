#!/usr/bin/env bash
# spec_report.sh — render SPEC.md exit-criteria as a green/red scoreboard.
#
# Per SPEC.md: every exit bar is a binary pass/fail verified by a
# named test or measurement. This script runs each bar's regression
# test (grouped for efficiency) and prints a table so ops can answer
# "is nix-ci currently production-ready?" in one terminal pane.
#
# A single red = not production-ready. 30 consecutive green days
# against nightly chaos is the formal exit criterion (T-NIGHTLY-GREEN).
#
# Usage: scripts/spec_report.sh [--no-run]
#   --no-run: don't actually run tests; just report what tests WOULD
#             map to which bars. Useful for CI dry-run / doc pass.
#
# Environment:
#   DATABASE_URL  must be set for tests that need Postgres.
#   SPEC_REPORT_SKIP  comma-separated bar IDs to skip (e.g. "S-MEM-10K,P-NIXOS-WORKER")
#
# Exit codes:
#   0  all measured bars green
#   1  one or more bars red (or failed to run)
#   2  misuse / missing dependency

set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v cargo >/dev/null; then
    echo "error: cargo not found in PATH" >&2
    exit 2
fi

NO_RUN=0
if [[ "${1:-}" == "--no-run" ]]; then
    NO_RUN=1
fi

SKIP="${SPEC_REPORT_SKIP:-}"
should_skip() {
    local bar="$1"
    [[ ",${SKIP}," == *",${bar},"* ]]
}

# Bar → test-target mapping. Format: "BAR-ID|description|test_target|filter"
# filter is a test-name substring passed to `cargo test`; empty means
# run the whole target.
#
# When a bar has multiple covering tests, list them comma-separated in
# filter; we'll run `cargo test` once per substring.
BARS=(
    # Correctness
    "C-CORRECT-1|dispatcher invariants under chaos|property||"
    "C-CORRECT-3|no drv silently dropped from sealed job|spec_coverage|drvs_accounted_after_seal"
    "C-CORRECT-4|cyclic graph fails job cleanly|edge_cases|seal_rejects_dep_cycle"
    "C-CORRECT-5|oversized ingest rejects cleanly|edge_cases|oversized_batch_rejected_with_413"

    # Durability / recovery
    "D-RESTART-1|coord SIGKILL mid-ingest; new job completes|resilience|restart_cancels_in_flight"
    "D-RESTART-2|coord SIGKILL mid-complete; no double-claim|resilience|concurrent_complete_same_claim"
    "D-RESTART-3|coord SIGKILL mid-terminal-write; consistent|resilience|terminal_writeback_idempotent"
    "D-STARTUP-1|10K-row boot < 5s|startup_bench|boot_with_10k_jobs"

    # Scale / latency (feature-gated)
    "S-CLAIM-P99|p99 claim < 200ms at 10K/500|load|production_scale_dag"
    "S-CLAIM-P50|p50 claim < 20ms at 10K/500|load|production_scale_dag"
    "S-INGEST-THR|ingest > 2K drvs/sec (conservative)|load|production_scale_dag"
    "S-MEM-10K|coordinator RSS < 1 GiB at 10K drvs|load|production_scale_dag"

    # Observability
    "O-METRIC-PARITY|terminal counter exact|spec_coverage|jobs_terminal_counter_parity"
    "O-CLAIMS-INFLIGHT|claims_in_flight -> 0 after quiesce|spec_coverage|claims_in_flight_converges"

    # Deployability
    "P-NIXOS-COORDINATOR|coordinator nixosTest|nix flake check|coordinator-smoke"
    "P-NIXOS-WORKER|worker nixosTest|nix flake check|worker-smoke"
    "P-BEARER-AUTH|bearer-auth; unauth -> 401|edge_cases|unauthenticated_request_rejected_with_401"
    "P-UPGRADE-SAFE|rolling upgrade preserves terminal rows|resilience|rolling_upgrade_preserves_terminal_rows"

    # H7 correctness hardening bars (added by this project, not
    # original SPEC but equally mission-critical).
    "H7.1-LEASE|claim lease refresh prevents false failure|claim_lease||"
    "H7.2-MAX-TRIES|max_tries enforced at claim|max_tries||"
    "H7.4-EVAL-ERRORS|eval errors surface in status|eval_errors||"

    # Process bars (non-test; reported as informational).
    "T-DEP-AUDIT|cargo deny gate|cargo deny|"
)

run_test_target() {
    local target="$1"
    local filter="$2"
    local features=""
    if [[ "$target" == "load" ]]; then
        features="--features load-test"
    fi
    if [[ "$target" == "property" || "$target" == "chaos" ]]; then
        features="--features chaos-test"
    fi
    if [[ -z "$filter" ]]; then
        (cd rust && cargo test $features --test "$target" --quiet 2>&1)
    else
        # Comma-separated filters: cargo test accepts one, so run each
        # and AND the exit codes.
        local ok=0
        IFS=',' read -ra FILTERS <<< "$filter"
        for f in "${FILTERS[@]}"; do
            if ! (cd rust && cargo test $features --test "$target" "$f" --quiet 2>&1); then
                ok=1
            fi
        done
        return $ok
    fi
}

run_cargo_deny() {
    if ! command -v cargo-deny >/dev/null; then
        echo "[skip: cargo-deny not installed]"
        return 2
    fi
    (cd rust && cargo deny --workspace check 2>&1 | tail -5)
}

run_nix_check() {
    local check="$1"
    if ! command -v nix >/dev/null; then
        echo "[skip: nix not installed]"
        return 2
    fi
    nix build ".#checks.x86_64-linux.${check}" --no-link 2>&1 | tail -5
}

RED="\033[31m"
GREEN="\033[32m"
YELLOW="\033[33m"
RESET="\033[0m"

RESULTS=()
ANY_RED=0

for row in "${BARS[@]}"; do
    IFS='|' read -r bar_id desc target filter <<< "$row"
    if should_skip "$bar_id"; then
        RESULTS+=("$bar_id|SKIP|$desc")
        continue
    fi
    if [[ $NO_RUN -eq 1 ]]; then
        RESULTS+=("$bar_id|DRY|$desc → $target ($filter)")
        continue
    fi

    set +e
    case "$target" in
        "nix flake check")
            OUT=$(run_nix_check "$filter")
            ;;
        "cargo deny")
            OUT=$(run_cargo_deny)
            ;;
        *)
            OUT=$(run_test_target "$target" "$filter")
            ;;
    esac
    rc=$?
    set -e

    if [[ $rc -eq 0 ]]; then
        RESULTS+=("$bar_id|PASS|$desc")
    elif [[ $rc -eq 2 ]]; then
        RESULTS+=("$bar_id|SKIP|$desc (dep missing)")
    else
        RESULTS+=("$bar_id|FAIL|$desc")
        ANY_RED=1
    fi
done

echo
echo "================================"
echo "SPEC.md exit-criteria scoreboard"
echo "================================"
printf "%-22s %-6s %s\n" "BAR" "STATUS" "DESCRIPTION"
printf "%-22s %-6s %s\n" "---" "------" "-----------"

for r in "${RESULTS[@]}"; do
    IFS='|' read -r bar status desc <<< "$r"
    case "$status" in
        PASS) color=$GREEN ;;
        FAIL) color=$RED ;;
        SKIP) color=$YELLOW ;;
        DRY)  color=$YELLOW ;;
    esac
    printf "%-22s ${color}%-6s${RESET} %s\n" "$bar" "$status" "$desc"
done

echo
if [[ $ANY_RED -eq 1 ]]; then
    echo -e "${RED}NOT PRODUCTION-READY${RESET} — at least one bar is red."
    exit 1
fi
if [[ $NO_RUN -eq 1 ]]; then
    echo "(dry-run only; use without --no-run to execute tests)"
fi
echo -e "${GREEN}scoreboard green${RESET} — run nightly chaos-scale for the 30-day T-NIGHTLY-GREEN bar."
