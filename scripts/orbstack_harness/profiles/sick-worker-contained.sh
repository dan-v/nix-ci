# sick-worker-contained profile: validate the fleet quarantine
# contract end-to-end. A small set of "sick" containers fails
# every claim with BuildFailure; healthy containers continue to
# service the DAG. Assertions in the driver check:
#   - nix_ci_worker_auto_quarantined_total > 0
#   - nix_ci_process_panics_total == 0 (panic isolation holds)
#   - Job reaches a terminal state (no hang)
#   - POST /admin/refute clears the failed_outputs cache entry

SCENARIO=sick-worker-contained
NUM_HEALTHY_CONTAINERS=${NUM_HEALTHY_CONTAINERS:-5}
NUM_SICK_CONTAINERS=${NUM_SICK_CONTAINERS:-3}
WORKERS_PER_CONTAINER=${WORKERS_PER_CONTAINER:-10}

# Small DAG; scenario is about quarantine not throughput.
LAYERS=3
WIDTH=100
FAN_IN=2

# Coordinator config has tuned quarantine thresholds.
COORD_CONFIG=sick-worker-contained.json

# Admin bearer (must match coordinator config).
ADMIN_BEARER=harness-admin-bearer

MAX_WAIT_SECS=${MAX_WAIT_SECS:-180}
