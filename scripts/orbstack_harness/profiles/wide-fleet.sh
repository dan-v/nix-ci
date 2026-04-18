# wide-fleet profile: push past the single-process port-pool ceiling
# by distributing claim load across many containers. DAG is large
# enough that a single-container run (scale_xl.rs) would also need
# ~2500 concurrent long-polls — exactly the ceiling we're breaking.

SCENARIO=wide-fleet
NUM_WORKER_CONTAINERS=${NUM_WORKER_CONTAINERS:-50}
WORKERS_PER_CONTAINER=${WORKERS_PER_CONTAINER:-50}
WORKER_MODE=healthy

# DAG shape. 5 × 2000 = 10k drvs, fan-in=2. Small enough to finish
# in tens of seconds on OrbStack, large enough to light up the
# O(1)-counter fast-exit in check_and_publish_terminal.
LAYERS=${LAYERS:-5}
WIDTH=${WIDTH:-2000}
FAN_IN=${FAN_IN:-2}

# Coordinator config: default (quarantine disabled).
COORD_CONFIG=wide-fleet.json

# SLO budget. Sanity bar — whole DAG should drain within a few
# minutes; prevents hangs from silently masquerading as slow runs.
MAX_WAIT_SECS=${MAX_WAIT_SECS:-600}
