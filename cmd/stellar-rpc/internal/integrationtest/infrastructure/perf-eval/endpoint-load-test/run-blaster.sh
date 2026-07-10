# Endpoint load-test leg (chained after the backfill leg). Concatenated after
# bootstrap-common.sh in the rendered EC2 user-data, so it relies on that file's
# env, helpers, bootstrap_box, and run_leg. The backfill box keeps its RPC
# serving after backfill and advertises itself via a serve-ready object at
# $READY_KEY; the runner here waits on that, seeds request data from the ledger
# window the RPC reports, then drives stellar-rpc-blaster against each
# read-path endpoint in serial and publishes the stats.
LEG_TITLE="Endpoint load test"
BLASTER_REPO="${BLASTER_REPO:-stellar/stellar-rpc-blaster}"
BLASTER_REF="${BLASTER_REF:-dev}" # branch, tag, or SHA
export BLASTER_DIR="${BLASTER_DIR:-$WORK_DIR/stellar-rpc-blaster}"

bootstrap_box

log "fetching stellar-rpc-blaster ($BLASTER_REPO@$BLASTER_REF)"
rm -rf "$BLASTER_DIR"
git init -q "$BLASTER_DIR"
git -C "$BLASTER_DIR" remote add origin "https://github.com/$BLASTER_REPO.git"
git -C "$BLASTER_DIR" fetch --depth 1 origin "$BLASTER_REF"
git -C "$BLASTER_DIR" checkout -q --detach FETCH_HEAD
BLASTER_SHA=$(git -C "$BLASTER_DIR" rev-parse HEAD)
export BLASTER_SHA
log "building stellar-rpc-blaster at $BLASTER_SHA"
make -C "$BLASTER_DIR" build

run_leg ./cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/endpoint-load-test/runner
