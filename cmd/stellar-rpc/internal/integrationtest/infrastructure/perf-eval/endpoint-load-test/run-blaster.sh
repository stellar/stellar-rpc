# Endpoint load-test leg (chained after the backfill leg). Concatenated after
# bootstrap-common.sh in the rendered EC2 user-data, so it relies on that file's
# env, helpers, bootstrap_box, and run_leg. The backfill box keeps its RPC
# serving after backfill and advertises itself via a serve-ready object at
# $READY_KEY; the runner here waits on that, seeds request data from the ledger
# window the RPC reports, then drives stellar-rpc-blaster against each
# read-path endpoint in serial and publishes the stats.
LEG_TITLE="Endpoint load test"
BLASTER_REPO="${BLASTER_REPO:-stellar/stellar-rpc-blaster}"
BLASTER_REF="${BLASTER_REF:-dev}" # branch or tag (not a bare SHA)
export BLASTER_DIR="${BLASTER_DIR:-$WORK_DIR/stellar-rpc-blaster}"

bootstrap_box

log "cloning + building stellar-rpc-blaster ($BLASTER_REPO@$BLASTER_REF)"
rm -rf "$BLASTER_DIR"
git clone --depth 1 --branch "$BLASTER_REF" "https://github.com/$BLASTER_REPO.git" "$BLASTER_DIR"
make -C "$BLASTER_DIR" build

run_leg ./cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/endpoint-load-test/runner
