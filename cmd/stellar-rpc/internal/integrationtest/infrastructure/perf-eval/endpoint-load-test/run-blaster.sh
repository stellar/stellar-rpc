# Endpoint load-test leg (chained after the backfill leg). Concatenated after
# bootstrap-common.sh in the rendered EC2 user-data, so it relies on that
# file's env, helpers, bootstrap_box, and run_leg. The runner fetches + builds
# stellar-rpc-blaster, waits on the serve-ready object the peer (CHAIN_PEER)
# box publishes, seeds request data from the ledger window the RPC reports,
# then drives blaster against each read-path endpoint in serial and publishes
# the stats. The shared perf-eval/gather command is the GHA-side half.
LEG_TITLE="Endpoint load test"

bootstrap_box
run_leg ./cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/endpoint-load-test/runner
