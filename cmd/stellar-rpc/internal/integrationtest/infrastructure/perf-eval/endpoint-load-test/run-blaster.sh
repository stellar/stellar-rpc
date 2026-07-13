# Endpoint load-test leg (chained after the backfill leg). Concatenated after 
# bootstrap-common.sh in the EC2 user-data, so it relies on that file's env, 
# helpers, bootstrap_box, and run_leg. The runner blasts the backfill box's RPC.
LEG_TITLE="Endpoint load test"

bootstrap_box
run_leg ./cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/endpoint-load-test/runner
