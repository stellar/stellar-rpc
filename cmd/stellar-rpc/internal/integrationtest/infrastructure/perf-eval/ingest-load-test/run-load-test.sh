# Apply-load ingestion leg. Relies on bootstrap-common.sh's env, helpers,
# bootstrap_box, and run_leg. It hands off to the leg runner, which streams
# the corpus from S3 and runs the ingest benchmark.
LEG_TITLE="Ingest load test"

log "clearing stale apply-load state"
rm -f /tmp/bench-results.json /tmp/load-test-ledgers-*.xdr.zstd

bootstrap_box
run_leg ./cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/ingest-load-test/runner
