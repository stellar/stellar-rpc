# Apply-load ingestion leg. Concatenated after bootstrap-common.sh in the
# rendered EC2 user-data, so it relies on that file's env, helpers, bootstrap_box,
# and run_leg. It hands off to `runner instantiate`, which streams the corpus
# from S3 and runs the ingest benchmark. The other half, `runner gather`, polls
# S3 for the result object.
LEG_TITLE="Ingest load test"

log "clearing stale apply-load state"
rm -f /tmp/bench-results.json /tmp/load-test-ledgers-*.xdr.zstd

bootstrap_box
run_leg ./cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/ingest-load-test/runner
