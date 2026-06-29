# Apply-load ingestion leg. Concatenated after bootstrap-common.sh in the
# rendered EC2 user-data, so it relies on that file's env, helpers (log,
# upload_result, bail), and bootstrap_box. It checks out the toolchain, then
# hands off to `runner instantiate`, which streams the corpus from S3 and runs
# the ingest benchmark. The other half, `runner gather`, polls S3 for the
# result object.
LEG_TITLE="Ingest load test"

log "clearing stale apply-load state"
rm -f /tmp/bench-results.json /tmp/load-test-ledgers-*.xdr.zstd

bootstrap_box

# The Go runner owns the verdict from here -> release the bootstrap trap. On
# success it publishes the result object itself; on any non-zero exit it has
# written the failure body to RESULTS_FILE (or died before doing so), so we
# publish the fail result it couldn't.
trap - ERR
export TARGET_SHA RUN_ID REPO WORK_DIR RESULTS_FILE BUCKET RESULT_KEY
if ! go run ./cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/ingest-load-test/runner instantiate; then
  log "go runner exited non-zero; publishing fail result"
  upload_result fail "$RESULTS_FILE"
  exit 1
fi
