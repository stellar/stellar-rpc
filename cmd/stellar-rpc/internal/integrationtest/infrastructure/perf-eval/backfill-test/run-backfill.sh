# Backfill ingestion leg. Concatenated after bootstrap-common.sh in the rendered
# EC2 user-data, so it relies on that file's env, helpers, bootstrap_box, and
# run_leg. It hands off to `runner instantiate`, which builds stellar-rpc and
# times a `--backfill` run against the pubnet datastore. The other half,
# `runner gather`, polls S3 for the result object.
#
# With SERVE_AFTER_BACKFILL=true (the chained endpoint load test), the runner
# keeps the daemon serving after the backfill result publishes and returns only
# once the blaster leg's result appears (or the serve deadline passes) -- the
# box then powers itself off (shutdown behavior is terminate).
LEG_TITLE="Backfill ingestion"

bootstrap_box
run_leg ./cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/backfill-test/runner

if [ "${SERVE_AFTER_BACKFILL:-}" = "true" ]; then
  log "serve phase over; powering off"
  shutdown -c 2>/dev/null || true
  shutdown -P +1 "endpoint load test complete" || log "WARN: could not schedule power-off"
fi
