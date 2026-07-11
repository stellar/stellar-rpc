# Backfill ingestion leg. Concatenated after bootstrap-common.sh in the rendered
# EC2 user-data, so it relies on that file's env, helpers, bootstrap_box, and
# run_leg. The runner builds stellar-rpc and times a `--backfill` run against
# the pubnet datastore (the shared perf-eval/gather command is the GHA-side
# half). With SERVE_AFTER_BACKFILL=true (the chained endpoint load test), the
# runner keeps the daemon serving after the backfill result publishes and
# powers the box off once the blaster leg's result appears (or the serve
# deadline passes).
LEG_TITLE="Backfill ingestion"

bootstrap_box
run_leg ./cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/backfill-test/runner
