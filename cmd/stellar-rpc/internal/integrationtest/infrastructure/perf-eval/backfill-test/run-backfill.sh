# Backfill ingestion leg. Concatenated after bootstrap-common.sh in the rendered
# EC2 user-data, so it relies on that file's env, helpers, bootstrap_box, and
# run_leg. It hands off to `runner instantiate`, which builds stellar-rpc and
# times a `--backfill` run against the pubnet datastore. The other half,
# `runner gather`, polls S3 for the result object.
LEG_TITLE="Backfill ingestion"

bootstrap_box
run_leg ./cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/backfill-test/runner
