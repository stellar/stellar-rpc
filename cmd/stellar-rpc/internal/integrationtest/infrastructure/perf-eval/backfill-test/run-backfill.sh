# Backfill ingestion leg. Concatenated after bootstrap-common.sh in the rendered
# EC2 user-data, so it relies on that file's env, helpers, bootstrap_box, and
# run_leg. The runner builds stellar-rpc and times a `--backfill` run from the
# datalake. With SERVE_AFTER_BACKFILL=true it keeps serving for the blaster leg.
LEG_TITLE="Backfill ingestion"

bootstrap_box
run_leg ./cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/backfill-test/runner
