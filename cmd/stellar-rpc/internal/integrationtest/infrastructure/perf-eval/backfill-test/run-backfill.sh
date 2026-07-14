# Backfill ingestion leg. Relies on bootstrap-common.sh's env, helpers,
# bootstrap_box, and run_leg. It hands off to `runner instantiate` to build 
# stellar-rpc and time a backfill run against the datalake. The other half, `runner 
# gather`, polls S3 for the result object.
LEG_TITLE="Backfill ingestion"

bootstrap_box
run_leg ./cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/backfill-test/runner
