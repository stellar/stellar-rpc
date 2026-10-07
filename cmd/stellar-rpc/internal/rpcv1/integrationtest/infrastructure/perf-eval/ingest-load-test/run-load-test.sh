# Apply-load ingestion leg. Relies on bootstrap-common.sh's env, helpers,
# bootstrap_box, and run_leg. It hands off to the leg runner, which streams
# the corpus from S3 and runs the ingest benchmark.
LEG_TITLE="Ingest load test"

log "clearing stale apply-load state"
rm -f /tmp/bench-results.json /tmp/load-test-ledgers-*.xdr.zstd

bootstrap_box

# The integration test binary links rpcv2's grocksdb + zstd; build them like CI's setup-go does.
log "building native libs (zstd, rocksdb)"
apt-get install -y -qq --no-install-recommends cmake ninja-build
./scripts/install-zstd.sh
ZSTD_HOME=/usr/local ./scripts/install-rocksdb.sh
ldconfig
export GOFLAGS=-tags=grocksdb_clean_link

run_leg ./cmd/stellar-rpc/internal/rpcv1/integrationtest/infrastructure/perf-eval/ingest-load-test/runner
