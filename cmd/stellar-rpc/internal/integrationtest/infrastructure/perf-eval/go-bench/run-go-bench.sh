# Go endpoint benchmark leg. Relies on bootstrap-common.sh's env, helpers,
# bootstrap_box, and run_leg. The runner benches the candidate checkout against 
# BASELINE_REF and publishes a benchstat comparison.
LEG_TITLE="Go endpoint benchmarks"

log "clearing stale go-bench state"
rm -rf "$WORK_DIR/stellar-rpc-baseline"
rm -f /tmp/baseline.txt /tmp/candidate.txt /tmp/benchstat.txt /tmp/bench-results.json

bootstrap_box
run_leg ./cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/go-bench/runner
