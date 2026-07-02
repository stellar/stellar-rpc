// Command runner drives the backfill ingestion leg. The shared harness owns the
// dispatch: `runner gather` polls S3 for the result object on the GHA runner,
// and `runner instantiate` (default) runs the on-box backfill below.
package main

import "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/harness"

var logger = harness.NewLogger()

func main() { harness.Run(instantiate) }
