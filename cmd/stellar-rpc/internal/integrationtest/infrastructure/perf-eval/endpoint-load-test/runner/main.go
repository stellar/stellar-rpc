// Command runner drives the endpoint load-test leg's on-box half: it waits for
// the backfill box's serve-ready object, seeds request data from it, and blasts
// its read-path endpoints. The GHA-runner half is the shared perf-eval/gather.
package main

import "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/harness"

var logger = harness.NewLogger()

func main() { harness.Run(instantiate) }
