// Command runner runs the backfill ingestion benchmark on the box (then keeps
// serving in handoff mode).
package main

import "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/integrationtest/infrastructure/perf-eval/harness"

var logger = harness.NewLogger()

func main() { harness.Run(instantiate) }
