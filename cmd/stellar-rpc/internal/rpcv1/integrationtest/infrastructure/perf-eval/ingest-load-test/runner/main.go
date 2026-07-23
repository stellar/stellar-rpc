// Command runner runs the apply-load ingestion benchmark on the box.
package main

import "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/integrationtest/infrastructure/perf-eval/harness"

var logger = harness.NewLogger()

func main() { harness.Run(instantiate) }
