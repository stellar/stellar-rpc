// Command gather is the GHA-runner half of every perf-eval leg: it polls S3
// for the result object the box publishes and relays it as step outputs.
package main

import "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/integrationtest/infrastructure/perf-eval/harness"

func main() { harness.Run(harness.Gather) }
