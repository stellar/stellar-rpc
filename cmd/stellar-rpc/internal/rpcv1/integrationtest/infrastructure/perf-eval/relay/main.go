// Command relay is the windowed poller for campaigns that outlive one GHA job:
// it polls S3 for the result object the box publishes and reports either the
// verdict or a handoff to the next poll job as step outputs.
package main

import "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/integrationtest/infrastructure/perf-eval/harness"

func main() { harness.Run(harness.Relay) }
