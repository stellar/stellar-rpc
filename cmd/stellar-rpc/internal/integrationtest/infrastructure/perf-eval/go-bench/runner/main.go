// Command runner runs the audited endpoint Go benchmarks on the box for the
// release candidate and a baseline release, and publishes a benchstat
// comparison of the two.
package main

import "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/harness"

var logger = harness.NewLogger()

func main() { harness.Run(instantiate) }
