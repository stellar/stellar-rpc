// Command runner drives the backfill ingestion leg's on-box half: it builds
// stellar-rpc and times a --backfill run against the pubnet datastore (then
// keeps serving in handoff mode). The GHA-runner half is the shared
// perf-eval/gather.
package main

import "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/harness"

var logger = harness.NewLogger()

func main() { harness.Run(instantiate) }
