// Command runner drives the apply-load ingestion leg. It has two subcommands,
// one per environment the leg spans:
//
//	runner instantiate   on the EC2 box, after the shell preamble has installed
//	                     the toolchain and checked out the repo: streams the
//	                     golden DB, stellar-core, and ledger bundles from S3
//	                     (sha-verified), runs the ingest benchmark, and writes
//	                     an ok/fail verdict.
//	runner gather        on the GHA runner: polls S3 for the result object the
//	                     instance publishes and relays the verdict + results as
//	                     step outputs. Shared across legs (perf-eval/harness).
//
// The two halves coordinate through a single S3 result object (harness.Result).
package main

import (
	"context"
	"fmt"
	"os"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/harness"
)

var logger = supportlog.New()

func main() {
	logger.SetLevel(supportlog.InfoLevel)

	cmd := "instantiate"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}

	ctx := context.Background()
	var err error
	switch cmd {
	case "instantiate":
		err = instantiate(ctx)
	case "gather":
		err = harness.Gather(ctx)
	default:
		fmt.Fprintf(os.Stderr, "usage: %s [instantiate|gather]\n", os.Args[0])
		os.Exit(64)
	}
	if err != nil {
		logger.Errorf("fatal: %v", err)
		os.Exit(1)
	}
}
