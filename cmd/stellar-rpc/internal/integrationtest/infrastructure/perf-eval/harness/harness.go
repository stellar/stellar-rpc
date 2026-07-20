// Package harness holds the generic EC2-leg machinery shared by the perf-eval
// legs. Each leg spins up an ephemeral box, bootstraps it identically, runs a
// leg-specific task, and reports back through one S3 result object.
//
// This package owns the parts that are identical across legs:
//
//	Gather         GHA-side: polls S3 for the result object the box publishes
//	               and relays the verdict + results as step outputs.
//	S3Fetcher      on-box: streams (and sha-verifies) corpus objects from S3.
//	PublishResult  on-box: writes the ok/fail result object the gatherer reads.
//	RunStreaming   on-box: runs a child, streaming output with a bounded tail.
//
// Leg-specific work (which corpus to fetch, which task to run) lives in each
// leg's own on-box runner command; Gather is its own command (perf-eval/gather)
// shared by all legs.
package harness

import (
	"context"
	"fmt"
	"os"
	"strings"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

// GetEnv reads the common leg environment.
func GetEnv() map[string]string {
	return map[string]string{
		"BUCKET":       Env("BUCKET", "stellar-rpc-ci-load-test"),
		"REGION":       Env("REGION", "us-east-1"),
		"WORK_DIR":     Env("WORK_DIR", "/data"),
		"RESULTS_FILE": Env("RESULTS_FILE", "/tmp/results.md"),
		"RESULT_KEY":   os.Getenv("RESULT_KEY"),
		"TARGET_SHA":   os.Getenv("TARGET_SHA"),
		"RUN_ID":       Env("RUN_ID", "manual"),
		"REPO":         Env("REPO", "stellar/stellar-rpc"),
	}
}

// NewLogger returns an Info-level logger (supportlog.New starts at WARN). Each
// leg's runner uses one for its own messages.
func NewLogger() *supportlog.Entry {
	l := supportlog.New()
	l.SetLevel(supportlog.InfoLevel)
	return l
}

var logger = NewLogger()

// Run executes a command's task, logging and exiting non-zero on error.
func Run(task func(context.Context) error) {
	if err := task(context.Background()); err != nil {
		logger.Errorf("fatal: %v", err)
		os.Exit(1)
	}
}

// Env returns the value of key, or def when unset/empty.
func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// appendOutputs appends lines to the GitHub Actions step-output file.
func appendOutputs(path string, lines ...string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprintln(f, strings.Join(lines, "\n"))
	return err
}
