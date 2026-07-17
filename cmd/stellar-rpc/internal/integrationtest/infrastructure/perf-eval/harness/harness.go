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
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

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

// RequireEnv returns the values of keys in order, erroring with every unset one.
func RequireEnv(keys ...string) ([]string, error) {
	vals := make([]string, len(keys))
	var missing []string
	for i, k := range keys {
		if vals[i] = os.Getenv(k); vals[i] == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required env: %s", strings.Join(missing, ", "))
	}
	return vals, nil
}

// BootDeadline returns the instant a box-side runner should bail by: budget
// minutes after box boot, minus margin. ok is false when the budget is unset.
func BootDeadline(budgetMinutes int, margin time.Duration) (time.Time, bool) {
	if budgetMinutes <= 0 {
		return time.Time{}, false
	}
	up, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return time.Time{}, false
	}
	var uptimeSecs float64
	if _, err := fmt.Sscanf(string(up), "%f", &uptimeSecs); err != nil {
		return time.Time{}, false
	}
	boot := time.Now().Add(-time.Duration(uptimeSecs * float64(time.Second)))
	return boot.Add(time.Duration(budgetMinutes)*time.Minute - margin), true
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
