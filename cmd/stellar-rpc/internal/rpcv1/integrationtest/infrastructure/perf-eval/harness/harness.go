// Package harness holds the generic EC2-leg machinery shared by the perf-eval
// legs. Each leg spins up an ephemeral box, bootstraps it identically, runs a
// leg-specific task, and reports back through one S3 result object.
//
// This package owns the parts that are identical across legs:
//
//	Gather         GHA-side: waits for the result object within one job's
//	               budget and relays found/passed as step outputs.
//	Relay          GHA-side: waits for the result object within one polling
//	               window of a longer campaign and relays ok/fail/running,
//	               so successive jobs can wait for the same box.
//	resultPoller   GHA-side: the polling loop, result validation, and
//	               diagnostics shared by Gather and Relay.
//	S3Fetcher      on-box: streams (and sha-verifies) corpus objects from S3.
//	PublishResult  on-box: writes the ok/fail result object the pollers read.
//	RunStreaming   on-box: runs a child, streaming output with a bounded tail.
//
// Leg-specific work (which corpus to fetch, which task to run) lives in each
// leg's own on-box runner command; Gather (perf-eval/gather) and Relay
// (perf-eval/relay) are their own commands shared by all legs.
package harness

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

// defaultResultsFile is where the pollers drop the box's markdown; the
// workflow summary step reads it, and legs default RESULTS_FILE to it.
const defaultResultsFile = "/tmp/results.md"

// GetEnv reads the common leg environment.
func GetEnv() map[string]string {
	return map[string]string{
		"BUCKET":       Env("BUCKET", "stellar-rpc-ci-load-test"),
		"REGION":       Env("REGION", "us-east-1"),
		"WORK_DIR":     Env("WORK_DIR", "/data"),
		"RESULTS_FILE": Env("RESULTS_FILE", defaultResultsFile),
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

// RequireEnvInts returns the integer values of keys, requiring each to be set
// and parseable.
func RequireEnvInts(keys ...string) (map[string]int, error) {
	vals, err := RequireEnv(keys...)
	if err != nil {
		return nil, err
	}
	ints := make(map[string]int, len(keys))
	for i, k := range keys {
		n, cerr := strconv.Atoi(vals[i])
		if cerr != nil {
			return nil, fmt.Errorf("%s: %w", k, cerr)
		}
		ints[k] = n
	}
	return ints, nil
}

// requirePositive rejects missing keys and values below one.
func requirePositive(ints map[string]int, keys ...string) error {
	for _, k := range keys {
		if ints[k] < 1 {
			return fmt.Errorf("%s must be positive, got %d", k, ints[k])
		}
	}
	return nil
}

// requireSeconds checks that positive seconds fit in a time.Duration.
func requireSeconds(ints map[string]int, keys ...string) error {
	if err := requirePositive(ints, keys...); err != nil {
		return err
	}
	for _, k := range keys {
		if int64(ints[k]) > int64((1<<63-1)/time.Second) {
			return fmt.Errorf("%s exceeds the maximum duration in seconds", k)
		}
	}
	return nil
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
