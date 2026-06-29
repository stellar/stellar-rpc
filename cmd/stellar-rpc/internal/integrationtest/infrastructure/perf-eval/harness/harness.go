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
// leg's own runner command, which dispatches to Gather or its own
// instantiate via a thin main.
package harness

import (
	"fmt"
	"os"
	"strings"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

var logger = func() *supportlog.Entry {
	l := supportlog.New()
	l.SetLevel(supportlog.InfoLevel)
	return l
}()

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
