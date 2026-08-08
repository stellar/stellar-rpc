package harness

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRelayState pins the handoff rule: only an exhausted budget turns a
// verdict-less window into a failure.
func TestRelayState(t *testing.T) {
	deadline := time.Unix(1_700_000_000, 0)
	for _, tc := range []struct {
		name string
		now  time.Time
		want string
	}{
		{"budget left", deadline.Add(-time.Hour), relayStateRunning},
		{"one second left", deadline.Add(-time.Second), relayStateRunning},
		{"deadline reached", deadline, relayStateFail},
		{"deadline passed", deadline.Add(time.Hour), relayStateFail},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, relayState(tc.now, deadline))
		})
	}
}

// relayEnv is the poller env contract, valid values throughout. Tests blank or
// corrupt one slot at a time from it.
var relayEnv = map[string]string{
	"INSTANCE_ID":           "i-0123456789abcdef0",
	"AWS_REGION":            "us-east-1",
	"GITHUB_OUTPUT":         "/dev/null",
	"BUCKET":                "stellar-rpc-ci-load-test",
	"RESULT_KEY":            "runs/1/campaign/result.json",
	"RUN_ID":                "1-1",
	"POLL_INTERVAL":         "30",
	"DEBUG_LOG_LINES":       "40",
	"DEBUG_LOG_EVERY_POLLS": "10",
	"WINDOW_SECONDS":        "19200",
	"DEADLINE_EPOCH":        "1700000000",
}

// setRelayEnv installs the contract with overrides applied; an empty override
// value stands for an unset variable.
func setRelayEnv(t *testing.T, overrides map[string]string) {
	t.Helper()
	for k, v := range relayEnv {
		if o, ok := overrides[k]; ok {
			v = o
		}
		t.Setenv(k, v)
	}
}

// TestRelayEnvValidation checks that a mis-plumbed workflow gets an error naming
// the bad slot rather than a panic or an AWS call. Every case here must fail
// before Relay reaches S3, which is why the env is otherwise complete.
func TestRelayEnvValidation(t *testing.T) {
	blankAll := map[string]string{}
	for k := range relayEnv {
		blankAll[k] = ""
	}
	for _, tc := range []struct {
		name      string
		overrides map[string]string
		wantMsg   string
	}{
		{"nothing set", blankAll, "missing required env"},
		{"no instance", map[string]string{"INSTANCE_ID": ""}, "INSTANCE_ID"},
		{"no deadline", map[string]string{"DEADLINE_EPOCH": ""}, "DEADLINE_EPOCH"},
		{"unparsable interval", map[string]string{"POLL_INTERVAL": "half a minute"}, "POLL_INTERVAL"},
		{"zero debug cadence", map[string]string{"DEBUG_LOG_EVERY_POLLS": "0"}, "DEBUG_LOG_EVERY_POLLS"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setRelayEnv(t, tc.overrides)
			err := Relay(context.Background())
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantMsg)
		})
	}
}
