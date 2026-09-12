package harness

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestLoadEnv checks that both commands read the same shared variables plus
// their own.
func TestLoadEnv(t *testing.T) {
	setRelayEnv(t, nil)
	t.Setenv("RESULTS_TIMEOUT", "300")

	var gather gatherConfig
	require.NoError(t, loadEnv(&gather))
	require.Equal(t, 5*time.Minute, gather.ResultsTimeout.duration())
	require.Equal(t, "i-0123456789abcdef0", gather.InstanceID)
	require.Equal(t, "us-east-1", gather.Region)
	require.Equal(t, "/dev/null", gather.GitHubOutput)
	require.Equal(t, "stellar-rpc-ci-load-test", gather.Bucket)
	require.Equal(t, "runs/1/campaign/result.json", gather.ResultKey)
	require.Equal(t, "1-1", gather.RunID)
	require.Equal(t, 30*time.Second, gather.PollInterval.duration())
	require.Equal(t, count(40), gather.DebugLogLines)
	require.Equal(t, count(10), gather.DebugLogEveryPolls)

	var relay relayConfig
	require.NoError(t, loadEnv(&relay))
	require.Equal(t, 19200*time.Second, relay.Window.duration())
	require.Equal(t, int64(1700000000), time.Time(relay.Deadline).Unix())
	require.Equal(t, gather.PollerConfig, relay.PollerConfig)
}

func TestLoadEnvNamesTheBadVariable(t *testing.T) {
	var cfg relayConfig

	setRelayEnv(t, map[string]string{"DEBUG_LOG_LINES": "abc"})
	require.ErrorContains(t, loadEnv(&cfg), "DEBUG_LOG_LINES")

	setRelayEnv(t, map[string]string{"POLL_INTERVAL": "0"})
	require.ErrorContains(t, loadEnv(&cfg), "POLL_INTERVAL")

	// t.Setenv has registered the restore, so the variable comes back after the test.
	setRelayEnv(t, nil)
	require.NoError(t, os.Unsetenv("WINDOW_SECONDS"))
	require.ErrorContains(t, loadEnv(&cfg), "WINDOW_SECONDS")

	blankAll := map[string]string{}
	for k := range relayEnv {
		blankAll[k] = ""
	}
	setRelayEnv(t, blankAll)
	err := loadEnv(&cfg)
	require.ErrorContains(t, err, "INSTANCE_ID")
	require.ErrorContains(t, err, "DEADLINE_EPOCH")
}

// TestLoadEnvDeadlineBeyond2038 pins that epoch seconds are read as 64 bits.
func TestLoadEnvDeadlineBeyond2038(t *testing.T) {
	setRelayEnv(t, map[string]string{"DEADLINE_EPOCH": "2147483648"})
	var cfg relayConfig
	require.NoError(t, loadEnv(&cfg))
	require.Equal(t, int64(2147483648), time.Time(cfg.Deadline).Unix())
}
