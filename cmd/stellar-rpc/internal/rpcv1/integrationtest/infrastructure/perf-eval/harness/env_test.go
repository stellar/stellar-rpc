package harness

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestLoadEnv checks that both commands read the same shared variables plus
// their own, and that a bad variable is named in the error.
func TestLoadEnv(t *testing.T) {
	setRelayEnv(t, nil)
	t.Setenv("RESULTS_TIMEOUT", "300")

	var gather gatherConfig
	require.NoError(t, loadEnv(&gather))
	require.Equal(t, 300, gather.ResultsTimeoutSecs)
	require.Equal(t, "i-0123456789abcdef0", gather.Poller.InstanceID)
	require.Equal(t, "us-east-1", gather.Poller.Region)
	require.Equal(t, "/dev/null", gather.Poller.GitHubOutput)
	require.Equal(t, "stellar-rpc-ci-load-test", gather.Poller.Bucket)
	require.Equal(t, "runs/1/campaign/result.json", gather.Poller.ResultKey)
	require.Equal(t, "1-1", gather.Poller.RunID)
	require.Equal(t, 40, gather.Poller.DebugLogLines)
	require.Equal(t, 10, gather.Poller.DebugLogEveryPolls)
	require.Equal(t, 30*time.Second, gather.Poller.pollInterval())

	var relay relayConfig
	require.NoError(t, loadEnv(&relay))
	require.Equal(t, 19200, relay.WindowSecs)
	require.Equal(t, 1700000000, relay.DeadlineEpoch)
	require.Equal(t, gather.Poller, relay.Poller)
}

func TestLoadEnvNamesTheBadVariable(t *testing.T) {
	var cfg relayConfig

	setRelayEnv(t, map[string]string{"DEBUG_LOG_LINES": "abc"})
	require.ErrorContains(t, loadEnv(&cfg), "DEBUG_LOG_LINES")

	setRelayEnv(t, map[string]string{"WINDOW_SECONDS": ""})
	require.ErrorContains(t, loadEnv(&cfg), "WINDOW_SECONDS")
}
