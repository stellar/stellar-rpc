package harness

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

func TestIsNotFound(t *testing.T) {
	require.True(t, isNotFound(&types.NoSuchKey{}))
	require.True(t, isNotFound(&smithy.GenericAPIError{Code: "NotFound"}))
	require.False(t, isNotFound(&smithy.GenericAPIError{Code: "AccessDenied"}))
	require.False(t, isNotFound(errors.New("i/o timeout")))
}

// TestResultRoundTrip guards the publisher/poller contract: what PublishResult
// writes must decode back to what Gather relays.
func TestResultRoundTrip(t *testing.T) {
	in := Result{
		SchemaVersion: 1, Verdict: "ok", Markdown: "# r",
		Bench: json.RawMessage(`{"x":1}`), RunID: "123-1", TargetSHA: "abc",
	}
	data, err := json.Marshal(in)
	require.NoError(t, err)
	var out Result
	require.NoError(t, json.Unmarshal(data, &out))
	require.Equal(t, in, out)
}

// TestRequireEnvInts covers the two ways a mis-plumbed workflow reaches the
// int keys: unset and unparseable.
func TestRequireEnvInts(t *testing.T) {
	t.Setenv("POLL_INTERVAL", "30")
	t.Setenv("DEBUG_LOG_EVERY_POLLS", "10")

	ints, err := RequireEnvInts("POLL_INTERVAL", "DEBUG_LOG_EVERY_POLLS")
	require.NoError(t, err)
	require.Equal(t, map[string]int{"POLL_INTERVAL": 30, "DEBUG_LOG_EVERY_POLLS": 10}, ints)

	_, err = RequireEnvInts("POLL_INTERVAL", "NOT_SET_AT_ALL")
	require.ErrorContains(t, err, "missing required env: NOT_SET_AT_ALL")

	t.Setenv("POLL_INTERVAL", "half a minute")
	_, err = RequireEnvInts("POLL_INTERVAL")
	require.ErrorContains(t, err, "POLL_INTERVAL")
}

func TestRequirePositive(t *testing.T) {
	ints := map[string]int{"A": 1, "ZERO": 0, "NEG": -1}
	require.NoError(t, requirePositive(ints, "A"))
	require.ErrorContains(t, requirePositive(ints, "A", "ZERO"), "ZERO must be positive, got 0")
	require.ErrorContains(t, requirePositive(ints, "NEG"), "NEG must be positive, got -1")
	// An absent key reads as zero, which is what the callers want flagged.
	require.ErrorContains(t, requirePositive(ints, "ABSENT"), "ABSENT must be positive, got 0")
}

func TestTailWriter(t *testing.T) {
	w := &tailWriter{max: 5}
	for range 1000 {
		_, err := w.Write([]byte("x"))
		require.NoError(t, err)
	}
	_, err := w.Write([]byte("END"))
	require.NoError(t, err)
	require.Equal(t, "xxEND", w.String())
	require.LessOrEqual(t, len(w.buf), w.max)
}
