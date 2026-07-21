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
