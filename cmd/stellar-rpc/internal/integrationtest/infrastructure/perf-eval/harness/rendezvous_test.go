package harness

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The chained-leg rendezvous hinges on these: a stop/ready object from another
// attempt of the same workflow run must be recognized, and its attempt ordered.
func TestRunIDHelpers(t *testing.T) {
	require.True(t, SameWorkflowRun("123-1", "123-2"))
	require.False(t, SameWorkflowRun("123-1", "124-1"))
	require.True(t, SameWorkflowRun("manual", "manual"))

	require.Equal(t, 1, RunAttempt("123-1"))
	require.Equal(t, 3, RunAttempt("123-3"))
	require.Equal(t, -1, RunAttempt("manual"))
	require.Equal(t, -1, RunAttempt("123-x"))
}
