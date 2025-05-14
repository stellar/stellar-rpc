package protocol

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimulatingNonRootAuth(t *testing.T) {
	var request SimulateTransactionRequest
	requestString := `{ "transaction": "pretend this is XDR" }`

	require.NoError(t, json.Unmarshal([]byte(requestString), &request))
	require.False(t, request.NonrootAuth) // ensure false if omitted

	requestString = `{ "transaction": "pretend this is XDR", "nonroot": true }`
	require.NoError(t, json.Unmarshal([]byte(requestString), &request))
	require.True(t, request.NonrootAuth)
}
