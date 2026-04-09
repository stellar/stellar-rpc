package integrationtest

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
)

// TestCORS ensures that we receive the correct CORS headers as a response to an HTTP request.
// Specifically, when we include an Origin header in the request, a stellar-rpc should response
// with a corresponding Access-Control-Allow-Origin.
func TestCORS(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	body := `{"jsonrpc": "2.0", "id": 1, "method": "getHealth"}`
	request, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		test.GetSorobanRPCURL(),
		bytes.NewBufferString(body),
	)
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	origin := "testorigin.com"
	request.Header.Set("Origin", origin)

	var client http.Client
	response, err := client.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	_, err = io.ReadAll(response.Body)
	require.NoError(t, err)

	accessControl := response.Header.Get("Access-Control-Allow-Origin")
	require.Equal(t, origin, accessControl)
}
