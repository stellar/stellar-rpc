package integrationtest

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/integrationtest/infrastructure"
)

// TestBuiltinRPCMethodsDisabled verifies that the jrpc2 library's built-in
// rpc.* methods (e.g. rpc.serverInfo, which leaks node lifetime metrics and
// process start time) are not reachable via the HTTP bridge.
func TestBuiltinRPCMethodsDisabled(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	body := `{"jsonrpc": "2.0", "id": 1, "method": "rpc.serverInfo"}`
	request, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		test.GetSorobanRPCURL(),
		bytes.NewBufferString(body),
	)
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")

	var client http.Client
	response, err := client.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	require.NoError(t, err)

	var resp struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal(raw, &resp))
	require.Nil(t, resp.Result, "rpc.serverInfo must not return a result")
	require.NotNil(t, resp.Error, "rpc.serverInfo must return a JSON-RPC error")
	// jrpc2 method-not-found error code.
	require.Equal(t, -32601, resp.Error.Code)
}
