package rpcv2test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// RPCError is the error object of a JSON-RPC 2.0 response.
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// RPCResponse is a JSON-RPC 2.0 response with the result left raw, so a test
// decodes only the fields it asserts on.
type RPCResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *RPCError       `json:"error"`
}

// PostRPC sends one JSON-RPC 2.0 request over HTTP and returns the decoded
// response. It speaks the raw wire on purpose: tests using it assert on wire
// error codes and on methods the SDK client does not expose (getEventsV2).
// params is a JSON literal, e.g. `{}` or `{"startLedger":2}`.
func PostRPC(t *testing.T, url, method, params string) RPCResponse {
	t.Helper()
	body := `{"jsonrpc":"2.0","id":1,"method":"` + method + `","params":` + params + `}`
	resp, err := http.Post(url, "application/json", bytes.NewReader([]byte(body))) //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close()
	var out RPCResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}
