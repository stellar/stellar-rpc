package rpcv2test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// RPCError is the error object of a JSON-RPC 2.0 response.
type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
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
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	var out RPCResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}
