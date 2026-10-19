package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/jsonrpc"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
)

// queryFormats is every --format value.
//
//nolint:gochecknoglobals // fixed vocabulary, read-only
var queryFormats = []string{protocol.FormatBase64, protocol.FormatJSON}

// servingConfig is the rpcv2 config the read methods are built with: the
// daemon's defaults.
func servingConfig() config.Config { return config.Config{}.WithDefaults() }

// maxLedgersLimit is the daemon's default getLedgers cap.
func maxLedgersLimit() uint {
	return *servingConfig().Service.Methods.GetLedgers.MaxItemsPerResponse
}

// maxTxPageLimit is the daemon's default getTransactions cap.
func maxTxPageLimit() uint {
	return *servingConfig().Service.Methods.GetTransactions.MaxItemsPerResponse
}

// readServer serves the daemon's read methods through the daemon's JSON-RPC
// bridge, in process and without HTTP. A call's latency covers the bridge's
// request parse, the wait for a handler slot, the handler and the response
// write; its handler time covers the handler alone, read view included.
type readServer struct {
	bridge *jsonrpc.Bridge
}

// newReadServer starts a read server over registry. The caller must close it
// before it closes registry.
func newReadServer(registry *query.Registry, passphrase string, logger *supportlog.Entry) *readServer {
	methods := rpcv2.ReadMethods(servingConfig(), registry, passphrase, logger)
	for name, h := range methods {
		methods[name] = timeHandler(h)
	}
	return &readServer{bridge: jsonrpc.NewBridge(methods, logger)}
}

// close cancels the running handlers and waits for them.
func (s *readServer) close() { s.bridge.Close() }

// call sends one request and returns its timing and item count. Only the
// bridge's ServeHTTP is timed. A successful response body is not decoded: the
// handler reports its result's item count through the request context.
func (s *readServer) call(ctx context.Context, method string, params any) (requestTiming, error) {
	body, err := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: rpcRequestID, Method: method, Params: params})
	if err != nil {
		return requestTiming{}, fmt.Errorf("encode %s request: %w", method, err)
	}
	var res handlerResult
	req, err := http.NewRequestWithContext(context.WithValue(ctx, handlerResultKey{}, &res),
		http.MethodPost, "/", bytes.NewReader(body))
	if err != nil {
		return requestTiming{}, fmt.Errorf("build %s request: %w", method, err)
	}
	req.Header.Set("Content-Type", "application/json")
	w := &responseBuffer{header: http.Header{}}

	start := time.Now()
	s.bridge.ServeHTTP(w, req)
	latency := time.Since(start)

	// The prefix check catches a response the bridge turned into an error
	// after the handler returned, such as a result that failed to encode.
	if w.status == http.StatusOK && res.ran && res.err == nil && bytes.HasPrefix(w.body.Bytes(), rpcSuccessPrefix) {
		return requestTiming{latency: latency, handlerLatency: res.latency, items: res.items}, nil
	}
	return requestTiming{}, responseError(ctx, method, w)
}

// responseError returns the error that the failed response in w reports. A
// canceled ctx returns ctx.Err().
func responseError(ctx context.Context, method string, w *responseBuffer) error {
	if w.status != http.StatusOK {
		return fmt.Errorf("%s: HTTP status %d: %s", method, w.status, w.body.Bytes())
	}
	var rsp rpcResponse
	if err := json.Unmarshal(w.body.Bytes(), &rsp); err != nil {
		return fmt.Errorf("decode %s response: %w", method, err)
	}
	if rsp.Error == nil {
		return fmt.Errorf("%s: unexpected response: %.200s", method, w.body.Bytes())
	}
	// The handler reports a canceled request as a JSON-RPC error.
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("%s: %w", method, rsp.Error)
}

// rpcRequestID is the ID of every request call sends.
const rpcRequestID = 1

// rpcSuccessPrefix starts the bridge's response to a call that succeeded.
//
//nolint:gochecknoglobals // read-only
var rpcSuccessPrefix = []byte(`{"jsonrpc":"2.0","id":` + strconv.Itoa(rpcRequestID) + `,"result":`)

// rpcRequest is a JSON-RPC 2.0 request.
type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

// rpcResponse is a JSON-RPC 2.0 response.
type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *jrpc2.Error    `json:"error"`
}

// handlerResultKey keys the request context's handler result slot, a
// *handlerResult that timeHandler fills.
type handlerResultKey struct{}

// handlerResult is what timeHandler records about one handler call.
type handlerResult struct {
	ran     bool
	latency time.Duration
	items   int
	err     error
}

// timeHandler returns h, recording its run time, item count and error in the
// request context's handler result slot when the context has one.
func timeHandler(h jrpc2.Handler) jrpc2.Handler {
	return func(ctx context.Context, req *jrpc2.Request) (any, error) {
		start := time.Now()
		res, err := h(ctx, req)
		if slot, ok := ctx.Value(handlerResultKey{}).(*handlerResult); ok {
			*slot = handlerResult{ran: true, latency: time.Since(start), items: itemCount(res), err: err}
		}
		return res, err
	}
}

// itemCount returns how many ledgers or transactions a getLedgers or
// getTransactions result holds, and 0 for any other result.
func itemCount(res any) int {
	switch r := res.(type) {
	case protocol.GetLedgersResponse:
		return len(r.Ledgers)
	case protocol.GetTransactionsResponse:
		return len(r.Transactions)
	default:
		return 0
	}
}

// responseBuffer is an http.ResponseWriter that keeps the response in memory.
type responseBuffer struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (w *responseBuffer) Header() http.Header { return w.header }

func (w *responseBuffer) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(p)
}

func (w *responseBuffer) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}
