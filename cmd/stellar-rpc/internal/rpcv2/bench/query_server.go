package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
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

// maxEventsLimit is the daemon's default queryEvents cap.
func maxEventsLimit() uint {
	return *servingConfig().Service.Methods.QueryEvents.MaxItemsPerResponse
}

// handlerConcurrency is how many handlers the read server runs at once: the
// daemon's bridge runs one per CPU.
func handlerConcurrency() int { return runtime.NumCPU() }

// readServer serves the daemon's read methods through the daemon's JSON-RPC
// bridge, in process and without HTTP. A call's latency covers the bridge's
// request parse, the wait for a handler slot, the handler and the response
// write; its handler time covers the handler alone, read view included.
type readServer struct {
	bridge jsonrpc.Bridge
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

// call sends one request and decodes its result into result. Only the bridge's
// ServeHTTP is timed: the request is encoded before the timer starts and the
// response is decoded after it stops. The timing has no items.
func (s *readServer) call(ctx context.Context, method string, params, result any) (requestTiming, error) {
	body, err := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: 1, Method: method, Params: params})
	if err != nil {
		return requestTiming{}, fmt.Errorf("encode %s request: %w", method, err)
	}
	var handlerLatency time.Duration
	req, err := http.NewRequestWithContext(context.WithValue(ctx, handlerTimeKey{}, &handlerLatency),
		http.MethodPost, "/", bytes.NewReader(body))
	if err != nil {
		return requestTiming{}, fmt.Errorf("build %s request: %w", method, err)
	}
	req.Header.Set("Content-Type", "application/json")
	w := &responseBuffer{header: http.Header{}}

	start := time.Now()
	s.bridge.ServeHTTP(w, req)
	latency := time.Since(start)

	if w.status != http.StatusOK {
		return requestTiming{}, fmt.Errorf("%s: HTTP status %d: %s", method, w.status, w.body.Bytes())
	}
	var rsp rpcResponse
	if err := json.Unmarshal(w.body.Bytes(), &rsp); err != nil {
		return requestTiming{}, fmt.Errorf("decode %s response: %w", method, err)
	}
	if rsp.Error != nil {
		// The handler reports a canceled request as a JSON-RPC error.
		if err := ctx.Err(); err != nil {
			return requestTiming{}, err
		}
		return requestTiming{}, fmt.Errorf("%s: %w", method, rsp.Error)
	}
	if err := json.Unmarshal(rsp.Result, result); err != nil {
		return requestTiming{}, fmt.Errorf("decode %s result: %w", method, err)
	}
	return requestTiming{latency: latency, handlerLatency: handlerLatency}, nil
}

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

// handlerTimeKey keys the request context's handler time slot, a
// *time.Duration that timeHandler sets.
type handlerTimeKey struct{}

// timeHandler returns h, writing its run time into the request context's
// handler time slot when the context has one.
func timeHandler(h jrpc2.Handler) jrpc2.Handler {
	return func(ctx context.Context, req *jrpc2.Request) (any, error) {
		start := time.Now()
		res, err := h(ctx, req)
		if slot, ok := ctx.Value(handlerTimeKey{}).(*time.Duration); ok {
			*slot = time.Since(start)
		}
		return res, err
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
