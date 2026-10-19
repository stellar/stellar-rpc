package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/jsonrpc"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
)

// executeQuery runs bench query with args and a new --out, and requires success.
func executeQuery(t *testing.T, args ...string) {
	t.Helper()
	cmd := newQueryCommand()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(append(args, "--out", filepath.Join(t.TempDir(), "out")))
	require.NoError(t, cmd.Execute())
}

// callResult sends one request through s's bridge and decodes its result into
// result.
func callResult(t *testing.T, s *readServer, method string, params, result any) {
	t.Helper()
	body, err := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: rpcRequestID, Method: method, Params: params})
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	w := &responseBuffer{header: http.Header{}}
	s.bridge.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.status, "%s", w.body.Bytes())
	var rsp rpcResponse
	require.NoError(t, json.Unmarshal(w.body.Bytes(), &rsp))
	require.Nil(t, rsp.Error)
	require.NoError(t, json.Unmarshal(rsp.Result, result))
}

// newTestReadServer serves methods through a bridge the way newReadServer
// does.
func newTestReadServer(t *testing.T, methods handler.Map) *readServer {
	t.Helper()
	for name, h := range methods {
		methods[name] = timeHandler(h)
	}
	s := &readServer{bridge: jsonrpc.NewBridge(methods, testLogger())}
	t.Cleanup(s.close)
	return s
}

// fixedHandler is a jrpc2.Handler that returns v and err.
func fixedHandler(v any, err error) jrpc2.Handler {
	return func(context.Context, *jrpc2.Request) (any, error) { return v, err }
}

// call succeeds only when the handler returned no error and the bridge wrote
// its result, and it counts the items of a getLedgers or getTransactions
// result.
func TestReadServerCall(t *testing.T) {
	s := newTestReadServer(t, handler.Map{
		protocol.GetLedgersMethodName: fixedHandler(
			protocol.GetLedgersResponse{Ledgers: make([]protocol.LedgerInfo, 3)}, nil),
		protocol.GetTransactionsMethodName: fixedHandler(
			protocol.GetTransactionsResponse{Transactions: make([]protocol.TransactionInfo, 2)}, nil),
		"other":       fixedHandler(7, nil),
		"fails":       fixedHandler(nil, errors.New("boom")),
		"unencodable": fixedHandler(func() {}, nil),
	})

	for method, want := range map[string]int{
		protocol.GetLedgersMethodName: 3, protocol.GetTransactionsMethodName: 2, "other": 0,
	} {
		timing, err := s.call(t.Context(), method, nil)
		require.NoError(t, err, method)
		assert.Equal(t, want, timing.items, method)
		assert.Positive(t, timing.latency, method)
		assert.LessOrEqual(t, timing.handlerLatency, timing.latency, method)
	}

	_, err := s.call(t.Context(), "fails", nil)
	require.ErrorContains(t, err, "boom")
	_, err = s.call(t.Context(), "unencodable", nil)
	require.ErrorContains(t, err, "unencodable:", "the bridge failed to encode the result")
	_, err = s.call(t.Context(), "missing", nil)
	require.ErrorContains(t, err, "missing:", "no handler ran")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = s.call(ctx, "fails", nil)
	require.ErrorIs(t, err, context.Canceled)
}

// newRequest is the constructor of one request type.
type newRequest func(*queryDataset, queryPlan) queryRequest

// requestItems narrows the starts ds picks to [lo, hi], sets the ledgers limit
// to the range's size, and runs one request.
func requestItems(ds *queryDataset, p queryPlan, newReq newRequest, lo, hi uint32) (int, error) {
	ds.FirstLedger, ds.LastLedger = lo, hi
	p.LedgersLimit = hi - lo + 1
	timing, err := newReq(ds, p)(context.Background(), rand.New(rand.NewPCG(defaultSeed, defaultSeed)))
	return timing.items, err
}

// A one-ledger ledgers request reads one ledger, a request past the latest
// ledger fails in the handler, and a ledgers request that returns fewer
// ledgers than its limit fails.
func TestQueryRequestReads(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	plan := testPlan(protocol.FormatBase64)
	ds, release, err := openDataset(testLogger(), queryOptions{DataDir: hotRoot, Plan: plan})
	require.NoError(t, err)
	defer release()
	// The first ledger of the fixture carries one transaction.
	first, last := ds.FirstLedger, ds.LastLedger

	for _, tc := range []struct {
		qtype  string
		newReq newRequest
	}{
		{queryTypeLedgers, ledgersRequest},
		{queryTypeTxPage, txPageRequest},
	} {
		t.Run(tc.qtype+" past the latest ledger", func(t *testing.T) {
			_, err := requestItems(ds, plan, tc.newReq, last+1, last+1)
			require.ErrorContains(t, err, fmt.Sprintf("start ledger (%d) must be between", last+1))
		})
	}

	t.Run("point read", func(t *testing.T) {
		items, err := requestItems(ds, plan, ledgersRequest, first, first)
		require.NoError(t, err)
		assert.Equal(t, 1, items)
	})

	t.Run("missing ledgers", func(t *testing.T) {
		// The dataset claims five ledgers past the last one the store holds.
		_, err := requestItems(ds, plan, ledgersRequest, last-4, last+5)
		require.ErrorContains(t, err, "getLedgers returned 5 of the 10 ledgers")
	})
}

// A hot run creates no hot database in the dataset.
func TestQueryHotRunAddsNoChunk(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	before := dirNames(t, geometry.NewLayout(hotRoot).HotRoot())
	executeQuery(t, "--data-dir", hotRoot,
		"--types", queryTypeLedgers, "--target-rps", "1000", "--duration", "10ms")
	assert.Equal(t, before, dirNames(t, geometry.NewLayout(hotRoot).HotRoot()))
}

// A cold run over two chunks reads across the chunk border and creates no hot
// database in the dataset.
func TestQueryColdTwoChunks(t *testing.T) {
	srcRoot := t.TempDir()
	packDir, _ := writeSourcePack(t, srcRoot, 0, chunk.LedgersPerChunk)
	_, _ = writeSourcePack(t, srcRoot, 1, chunk.LedgersPerChunk)
	coldRoot := t.TempDir()
	require.NoError(t, runCold(context.Background(), testLogger(), coldOptions{
		Source:     sourceConfig{Kind: sourcePack, PackDir: packDir},
		StartChunk: 0,
		NumChunks:  2,
		Workers:    2,
		ColdRoot:   coldRoot,
		OutDir:     filepath.Join(t.TempDir(), "csv"),
	}))

	plan := testPlan(protocol.FormatBase64)
	ds, release, err := openDataset(testLogger(), queryOptions{DataDir: coldRoot, Plan: plan})
	require.NoError(t, err)
	assert.Equal(t, chunk.ID(1).LastLedger(), ds.LastLedger)
	// Five ledgers on each side of the border; only chunk 1's first ledger
	// carries a transaction.
	lo, hi := chunk.ID(0).LastLedger()-4, chunk.ID(1).FirstLedger()+4
	items, err := requestItems(ds, plan, ledgersRequest, lo, hi)
	require.NoError(t, err)
	assert.Equal(t, 10, items)
	plan.TxPageLimit = 1
	items, err = requestItems(ds, plan, txPageRequest, lo, lo)
	require.NoError(t, err)
	assert.Equal(t, 1, items)
	release()

	before := dirNames(t, geometry.NewLayout(coldRoot).HotRoot())
	executeQuery(t, "--data-dir", coldRoot,
		"--types", queryTypeLedgers+","+queryTypeTxPage, "--target-rps", "1000", "--duration", "10ms",
		"--format", protocol.FormatJSON)
	assert.Equal(t, before, dirNames(t, geometry.NewLayout(coldRoot).HotRoot()))
}
