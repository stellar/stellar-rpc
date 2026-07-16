package serve

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	proto "github.com/stellar/go-stellar-sdk/protocols/stellarcore"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/latencytrack"
)

// fakeHealthSignal is a programmable HealthSignal.
type fakeHealthSignal struct {
	ready     bool
	lastClose time.Time
}

func (f *fakeHealthSignal) Ready() bool { return f.ready }

func (f *fakeHealthSignal) LastCommitClose() (time.Time, bool) {
	return f.lastClose, !f.lastClose.IsZero()
}

// fakeFastCoreClient answers every getLedgerEntries probe with an empty
// (all-not-found) response.
type fakeFastCoreClient struct{ calls int }

func (f *fakeFastCoreClient) GetLedgerEntries(
	_ context.Context, _ uint32, keys ...xdr.LedgerKey,
) (proto.GetLedgerEntryResponse, error) {
	f.calls++
	entries := make([]proto.LedgerEntryResponse, 0, len(keys))
	for range keys {
		entries = append(entries, proto.LedgerEntryResponse{State: proto.LedgerEntryStateNotFound})
	}
	return proto.GetLedgerEntryResponse{Entries: entries}, nil
}

func testServingConfig(t *testing.T) config.ServingConfig {
	t.Helper()
	cfg, err := config.ParseConfig([]byte("[serving]\nendpoint = \"127.0.0.1:0\"\n"))
	require.NoError(t, err)
	return cfg.Serving
}

func newTestServer(t *testing.T, health HealthSignal, fast *fakeFastCoreClient) (*Server, *latencytrack.Set) {
	t.Helper()
	latency := new(latencytrack.Set)
	srvCfg := Config{
		Logger:            silentLogger(),
		PromRegistry:      prometheus.NewRegistry(),
		Latency:           latency,
		Health:            health,
		NetworkPassphrase: network.TestNetworkPassphrase,
		Serving:           testServingConfig(t),
		RetentionChunks:   0,
	}
	if fast != nil {
		srvCfg.FastCoreClient = fast
	}
	srv, err := NewServer(srvCfg)
	require.NoError(t, err)
	t.Cleanup(srv.Close)
	return srv, latency
}

// rpcCall posts one JSON-RPC request and returns the decoded result or error,
// plus the raw response body for wire-level assertions.
type rpcErrorObj struct {
	Code    int64  `json:"code"`
	Message string `json:"message"`
}

func rpcCall(t *testing.T, url, method string, params any) (json.RawMessage, *rpcErrorObj, []byte) {
	t.Helper()
	reqBody := map[string]any{"jsonrpc": "2.0", "id": 1, "method": method}
	if params != nil {
		reqBody["params"] = params
	}
	payload, err := json.Marshal(reqBody)
	require.NoError(t, err)
	resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var decoded struct {
		Result json.RawMessage `json:"result"`
		Error  *rpcErrorObj    `json:"error"`
	}
	require.NoError(t, json.Unmarshal(body, &decoded), "response body: %s", body)
	return decoded.Result, decoded.Error, body
}

func requireBackfillError(t *testing.T, rpcErr *rpcErrorObj) {
	t.Helper()
	require.NotNil(t, rpcErr)
	assert.EqualValues(t, -32603, rpcErr.Code)
	assert.Equal(t, "backfill in progress; query serving not started", rpcErr.Message)
}

func TestServer_GateLifecycleAndEndpoints(t *testing.T) {
	fx := buildFixture(t)
	health := &fakeHealthSignal{ready: true, lastClose: time.Now()}
	fast := &fakeFastCoreClient{}
	srv, _ := newTestServer(t, health, fast)

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	// --- Gate closed: every data method answers "backfill in progress"... ---
	for _, method := range []string{
		protocol.GetLedgersMethodName, protocol.GetTransactionsMethodName,
		protocol.GetTransactionMethodName, protocol.GetEventsMethodName,
		protocol.GetLatestLedgerMethodName, protocol.GetNetworkMethodName,
		protocol.GetVersionInfoMethodName, protocol.GetLedgerEntriesMethodName,
	} {
		_, rpcErr, _ := rpcCall(t, ts.URL, method, map[string]any{})
		requireBackfillError(t, rpcErr)
	}

	// ...while getHealth reports the phase as a SUCCESS response...
	res, rpcErr, _ := rpcCall(t, ts.URL, protocol.GetHealthMethodName, nil)
	require.Nil(t, rpcErr)
	var healthRes protocol.GetHealthResponse
	require.NoError(t, json.Unmarshal(res, &healthRes))
	assert.Equal(t, "backfill in progress", healthRes.Status)
	assert.Zero(t, healthRes.LatestLedger)

	// ...metrics answers with both series groups...
	res, rpcErr, _ = rpcCall(t, ts.URL, metricsMethodName, nil)
	require.Nil(t, rpcErr)
	var metricsRes struct {
		Ingestion map[string]json.RawMessage `json:"ingestion"`
		RPC       map[string]json.RawMessage `json:"rpc"`
	}
	require.NoError(t, json.Unmarshal(res, &metricsRes))
	require.NotNil(t, metricsRes.Ingestion)
	require.NotNil(t, metricsRes.RPC)
	assert.Contains(t, metricsRes.RPC, protocol.GetHealthMethodName,
		"the getHealth call above must already have recorded a latency sample")

	// ...and the stubs are unsupported in every phase.
	for _, method := range []string{
		protocol.SendTransactionMethodName, protocol.SimulateTransactionMethodName,
		protocol.GetFeeStatsMethodName,
	} {
		_, rpcErr, _ := rpcCall(t, ts.URL, method, map[string]any{})
		require.NotNil(t, rpcErr)
		assert.EqualValues(t, -32601, rpcErr.Code, "method %s", method)
		assert.Contains(t, rpcErr.Message, "not supported by the full-history service")
	}

	// --- Open the gate over the fixture registry. ---
	runCtx, cancelRun := context.WithCancel(context.Background())
	require.NoError(t, srv.ServeReads(runCtx, fx.reg))

	// getHealth flips to the v1 semantics: healthy, real range, full-history
	// window = the served span.
	res, rpcErr, _ = rpcCall(t, ts.URL, protocol.GetHealthMethodName, nil)
	require.Nil(t, rpcErr)
	require.NoError(t, json.Unmarshal(res, &healthRes))
	assert.Equal(t, "healthy", healthRes.Status)
	assert.Equal(t, fx.latest, healthRes.LatestLedger)
	assert.EqualValues(t, chunk.FirstLedgerSeq, healthRes.OldestLedger)
	assert.Equal(t, fx.latest-chunk.FirstLedgerSeq+1, healthRes.LedgerRetentionWindow)

	// getLatestLedger.
	res, rpcErr, _ = rpcCall(t, ts.URL, protocol.GetLatestLedgerMethodName, nil)
	require.Nil(t, rpcErr)
	var latestRes protocol.GetLatestLedgerResponse
	require.NoError(t, json.Unmarshal(res, &latestRes))
	assert.Equal(t, fx.latest, latestRes.Sequence)

	// getLedgers over the cold→hot boundary.
	res, rpcErr, _ = rpcCall(t, ts.URL, protocol.GetLedgersMethodName,
		map[string]any{"startLedger": fxChunkHot.FirstLedger() - 2, "pagination": map[string]any{"limit": 4}})
	require.Nil(t, rpcErr)
	var ledgersRes protocol.GetLedgersResponse
	require.NoError(t, json.Unmarshal(res, &ledgersRes))
	require.Len(t, ledgersRes.Ledgers, 4)
	assert.Equal(t, fxChunkHot.FirstLedger()-2, ledgersRes.Ledgers[0].Sequence)

	// getTransaction: one cold (through the window .idx) and one hot.
	for _, tx := range []fxTx{fx.coldTxs[0], fx.hotTxs[0]} {
		res, rpcErr, _ = rpcCall(t, ts.URL, protocol.GetTransactionMethodName,
			map[string]any{"hash": tx.hash.HexString()})
		require.Nil(t, rpcErr)
		var txRes protocol.GetTransactionResponse
		require.NoError(t, json.Unmarshal(res, &txRes))
		wantStatus := protocol.TransactionStatusSuccess
		if !tx.successful {
			wantStatus = protocol.TransactionStatusFailed
		}
		assert.Equal(t, wantStatus, txRes.Status)
		assert.EqualValues(t, tx.seq, txRes.Ledger)
	}

	// getTransactions from the hot chunk's first ledger.
	res, rpcErr, _ = rpcCall(t, ts.URL, protocol.GetTransactionsMethodName,
		map[string]any{"startLedger": fxChunkHot.FirstLedger()})
	require.Nil(t, rpcErr)
	var txsRes protocol.GetTransactionsResponse
	require.NoError(t, json.Unmarshal(res, &txsRes))
	assert.NotEmpty(t, txsRes.Transactions)

	// getEvents over the cold chunk: real events, and the raw wire bytes must
	// not carry the deprecated inSuccessfulContractCall key — v2 drops it,
	// while the SDK's response struct would emit it on every event.
	res, rpcErr, rawBody := rpcCall(t, ts.URL, protocol.GetEventsMethodName,
		map[string]any{"startLedger": chunk.FirstLedgerSeq})
	require.Nil(t, rpcErr)
	var eventsRes getEventsResponse
	require.NoError(t, json.Unmarshal(res, &eventsRes))
	require.NotEmpty(t, eventsRes.Events, "fixture cold chunk carries events")
	assert.NotContains(t, string(rawBody), "inSuccessfulContractCall")
	for _, wantKey := range []string{
		`"type"`, `"ledger"`, `"ledgerClosedAt"`, `"contractId"`, `"id"`,
		`"operationIndex"`, `"transactionIndex"`, `"txHash"`, `"topic"`, `"value"`,
	} {
		assert.Contains(t, string(rawBody), wantKey)
	}

	// getNetwork echoes the assembly passphrase (no friendbot configured).
	res, rpcErr, _ = rpcCall(t, ts.URL, protocol.GetNetworkMethodName, nil)
	require.Nil(t, rpcErr)
	var netRes protocol.GetNetworkResponse
	require.NoError(t, json.Unmarshal(res, &netRes))
	assert.Equal(t, network.TestNetworkPassphrase, netRes.Passphrase)
	assert.Empty(t, netRes.FriendbotURL)

	// getVersionInfo: no CaptiveStellarCore handle in this daemon, so the core
	// version reads "unavailable" (the nil-guard) and the protocol version
	// comes from the latest ledger.
	res, rpcErr, _ = rpcCall(t, ts.URL, protocol.GetVersionInfoMethodName, nil)
	require.Nil(t, rpcErr)
	var verRes protocol.GetVersionInfoResponse
	require.NoError(t, json.Unmarshal(res, &verRes))
	assert.Equal(t, "unavailable", verRes.CaptiveCoreVersion)

	// getLedgerEntries through the fake query client: a valid key that does
	// not exist reads back as an empty entry set at the served latest.
	accountID := xdr.MustAddress("GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H")
	key := xdr.LedgerKey{
		Type:    xdr.LedgerEntryTypeAccount,
		Account: &xdr.LedgerKeyAccount{AccountId: accountID},
	}
	keyB64, err := xdr.MarshalBase64(key)
	require.NoError(t, err)
	res, rpcErr, _ = rpcCall(t, ts.URL, protocol.GetLedgerEntriesMethodName,
		map[string]any{"keys": []string{keyB64}})
	require.Nil(t, rpcErr)
	var entriesRes protocol.GetLedgerEntriesResponse
	require.NoError(t, json.Unmarshal(res, &entriesRes))
	assert.Empty(t, entriesRes.Entries)
	assert.Equal(t, fx.latest, entriesRes.LatestLedger)
	assert.Positive(t, fast.calls)

	// metrics now carries per-endpoint samples for the calls above.
	res, rpcErr, _ = rpcCall(t, ts.URL, metricsMethodName, nil)
	require.Nil(t, rpcErr)
	var metricsAfter struct {
		RPC map[string]struct {
			Count uint64 `json:"count"`
		} `json:"rpc"`
	}
	require.NoError(t, json.Unmarshal(res, &metricsAfter))
	assert.Positive(t, metricsAfter.RPC[protocol.GetLedgersMethodName].Count)
	assert.Positive(t, metricsAfter.RPC[protocol.GetEventsMethodName].Count)

	// --- The run ends: the gate closes again. ---
	cancelRun()
	require.Eventually(t, func() bool {
		_, rpcErr, _ := rpcCall(t, ts.URL, protocol.GetLedgersMethodName, map[string]any{})
		return rpcErr != nil && rpcErr.Message == errBackfillInProgress.Message
	}, 5*time.Second, 10*time.Millisecond, "gate must close when the run's ctx ends")
}

func TestServer_HealthUnhealthyWhenStale(t *testing.T) {
	fx := buildFixture(t)
	// No live commit observed and the fixture's ledger close times are the
	// epoch — the range fallback clock — so the healthy threshold is blown.
	srv, _ := newTestServer(t, &fakeHealthSignal{}, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	require.NoError(t, srv.ServeReads(t.Context(), fx.reg))

	_, rpcErr, _ := rpcCall(t, ts.URL, protocol.GetHealthMethodName, nil)
	require.NotNil(t, rpcErr)
	assert.Contains(t, rpcErr.Message, "since last known ledger closed is too high")
}

func TestServer_GetLedgerEntriesDisabledWithoutQueryServer(t *testing.T) {
	srv, _ := newTestServer(t, &fakeHealthSignal{}, nil) // no FastCoreClient
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	_, rpcErr, _ := rpcCall(t, ts.URL, protocol.GetLedgerEntriesMethodName, map[string]any{})
	require.NotNil(t, rpcErr)
	assert.EqualValues(t, -32601, rpcErr.Code)
	assert.Contains(t, rpcErr.Message, "captive-core query server is off")
}

// TestEventInfoMirrorsProtocolShape pins the local getEvents response structs
// to the SDK's: same JSON tags, same order, minus exactly the deprecated
// inSuccessfulContractCall field — so an SDK field addition breaks this test
// instead of silently diverging the v2 wire shape.
func TestEventInfoMirrorsProtocolShape(t *testing.T) {
	tagsOf := func(typ reflect.Type) []string {
		tags := make([]string, 0, typ.NumField())
		for i := range typ.NumField() {
			tags = append(tags, typ.Field(i).Tag.Get("json"))
		}
		return tags
	}

	sdkTags := tagsOf(reflect.TypeOf(protocol.EventInfo{}))
	wantTags := make([]string, 0, len(sdkTags))
	for _, tag := range sdkTags {
		if tag == "inSuccessfulContractCall" {
			continue
		}
		wantTags = append(wantTags, tag)
	}
	assert.Equal(t, wantTags, tagsOf(reflect.TypeOf(eventInfo{})),
		"serve.eventInfo must mirror protocol.EventInfo minus the deprecated field")
	assert.Len(t, sdkTags, len(wantTags)+1,
		"protocol.EventInfo must carry exactly one field serve.eventInfo drops")

	respTags := tagsOf(reflect.TypeOf(protocol.GetEventsResponse{}))
	haveRespTags := tagsOf(reflect.TypeOf(getEventsResponse{}))
	assert.Equal(t, respTags, haveRespTags,
		"serve.getEventsResponse must mirror protocol.GetEventsResponse tag-for-tag")
}
