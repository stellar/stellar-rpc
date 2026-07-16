package fullhistory

// =============================================================================
// SERVING SMOKE E2E — the full daemon with [serving].endpoint set, driven over
// real HTTP through both phases:
//
//   1. BACKFILLING (the backend's Tip is held): the JSON-RPC listener is
//      already up; data methods answer "backfill in progress", getHealth
//      reports the phase as a success, `metrics` answers, stubs answer
//      "not supported", and the admin listener serves /debug/pprof.
//   2. SERVING (Tip released; a young network, so backfill is a no-op and the
//      fake core's frames commit live): getHealth flips to healthy and every
//      served endpoint answers real data; `metrics` carries both ingestion and
//      per-endpoint quantiles; the raw getEvents body has no
//      inSuccessfulContractCall key.
//
// The captive-core query server cannot run here (no real core); the injected
// fastCoreClient seam covers getLedgerEntries' wiring instead. The devbox run
// covers the real query server.
// =============================================================================

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	proto "github.com/stellar/go-stellar-sdk/protocols/stellarcore"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/fhtest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/registry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// gatedTipBackend is a fakeBackend whose Tip BLOCKS until release is closed —
// holding the daemon inside its first backfill pass so the test can assert the
// gated phase deterministically. (An erroring Tip would burn the retry budget
// and fail the run; a blocking one just waits.)
type gatedTipBackend struct {
	fakeBackend
	release chan struct{}
}

func (b *gatedTipBackend) Tip(ctx context.Context) (uint32, error) {
	select {
	case <-b.release:
		return b.fakeBackend.tip, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

type servingFastCoreClient struct{}

func (servingFastCoreClient) GetLedgerEntries(
	_ context.Context, _ uint32, keys ...xdr.LedgerKey,
) (proto.GetLedgerEntryResponse, error) {
	entries := make([]proto.LedgerEntryResponse, 0, len(keys))
	for range keys {
		entries = append(entries, proto.LedgerEntryResponse{State: proto.LedgerEntryStateNotFound})
	}
	return proto.GetLedgerEntryResponse{Entries: entries}, nil
}

// servingConfigPath is e2eConfigPath plus a [serving] section: JSON-RPC and
// admin listeners on kernel-picked ports, full history.
func servingConfigPath(t *testing.T, dataDir string) string {
	t.Helper()
	cfgPath := filepath.Join(t.TempDir(), "daemon.toml")
	body := fmt.Sprintf(`
[service]
default_data_dir = %q

[serving]
endpoint = "127.0.0.1:0"
admin_endpoint = "127.0.0.1:0"

[retention]
earliest_ledger = "genesis"
retention_chunks = 0

[ingestion]
captive_core_config = "/dev/null"

[logging]
level = "error"
format = "text"
`, dataDir)
	require.NoError(t, os.WriteFile(cfgPath, []byte(body), 0o644))
	return cfgPath
}

// smokeCall posts one JSON-RPC request, returning result, error, and raw body.
func smokeCall(t *testing.T, url, method string, params any) (json.RawMessage, *struct {
	Code    int64  `json:"code"`
	Message string `json:"message"`
}, []byte,
) {
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
		Error  *struct {
			Code    int64  `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal(body, &decoded), "body: %s", body)
	return decoded.Result, decoded.Error, body
}

func TestE2E_ServingSmoke_GateThenEndpoints(t *testing.T) {
	dataDir := t.TempDir()
	cfgPath := servingConfigPath(t, dataDir)

	// Frames close "now" so getHealth's freshness check passes once they commit.
	closeTime := time.Now().Unix()
	frames := map[uint32][]byte{}
	tip := uint32(chunk.FirstLedgerSeq + 5)
	for seq := uint32(chunk.FirstLedgerSeq); seq <= tip+3; seq++ {
		frames[seq] = fhtest.ZeroTxLCMBytesAt(t, seq, closeTime)
	}
	core := &e2eCore{frames: frames}
	backend := &gatedTipBackend{fakeBackend: fakeBackend{tip: tip}, release: make(chan struct{})}

	rpcAddrCh := make(chan string, 1)
	adminAddrCh := make(chan string, 1)
	var served atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	opts := daemonOptions{
		Backend: backend,
		Core:    core,
		ServeReads: func(context.Context, *registry.Registry) error {
			served.Add(1)
			return nil
		},
		Logger:         silentLogger(),
		Metrics:        &e2eMetrics{},
		RestartBackoff: 10 * time.Millisecond,
		adminUp:        func(addr string) { adminAddrCh <- addr },
		rpcUp:          func(addr string) { rpcAddrCh <- addr },
		fastCoreClient: servingFastCoreClient{},
	}
	go func() { done <- runDaemonWith(ctx, cfgPath, opts) }()

	var rpcURL string
	select {
	case addr := <-rpcAddrCh:
		rpcURL = "http://" + addr
	case <-time.After(10 * time.Second):
		t.Fatal("JSON-RPC listener did not come up")
	}

	// ------------------------- Phase 1: backfilling -------------------------
	require.Zero(t, served.Load(), "run must still be inside the held backfill pass")

	_, rpcErr, _ := smokeCall(t, rpcURL, protocol.GetLedgersMethodName,
		map[string]any{"startLedger": chunk.FirstLedgerSeq})
	require.NotNil(t, rpcErr)
	assert.EqualValues(t, -32603, rpcErr.Code)
	assert.Equal(t, "backfill in progress; query serving not started", rpcErr.Message)

	res, rpcErr, _ := smokeCall(t, rpcURL, protocol.GetHealthMethodName, nil)
	require.Nil(t, rpcErr)
	var health protocol.GetHealthResponse
	require.NoError(t, json.Unmarshal(res, &health))
	assert.Equal(t, "backfill in progress", health.Status)

	res, rpcErr, _ = smokeCall(t, rpcURL, "metrics", nil)
	require.Nil(t, rpcErr)
	var metricsRes struct {
		Ingestion map[string]json.RawMessage `json:"ingestion"`
		RPC       map[string]json.RawMessage `json:"rpc"`
	}
	require.NoError(t, json.Unmarshal(res, &metricsRes))
	assert.Contains(t, metricsRes.Ingestion, "backfill.chunk",
		"the backfill series exists from daemon start")

	_, rpcErr, _ = smokeCall(t, rpcURL, protocol.SendTransactionMethodName, map[string]any{})
	require.NotNil(t, rpcErr)
	assert.EqualValues(t, -32601, rpcErr.Code)
	assert.Contains(t, rpcErr.Message, "not supported by the full-history service")

	// The admin listener is up during backfill too, pprof absorbed.
	select {
	case adminAddr := <-adminAddrCh:
		resp, err := http.Get("http://" + adminAddr + "/debug/pprof/cmdline")
		require.NoError(t, err)
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	case <-time.After(10 * time.Second):
		t.Fatal("admin listener did not come up")
	}

	// ------------------------- Phase 2: serving -----------------------------
	close(backend.release)

	// Wait for healthy AND the whole backlog committed (the loop then idles at
	// the synthetic tip), so every assertion below sees the same final range.
	lastFrame := tip + 3
	require.Eventually(t, func() bool {
		res, rpcErr, _ := smokeCall(t, rpcURL, protocol.GetHealthMethodName, nil)
		if rpcErr != nil {
			return false
		}
		var h protocol.GetHealthResponse
		return json.Unmarshal(res, &h) == nil && h.Status == "healthy" && h.LatestLedger >= lastFrame
	}, 20*time.Second, 50*time.Millisecond, "gate must open and the full frame backlog must commit")
	assert.Positive(t, served.Load(), "the injected ServeReads hook runs after the gate publish")

	res, rpcErr, _ = smokeCall(t, rpcURL, protocol.GetLatestLedgerMethodName, nil)
	require.Nil(t, rpcErr)
	var latest protocol.GetLatestLedgerResponse
	require.NoError(t, json.Unmarshal(res, &latest))
	assert.GreaterOrEqual(t, latest.Sequence, uint32(chunk.FirstLedgerSeq))

	res, rpcErr, _ = smokeCall(t, rpcURL, protocol.GetLedgersMethodName,
		map[string]any{"startLedger": chunk.FirstLedgerSeq, "pagination": map[string]any{"limit": 2}})
	require.Nil(t, rpcErr)
	var ledgers protocol.GetLedgersResponse
	require.NoError(t, json.Unmarshal(res, &ledgers))
	require.Len(t, ledgers.Ledgers, 2)
	assert.EqualValues(t, chunk.FirstLedgerSeq, ledgers.Ledgers[0].Sequence)

	// getTransaction on an unknown hash: a SUCCESS response with NOT_FOUND —
	// the whole read chain works even with no transactions in the frames.
	res, rpcErr, _ = smokeCall(t, rpcURL, protocol.GetTransactionMethodName,
		map[string]any{"hash": "0000000000000000000000000000000000000000000000000000000000000000"})
	require.Nil(t, rpcErr)
	var tx protocol.GetTransactionResponse
	require.NoError(t, json.Unmarshal(res, &tx))
	assert.Equal(t, protocol.TransactionStatusNotFound, tx.Status)

	res, rpcErr, _ = smokeCall(t, rpcURL, protocol.GetTransactionsMethodName,
		map[string]any{"startLedger": chunk.FirstLedgerSeq})
	require.Nil(t, rpcErr)
	var txs protocol.GetTransactionsResponse
	require.NoError(t, json.Unmarshal(res, &txs))
	assert.Empty(t, txs.Transactions, "zero-tx frames")
	assert.NotZero(t, txs.LatestLedger)

	// getEvents over HTTP: empty events on zero-tx frames, and the raw wire
	// bytes carry no deprecated inSuccessfulContractCall key. (The serve
	// package's server test pins the same on event-BEARING responses.)
	res, rpcErr, rawBody := smokeCall(t, rpcURL, protocol.GetEventsMethodName,
		map[string]any{"startLedger": chunk.FirstLedgerSeq})
	require.Nil(t, rpcErr)
	var events struct {
		Events []json.RawMessage `json:"events"`
	}
	require.NoError(t, json.Unmarshal(res, &events))
	assert.Empty(t, events.Events)
	assert.NotContains(t, string(rawBody), "inSuccessfulContractCall")

	res, rpcErr, _ = smokeCall(t, rpcURL, protocol.GetNetworkMethodName, nil)
	require.Nil(t, rpcErr)
	var network protocol.GetNetworkResponse
	require.NoError(t, json.Unmarshal(res, &network))
	assert.Equal(t, core.NetworkPassphrase(), network.Passphrase)

	res, rpcErr, _ = smokeCall(t, rpcURL, protocol.GetVersionInfoMethodName, nil)
	require.Nil(t, rpcErr)
	var version protocol.GetVersionInfoResponse
	require.NoError(t, json.Unmarshal(res, &version))
	assert.Equal(t, "unavailable", version.CaptiveCoreVersion)

	// getLedgerEntries through the injected query-client seam.
	accountID := xdr.MustAddress("GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H")
	key := xdr.LedgerKey{Type: xdr.LedgerEntryTypeAccount, Account: &xdr.LedgerKeyAccount{AccountId: accountID}}
	keyB64, err := xdr.MarshalBase64(key)
	require.NoError(t, err)
	res, rpcErr, _ = smokeCall(t, rpcURL, protocol.GetLedgerEntriesMethodName,
		map[string]any{"keys": []string{keyB64}})
	require.Nil(t, rpcErr)
	var entries protocol.GetLedgerEntriesResponse
	require.NoError(t, json.Unmarshal(res, &entries))
	assert.Empty(t, entries.Entries)
	assert.NotZero(t, entries.LatestLedger)

	// metrics: live per-ledger ingestion series and per-endpoint quantiles.
	res, rpcErr, _ = smokeCall(t, rpcURL, "metrics", nil)
	require.Nil(t, rpcErr)
	var metricsAfter struct {
		Ingestion map[string]struct {
			Count uint64 `json:"count"`
		} `json:"ingestion"`
		RPC map[string]struct {
			Count uint64 `json:"count"`
		} `json:"rpc"`
	}
	require.NoError(t, json.Unmarshal(res, &metricsAfter))
	assert.Positive(t, metricsAfter.Ingestion["ingest.e2e"].Count, "live commits recorded")
	assert.Positive(t, metricsAfter.RPC[protocol.GetLedgersMethodName].Count)
	assert.Positive(t, metricsAfter.RPC[protocol.GetHealthMethodName].Count)

	waitClean(t, cancel, done)
}
