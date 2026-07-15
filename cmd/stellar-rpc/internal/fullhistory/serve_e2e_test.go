package fullhistory

// =============================================================================
// End-to-end query integration of the full-history daemon with [serve] enabled.
//
// This is the query POC's top-level gate: it boots the REAL daemon
// (runDaemonWith) with a bound JSON-RPC read server, ingests one FULL chunk plus
// a few live ledgers through the fake ledger source, lets the real lifecycle
// freeze chunk 0 into cold artifacts, then drives the four read methods over real
// HTTP against BOTH tiers:
//
//	(a) getLedgers spanning the cold→hot seam,
//	(b) getTransaction for a hash in the cold chunk AND one in the live chunk,
//	(c) getTransactions across the seam with cursor continuation,
//	(d) getEvents with a contract filter matching events in both tiers,
//	(e) negatives: getLedgers below the floor errors; an unknown tx hash → NOT_FOUND.
//
// What is real: everything inside the process (config load, catalog, backfill,
// the atomic per-ledger hot WriteBatch, the boundary handoff, the lifecycle
// freeze/discard, the serve Registry + View publishing, and the reused v1
// handlers behind the db adapters). Faked: only the two injected external
// boundaries — the ledger source (fake core) and the network passphrase seam
// (an injected fake Core carries none, so the test supplies PublicNetwork).
//
// cpi=1 makes chunk 0's one-chunk tx-hash window terminal the instant it freezes,
// so the cold .idx the by-hash lookup resolves against exists after one boundary.
// =============================================================================

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/fhtest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// serveE2EConfigPath writes a daemon TOML like e2eConfigPath but with a [serve]
// section bound to an OS-assigned port (endpoint 127.0.0.1:0). Genesis floor +
// full-history retention so nothing prunes; the real port is surfaced via the
// serveAddr test seam, not the config.
func serveE2EConfigPath(t *testing.T, dataDir string) string {
	t.Helper()
	cfgPath := filepath.Join(t.TempDir(), "daemon.toml")
	body := fmt.Sprintf(`
[service]
default_data_dir = %q

[retention]
earliest_ledger = "genesis"
retention_chunks = 0

[ingestion]
captive_core_config = "/dev/null"

[serve]
endpoint = "127.0.0.1:0"

[logging]
level = "error"
format = "text"
`, dataDir)
	require.NoError(t, os.WriteFile(cfgPath, []byte(body), 0o644))
	return cfgPath
}

// eventContractID is the fixed contract whose events straddle the cold→hot seam.
func eventContractID() xdr.ContractId {
	var c xdr.ContractId
	c[0] = 0xAA
	return c
}

// oneTxEventLCM is oneTxLCMBytes plus a single operation-level contract event
// (contract type, one symbol topic + symbol data) emitted by contract
// eventContractID. It returns the wire bytes and the network-hashed transaction
// hash (hashed with PublicNetworkPassphrase, the passphrase the read server is
// configured with) so the caller can assert both a getTransaction hash lookup
// and a getEvents contract-filter match against the same ledger.
func oneTxEventLCM(t *testing.T, seq uint32, src xdr.MuxedAccount, data string) ([]byte, [32]byte) {
	t.Helper()
	cid := eventContractID()
	topicSym := xdr.ScSymbol("seam")
	dataSym := xdr.ScSymbol(data)
	ev := xdr.ContractEvent{
		ContractId: &cid,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: []xdr.ScVal{{Type: xdr.ScValTypeScvSymbol, Sym: &topicSym}},
				Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &dataSym},
			},
		},
	}
	envelope := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{
				SourceAccount: src,
				SeqNum:        xdr.SequenceNumber(seq), // unique per ledger ⇒ unique hash
				Ext:           xdr.TransactionExt{V: 1, SorobanData: &xdr.SorobanTransactionData{}},
			},
		},
	}
	hash, err := network.HashTransactionInEnvelope(envelope, network.PublicNetworkPassphrase)
	require.NoError(t, err)
	comp := []xdr.TxSetComponent{{
		Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
		TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
			Txs: []xdr.TransactionEnvelope{envelope},
		},
	}}
	lcm := xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(uint64(seq) * 10)},
					LedgerSeq: xdr.Uint32(seq),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{Phases: []xdr.TransactionPhase{{V: 0, V0Components: &comp}}},
			},
			TxProcessing: []xdr.TransactionResultMetaV1{{
				TxApplyProcessing: xdr.TransactionMeta{
					V:  4,
					V4: &xdr.TransactionMetaV4{Operations: []xdr.OperationMetaV2{{Events: []xdr.ContractEvent{ev}}}},
				},
				Result: xdr.TransactionResultPair{
					TransactionHash: hash,
					Result: xdr.TransactionResult{
						FeeCharged: 100,
						Result: xdr.TransactionResultResult{
							Code:    xdr.TransactionResultCodeTxSuccess,
							Results: &[]xdr.OperationResult{},
						},
					},
				},
			}},
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw, hash
}

// postRPC POSTs a JSON-RPC request body and decodes the response envelope into a
// generic map (result/error), mirroring serve/server_test.go's postJSON.
func postRPC(t *testing.T, url, body string) map[string]any {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, bytes.NewReader([]byte(body)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal(raw, &out), "response: %s", raw)
	return out
}

// asMap/asSlice/asString are checked type-assertion helpers for the decoded
// JSON-RPC envelopes: they fail the test loudly instead of panicking on a shape
// mismatch (forcetypeassert), keeping the assertions below readable.
func asMap(t *testing.T, v any) map[string]any {
	t.Helper()
	m, ok := v.(map[string]any)
	require.True(t, ok, "expected a JSON object, got %T", v)
	return m
}

func asSlice(t *testing.T, v any) []any {
	t.Helper()
	s, ok := v.([]any)
	require.True(t, ok, "expected a JSON array, got %T", v)
	return s
}

func asString(t *testing.T, v any) string {
	t.Helper()
	s, ok := v.(string)
	require.True(t, ok, "expected a JSON string, got %T", v)
	return s
}

// TestServeE2E_QueryHotAndCold is the query POC's end-to-end gate — see the file
// header for the (a)-(e) coverage list. NOT skipped in -short: it is the only
// test that exercises the wired read path over real HTTP; the generous
// require.Eventually budgets (matching e2e_test.go) absorb the synced-WriteBatch
// + freeze cost of one full chunk.
//
//nolint:funlen // one linear end-to-end scenario asserted step by step
func TestServeE2E_QueryHotAndCold(t *testing.T) {
	dataDir := t.TempDir()

	const c0 = chunk.ID(0)
	const c1 = chunk.ID(1)
	c0First := c0.FirstLedger()
	c0Last := c0.LastLedger() // chunk 0's last ledger — the boundary into chunk 1
	c1First := c1.FirstLedger()
	liveLast := c1First + 2 // a few live ledgers past the boundary

	// One shared source account; a per-seq SeqNum makes each tx hash unique.
	src := xdr.MustMuxedAddress(keypair.MustRandom().Address())
	coldRaw, coldHash := oneTxEventLCM(t, c0Last, src, "cold") // → frozen cold chunk 0
	hotRaw, hotHash := oneTxEventLCM(t, c1First, src, "hot")   // → live hot chunk 1

	frames := make(map[uint32][]byte, int(chunk.LedgersPerChunk)+3)
	for seq := c0First; seq <= c0Last; seq++ {
		if seq == c0Last {
			frames[seq] = coldRaw
		} else {
			frames[seq] = fhtest.ZeroTxLCMBytes(t, seq)
		}
	}
	frames[c1First] = hotRaw
	frames[c1First+1] = fhtest.ZeroTxLCMBytes(t, c1First+1)
	frames[liveLast] = fhtest.ZeroTxLCMBytes(t, liveLast)

	core := &e2eCore{frames: frames}
	metrics := &e2eMetrics{}

	// Capture the read server's OS-assigned address via the test seam.
	addrCh := make(chan net.Addr, 1)

	cfgPath := serveE2EConfigPath(t, dataDir)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- runDaemonWith(ctx, cfgPath, daemonOptions{
			Backend:              &fakeBackend{tip: chunk.FirstLedgerSeq + 5}, // young ⇒ no backfill, ingest from genesis
			Core:                 core,
			Logger:               silentLogger(),
			Metrics:              metrics,
			RestartBackoff:       10 * time.Millisecond,
			chunksPerTxhashIndex: 1,
			passphrase:           network.PublicNetworkPassphrase,
			serveAddr:            func(a net.Addr) { addrCh <- a },
		})
	}()
	// Clean shutdown: cancel and require the daemon returned nil (ctx cancel is clean).
	defer func() {
		cancel()
		select {
		case err := <-errCh:
			require.NoError(t, err, "ctx cancel is a clean daemon shutdown")
		case <-time.After(60 * time.Second):
			t.Fatal("daemon did not shut down cleanly after ctx cancel")
		}
	}()

	// The wired ServeReads launches StartServer and reports the bound address; a
	// pre-serve daemon return (a startup failure) surfaces here instead of a hang.
	var addr net.Addr
	select {
	case addr = <-addrCh:
	case err := <-errCh:
		t.Fatalf("daemon returned before the read server bound: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("read server never bound (ServeReads wiring absent?)")
	}
	base := "http://" + addr.String()

	// Poll /ready — it latches true once the ingestion loop commits its first ledger.
	require.Eventually(t, func() bool {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/ready", nil)
		if err != nil {
			return false
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 60*time.Second, 100*time.Millisecond, "read server must become ready")

	// Wait until ingestion crosses the chunk-0 boundary and commits the live
	// ledgers into chunk 1 (proves the boundary handoff + HotOpened fired). 480s
	// absorbs 10k synced per-ledger WriteBatches on a contended box.
	require.Eventually(t, func() bool {
		return core.delivered.Load() >= liveLast
	}, 480*time.Second, 200*time.Millisecond, "ingestion must cross into chunk 1")

	// Wait for the boundary tick to freeze chunk 0 into cold artifacts and discard
	// its hot DB — after which TickCompleted republishes chunk 0 as cold.
	require.Eventually(t, func() bool {
		return metrics.discardedCount() >= 1
	}, 120*time.Second, 100*time.Millisecond, "the boundary tick must freeze+discard chunk 0")

	// -------------------------------------------------------------------------
	// (a) getLedgers spanning the cold→hot seam: [c0Last-1, liveLast] straddles
	// the frozen chunk 0 and the live chunk 1, returned as one contiguous page.
	// Wrapped in Eventually to absorb the discard→TickCompleted publish gap.
	// -------------------------------------------------------------------------
	require.Eventually(t, func() bool {
		got := postRPC(t, base+"/", ledgersReq(c0Last-1, 10))
		res, ok := got["result"].(map[string]any)
		if !ok {
			return false
		}
		ledgers, _ := res["ledgers"].([]any)
		return len(ledgers) == int(liveLast-(c0Last-1)+1)
	}, 60*time.Second, 200*time.Millisecond, "getLedgers must span the cold→hot seam")

	seam := postRPC(t, base+"/", ledgersReq(c0Last-1, 10))
	seamRes := asMap(t, seam["result"])
	assert.EqualValues(t, liveLast, seamRes["latestLedger"], "latest is the live tip")
	seamLedgers := asSlice(t, seamRes["ledgers"])
	firstSeq := asMap(t, seamLedgers[0])["sequence"]
	lastSeq := asMap(t, seamLedgers[len(seamLedgers)-1])["sequence"]
	assert.EqualValues(t, c0Last-1, firstSeq, "seam page starts in the cold chunk")
	assert.EqualValues(t, liveLast, lastSeq, "seam page ends in the live chunk")

	// -------------------------------------------------------------------------
	// (b) getTransaction: the cold hash resolves through the frozen .idx + cold
	// ledger pack; the live hash through the hot txhash CF + hot ledger.
	// -------------------------------------------------------------------------
	require.Eventually(t, func() bool {
		return txStatus(t, base, coldHash) == "SUCCESS"
	}, 60*time.Second, 200*time.Millisecond, "cold tx must resolve from frozen history")
	coldTx := asMap(t, postRPC(t, base+"/", txReq(coldHash))["result"])
	assert.EqualValues(t, c0Last, coldTx["ledger"], "cold tx is in chunk 0's last ledger")

	hotTx := asMap(t, postRPC(t, base+"/", txReq(hotHash))["result"])
	assert.Equal(t, "SUCCESS", hotTx["status"], "live tx must resolve from the hot tier")
	assert.EqualValues(t, c1First, hotTx["ledger"], "live tx is in chunk 1's first ledger")

	// -------------------------------------------------------------------------
	// (c) getTransactions across the seam with cursor continuation: page 1 (from
	// c0Last, limit 1) returns the cold tx; the follow-up cursor page returns the
	// live tx — proving continuation across the cold→hot boundary.
	// -------------------------------------------------------------------------
	page1 := asMap(t, postRPC(t, base+"/", txsReqStart(c0Last, 1))["result"])
	p1txs := asSlice(t, page1["transactions"])
	require.Len(t, p1txs, 1, "page 1 returns exactly the cold tx (limit 1)")
	assert.Equal(t, hex.EncodeToString(coldHash[:]), asMap(t, p1txs[0])["txHash"])
	cursor1 := asString(t, page1["cursor"])

	page2 := asMap(t, postRPC(t, base+"/", txsReqCursor(cursor1, 5))["result"])
	p2txs := asSlice(t, page2["transactions"])
	require.NotEmpty(t, p2txs, "the cursor page resumes past the cold tx")
	var sawHot bool
	for _, tx := range p2txs {
		if asMap(t, tx)["txHash"] == hex.EncodeToString(hotHash[:]) {
			sawHot = true
		}
	}
	assert.True(t, sawHot, "getTransactions cursor continuation reaches the live tx across the seam")

	// -------------------------------------------------------------------------
	// (d) getEvents with a contract filter matching events in BOTH tiers: contract
	// eventContractID emitted at c0Last (cold) and c1First (hot); the filter page
	// returns both in ascending order across the seam.
	// -------------------------------------------------------------------------
	evCID := eventContractID()
	cStrkey, err := strkey.Encode(strkey.VersionByteContract, evCID[:])
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		got := postRPC(t, base+"/", eventsReq(c0Last, cStrkey))
		res, ok := got["result"].(map[string]any)
		if !ok {
			return false
		}
		evs, _ := res["events"].([]any)
		return len(evs) == 2
	}, 60*time.Second, 200*time.Millisecond, "getEvents must match events in both tiers")

	evResp := asMap(t, postRPC(t, base+"/", eventsReq(c0Last, cStrkey))["result"])
	evs := asSlice(t, evResp["events"])
	require.Len(t, evs, 2)
	assert.EqualValues(t, c0Last, asMap(t, evs[0])["ledger"], "first event is cold")
	assert.EqualValues(t, c1First, asMap(t, evs[1])["ledger"], "second event is hot")
	for _, e := range evs {
		assert.Equal(t, cStrkey, asMap(t, e)["contractId"], "only the filtered contract matched")
	}

	// -------------------------------------------------------------------------
	// (e) negatives.
	// -------------------------------------------------------------------------
	// A startLedger below the served floor (genesis) errors with the range surfaced.
	below := postRPC(t, base+"/", ledgersReq(c0First-1, 5))
	require.NotNil(t, below["error"], "getLedgers below the floor must error")
	belowMsg := fmt.Sprint(asMap(t, below["error"])["message"])
	assert.Contains(t, belowMsg, "oldest ledger", "the below-floor error surfaces the available range")
	assert.Contains(t, belowMsg, "latest ledger", "the below-floor error surfaces the available range")

	// An unknown tx hash is NOT_FOUND (a well-formed response, not an error).
	var miss [32]byte
	miss[0], miss[31] = 0xDE, 0xAD
	unknown := postRPC(t, base+"/", txReq(miss))
	require.Nil(t, unknown["error"], "an unknown hash is a normal NOT_FOUND response, not an error")
	assert.Equal(t, "NOT_FOUND", asMap(t, unknown["result"])["status"])
}

// ---- JSON-RPC request builders (raw wire strings keep the test readable). ----

func ledgersReq(start uint32, limit int) string {
	return fmt.Sprintf(
		`{"jsonrpc":"2.0","id":1,"method":"getLedgers","params":{"startLedger":%d,"pagination":{"limit":%d}}}`,
		start, limit,
	)
}

func txReq(hash [32]byte) string {
	return fmt.Sprintf(
		`{"jsonrpc":"2.0","id":1,"method":"getTransaction","params":{"hash":%q}}`,
		hex.EncodeToString(hash[:]),
	)
}

func txsReqStart(start uint32, limit int) string {
	return fmt.Sprintf(
		`{"jsonrpc":"2.0","id":1,"method":"getTransactions","params":{"startLedger":%d,"pagination":{"limit":%d}}}`,
		start, limit,
	)
}

func txsReqCursor(cursor string, limit int) string {
	return fmt.Sprintf(
		`{"jsonrpc":"2.0","id":1,"method":"getTransactions","params":{"pagination":{"cursor":%q,"limit":%d}}}`,
		cursor, limit,
	)
}

func eventsReq(start uint32, contractID string) string {
	return fmt.Sprintf(
		`{"jsonrpc":"2.0","id":1,"method":"getEvents","params":{"startLedger":%d,`+
			`"filters":[{"contractIds":[%q]}],"pagination":{"limit":10}}}`,
		start, contractID,
	)
}

// txStatus POSTs a getTransaction and returns its result.status (or "" on error).
func txStatus(t *testing.T, base string, hash [32]byte) string {
	t.Helper()
	got := postRPC(t, base+"/", txReq(hash))
	res, ok := got["result"].(map[string]any)
	if !ok {
		return ""
	}
	s, _ := res["status"].(string)
	return s
}
