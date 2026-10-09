package methods

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/feewindow"
)

// A CAP-0088 close time with a sub-second part, so serving closeTimeMs (or a
// rounded value) anywhere RPC reports the whole-second closeTime would show.
const (
	msLedgerSeq   = 200
	msCloseTimeMs = 1_760_000_123_456
	msCloseTime   = msCloseTimeMs / 1000
)

// TestMillisecondCloseTimeLedger ingests a protocol 30 ledger whose
// StellarValue uses one of the CAP-0088 millisecond close time arms and checks
// that RPC serves it unchanged: every close time it reports is the
// whole-second closeTime, and the header keeps its arm (and closeTimeMs) in
// both XDR and xdr2json output.
func TestMillisecondCloseTimeLedger(t *testing.T) {
	sig := xdr.LedgerCloseValueSignature{
		NodeId: xdr.NodeId{
			Type:    xdr.PublicKeyTypePublicKeyTypeEd25519,
			Ed25519: &xdr.Uint256{1, 2, 3},
		},
		Signature: bytes.Repeat([]byte{7}, 64),
	}
	for _, tc := range []struct {
		arm        string // JSON name of the StellarValue.ext arm
		ext        xdr.StellarValueExt
		emptyTxSet bool
	}{
		{
			arm: "signed_ms",
			ext: xdr.StellarValueExt{
				V: xdr.StellarValueTypeStellarValueSignedMs,
				SignedMsValue: &xdr.StellarValueSignedMsValue{
					CloseTimeMs:      msCloseTimeMs,
					LcValueSignature: sig,
				},
			},
		},
		{
			arm: "empty_tx_set_ms",
			ext: xdr.StellarValueExt{
				V: xdr.StellarValueTypeStellarValueEmptyTxSetMs,
				ProposedMsValue: &xdr.StellarValueProposedMsValue{
					CloseTimeMs:           msCloseTimeMs,
					TxSetHash:             xdr.Hash{3},
					PreviousLedgerHash:    xdr.Hash{4},
					PreviousLedgerVersion: 30,
					LcValueSignature:      sig,
				},
			},
			emptyTxSet: true,
		},
	} {
		t.Run(tc.arm, func(t *testing.T) {
			lcm := msCloseTimeLedger(tc.ext, tc.emptyTxSet)
			testDB, feeWindows := ingestLedger(t, lcm)
			ledgerReader := db.NewLedgerReader(testDB)

			checkLatestLedgerMs(t, ledgerReader, lcm)
			checkGetLedgersMs(t, ledgerReader, lcm, tc.arm)
			checkFeeStatsMs(t, feeWindows, ledgerReader)
			if !tc.emptyTxSet {
				checkTransactionAndEventsMs(t, testDB, ledgerReader)
			}
		})
	}
}

// msCloseTimeLedger returns a protocol 30 ledger at msLedgerSeq whose
// StellarValue has the given ext and a closeTime of msCloseTime. Unless
// emptyTxSet is set it holds one successful transaction emitting a contract
// event.
func msCloseTimeLedger(ext xdr.StellarValueExt, emptyTxSet bool) xdr.LedgerCloseMeta {
	lcm, _ := txMetaWithEvents(msLedgerSeq-100, true)
	if emptyTxSet {
		lcm.V2.TxProcessing = nil
		lcm.V2.TxSet.V1TxSet.Phases = nil
	}
	header := &lcm.V2.LedgerHeader.Header
	header.LedgerVersion = 30
	header.ScpValue.CloseTime = msCloseTime
	header.ScpValue.Ext = ext
	return lcm
}

// ingestLedger writes lcm the way the ingestion service does: ledger,
// transactions and events in one DB transaction, then the fee windows, then
// the commit.
func ingestLedger(t *testing.T, lcm xdr.LedgerCloseMeta) (*db.DB, *feewindow.FeeWindows) {
	testDB := NewTestDB(t)
	rw := db.NewReadWriter(log.DefaultLogger, testDB, interfaces.MakeNoOpDeamon(), 100, passphrase)
	tx, err := rw.NewTx(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.LedgerWriter().InsertLedger(lcm))
	require.NoError(t, tx.TransactionWriter().InsertTransactions(lcm))
	require.NoError(t, tx.EventWriter().InsertEvents(lcm))
	feeWindows := feewindow.NewFeeWindows(10, 10, passphrase, testDB)
	require.NoError(t, feeWindows.IngestFees(lcm))
	require.NoError(t, tx.Commit(lcm, nil))
	return testDB, feeWindows
}

func checkLatestLedgerMs(t *testing.T, ledgerReader db.LedgerReader, lcm xdr.LedgerCloseMeta) {
	respI, err := NewGetLatestLedgerHandler(ledgerReader)(t.Context(), &jrpc2.Request{})
	require.NoError(t, err)
	resp, ok := respI.(protocol.GetLatestLedgerResponse)
	require.True(t, ok)

	assert.Equal(t, uint32(msLedgerSeq), resp.Sequence)
	assert.Equal(t, uint32(30), resp.ProtocolVersion)
	assert.Equal(t, int64(msCloseTime), resp.LedgerCloseTime)
	assert.Equal(t, mustMarshalBase64(t, lcm.V2.LedgerHeader.Header), resp.LedgerHeader)
	assert.Equal(t, mustMarshalBase64(t, lcm), resp.LedgerMetadata)
}

func checkGetLedgersMs(t *testing.T, ledgerReader db.LedgerReader, lcm xdr.LedgerCloseMeta, arm string) {
	handler := ledgersHandler{ledgerReader: ledgerReader, maxLimit: 10, defaultLimit: 10}

	resp, err := handler.getLedgers(t.Context(), protocol.GetLedgersRequest{StartLedger: msLedgerSeq})
	require.NoError(t, err)
	assert.Equal(t, int64(msCloseTime), resp.LatestLedgerCloseTime)
	assert.Equal(t, int64(msCloseTime), resp.OldestLedgerCloseTime)
	require.Len(t, resp.Ledgers, 1)
	assert.Equal(t, int64(msCloseTime), resp.Ledgers[0].LedgerCloseTime)
	assert.Equal(t, mustMarshalBase64(t, lcm.V2.LedgerHeader), resp.Ledgers[0].LedgerHeader)
	assert.Equal(t, mustMarshalBase64(t, lcm), resp.Ledgers[0].LedgerMetadata)

	resp, err = handler.getLedgers(t.Context(), protocol.GetLedgersRequest{
		StartLedger: msLedgerSeq,
		Format:      protocol.FormatJSON,
	})
	require.NoError(t, err)
	require.Len(t, resp.Ledgers, 1)
	assert.Equal(t, int64(msCloseTime), resp.Ledgers[0].LedgerCloseTime)

	// xdr2json must decode the new arm, in the header and inside the meta.
	closeTime, closeTimeMs := strconv.FormatInt(msCloseTime, 10), strconv.FormatInt(msCloseTimeMs, 10)
	headerJSON := resp.Ledgers[0].LedgerHeaderJSON
	assert.Equal(t, closeTime, jsonField(t, headerJSON, "header", "scp_value", "close_time"))
	assert.Equal(t, closeTimeMs, jsonField(t, headerJSON, "header", "scp_value", "ext", arm, "close_time_ms"))
	assert.Equal(t, closeTimeMs, jsonField(t, resp.Ledgers[0].LedgerMetadataJSON,
		"v2", "ledger_header", "header", "scp_value", "ext", arm, "close_time_ms"))
}

func checkFeeStatsMs(t *testing.T, feeWindows *feewindow.FeeWindows, ledgerReader db.LedgerReader) {
	respI, err := NewGetFeeStatsHandler(feeWindows, ledgerReader, log.DefaultLogger)(t.Context(), &jrpc2.Request{})
	require.NoError(t, err)
	resp, ok := respI.(protocol.GetFeeStatsResponse)
	require.True(t, ok)
	assert.Equal(t, uint32(msLedgerSeq), resp.LatestLedger)
}

func checkTransactionAndEventsMs(t *testing.T, testDB *db.DB, ledgerReader db.LedgerReader) {
	txReader := db.NewTransactionReader(log.DefaultLogger, testDB, passphrase)
	tx, err := GetTransaction(t.Context(), log.DefaultLogger, txReader, ledgerReader,
		protocol.GetTransactionRequest{Hash: txHash(msLedgerSeq - 100).HexString()})
	require.NoError(t, err)
	assert.Equal(t, protocol.TransactionStatusSuccess, tx.Status)
	assert.Equal(t, uint32(msLedgerSeq), tx.Ledger)
	assert.Equal(t, int64(msCloseTime), tx.LedgerCloseTime)
	assert.Equal(t, int64(msCloseTime), tx.LatestLedgerCloseTime)

	handler := eventsRPCHandler{
		dbReader:     db.NewEventReader(log.DefaultLogger, testDB, passphrase),
		maxLimit:     10,
		defaultLimit: 10,
		ledgerReader: ledgerReader,
	}
	events, err := handler.getEvents(t.Context(), protocol.GetEventsRequest{StartLedger: msLedgerSeq})
	require.NoError(t, err)
	require.Len(t, events.Events, 1)
	assert.Equal(t, time.Unix(msCloseTime, 0).UTC().Format(time.RFC3339), events.Events[0].LedgerClosedAt)
	assert.Equal(t, int64(msCloseTime), events.LatestLedgerCloseTime)
}

func mustMarshalBase64(t *testing.T, v any) string {
	b64, err := xdr.MarshalBase64(v)
	require.NoError(t, err)
	return b64
}

// jsonField follows keys through nested JSON objects in raw and returns the
// value found there as written (a JSON number or the contents of a string).
func jsonField(t *testing.T, raw json.RawMessage, keys ...string) string {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	require.NoError(t, dec.Decode(&v))
	for _, key := range keys {
		obj, ok := v.(map[string]any)
		require.True(t, ok, "expected an object holding %q in %s", key, raw)
		v, ok = obj[key]
		require.True(t, ok, "missing %q in %s", key, raw)
	}
	return fmt.Sprint(v)
}
