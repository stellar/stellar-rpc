package serve

import (
	"encoding/hex"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/fhtest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/txhash"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
)

// oneTxLCM is fhtest.ZeroTxLCMBytes plus one network-hashed tx (unique SeqNum ⇒
// unique hash), so ExtractTxHashes yields exactly one key for seq. Returns the
// wire bytes and the real tx hash a by-hash lookup resolves. Mirror of
// daemon_test.go's oneTxLCMBytes, kept here to avoid a _test cross-package dep.
func oneTxLCM(t *testing.T, seq uint32) ([]byte, [32]byte) {
	t.Helper()
	src := xdr.MustMuxedAddress(keypair.MustRandom().Address())
	envelope := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{
				SourceAccount: src,
				SeqNum:        xdr.SequenceNumber(seq),
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
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(0)},
					LedgerSeq: xdr.Uint32(seq),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{Phases: []xdr.TransactionPhase{{V: 0, V0Components: &comp}}},
			},
			TxProcessing: []xdr.TransactionResultMetaV1{{
				TxApplyProcessing: xdr.TransactionMeta{V: 4, V4: &xdr.TransactionMetaV4{}},
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

// unknownHash builds a deterministic 32-byte hash for the never-committed miss.
func unknownHash(n uint64) [32]byte {
	var h [32]byte
	for i := range 8 {
		h[i] = byte(n >> (8 * i))
	}
	return h
}

// buildHotTxFixture ingests one tx-bearing ledger into a live hot chunk 0 and
// publishes it, so a by-hash lookup resolves through the hot (exact) probe.
func buildHotTxFixture(t *testing.T) (*Registry, geometry.Layout, [32]byte, uint32) {
	t.Helper()
	cat, layout := newTestCatalog(t)
	seq := chunk.ID(0).FirstLedger()
	raw, hash := oneTxLCM(t, seq)

	require.NoError(t, cat.BeginHotCreate(chunk.ID(0)))
	dbHot, err := hotchunk.Open(layout.HotChunkPath(chunk.ID(0)), chunk.ID(0), silentLogger())
	require.NoError(t, err)
	_, err = dbHot.IngestLedger(seq, xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	require.NoError(t, cat.FinishHotCreate(chunk.ID(0)))
	t.Cleanup(func() { _ = dbHot.Close() })

	r := NewRegistry(cat, fhtest.RetentionFor(t, cat, 0), silentLogger())
	require.NoError(t, r.BuildInitial(seq))
	r.HotOpened(chunk.ID(0), dbHot) // chunk 0 is the resume chunk

	return r, layout, hash, seq
}

// buildColdTxFixture freezes one tx-bearing ledger into a cold pack at chunk 0's
// first ledger AND a matching frozen tx-hash window index, so a by-hash lookup
// resolves through the cold (fingerprint) probe and is verified against the cold
// ledger. earliestPin > 0 pins earliest_ledger (used to push the retention floor
// above the tx's ledger for the below-floor case); 0 leaves full history.
func buildColdTxFixture(t *testing.T, earliestPin uint32) (*Registry, geometry.Layout, [32]byte, uint32) {
	t.Helper()
	cat, layout := newTestCatalog(t)
	if earliestPin > 0 {
		require.NoError(t, cat.PinEarliestLedger(earliestPin))
	}
	seq := chunk.ID(0).FirstLedger()
	raw, hash := oneTxLCM(t, seq)

	writeColdLedgerPack(t, layout, chunk.ID(0), seq, raw)
	freezeLedgers(t, cat, chunk.ID(0))

	var entry txhash.ColdEntry
	copy(entry.Key[:], hash[:])
	entry.Seq = seq
	freezeTxIndex(t, cat, layout, geometry.TxHashIndexID(0), chunk.ID(0), chunk.ID(0),
		chunk.ID(0).FirstLedger(), chunk.ID(0).LastLedger(), []txhash.ColdEntry{entry})

	r := NewRegistry(cat, fhtest.RetentionFor(t, cat, 0), silentLogger())
	require.NoError(t, r.BuildInitial(seq))
	return r, layout, hash, seq
}

func TestTxReader_HotHit(t *testing.T) {
	r, layout, hash, seq := buildHotTxFixture(t)
	txr := NewTransactionReader(r, layout, network.PublicNetworkPassphrase)

	got, err := txr.GetTransaction(t.Context(), xdr.Hash(hash))
	require.NoError(t, err)
	assert.EqualValues(t, seq, got.Ledger.Sequence)
	assert.Equal(t, hex.EncodeToString(hash[:]), got.TransactionHash)
	assert.True(t, got.Successful)
	assert.NotEmpty(t, got.Envelope, "envelope XDR carried through")
	assert.NotEmpty(t, got.Result, "result XDR carried through")
	assert.NotEmpty(t, got.Meta, "meta XDR carried through")
}

func TestTxReader_ColdHit(t *testing.T) {
	r, layout, hash, seq := buildColdTxFixture(t, 0)
	txr := NewTransactionReader(r, layout, network.PublicNetworkPassphrase)

	got, err := txr.GetTransaction(t.Context(), xdr.Hash(hash))
	require.NoError(t, err, "the frozen .idx candidate resolves + verifies against the cold ledger")
	assert.EqualValues(t, seq, got.Ledger.Sequence)
	assert.Equal(t, hex.EncodeToString(hash[:]), got.TransactionHash)
	assert.NotEmpty(t, got.Envelope)
}

func TestTxReader_Miss(t *testing.T) {
	r, layout, _, _ := buildColdTxFixture(t, 0)
	txr := NewTransactionReader(r, layout, network.PublicNetworkPassphrase)

	_, err := txr.GetTransaction(t.Context(), xdr.Hash(unknownHash(0xDEADBEEF)))
	assert.ErrorIs(t, err, db.ErrNoTransaction, "an uncommitted hash is a clean not-found")
}

// A cold candidate below the retention floor is skipped at the index seam (a
// window index may keep naming pruned ledgers, design R2), so it reads as a
// clean index miss and ends as db.ErrNoTransaction — never pruned data, never
// an incomplete-lookup error.
func TestTxReader_BelowFloorSkipped(t *testing.T) {
	// Pin earliest to chunk 1's first ledger ⇒ floor sits above chunk 0's tx.
	r, layout, hash, _ := buildColdTxFixture(t, chunk.ID(1).FirstLedger())
	txr := NewTransactionReader(r, layout, network.PublicNetworkPassphrase)

	_, err := txr.GetTransaction(t.Context(), xdr.Hash(hash))
	assert.ErrorIs(t, err, db.ErrNoTransaction, "below-floor candidate skips to a clean not-found")
}

// Step: the real getTransaction handler, mounted over the adapter, returns a
// found tx and a NotFound for an unknown hash.
func TestTxReader_GetTransactionHandler(t *testing.T) {
	r, layout, hash, seq := buildColdTxFixture(t, 0)
	txr := NewTransactionReader(r, layout, network.PublicNetworkPassphrase)
	lr := NewLedgerReader(r, layout)
	h := methods.NewGetTransactionHandler(silentLogger(), txr, lr)

	// Found.
	found := getTxRequest(t, hex.EncodeToString(hash[:]))
	res, err := h(t.Context(), found)
	require.NoError(t, err)
	resp, ok := res.(protocol.GetTransactionResponse)
	require.True(t, ok)
	assert.Equal(t, protocol.TransactionStatusSuccess, resp.Status)
	assert.EqualValues(t, seq, resp.Ledger)

	// NotFound.
	miss := unknownHash(0x1234)
	missing := getTxRequest(t, hex.EncodeToString(miss[:]))
	res, err = h(t.Context(), missing)
	require.NoError(t, err)
	resp, ok = res.(protocol.GetTransactionResponse)
	require.True(t, ok)
	assert.Equal(t, protocol.TransactionStatusNotFound, resp.Status)
}

func getTxRequest(t *testing.T, hash string) *jrpc2.Request {
	t.Helper()
	requests, err := jrpc2.ParseRequests([]byte(
		`{"jsonrpc":"2.0","id":1,"method":"getTransaction","params":{"hash":"` + hash + `"}}`,
	))
	require.NoError(t, err)
	require.Len(t, requests, 1)
	return requests[0].ToRequest()
}
