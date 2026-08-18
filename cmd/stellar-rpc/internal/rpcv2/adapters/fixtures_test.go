package adapters

// The shared fixture layer for the adapters tests: catalog/registry setup,
// LCM builders, hot-chunk seeding, frozen-artifact writers, and scan
// collectors. Behavior lives in the per-adapter _test files; everything a
// test may seed lives here.

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
)

const testChunk = chunk.ID(5)

func silentLogger() *supportlog.Entry {
	log := supportlog.New()
	log.SetLevel(logrus.PanicLevel)
	return log
}

func openTestCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	idxLayout, err := geometry.NewTxHashIndexLayout(geometry.ChunksPerTxhashIndex)
	require.NoError(t, err)
	cat, err := catalog.Open(
		filepath.Join(t.TempDir(), "rocksdb"), geometry.NewLayout(t.TempDir()), idxLayout, silentLogger(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })
	return cat
}

// closeTimeFor derives a per-ledger close time so range and point reads can
// assert they decoded the right ledger, not just the right sequence.
func closeTimeFor(seq uint32) int64 { return int64(seq) * 7 }

func seqRange(lo, hi uint32) []uint32 {
	out := make([]uint32, 0, hi-lo+1)
	for s := lo; s <= hi; s++ {
		out = append(out, s)
	}
	return out
}

// ───────────────────────── LCM builders ─────────────────────────

func lcmBytes(t *testing.T, seq uint32) []byte {
	t.Helper()
	lcm := xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(closeTimeFor(seq))},
					LedgerSeq: xdr.Uint32(seq),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{Phases: nil},
			},
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}

// txSpec describes one transaction of a fixture ledger: its success, its
// per-operation contract events (one operation), and its V4 top-level stage
// events.
type txSpec struct {
	failed   bool
	events   []xdr.ContractEvent
	txEvents []xdr.TransactionEvent
}

// fixtureTx is one transaction as built into a fixture ledger, keeping the
// pieces tests assert against (raw XDR fields and the envelope hash).
type fixtureTx struct {
	hash     xdr.Hash
	envelope xdr.TransactionEnvelope
	result   xdr.TransactionResult
	meta     xdr.TransactionMeta
}

// lcmWithTxs builds a V2 LedgerCloseMeta for seq holding one real transaction
// per spec (random source account, pubnet-hashed envelope), with close time
// closeTimeFor(seq).
func lcmWithTxs(t *testing.T, seq uint32, specs ...txSpec) ([]byte, []fixtureTx) {
	t.Helper()
	envelopes := make([]xdr.TransactionEnvelope, len(specs))
	processing := make([]xdr.TransactionResultMetaV1, len(specs))
	txs := make([]fixtureTx, len(specs))
	for i, spec := range specs {
		envelope := xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{
				Tx: xdr.Transaction{
					SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
					Ext: xdr.TransactionExt{
						V:           1,
						SorobanData: &xdr.SorobanTransactionData{},
					},
				},
			},
		}
		hash, err := network.HashTransactionInEnvelope(envelope, network.PublicNetworkPassphrase)
		require.NoError(t, err)

		code := xdr.TransactionResultCodeTxSuccess
		if spec.failed {
			code = xdr.TransactionResultCodeTxFailed
		}
		opResults := []xdr.OperationResult{}
		result := xdr.TransactionResult{
			FeeCharged: 100,
			Result:     xdr.TransactionResultResult{Code: code, Results: &opResults},
		}
		meta := xdr.TransactionMeta{
			V: 4,
			V4: &xdr.TransactionMetaV4{
				Operations: []xdr.OperationMetaV2{{Events: spec.events}},
				Events:     spec.txEvents,
			},
		}
		envelopes[i] = envelope
		processing[i] = xdr.TransactionResultMetaV1{
			Result:            xdr.TransactionResultPair{TransactionHash: hash, Result: result},
			TxApplyProcessing: meta,
		}
		txs[i] = fixtureTx{hash: hash, envelope: envelope, result: result, meta: meta}
	}
	return marshalV2LCM(t, seq, envelopes, processing), txs
}

// feeBumpLCM builds a V2 LedgerCloseMeta for seq holding one successful
// fee-bump transaction, returning (bytes, outer hash, inner hash).
func feeBumpLCM(t *testing.T, seq uint32) ([]byte, xdr.Hash, xdr.Hash) {
	t.Helper()
	inner := xdr.TransactionV1Envelope{
		Tx: xdr.Transaction{
			SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
		},
	}
	innerHash, err := network.HashTransactionInEnvelope(
		xdr.TransactionEnvelope{Type: xdr.EnvelopeTypeEnvelopeTypeTx, V1: &inner},
		network.PublicNetworkPassphrase)
	require.NoError(t, err)

	envelope := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTxFeeBump,
		FeeBump: &xdr.FeeBumpTransactionEnvelope{
			Tx: xdr.FeeBumpTransaction{
				FeeSource: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
				InnerTx: xdr.FeeBumpTransactionInnerTx{
					Type: xdr.EnvelopeTypeEnvelopeTypeTx,
					V1:   &inner,
				},
			},
		},
	}
	outerHash, err := network.HashTransactionInEnvelope(envelope, network.PublicNetworkPassphrase)
	require.NoError(t, err)

	opResults := []xdr.OperationResult{}
	result := xdr.TransactionResult{
		FeeCharged: 200,
		Result: xdr.TransactionResultResult{
			Code: xdr.TransactionResultCodeTxFeeBumpInnerSuccess,
			InnerResultPair: &xdr.InnerTransactionResultPair{
				TransactionHash: innerHash,
				Result: xdr.InnerTransactionResult{
					Result: xdr.InnerTransactionResultResult{
						Code:    xdr.TransactionResultCodeTxSuccess,
						Results: &opResults,
					},
				},
			},
		},
	}
	processing := []xdr.TransactionResultMetaV1{{
		Result:            xdr.TransactionResultPair{TransactionHash: outerHash, Result: result},
		TxApplyProcessing: xdr.TransactionMeta{V: 4, V4: &xdr.TransactionMetaV4{}},
	}}
	raw := marshalV2LCM(t, seq, []xdr.TransactionEnvelope{envelope}, processing)
	return raw, outerHash, innerHash
}

func marshalV2LCM(
	t *testing.T, seq uint32, envelopes []xdr.TransactionEnvelope, processing []xdr.TransactionResultMetaV1,
) []byte {
	t.Helper()
	comp := []xdr.TxSetComponent{{
		Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
		TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
			Txs: envelopes,
		},
	}}
	lcm := xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(closeTimeFor(seq))},
					LedgerSeq: xdr.Uint32(seq),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{Phases: []xdr.TransactionPhase{{V: 0, V0Components: &comp}}},
			},
			TxProcessing: processing,
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}

// lcmV1WithClassicTx builds a V1 LedgerCloseMeta — the shape full history
// serves for pre-V2 ledgers — holding one successful classic (non-Soroban)
// transaction, returning (bytes, tx hash).
func lcmV1WithClassicTx(t *testing.T, seq uint32) ([]byte, xdr.Hash) {
	t.Helper()
	envelope := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{
				SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
			},
		},
	}
	hash, err := network.HashTransactionInEnvelope(envelope, network.PublicNetworkPassphrase)
	require.NoError(t, err)
	opResults := []xdr.OperationResult{}
	result := xdr.TransactionResult{
		FeeCharged: 100,
		Result:     xdr.TransactionResultResult{Code: xdr.TransactionResultCodeTxSuccess, Results: &opResults},
	}
	comp := []xdr.TxSetComponent{{
		Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
		TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
			Txs: []xdr.TransactionEnvelope{envelope},
		},
	}}
	lcm := xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(closeTimeFor(seq))},
					LedgerSeq: xdr.Uint32(seq),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{Phases: []xdr.TransactionPhase{{V: 0, V0Components: &comp}}},
			},
			TxProcessing: []xdr.TransactionResultMeta{{
				Result:            xdr.TransactionResultPair{TransactionHash: hash, Result: result},
				TxApplyProcessing: xdr.TransactionMeta{V: 3, V3: &xdr.TransactionMetaV3{}},
			}},
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw, hash
}

func contractEventFixture(contractByte byte, topic string) xdr.ContractEvent {
	var contractID xdr.ContractId
	contractID[0] = contractByte
	sym := xdr.ScSymbol(topic)
	return xdr.ContractEvent{
		ContractId: &contractID,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: []xdr.ScVal{{Type: xdr.ScValTypeScvSymbol, Sym: &sym}},
				Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
			},
		},
	}
}

func mustMarshal(t *testing.T, v interface{ MarshalBinary() ([]byte, error) }) []byte {
	t.Helper()
	raw, err := v.MarshalBinary()
	require.NoError(t, err)
	return raw
}

// ───────────────────────── Hot seeding ─────────────────────────

// seedHotLedgers opens chunk c's hot DB, commits the given seqs (which must
// start at the chunk's first ledger — the events CF requires it), marks the
// chunk ready, and publishes the handle.
func seedHotLedgers(t *testing.T, cat *catalog.Catalog, r *query.Registry, c chunk.ID, seqs ...uint32) {
	t.Helper()
	db, err := hotchunk.Open(cat.Layout().HotChunkPath(c), c, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	for _, seq := range seqs {
		rpcv2test.IngestLedger(t, db, seq, lcmBytes(t, seq))
	}
	require.NoError(t, cat.FlipHotReady(c))
	r.PublishHandle(c, db)
}

// seedHotChunkLCMs ingests raw LCMs contiguously from c's first ledger (the
// events CF requires a chunk to start at its first ledger), marks the chunk
// ready, and publishes the handle.
func seedHotChunkLCMs(t *testing.T, cat *catalog.Catalog, r *query.Registry, c chunk.ID, lcms ...[]byte) {
	t.Helper()
	db, err := hotchunk.Open(cat.Layout().HotChunkPath(c), c, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	for i, raw := range lcms {
		rpcv2test.IngestLedger(t, db, c.FirstLedger()+uint32(i), raw)
	}
	require.NoError(t, cat.FlipHotReady(c))
	r.PublishHandle(c, db)
}

// ───────────────────────── Frozen artifacts ─────────────────────────

// writeFrozenLedgerPack writes a real cold ledgers.pack holding lcms from c's
// first ledger and flips the chunk's ledgers artifact frozen.
func writeFrozenLedgerPack(t *testing.T, cat *catalog.Catalog, c chunk.ID, lcms ...[]byte) {
	t.Helper()
	path := cat.Layout().LedgerPackPath(c)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	w, err := ledger.NewColdWriter(path, c.FirstLedger(), ledger.ColdWriterOptions{})
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	for i, raw := range lcms {
		require.NoError(t, w.AppendLedger(c.FirstLedger()+uint32(i), raw))
	}
	require.NoError(t, w.Commit())
	require.NoError(t, cat.FlipChunkFrozen(c, geometry.KindLedgers))
}

// writeFrozenTxIndex builds a real .idx for window 0 covering [lo, hi] with the
// given hash→seq entries and commits its coverage frozen.
func writeFrozenTxIndex(t *testing.T, cat *catalog.Catalog, lo, hi chunk.ID, entries map[xdr.Hash]uint32) {
	t.Helper()
	cov, err := cat.MarkTxHashIndexFreezing(0, lo, hi)
	require.NoError(t, err)

	cold := make([]txhash.ColdEntry, 0, len(entries))
	for h, seq := range entries {
		var e txhash.ColdEntry
		copy(e.Key[:], h[:txhash.ColdKeySize])
		e.Seq = seq
		cold = append(cold, e)
	}
	slices.SortFunc(cold, func(a, b txhash.ColdEntry) int { return bytes.Compare(a.Key[:], b.Key[:]) })
	bin := filepath.Join(t.TempDir(), txhash.ColdBinName(lo))
	require.NoError(t, txhash.WriteColdBin(bin, cold))

	idxPath := cat.Layout().TxHashIndexFilePath(cov)
	require.NoError(t, os.MkdirAll(filepath.Dir(idxPath), 0o755))
	require.NoError(t, txhash.BuildColdIndex(
		context.Background(), []string{bin}, idxPath, lo.FirstLedger(), hi.LastLedger()))
	require.NoError(t, cat.CommitTxHashIndex(cov))
}
