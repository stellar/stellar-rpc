// Package rpcv2test holds test fixtures shared across the rpcv2 packages —
// the minimal ledger-close-meta builders several packages' tests would otherwise
// each hand-copy. It is imported only from _test files; nothing in it ships in
// the daemon binary.
package rpcv2test

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
)

// SilentLogger returns a logger that drops everything below panic level — for
// tests that need a logger wired through but never read its output.
func SilentLogger() *supportlog.Entry {
	log := supportlog.New()
	log.SetLevel(logrus.PanicLevel)
	return log
}

// OpenTestCatalog opens a throwaway catalog — RocksDB under one temp dir,
// artifacts under another, cpi-wide tx-hash indexes, silent logger, closed on
// test cleanup. Returns the catalog and the artifact root. Most tests pass
// geometry.ChunksPerTxhashIndex for cpi.
func OpenTestCatalog(t *testing.T, cpi uint32) (*catalog.Catalog, string) {
	t.Helper()
	return OpenTestCatalogWith(t, cpi, SilentLogger())
}

// OpenTestCatalogWith is OpenTestCatalog with the caller's logger, for tests
// that assert on what the catalog logs.
func OpenTestCatalogWith(t *testing.T, cpi uint32, logger *supportlog.Entry) (*catalog.Catalog, string) {
	t.Helper()
	artifactRoot := t.TempDir()
	idxLayout, err := geometry.NewTxHashIndexLayout(cpi)
	require.NoError(t, err)
	cat, err := catalog.Open(
		filepath.Join(t.TempDir(), "rocksdb"), geometry.NewLayout(artifactRoot), idxLayout, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })
	return cat, artifactRoot
}

// SeedHotChunkLCMs opens chunk c's hot DB under cat's layout, ingests lcms
// contiguously from the chunk's first ledger (the events CF requires a chunk
// to start there), flips the chunk ready, then hands the open DB to publish —
// the caller's registry hookup, since this package cannot import the query
// package (query's own tests import this one). The DB closes on test cleanup.
//
//	rpcv2test.SeedHotChunkLCMs(t, cat, c,
//		func(db *hotchunk.DB) { registry.PublishHandle(c, db) },
//		lcms...)
func SeedHotChunkLCMs(
	t *testing.T, cat *catalog.Catalog, c chunk.ID, publish func(*hotchunk.DB), lcms ...[]byte,
) {
	t.Helper()
	db, err := hotchunk.Open(cat.Layout().HotChunkPath(c), c, SilentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	for i, raw := range lcms {
		IngestLedger(t, db, c.FirstLedger()+uint32(i), raw)
	}
	require.NoError(t, cat.FlipHotReady(c))
	publish(db)
}

// WriteFrozenLedgerPack writes a real cold ledgers.pack for chunk c holding
// lcms from the chunk's first ledger, and flips the chunk's ledgers artifact
// frozen. AppendLedger stores raw bytes, so marker payloads work as well as
// real LCMs.
func WriteFrozenLedgerPack(t *testing.T, cat *catalog.Catalog, c chunk.ID, lcms ...[]byte) {
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

// ContractEventFixture builds a minimal operation-level contract event whose
// contract id starts with contractByte and whose single topic and data are
// both the symbol topic.
func ContractEventFixture(contractByte byte, topic string) xdr.ContractEvent {
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

// IngestLedger commits one raw ledger into db the way production does: the
// shared ExtractLedgerTxParts walk, then hotchunk's atomic write over its
// output. Tests that just need ledgers in a hot DB use this instead of
// hand-running the walk.
func IngestLedger(t *testing.T, db *hotchunk.DB, seq uint32, raw []byte) {
	t.Helper()
	txParts, err := sdkingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	_, err = db.IngestLedger(seq, xdr.LedgerCloseMetaView(raw), txParts)
	require.NoError(t, err)
}

// RetentionFor builds a geometry.Retention over cat's earliest_ledger pin, or
// chunk 0 (full history) when a test has not pinned one.
func RetentionFor(t *testing.T, cat *catalog.Catalog, size uint32) geometry.Retention {
	t.Helper()
	earliest, pinned, err := cat.EarliestLedger()
	require.NoError(t, err)
	ec := chunk.ID(0)
	if pinned {
		ec = chunk.IDFromLedger(earliest)
	}
	return geometry.NewRetention(size, ec)
}

// ZeroTxLCMBytes returns the marshaled bytes of a minimal, zero-transaction
// LedgerCloseMeta (V2) for ledger seq — the fixture ingestion/backfill/lifecycle
// tests feed in when they need a valid but empty ledger.
func ZeroTxLCMBytes(t *testing.T, seq uint32) []byte {
	t.Helper()
	return ZeroTxLCMBytesAt(t, seq, 0)
}

// ZeroTxLCMBytesAt is ZeroTxLCMBytes with an explicit close time, for tests
// that assert against a known close-time value.
func ZeroTxLCMBytesAt(t *testing.T, seq uint32, closeTimeUnix int64) []byte {
	t.Helper()
	lcm := xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					//nolint:gosec // test close times are small positives
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(closeTimeUnix)},
					LedgerSeq: xdr.Uint32(seq),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{Phases: nil},
			},
			TxProcessing: nil,
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}

// EventLCMBytes returns the marshaled bytes of a single-transaction
// LedgerCloseMeta (V2) for ledger seq whose transaction carries one
// operation-level contract event — the fixture for tests that need ledgers
// with tx-hash and event payloads to extract. The random source account gives
// each call a distinct, valid pubnet transaction hash.
func EventLCMBytes(t *testing.T, seq uint32) []byte {
	t.Helper()
	ev := ContractEventFixture(0xab, "fhtest")
	meta := xdr.TransactionMeta{
		V:  4,
		V4: &xdr.TransactionMetaV4{Operations: []xdr.OperationMetaV2{{Events: []xdr.ContractEvent{ev}}}},
	}

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

	opResults := []xdr.OperationResult{}
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
				TxApplyProcessing: meta,
				Result: xdr.TransactionResultPair{
					TransactionHash: hash,
					Result: xdr.TransactionResult{
						FeeCharged: 100,
						Result: xdr.TransactionResultResult{
							Code:    xdr.TransactionResultCodeTxSuccess,
							Results: &opResults,
						},
					},
				},
			}},
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}

// FeeTxLCMBytes returns the marshaled bytes of a single-transaction
// LedgerCloseMeta (V2) for ledger seq whose result carries feeCharged and ONE
// op result, so fee extraction (ingest.FeesFromTxParts) observes exactly one
// classic per-op fee of feeCharged. The other fixtures carry EMPTY op-result
// lists, which the fee classification skips as never-ran transactions —
// fee-window tests need this one.
func FeeTxLCMBytes(t *testing.T, seq uint32, feeCharged int64) []byte {
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
	opResults := []xdr.OperationResult{{Code: xdr.OperationResultCodeOpNotSupported}}
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
						FeeCharged: xdr.Int64(feeCharged),
						Result: xdr.TransactionResultResult{
							Code:    xdr.TransactionResultCodeTxFailed,
							Results: &opResults,
						},
					},
				},
			}},
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}

// ReadColdBin reads back a cold txhash .bin file written by
// txhash.WriteColdBin, verifying the header count against the file size.
// Test-side mirror of the on-disk contract (production consumes .bin files
// via the index builder's streaming scan, never a full read-back).
func ReadColdBin(t *testing.T, path string) []txhash.ColdEntry {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	const headerSize = 8
	entrySize := txhash.ColdKeySize + 4

	var header [headerSize]byte
	_, err = io.ReadFull(f, header[:])
	require.NoError(t, err)
	count := binary.LittleEndian.Uint64(header[:])

	info, err := f.Stat()
	require.NoError(t, err)
	// Divide the trusted size rather than multiplying the untrusted
	// count (mirrors production's overflow-safe coldBinCount check).
	body := info.Size() - headerSize
	require.GreaterOrEqual(t, body, int64(0), "cold .bin shorter than its header")
	require.Zero(t, body%int64(entrySize), "cold .bin body is not whole entries")
	require.EqualValues(t, count, body/int64(entrySize),
		"cold .bin size must match its declared entry count")

	entries := make([]txhash.ColdEntry, count)
	buf := make([]byte, entrySize)
	for i := range entries {
		_, err = io.ReadFull(f, buf)
		require.NoError(t, err)
		copy(entries[i].Key[:], buf[:txhash.ColdKeySize])
		entries[i].Seq = binary.LittleEndian.Uint32(buf[txhash.ColdKeySize:])
	}
	return entries
}
