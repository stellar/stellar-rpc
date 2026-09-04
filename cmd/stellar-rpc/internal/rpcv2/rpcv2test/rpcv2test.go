// Package rpcv2test holds test fixtures shared across the rpcv2 packages —
// the minimal ledger-close-meta builders several packages' tests would otherwise
// each hand-copy. It is imported only from _test files; nothing in it ships in
// the daemon binary.
package rpcv2test

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"slices"
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
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
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

// SymbolContractEvent returns a contract event whose topics and data
// are ScSymbols: the shape event-query tests build fixtures from and
// later match against by the data label.
func SymbolContractEvent(contractID xdr.ContractId, data string, topics ...string) xdr.ContractEvent {
	topicVals := make([]xdr.ScVal, len(topics))
	for i := range topics {
		sym := xdr.ScSymbol(topics[i])
		topicVals[i] = xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
	}
	dataSym := xdr.ScSymbol(data)
	return xdr.ContractEvent{
		ContractId: &contractID,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: topicVals,
				Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &dataSym},
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
	return V2LCMBytes(t, seq, closeTimeUnix, nil, nil)
}

// V2LCMBytes returns the marshaled bytes of a V2 LedgerCloseMeta for ledger
// seq: the given close time, the envelopes as the txset's one phase (no phase
// at all when empty), and processing as the apply results. Every V2 fixture
// builder delegates here so the skeleton exists once.
func V2LCMBytes(
	t *testing.T, seq uint32, closeTimeUnix int64,
	envelopes []xdr.TransactionEnvelope, processing []xdr.TransactionResultMetaV1,
) []byte {
	t.Helper()
	var phases []xdr.TransactionPhase
	if len(envelopes) > 0 {
		comp := []xdr.TxSetComponent{{
			Type:                  xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
			TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{Txs: envelopes},
		}}
		phases = []xdr.TransactionPhase{{V: 0, V0Components: &comp}}
	}
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
				V1TxSet: &xdr.TransactionSetV1{Phases: phases},
			},
			TxProcessing: processing,
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}

// WriteColdTxIndexFile builds a real cold tx-hash .idx at cov's layout path
// from the given hash→seq entries, through the production WriteColdBin +
// BuildColdIndex pipeline. Catalog state (freezing/frozen coverage keys) stays
// the caller's to manage.
func WriteColdTxIndexFile(
	t *testing.T, cat *catalog.Catalog, cov geometry.TxHashIndexCoverage, entries map[xdr.Hash]uint32,
) {
	t.Helper()
	master := cat.Secret()
	secret := txhash.ColdIndexSecret(master[:], uint32(cov.Index))
	cold := make([]txhash.ColdEntry, 0, len(entries))
	for h, seq := range entries {
		var e txhash.ColdEntry
		e.Key = stores.BlindKey(secret, h[:txhash.ColdKeySize])
		e.Seq = seq
		cold = append(cold, e)
	}
	slices.SortFunc(cold, func(a, b txhash.ColdEntry) int { return bytes.Compare(a.Key[:], b.Key[:]) })
	bin := filepath.Join(t.TempDir(), txhash.ColdBinName(cov.Lo))
	require.NoError(t, txhash.WriteColdBin(bin, secret, cold))

	idxPath := cat.Layout().TxHashIndexFilePath(cov)
	require.NoError(t, os.MkdirAll(filepath.Dir(idxPath), 0o755))
	require.NoError(t, txhash.BuildColdIndex(
		context.Background(), []string{bin}, idxPath, cov.Lo.FirstLedger(), cov.Hi.LastLedger()))
}

// EventLCMBytes returns the marshaled bytes of a single-transaction
// LedgerCloseMeta (V2) for ledger seq whose transaction carries one
// operation-level contract event — the fixture for tests that need ledgers
// with tx-hash and event payloads to extract. The random source account gives
// each call a distinct, valid pubnet transaction hash.
func EventLCMBytes(t *testing.T, seq uint32) []byte {
	t.Helper()
	var contractID xdr.ContractId
	contractID[0] = 0xab
	return EventsLCMBytes(t, seq, SymbolContractEvent(contractID, "fhtest", "fhtest"))
}

// EventsLCMBytes is EventsLCMBytesAt at close time zero.
func EventsLCMBytes(t *testing.T, seq uint32, evs ...xdr.ContractEvent) []byte {
	t.Helper()
	return EventsLCMBytesAt(t, seq, 0, evs...)
}

// EventsLCMBytesAt returns the marshaled bytes of a single-transaction
// LedgerCloseMeta (V2) for ledger seq, closed at closeTimeUnix, whose
// transaction carries evs as the events of one operation. The random source
// account gives each call a distinct, valid pubnet transaction hash.
func EventsLCMBytesAt(t *testing.T, seq uint32, closeTimeUnix int64, evs ...xdr.ContractEvent) []byte {
	t.Helper()
	meta := xdr.TransactionMeta{
		V:  4,
		V4: &xdr.TransactionMetaV4{Operations: []xdr.OperationMetaV2{{Events: evs}}},
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
	processing := []xdr.TransactionResultMetaV1{{
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
	}}
	return V2LCMBytes(t, seq, closeTimeUnix, []xdr.TransactionEnvelope{envelope}, processing)
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
	processing := []xdr.TransactionResultMetaV1{{
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
	}}
	return V2LCMBytes(t, seq, 0, []xdr.TransactionEnvelope{envelope}, processing)
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

	// Prelude (magic "SBIN", version, 3 reserved) + uint64-LE count + secret.
	const preludeSize = 8
	const headerSize = preludeSize + 8 + stores.SecretLen
	entrySize := txhash.ColdKeySize + 4

	var header [headerSize]byte
	_, err = io.ReadFull(f, header[:])
	require.NoError(t, err)
	require.Equal(t, []byte("SBIN"), header[:4], "cold .bin magic")
	require.Equal(t, byte(1), header[4], "cold .bin version")
	count := binary.LittleEndian.Uint64(header[preludeSize : preludeSize+8])

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
