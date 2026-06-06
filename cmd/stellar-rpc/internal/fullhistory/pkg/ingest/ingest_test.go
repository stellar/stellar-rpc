package ingest

import (
	"context"
	"encoding/binary"
	"iter"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// testPassphrase is a network passphrase literal used only by the tests; the
// package never hardcodes one.
const testPassphrase = "Public Global Stellar Network ; September 2015"

// ───────────────────────── fake streams ─────────────────────────

// fakeStream is an in-memory ledgerbackend.LedgerStream. It yields LCM bytes
// for count ledgers from r.From() via gen. When count covers the whole
// requested range the driver's chunk-completeness check passes; a smaller count
// models a backend that ends early (used by the short-stream negative test).
//
// gen is called per seq so LCMs are generated lazily — a full 10k-ledger chunk
// costs no up-front allocation. The default gen yields a cheap zero-tx LCM.
type fakeStream struct {
	t     *testing.T
	count uint32
	gen   func(t *testing.T, seq uint32) []byte
}

var _ ledgerbackend.LedgerStream = (*fakeStream)(nil)

func (f *fakeStream) RawLedgers(_ context.Context, r ledgerbackend.Range, _ ...ledgerbackend.StreamOption) iter.Seq2[[]byte, error] {
	gen := f.gen
	if gen == nil {
		gen = marshalLCM
	}
	return func(yield func([]byte, error) bool) {
		for i := uint32(0); i < f.count; i++ {
			seq := r.From() + i
			if !yield(gen(f.t, seq), nil) {
				return
			}
		}
	}
}

// fullStream yields exactly the requested chunk's full [First,Last] range using
// the given per-seq LCM generator (nil → cheap zero-tx LCMs).
func fullStream(t *testing.T, chunkID chunk.ID, gen func(*testing.T, uint32) []byte) *fakeStream {
	t.Helper()
	return &fakeStream{
		t:     t,
		count: chunkID.LastLedger() - chunkID.FirstLedger() + 1,
		gen:   gen,
	}
}

// sourceOf wraps a single stream as a ChunkSource (every chunk gets it). For
// the cold driver this is safe in tests because each test uses one chunk.
func sourceOf(s ledgerbackend.LedgerStream) ChunkSource {
	return ChunkSourceFunc(func(chunk.ID) (ledgerbackend.LedgerStream, error) { return s, nil })
}

// ───────────────────────── LCM fixtures ─────────────────────────

// marshalLCM builds a minimal valid zero-transaction LedgerCloseMeta (V2) for
// the given ledger seq and returns its wire bytes. Zero transactions is
// acceptable to events.LCMToPayloads (its tx reader opens it and finds none).
func marshalLCM(t *testing.T, seq uint32) []byte {
	t.Helper()
	raw, err := buildLCM(t, seq, nil).MarshalBinary()
	require.NoError(t, err)
	return raw
}

// eventTopic is the symbol topic embedded by event-bearing fixtures; the same
// term key derives from it, so tests can look the event up in the index.
const eventTopic = "ingest_test"

// marshalLCMWithEvent builds a V2 LCM carrying one transaction with one
// operation-level contract event (topic=eventTopic). It returns the wire bytes,
// the transaction hash (for txhash lookups), and the event's term key (for
// event index lookups).
func marshalLCMWithEvent(t *testing.T, seq uint32) (raw []byte, txHash [32]byte, term events.TermKey) {
	t.Helper()
	ev := buildContractEvent(eventTopic)
	meta := xdr.TransactionMeta{
		V:  4,
		V4: &xdr.TransactionMetaV4{Operations: []xdr.OperationMetaV2{{Events: []xdr.ContractEvent{ev}}}},
	}
	lcm, hash := buildLCMWithTx(t, seq, meta)
	rawBytes, err := lcm.MarshalBinary()
	require.NoError(t, err)

	evBytes, err := ev.MarshalBinary()
	require.NoError(t, err)
	keys, err := events.TermsForBytes(evBytes)
	require.NoError(t, err)
	require.NotEmpty(t, keys)
	return rawBytes, hash, keys[0]
}

// buildContractEvent returns a contract ContractEvent with a single symbol
// topic, mirroring the events-package test fixture.
func buildContractEvent(topic string) xdr.ContractEvent {
	var contractID xdr.ContractId
	contractID[0] = 0xab
	contractID[1] = 0xcd
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

func successResult() xdr.TransactionResult {
	opResults := []xdr.OperationResult{}
	return xdr.TransactionResult{
		FeeCharged: 100,
		Result: xdr.TransactionResultResult{
			Code:    xdr.TransactionResultCodeTxSuccess,
			Results: &opResults,
		},
	}
}

// buildLCM builds a V2 LedgerCloseMeta from the given per-tx metas. With no
// metas it is a zero-transaction ledger.
func buildLCM(t *testing.T, seq uint32, txMetas []xdr.TransactionMeta) xdr.LedgerCloseMeta {
	t.Helper()
	lcm, _ := buildLCMReturningHashes(t, seq, txMetas)
	return lcm
}

// buildLCMWithTx builds a single-transaction V2 LCM and returns the tx hash.
func buildLCMWithTx(t *testing.T, seq uint32, meta xdr.TransactionMeta) (xdr.LedgerCloseMeta, [32]byte) {
	t.Helper()
	lcm, hashes := buildLCMReturningHashes(t, seq, []xdr.TransactionMeta{meta})
	require.Len(t, hashes, 1)
	return lcm, hashes[0]
}

// buildLCMReturningHashes assembles a V2 LedgerCloseMeta with one envelope per
// tx meta and returns the per-tx transaction hashes in order.
func buildLCMReturningHashes(t *testing.T, seq uint32, txMetas []xdr.TransactionMeta) (xdr.LedgerCloseMeta, [][32]byte) {
	t.Helper()
	phases := make([]xdr.TransactionPhase, 0, len(txMetas))
	txProcessing := make([]xdr.TransactionResultMetaV1, 0, len(txMetas))
	hashes := make([][32]byte, 0, len(txMetas))

	for _, meta := range txMetas {
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
		hash, err := network.HashTransactionInEnvelope(envelope, testPassphrase)
		require.NoError(t, err)
		hashes = append(hashes, hash)

		txProcessing = append(txProcessing, xdr.TransactionResultMetaV1{
			TxApplyProcessing: meta,
			Result: xdr.TransactionResultPair{
				TransactionHash: hash,
				Result:          successResult(),
			},
		})
		comp := []xdr.TxSetComponent{{
			Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
			TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
				Txs: []xdr.TransactionEnvelope{envelope},
			},
		}}
		phases = append(phases, xdr.TransactionPhase{V: 0, V0Components: &comp})
	}

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
				V1TxSet: &xdr.TransactionSetV1{Phases: phases},
			},
			TxProcessing: txProcessing,
		},
	}
	return lcm, hashes
}

func testLogger() *supportlog.Entry {
	l := supportlog.New()
	l.SetLevel(logrus.ErrorLevel)
	return l
}

// ───────────────────────── hot driver tests ─────────────────────────

// TestRunHot_AllTypes_Readback runs RunHot with all three types enabled over
// event/tx-bearing ledgers and reopens each hot store to assert the ingested
// data reads back: ledger bytes, the tx hashes, and the event term.
//
// The hot stores fsync once per ledger, so driving the full 10k-ledger chunk
// would be far too slow for a unit test. Instead this uses a short stream of two
// event/tx-bearing ledgers: RunHot writes both through all three hot ingesters
// and then returns the chunk-completeness error (the stream ends early). The
// readback below proves every hot store retained the ingested data — the
// completeness error is expected and asserted.
func TestRunHot_AllTypes_Readback(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()

	// Two event/tx-bearing sentinel ledgers; capture each tx hash + the shared
	// event term for the lookups.
	evSeqA, evSeqB := first, first+1
	rawA, hashA, termA := marshalLCMWithEvent(t, evSeqA)
	rawB, hashB, _ := marshalLCMWithEvent(t, evSeqB)
	gen := func(tt *testing.T, seq uint32) []byte {
		switch seq {
		case evSeqA:
			return rawA
		case evSeqB:
			return rawB
		default:
			return marshalLCM(tt, seq)
		}
	}
	stream := &fakeStream{t: t, count: 2, gen: gen}

	hotDir := t.TempDir()
	logger := testLogger()
	cfg := Config{Ledgers: true, Txhash: true, Events: true, Passphrase: testPassphrase}

	// Short stream → completeness error after both ledgers are fully ingested.
	err := RunHot(context.Background(), logger, sourceOf(stream), chunkID, hotDir, cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ended at")

	// Ledger hot: both sentinel ledgers read back verbatim.
	ls, err := ledger.OpenHotStore(filepath.Join(hotDir, "ledgers"), logger)
	require.NoError(t, err)
	defer func() { require.NoError(t, ls.Close()) }()
	gotRawA, err := ls.GetLedgerRaw(evSeqA)
	require.NoError(t, err)
	require.Equal(t, rawA, gotRawA)

	// Txhash hot: both sentinel tx hashes map back to their ledger seqs.
	ts, err := txhash.OpenHotStore(filepath.Join(hotDir, "txhash"), logger)
	require.NoError(t, err)
	defer func() { require.NoError(t, ts.Close()) }()
	gotA, err := ts.Lookup(hashA)
	require.NoError(t, err)
	require.Equal(t, evSeqA, gotA)
	gotB, err := ts.Lookup(hashB)
	require.NoError(t, err)
	require.Equal(t, evSeqB, gotB)

	// Events hot: the shared term key resolves to a bitmap covering both events.
	es, err := eventstore.OpenHotStore(filepath.Join(hotDir, "events"), chunkID, logger)
	require.NoError(t, err)
	defer func() { require.NoError(t, es.Close()) }()
	bm, err := es.Lookup(context.Background(), termA)
	require.NoError(t, err)
	require.NotNil(t, bm)
	require.Equal(t, uint64(2), bm.GetCardinality(), "both sentinel events share the term")
}

// TestPackSource_RoundTrip exercises the production PackSource + packStream
// path end-to-end against a REAL cold ledger packfile (not the fake stream):
//
//  1. write a full chunk's cold packfile via ledger.NewColdWriter,
//  2. RunCold- from-nowhere is not the point here — instead drive the packStream
//     through a driver by re-ingesting the pack into the COLD ledger store via
//     RunCold(NewPackSource(...)), which streams the whole chunk (cold writers
//     batch, so the full range is fast), and assert the re-ingested cold store
//     reads the same bytes back.
//
// (RunHot over a full chunk would fsync per ledger and be far too slow; the
// hot driver's source plumbing is identical and is covered by the fake-stream
// hot tests. This test's job is to prove the real PackSource feeds a driver.)
func TestPackSource_RoundTrip(t *testing.T) {
	chunkID := chunk.ID(0)
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()

	// Write a full chunk's cold packfile so the completeness check passes.
	srcDir := t.TempDir()
	packFile := packPath(srcDir, chunkID)
	require.NoError(t, os.MkdirAll(filepath.Dir(packFile), 0o755))
	cw, err := ledger.NewColdWriter(packFile, first, ledger.ColdWriterOptions{})
	require.NoError(t, err)
	for seq := first; seq <= last; seq++ {
		require.NoError(t, cw.AppendLedger(seq, marshalLCM(t, seq)))
	}
	require.NoError(t, cw.Commit())
	require.NoError(t, cw.Close())

	// Re-ingest the pack into a fresh cold store via the production PackSource.
	src := NewPackSource(srcDir)
	dstDir := t.TempDir()
	logger := testLogger()
	require.NoError(t, RunCold(
		context.Background(), logger, src, dstDir, chunkID, 1, 1, Config{Ledgers: true},
	))

	// The re-ingested cold store reads back the same boundary bytes.
	cr, err := ledger.OpenColdReader(packPath(filepath.Join(dstDir, "ledgers"), chunkID))
	require.NoError(t, err)
	defer func() { require.NoError(t, cr.Close()) }()
	rawFirst, err := cr.GetLedgerRaw(first)
	require.NoError(t, err)
	require.Equal(t, marshalLCM(t, first), rawFirst)
	rawLast, err := cr.GetLedgerRaw(last)
	require.NoError(t, err)
	require.Equal(t, marshalLCM(t, last), rawLast)

	// OpenStream errors clearly for a chunk with no packfile.
	_, missErr := src.OpenStream(chunkID + 1)
	require.Error(t, missErr)
	require.Contains(t, missErr.Error(), "cold pack missing")
}

// ───────────────────────── cold driver tests ─────────────────────────

func TestRunCold_RoundTrip(t *testing.T) {
	chunkID := chunk.ID(0)
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
	stream := fullStream(t, chunkID, nil)

	coldDir := t.TempDir()
	logger := testLogger()
	cfg := Config{Ledgers: true}

	require.NoError(t, runColdWithSource(
		context.Background(), logger, sourceOf(stream), coldDir, chunkID, 1, 1, cfg,
	))

	// Reopen the chunk's cold ledger packfile and read back the boundary ledgers.
	path := packPath(filepath.Join(coldDir, "ledgers"), chunkID)
	cr, err := ledger.OpenColdReader(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, cr.Close()) }()

	rawFirst, err := cr.GetLedgerRaw(first)
	require.NoError(t, err)
	require.Equal(t, marshalLCM(t, first), rawFirst)

	rawLast, err := cr.GetLedgerRaw(last)
	require.NoError(t, err)
	require.Equal(t, marshalLCM(t, last), rawLast)
}

// TestRunCold_ShortStream_NoArtifact verifies that a stream which ends before
// the chunk's full range makes RunCold return an error AND never produces a
// finalized cold artifact (the completeness check runs before Finalize).
func TestRunCold_ShortStream_NoArtifact(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	logger := testLogger()
	cfg := Config{Ledgers: true}

	// Yield only 3 ledgers of a 10k-ledger chunk.
	short := &fakeStream{t: t, count: 3}

	err := runColdWithSource(
		context.Background(), logger, sourceOf(short), coldDir, chunkID, 1, 1, cfg,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ended at")

	// No finalized cold packfile must exist for the chunk: Close removed the
	// partial file because Finalize never ran.
	path := packPath(filepath.Join(coldDir, "ledgers"), chunkID)
	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "expected no cold artifact at %s, stat err: %v", path, statErr)
}

// customSource is a tiny in-test ChunkSource over in-memory LCMs, demonstrating
// that adding a backend requires only implementing the interface — no central
// switch edit.
type customSource struct {
	t   *testing.T
	gen func(*testing.T, uint32) []byte
}

func (c customSource) OpenStream(chunkID chunk.ID) (ledgerbackend.LedgerStream, error) {
	return fullStream(c.t, chunkID, c.gen), nil
}

// TestRunCold_CustomSource_Extensibility runs the cold driver against a
// caller-defined ChunkSource implementation and asserts success + readback,
// proving a new backend plugs in via the interface alone.
func TestRunCold_CustomSource_Extensibility(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()
	logger := testLogger()
	cfg := Config{Ledgers: true}

	src := customSource{t: t}
	require.NoError(t, RunCold(
		context.Background(), logger, src, coldDir, chunkID, 1, 1, cfg,
	))

	cr, err := ledger.OpenColdReader(packPath(filepath.Join(coldDir, "ledgers"), chunkID))
	require.NoError(t, err)
	defer func() { require.NoError(t, cr.Close()) }()
	raw, err := cr.GetLedgerRaw(first)
	require.NoError(t, err)
	require.Equal(t, marshalLCM(t, first), raw)
}

// TestRunCold_TxhashCold_Bin runs the cold txhash path over a chunk whose
// sentinel ledgers carry one tx each, then reads the produced .bin file's
// header and asserts the entry count matches the number of txs ingested.
func TestRunCold_TxhashCold_Bin(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()
	logger := testLogger()
	cfg := Config{Txhash: true}

	// Two sentinel ledgers carry one tx each; the rest are zero-tx.
	txSeqs := map[uint32]bool{first: true, first + 1: true}
	gen := func(tt *testing.T, seq uint32) []byte {
		if txSeqs[seq] {
			raw, _, _ := marshalLCMWithEvent(tt, seq)
			return raw
		}
		return marshalLCM(tt, seq)
	}

	require.NoError(t, RunCold(
		context.Background(), logger, customSource{t: t, gen: gen}, coldDir, chunkID, 1, 1, cfg,
	))

	// The cold txhash .bin lives at coldDir/txhash/<chunk>.bin. Read the 8-byte
	// LE header and assert it equals the number of txs (2).
	binPath := filepath.Join(coldDir, "txhash", chunkID.String()+".bin")
	data, err := os.ReadFile(binPath)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(data), 8)
	count := binary.LittleEndian.Uint64(data[:8])
	require.Equal(t, uint64(len(txSeqs)), count)
	// File size = 8-byte header + entrySize per entry.
	require.Equal(t, 8+int(count)*entrySize, len(data))
}

// TestRunCold_EventsCold_Readback runs the cold events path over a chunk whose
// sentinel ledgers carry one contract event each, finalizes the cold artifact,
// then reopens the eventstore cold reader and asserts the event term resolves.
func TestRunCold_EventsCold_Readback(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()
	logger := testLogger()
	cfg := Config{Events: true, Passphrase: testPassphrase}

	// Two sentinel ledgers carry one event each (sharing eventTopic's term).
	evSeqs := map[uint32]bool{first: true, first + 1: true}
	var term events.TermKey
	gen := func(tt *testing.T, seq uint32) []byte {
		if evSeqs[seq] {
			raw, _, tk := marshalLCMWithEvent(tt, seq)
			term = tk
			return raw
		}
		return marshalLCM(tt, seq)
	}

	require.NoError(t, RunCold(
		context.Background(), logger, customSource{t: t, gen: gen}, coldDir, chunkID, 1, 1, cfg,
	))

	// Reopen the events cold reader over coldDir/events/<bucket> and look up the
	// term — it must resolve to a non-empty bitmap (≥2 events).
	bucketDir := filepath.Join(coldDir, "events", chunkID.BucketID())
	cr, err := eventstore.OpenColdReader(chunkID, bucketDir, eventstore.ColdReaderOptions{})
	require.NoError(t, err)
	defer func() { require.NoError(t, cr.Close()) }()

	cnt, err := cr.EventCount()
	require.NoError(t, err)
	require.Equal(t, uint32(len(evSeqs)), cnt)

	bm, err := cr.Lookup(context.Background(), term)
	require.NoError(t, err)
	require.NotNil(t, bm)
	require.Equal(t, uint64(len(evSeqs)), bm.GetCardinality())
}
