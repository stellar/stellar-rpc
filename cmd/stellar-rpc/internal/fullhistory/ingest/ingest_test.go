package ingest

import (
	"bytes"
	"context"
	"errors"
	"iter"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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

// testPassphrase is a network passphrase literal used only by the test fixtures
// (transaction-hash derivation); the package itself never hardcodes one.
const testPassphrase = "Public Global Stellar Network ; September 2015"

// ───────────────────────── test metric sink ─────────────────────────

type hotCall struct {
	dataType string
	items    int
	err      error
}

type coldCall struct {
	dataType string
	items    int
	err      error
}

type stageCall struct {
	dataType string
	tier     string
	stage    string
	items    int
}

// testSink records every MetricSink call for assertions. Safe for concurrent
// use (HotIngest fires from the per-ledger fan-out goroutines).
type testSink struct {
	mu              sync.Mutex
	hotIngests      []hotCall
	coldIngests     []coldCall
	stages          []stageCall
	hotLedgerTotals int
	coldChunkTotals int
}

func (s *testSink) HotIngest(dataType string, _ time.Duration, items int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hotIngests = append(s.hotIngests, hotCall{dataType, items, err})
}

func (s *testSink) ColdIngest(dataType string, _ time.Duration, items int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.coldIngests = append(s.coldIngests, coldCall{dataType, items, err})
}

func (s *testSink) HotLedgerTotal(time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hotLedgerTotals++
}

func (s *testSink) ColdChunkTotal(time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.coldChunkTotals++
}

func (s *testSink) IngestStage(dataType, tier, stage string, _ time.Duration, items int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stages = append(s.stages, stageCall{dataType, tier, stage, items})
}

// stageCounts counts IngestStage calls keyed "dataType/tier/stage".
func (s *testSink) stageCounts() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := map[string]int{}
	for _, c := range s.stages {
		m[c.dataType+"/"+c.tier+"/"+c.stage]++
	}
	return m
}

func (s *testSink) hotDataTypes() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := map[string]int{}
	for _, c := range s.hotIngests {
		m[c.dataType]++
	}
	return m
}

func (s *testSink) coldDataTypes() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := map[string]int{}
	for _, c := range s.coldIngests {
		m[c.dataType]++
	}
	return m
}

// coldErrorTypes counts cold per-ingester calls that carried a non-nil error,
// keyed by data type.
func (s *testSink) coldErrorTypes() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := map[string]int{}
	for _, c := range s.coldIngests {
		if c.err != nil {
			m[c.dataType]++
		}
	}
	return m
}

// ───────────────────────── fake streams ─────────────────────────

// fakeStream is an in-memory ledgerbackend.LedgerStream. It yields LCM bytes for
// count ledgers from r.From() via gen. When count covers the whole requested
// range the driver's chunk-completeness check passes; a smaller count models a
// backend that ends early (used by the short-stream negative test).
type fakeStream struct {
	t     *testing.T
	count uint32
	gen   func(t *testing.T, seq uint32) []byte
}

var _ ledgerbackend.LedgerStream = (*fakeStream)(nil)

func (f *fakeStream) RawLedgers(
	_ context.Context, r ledgerbackend.Range, _ ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	gen := f.gen
	if gen == nil {
		gen = marshalLCM
	}
	return func(yield func([]byte, error) bool) {
		for i := range f.count {
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

// packPath returns a chunk's cold pack path under a per-type ledgers root. The
// production packPath moved into the ledger store package alongside NewPackStream,
// so tests keep their own copy for readback assertions.
//
//nolint:unparam // chunk-general helper; every current caller uses chunk 0
func packPath(ledgersRoot string, c chunk.ID) string {
	return filepath.Join(ledgersRoot, c.BucketID(), ledger.PackName(c))
}

// coldDirsAt resolves chunk c's three cold-artifact paths under one dir's per-type
// roots — mirroring what geometry.Layout derives in production, so the readback
// helpers (packPath/txhashBinPath) find what the ingesters wrote.
func coldDirsAt(dir string, c chunk.ID) ColdDirs {
	return ColdDirs{
		LedgerPack: packPath(filepath.Join(dir, dataTypeLedgers), c),
		TxhashBin:  txhashBinPath(filepath.Join(dir, dataTypeTxhash)),
		EventsDir:  filepath.Join(dir, dataTypeEvents, c.BucketID()),
	}
}

// rawChunk opens s over chunkID's full [First,Last] range, returning the raw
// ledger iterator the source-blind WriteColdChunk and drain consume. Tests pass a
// fake LedgerStream (fakeStream/fullStream) and get its iterator here.
//
//nolint:unparam // chunk-general helper; every current caller uses chunk 0
func rawChunk(s ledgerbackend.LedgerStream, chunkID chunk.ID) iter.Seq2[[]byte, error] {
	return s.RawLedgers(context.Background(), ledgerbackend.BoundedRange(chunkID.FirstLedger(), chunkID.LastLedger()))
}

// ───────────────────────── LCM fixtures ─────────────────────────

// marshalLCM builds a minimal valid zero-transaction LedgerCloseMeta (V2) for
// the given ledger seq and returns its wire bytes.
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
// the transaction hash (for txhash lookups), and the event's term key (for event
// index lookups).
func marshalLCMWithEvent(t *testing.T, seq uint32) ([]byte, [32]byte, events.TermKey) {
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

// buildLCM builds a V2 LedgerCloseMeta from the given per-tx metas. With no metas
// it is a zero-transaction ledger.
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

// buildLCMReturningHashes assembles a V2 LedgerCloseMeta with one envelope per tx
// meta and returns the per-tx transaction hashes in order.
func buildLCMReturningHashes(
	t *testing.T, seq uint32, txMetas []xdr.TransactionMeta,
) (xdr.LedgerCloseMeta, [][32]byte) {
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

// viewOf marshals a zero-tx LCM fixture and returns it as a view.
func viewOf(t *testing.T, seq uint32) xdr.LedgerCloseMetaView {
	t.Helper()
	return xdr.LedgerCloseMetaView(marshalLCM(t, seq))
}

// marshalV0LCM builds a minimal V0 (pre-Soroban) LedgerCloseMeta with no
// transactions and returns its wire bytes. V0 LCMs carry no contract events,
// so the events ingesters record them as a zero-payload ledger.
func marshalV0LCM(t *testing.T, seq uint32) []byte {
	t.Helper()
	lcm := xdr.LedgerCloseMeta{V: 0, V0: &xdr.LedgerCloseMetaV0{
		LedgerHeader: xdr.LedgerHeaderHistoryEntry{
			Header: xdr.LedgerHeader{LedgerSeq: xdr.Uint32(seq)},
		},
	}}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}

// seqStream is a ledgerbackend.LedgerStream that yields LCMs for an explicit
// list of ledger sequences (in order), regardless of the requested range. It
// models a backend that hands back a duplicate / out-of-order / wrong-but-
// right-count sequence, exercising the drain seq guard.
type seqStream struct {
	t    *testing.T
	seqs []uint32
}

var _ ledgerbackend.LedgerStream = (*seqStream)(nil)

func (s *seqStream) RawLedgers(
	_ context.Context, _ ledgerbackend.Range, _ ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		for _, seq := range s.seqs {
			if !yield(marshalLCM(s.t, seq), nil) {
				return
			}
		}
	}
}

// errAtSeqStream yields valid LCMs until it reaches errAtSeq, where it yields
// (nil, err) — modeling a backend that fails mid-stream. Used to exercise the
// drain RawLedgers error path.
type errAtSeqStream struct {
	t        *testing.T
	errAtSeq uint32
	err      error
}

var _ ledgerbackend.LedgerStream = (*errAtSeqStream)(nil)

func (s *errAtSeqStream) RawLedgers(
	_ context.Context, r ledgerbackend.Range, _ ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		for seq := r.From(); ; seq++ {
			if seq == s.errAtSeq {
				yield(nil, s.err)
				return
			}
			if !yield(marshalLCM(s.t, seq), nil) {
				return
			}
		}
	}
}

// ───────────────────────── per-ingester unit tests ─────────────────────────

// TestLedgerColdIngester_Readback ingests one ledger via the cold ledger
// ingester, finalizes, and reads back through the cold reader.
func TestLedgerColdIngester_Readback(t *testing.T) {
	chunkID := chunk.ID(0)
	seq := chunkID.FirstLedger()
	raw := marshalLCM(t, seq)
	coldDir := t.TempDir()

	ing, err := NewLedgerColdIngester(packPath(coldDir, chunkID), chunkID, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, ing.Close()) }()

	require.NoError(t, ing.Ingest(context.Background(), seq, xdr.LedgerCloseMetaView(raw)))
	require.NoError(t, ing.Finalize(context.Background()))

	cr, err := ledger.OpenColdReader(packPath(coldDir, chunkID))
	require.NoError(t, err)
	defer func() { require.NoError(t, cr.Close()) }()
	got, err := cr.GetLedgerRaw(seq)
	require.NoError(t, err)
	require.Equal(t, raw, got)
}

// txhashBinPath composes the documented raw-txhash chunk path under root for
// the tests' fixed chunk 0: {root}/{bucketID:05d}/{chunkID:08d}.bin.
func txhashBinPath(root string) string {
	c := chunk.ID(0)
	return filepath.Join(root, c.BucketID(), txhash.ColdBinName(c))
}

// TestTxhashColdIngester_Bin ingests two tx-bearing ledgers via the cold txhash
// ingester, finalizes, and reads the .bin back through the store codec.
func TestTxhashColdIngester_Bin(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()

	ing, err := NewTxhashColdIngester(txhashBinPath(coldDir), chunkID, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, ing.Close()) }()

	for _, seq := range []uint32{first, first + 1} {
		raw, _, _ := marshalLCMWithEvent(t, seq)
		require.NoError(t, ing.Ingest(context.Background(), seq, xdr.LedgerCloseMetaView(raw)))
	}
	require.NoError(t, ing.Finalize(context.Background()))

	entries, err := txhash.ReadColdBin(txhashBinPath(coldDir))
	require.NoError(t, err)
	require.Len(t, entries, 2)
}

// TestEventsColdIngester_Readback ingests two event-bearing ledgers via the cold
// events ingester, finalizes, and resolves the term through the cold reader.
func TestEventsColdIngester_Readback(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()

	ing, err := NewEventsColdIngester(filepath.Join(coldDir, chunkID.BucketID()), chunkID, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, ing.Close()) }()

	var term events.TermKey
	for _, seq := range []uint32{first, first + 1} {
		raw, _, tk := marshalLCMWithEvent(t, seq)
		term = tk
		require.NoError(t, ing.Ingest(context.Background(), seq, xdr.LedgerCloseMetaView(raw)))
	}
	require.NoError(t, ing.Finalize(context.Background()))

	bucketDir := filepath.Join(coldDir, chunkID.BucketID())
	cr, err := eventstore.OpenColdReader(chunkID, bucketDir, eventstore.ColdReaderOptions{})
	require.NoError(t, err)
	defer func() { require.NoError(t, cr.Close()) }()
	cnt, err := cr.EventCount()
	require.NoError(t, err)
	require.Equal(t, uint32(2), cnt)
	bm, err := cr.Lookup(context.Background(), term)
	require.NoError(t, err)
	require.NotNil(t, bm)
	require.Equal(t, uint64(2), bm.GetCardinality())
}

// ───────────────────────── V0 (pre-Soroban) events handling ─────────────────────────

// TestEventsColdIngester_V0KeepsOffsetsContiguous ingests a V0 ledger followed by
// an event-bearing V2 ledger and asserts: the V0 ledger does not error, and the
// LedgerOffsets stay contiguous (both ledgers present, the event-bearing one's
// single event ID immediately follows the empty V0 ledger).
func TestEventsColdIngester_V0KeepsOffsetsContiguous(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()

	ing, err := NewEventsColdIngester(filepath.Join(coldDir, chunkID.BucketID()), chunkID, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, ing.Close()) }()

	// Ledger `first`: V0 → zero events, no error.
	require.NoError(t, ing.Ingest(context.Background(), first, xdr.LedgerCloseMetaView(marshalV0LCM(t, first))))
	// Ledger `first+1`: one contract event.
	rawEv, _, term := marshalLCMWithEvent(t, first+1)
	require.NoError(t, ing.Ingest(context.Background(), first+1, xdr.LedgerCloseMetaView(rawEv)))
	require.NoError(t, ing.Finalize(context.Background()))

	bucketDir := filepath.Join(coldDir, chunkID.BucketID())
	cr, err := eventstore.OpenColdReader(chunkID, bucketDir, eventstore.ColdReaderOptions{})
	require.NoError(t, err)
	defer func() { require.NoError(t, cr.Close()) }()

	// One event total, from the V2 ledger.
	cnt, err := cr.EventCount()
	require.NoError(t, err)
	require.Equal(t, uint32(1), cnt)

	// Offsets are contiguous: both ledgers recorded, V0 contributes [0,0), the
	// event-bearing ledger contributes exactly event ID 0.
	offsets, err := cr.Offsets()
	require.NoError(t, err)
	require.Equal(t, 2, offsets.LedgerCount(), "both V0 and V2 ledgers recorded")
	require.Equal(t, first, offsets.StartLedger())
	v0Start, v0End, err := offsets.EventIDs(first)
	require.NoError(t, err)
	require.Equal(t, uint32(0), v0Start)
	require.Equal(t, uint32(0), v0End, "V0 ledger has an empty event range")
	evStart, evEnd, err := offsets.EventIDs(first + 1)
	require.NoError(t, err)
	require.Equal(t, uint32(0), evStart, "event ID follows the empty V0 ledger contiguously")
	require.Equal(t, uint32(1), evEnd)

	// And the event is queryable by its term.
	bm, err := cr.Lookup(context.Background(), term)
	require.NoError(t, err)
	require.NotNil(t, bm)
	require.Equal(t, uint64(1), bm.GetCardinality())
}

// TestWriteColdChunk_EventlessChunk_FullyReadable drives a full cold chunk of V0
// (pre-Soroban, eventless) ledgers with Events enabled — the common backfill
// case for early history. The whole chunk has zero contract events;
// eventstore.WriteColdIndex publishes a valid EMPTY index for it, so all
// three cold artifacts exist and the chunk is fully readable: a term-filtered
// Lookup resolves to "no matches" through the ordinary path instead of a
// missing-file error.
func TestWriteColdChunk_EventlessChunk_FullyReadable(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	logger := testLogger()
	sink := &testSink{}

	// Every ledger in the chunk is a V0 (pre-Soroban) ledger → zero events.
	require.NoError(t, WriteColdChunk(
		context.Background(), logger, chunkID, rawChunk(fullStream(t, chunkID, marshalV0LCM), chunkID),
		coldDirsAt(coldDir, chunkID), sink, Config{Events: true},
	))

	bucketDir := filepath.Join(coldDir, dataTypeEvents, chunkID.BucketID())

	// All three cold artifacts exist (events.pack + the empty index pair).
	for _, name := range []string{
		eventstore.EventsPackName(chunkID),
		eventstore.IndexPackName(chunkID),
		eventstore.IndexHashName(chunkID),
	} {
		_, statErr := os.Stat(filepath.Join(bucketDir, name))
		require.NoError(t, statErr, "eventless chunk must publish %s", name)
	}

	// The chunk is readable end to end: zero events, and a filtered lookup
	// misses cleanly rather than erroring on a missing index.
	cr, err := eventstore.OpenColdReader(chunkID, bucketDir, eventstore.ColdReaderOptions{})
	require.NoError(t, err)
	defer func() { require.NoError(t, cr.Close()) }()
	cnt, err := cr.EventCount()
	require.NoError(t, err)
	require.Zero(t, cnt)
	_, lerr := cr.Lookup(context.Background(), events.ComputeTermKey([]byte("any"), events.FieldContractID))
	require.ErrorIs(t, lerr, eventstore.ErrTermNotFound)

	// Metrics still fired: one aggregate per-chunk, one (clean) per-ingester.
	require.Equal(t, 1, sink.coldChunkTotals, "ColdChunkTotal must fire for an eventless chunk")
	require.Equal(t, 1, sink.coldDataTypes()[dataTypeEvents], "one ColdIngest for events")
	require.Zero(t, sink.coldErrorTypes()[dataTypeEvents], "eventless chunk is not an error")
}

// ───────────────────────── ColdService tests ─────────────────────────

// TestColdService_Success drives ledger+txhash+events cold ingesters through a
// ColdService and asserts readback plus the metrics signals.
func TestColdService_Success(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()
	sink := &testSink{}

	ings, err := buildColdIngesters(coldDirsAt(coldDir, chunkID), chunkID, sink, Config{Ledgers: true, Txhash: true, Events: true})
	require.NoError(t, err)
	service := NewColdService(ings, sink)
	defer func() { require.NoError(t, service.Close()) }()

	var term events.TermKey
	for _, seq := range []uint32{first, first + 1} {
		raw, _, tk := marshalLCMWithEvent(t, seq)
		term = tk
		require.NoError(t, service.Ingest(context.Background(), seq, xdr.LedgerCloseMetaView(raw)))
	}
	require.NoError(t, service.Finalize(context.Background()))

	// Ledger cold readback: tx hashes use random keypairs, so bytes can't be
	// regenerated for comparison — assert the boundary ledger reads back and
	// decodes to the right sequence.
	lcr, err := ledger.OpenColdReader(packPath(filepath.Join(coldDir, dataTypeLedgers), chunkID))
	require.NoError(t, err)
	defer func() { require.NoError(t, lcr.Close()) }()
	gotFirst, err := lcr.GetLedgerRaw(first)
	require.NoError(t, err)
	var decoded xdr.LedgerCloseMeta
	require.NoError(t, decoded.UnmarshalBinary(gotFirst))
	require.Equal(t, first, decoded.LedgerSequence())

	// Events cold readback.
	ecr, err := eventstore.OpenColdReader(
		chunkID, filepath.Join(coldDir, dataTypeEvents, chunkID.BucketID()), eventstore.ColdReaderOptions{})
	require.NoError(t, err)
	defer func() { require.NoError(t, ecr.Close()) }()
	bm, err := ecr.Lookup(context.Background(), term)
	require.NoError(t, err)
	require.Equal(t, uint64(2), bm.GetCardinality())

	// Txhash .bin count.
	binEntries, err := txhash.ReadColdBin(txhashBinPath(filepath.Join(coldDir, dataTypeTxhash)))
	require.NoError(t, err)
	require.Len(t, binEntries, 2)

	// Metrics: one ColdChunkTotal, one ColdIngest per data type, no errors.
	require.Equal(t, 1, sink.coldChunkTotals)
	cdt := sink.coldDataTypes()
	require.Equal(t, 1, cdt[dataTypeLedgers])
	require.Equal(t, 1, cdt[dataTypeTxhash])
	require.Equal(t, 1, cdt[dataTypeEvents])
	require.Empty(t, sink.coldErrorTypes(), "success path records no ingester errors")

	// Per-stage signals: per-ledger cold stages fired once per (non-empty)
	// ledger, the per-chunk finalize stage once per ingester. The exact map is
	// asserted so an unexpected stage emission (or a missing one) also fails —
	// events now emits term_index/write for every ledger, and txhash's extract
	// spans its whole per-ledger Ingest.
	require.Equal(t, map[string]int{
		dataTypeLedgers + "/" + tierCold + "/" + stageWrite:    2,
		dataTypeLedgers + "/" + tierCold + "/" + stageFinalize: 1,
		dataTypeTxhash + "/" + tierCold + "/" + stageExtract:   2,
		dataTypeTxhash + "/" + tierCold + "/" + stageFinalize:  1,
		dataTypeEvents + "/" + tierCold + "/" + stageExtract:   2,
		dataTypeEvents + "/" + tierCold + "/" + stageTermIndex: 2,
		dataTypeEvents + "/" + tierCold + "/" + stageWrite:     2,
		dataTypeEvents + "/" + tierCold + "/" + stageFinalize:  1,
	}, sink.stageCounts())

	// No double-emit: the deferred Close (after this body) must not add a second
	// ColdIngest or ColdChunkTotal, since Finalize already emitted.
	require.NoError(t, service.Close())
	require.Equal(t, 1, sink.coldChunkTotals, "Close after Finalize must not re-emit the aggregate")
	require.Len(t, sink.coldIngests, 3, "Close after Finalize must not re-emit per-ingester signals")
}

// failingCold is a ColdIngester whose Ingest always fails, modeling a mid-chunk
// error. Finalize must NOT run on this path.
type failingCold struct {
	finalized bool
	closed    bool
}

var errFailingCold = errors.New("failingCold: induced ingest failure")

func (f *failingCold) Ingest(context.Context, uint32, xdr.LedgerCloseMetaView) error {
	return errFailingCold
}
func (f *failingCold) Finalize(context.Context) error { f.finalized = true; return nil }
func (f *failingCold) Close() error                   { f.closed = true; return nil }

// TestColdService_FailurePath_NoArtifact uses two real cold ingesters plus a
// failing sibling: ColdService.Ingest returns the sibling's error, Finalize is
// not called, the deferred Close drops the partial ledger pack, and no finalized
// artifact remains. It asserts the aggregate ColdChunkTotal still fires for the
// attempt, but the two real ingesters emit NO per-ingester ColdIngest: each
// ingested cleanly (no terminal error of its own) and never finalized, and Close
// no longer emits — so a chunk abandoned by a sibling leaves no phantom sample.
func TestColdService_FailurePath_NoArtifact(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	sink := &testSink{}

	realLedger, err := NewLedgerColdIngester(packPath(filepath.Join(coldDir, dataTypeLedgers), chunkID), chunkID, sink)
	require.NoError(t, err)
	realEvents, err := NewEventsColdIngester(filepath.Join(coldDir, dataTypeEvents, chunkID.BucketID()), chunkID, sink)
	require.NoError(t, err)
	failing := &failingCold{}
	service := NewColdService([]ColdIngester{realLedger, realEvents, failing}, sink)

	// First ledger: the real ingesters succeed, failing returns an error → the
	// sequential Ingest aborts the ledger with the sibling's error.
	err = service.Ingest(context.Background(), chunkID.FirstLedger(), viewOf(t, chunkID.FirstLedger()))
	require.ErrorIs(t, err, errFailingCold)
	require.False(t, failing.finalized, "Finalize must not run on the failure path")

	// Nothing has emitted: the real ingesters ingested cleanly (no terminal error)
	// and never finalized; the mock sibling records nothing.
	require.Empty(t, sink.coldDataTypes(), "no per-ingester ColdIngest on the sibling-failure path")
	require.Zero(t, sink.coldChunkTotals, "no ColdChunkTotal before Close")

	// Close drops partials and emits the aggregate only.
	require.NoError(t, service.Close())
	require.True(t, failing.closed)

	require.Empty(t, sink.coldDataTypes(), "a chunk abandoned by a sibling emits no per-ingester ColdIngest")
	require.Equal(t, 1, sink.coldChunkTotals, "exactly one aggregate ColdChunkTotal")

	// No finalized ledger pack must exist.
	path := packPath(filepath.Join(coldDir, dataTypeLedgers), chunkID)
	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "expected no cold ledger artifact at %s, stat err: %v", path, statErr)
}

// TestColdIngester_Failure_RecordsErrorMetric drives a real cold ledger ingester
// so its OWN Ingest fails (recording firstErr), then Close. The failure is an
// out-of-order seq: the per-chunk ColdWriter expects the chunk's first ledger,
// so AppendLedger rejects a later one. Per #765 a failed cold chunk must record
// a per-ingester error count and an aggregate duration sample. A terminal Ingest
// error emits the single per-ingester ColdIngest right there (Close no longer
// emits), so the error-carrying sample is present after Ingest returns.
func TestColdIngester_Failure_RecordsErrorMetric(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	sink := &testSink{}

	realLedger, err := NewLedgerColdIngester(packPath(filepath.Join(coldDir, dataTypeLedgers), chunkID), chunkID, sink)
	require.NoError(t, err)
	service := NewColdService([]ColdIngester{realLedger}, sink)

	// An out-of-order seq makes the writer's own AppendLedger fail inside the
	// ingester's Ingest, so it records its firstErr and emits the error-carrying
	// ColdIngest. (drain would never feed this — the test targets the ingester's
	// metric path directly.)
	wrongSeq := chunkID.FirstLedger() + 5
	require.Error(t, service.Ingest(context.Background(), wrongSeq, viewOf(t, wrongSeq)))
	require.Equal(t, 1, sink.coldDataTypes()[dataTypeLedgers], "the failed Ingest emits its ColdIngest immediately")

	// Finalize is skipped on this path; Close emits nothing more.
	require.NoError(t, service.Close())

	// Exactly one ColdIngest for ledgers, carrying the error, plus one aggregate.
	require.Equal(t, 1, sink.coldDataTypes()[dataTypeLedgers])
	require.Equal(t, 1, sink.coldErrorTypes()[dataTypeLedgers], "the recorded ColdIngest carries the ingest error")
	require.Equal(t, 1, sink.coldChunkTotals)
}

// ───────────────────────── metrics sink tests ─────────────────────────

// TestPrometheusSink_Smoke asserts NewPrometheusSink registers without panic and
// its methods don't error.
func TestPrometheusSink_Smoke(t *testing.T) {
	reg := prometheus.NewRegistry()
	require.NotPanics(t, func() {
		sink := NewPrometheusSink(reg, "test")
		sink.HotIngest(dataTypeLedgers, time.Millisecond, 1, nil)
		sink.HotIngest(dataTypeEvents, time.Millisecond, 3, errFailingCold)
		sink.ColdIngest(dataTypeTxhash, time.Second, 100, nil)
		sink.HotLedgerTotal(time.Millisecond)
		sink.ColdChunkTotal(time.Second)
		sink.IngestStage(dataTypeEvents, tierHot, stageExtract, time.Millisecond, 3)
		sink.IngestStage(dataTypeEvents, tierCold, stageFinalize, time.Second, 0)
	})

	mfs, err := reg.Gather()
	require.NoError(t, err)
	require.NotEmpty(t, mfs)
}

// ───────────────────────── cold driver tests ─────────────────────────

func TestWriteColdChunk_RoundTrip(t *testing.T) {
	chunkID := chunk.ID(0)
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
	stream := fullStream(t, chunkID, nil)

	coldDir := t.TempDir()
	logger := testLogger()
	sink := &testSink{}

	require.NoError(t, WriteColdChunk(
		context.Background(), logger, chunkID, rawChunk(stream, chunkID), coldDirsAt(coldDir, chunkID), sink, Config{Ledgers: true},
	))

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

	require.Equal(t, 1, sink.coldChunkTotals)
	require.Equal(t, 1, sink.coldDataTypes()[dataTypeLedgers])
}

// TestWriteColdChunk_ShortStream_NoArtifact verifies a short stream makes WriteColdChunk error
// AND never produces a finalized cold artifact (completeness runs before
// Finalize, deferred Close drops the partial).
func TestWriteColdChunk_ShortStream_NoArtifact(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	logger := testLogger()

	short := &fakeStream{t: t, count: 3}
	err := WriteColdChunk(
		context.Background(), logger, chunkID, rawChunk(short, chunkID), coldDirsAt(coldDir, chunkID), nil, Config{Ledgers: true},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ended at")

	path := packPath(filepath.Join(coldDir, "ledgers"), chunkID)
	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "expected no cold artifact at %s, stat err: %v", path, statErr)
}

// TestWriteColdChunk_TxhashCold_Bin runs the cold txhash driver over a chunk whose
// sentinel ledgers carry one tx each and asserts the .bin entry count.
func TestWriteColdChunk_TxhashCold_Bin(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()
	logger := testLogger()

	txSeqs := map[uint32]bool{first: true, first + 1: true}
	gen := func(tt *testing.T, seq uint32) []byte {
		if txSeqs[seq] {
			raw, _, _ := marshalLCMWithEvent(tt, seq)
			return raw
		}
		return marshalLCM(tt, seq)
	}

	require.NoError(t, WriteColdChunk(
		context.Background(), logger, chunkID, rawChunk(fullStream(t, chunkID, gen), chunkID),
		coldDirsAt(coldDir, chunkID), nil, Config{Txhash: true},
	))

	entries, err := txhash.ReadColdBin(txhashBinPath(filepath.Join(coldDir, dataTypeTxhash)))
	require.NoError(t, err)
	require.Len(t, entries, len(txSeqs))
}

// TestWriteColdChunk_EventsCold_Readback runs the cold events driver over a chunk whose
// sentinel ledgers carry one event each and resolves the term post-Finalize.
func TestWriteColdChunk_EventsCold_Readback(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()
	logger := testLogger()

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

	require.NoError(t, WriteColdChunk(
		context.Background(), logger, chunkID, rawChunk(fullStream(t, chunkID, gen), chunkID),
		coldDirsAt(coldDir, chunkID), nil, Config{Events: true},
	))

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

// ───────────────────────── drain seq guard (P0-1) ─────────────────────────

// TestWriteColdChunk_OutOfOrderSeq_NoArtifact feeds a stream that yields a ledger out
// of expected order (the second ledger repeats the first's seq — right total
// count, wrong sequence). drain must reject it with the mismatch error before
// any Finalize, and leave no cold artifact behind.
func TestWriteColdChunk_OutOfOrderSeq_NoArtifact(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	last := chunkID.LastLedger()
	coldDir := t.TempDir()
	logger := testLogger()

	// Build a full-length seq list, then corrupt the second entry to a
	// duplicate of the first: same count as a valid stream, wrong order.
	seqs := make([]uint32, 0, last-first+1)
	for s := first; s <= last; s++ {
		seqs = append(seqs, s)
	}
	require.GreaterOrEqual(t, len(seqs), 2)
	seqs[1] = seqs[0] // duplicate/out-of-order while keeping the count intact

	stream := &seqStream{t: t, seqs: seqs}
	err := WriteColdChunk(
		context.Background(), logger, chunkID, rawChunk(stream, chunkID), coldDirsAt(coldDir, chunkID), nil, Config{Ledgers: true},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "yielded ledger")
	require.Contains(t, err.Error(), "expected")

	// No finalized artifact: the deferred Close dropped the partial pack.
	path := packPath(filepath.Join(coldDir, dataTypeLedgers), chunkID)
	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "expected no cold artifact at %s, stat err: %v", path, statErr)
}

// TestDrain_TxhashSeqGuard asserts the guard also fires on the txhash path,
// where a wrong-but-right-count sequence would otherwise be silently absorbed
// (each ledger keys on its own LCM seq).
func TestDrain_TxhashSeqGuard(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	last := chunkID.LastLedger()
	coldDir := t.TempDir()
	logger := testLogger()

	seqs := make([]uint32, 0, last-first+1)
	for s := first; s <= last; s++ {
		seqs = append(seqs, s)
	}
	require.GreaterOrEqual(t, len(seqs), 2)
	// Corrupt the SECOND ledger so at least one valid ledger is ingested
	// before the guard fires.
	seqs[1] += 100

	err := WriteColdChunk(
		context.Background(), logger, chunkID, rawChunk(&seqStream{t: t, seqs: seqs}, chunkID),
		coldDirsAt(coldDir, chunkID), nil, Config{Txhash: true},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "yielded ledger")

	binPath := txhashBinPath(filepath.Join(coldDir, dataTypeTxhash))
	_, statErr := os.Stat(binPath)
	require.True(t, os.IsNotExist(statErr), "expected no .bin at %s, stat err: %v", binPath, statErr)
}

// TestWriteColdChunk_DrainStreamError_NoArtifact exercises the drain mid-stream error
// path: the backend yields valid ledgers, then hands back (nil, err) at a seq in
// the middle of the chunk. drain must wrap the error with RawLedgers + the seq,
// short-circuit before Finalize (so no cold artifact is committed), and the
// deferred Close must drop the partial.
func TestWriteColdChunk_DrainStreamError_NoArtifact(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()
	logger := testLogger()

	failAt := first + 3
	wantErr := errors.New("induced mid-stream backend failure")
	stream := &errAtSeqStream{t: t, errAtSeq: failAt, err: wantErr}

	err := WriteColdChunk(
		context.Background(), logger, chunkID, rawChunk(stream, chunkID), coldDirsAt(coldDir, chunkID), nil, Config{Ledgers: true},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, wantErr, "the backend error must propagate")
	require.Contains(t, err.Error(), "RawLedgers", "error wraps RawLedgers")
	require.Contains(t, err.Error(), strconv.FormatUint(uint64(failAt), 10), "error names the failing seq")

	// Finalize never ran → no finalized artifact; deferred Close dropped the partial.
	path := packPath(filepath.Join(coldDir, dataTypeLedgers), chunkID)
	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "expected no cold artifact at %s, stat err: %v", path, statErr)
}

// The txhash .bin codec itself — atomic publish, create/rename failure
// cleanup, layout, and the reader round-trip — is owned and tested by
// pkg/stores/txhash (cold_bin_test.go); these tests only cover the
// ingester-level behavior on top of it.

// ───────────────────────── hot ingester failure path (P1-c) ─────────────────────────

// ───────────────────────── cold txhash .bin content (P1-d) ─────────────────────────

// TestTxhashColdIngester_BinContent ingests two tx-bearing ledgers, finalizes,
// then reads the .bin back through the store codec and asserts the contract
// the deferred streamhash builder relies on: each key == the fixture tx hash
// truncated to txhash.ColdKeySize (pinned to streamhash.MinKeySize by the
// codec), each seq == the ledger it was ingested in, and entries are in
// non-decreasing key order.
func TestTxhashColdIngester_BinContent(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()

	ing, err := NewTxhashColdIngester(txhashBinPath(coldDir), chunkID, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, ing.Close()) }()

	// Capture each fixture hash + the seq it was ingested in.
	wantSeqByKey := map[[txhash.ColdKeySize]byte]uint32{}
	for _, seq := range []uint32{first, first + 1} {
		raw, hash, _ := marshalLCMWithEvent(t, seq)
		var key [txhash.ColdKeySize]byte
		copy(key[:], hash[:txhash.ColdKeySize])
		wantSeqByKey[key] = seq
		require.NoError(t, ing.Ingest(context.Background(), seq, xdr.LedgerCloseMetaView(raw)))
	}
	require.NoError(t, ing.Finalize(context.Background()))

	entries, err := txhash.ReadColdBin(txhashBinPath(coldDir))
	require.NoError(t, err)
	require.Len(t, entries, 2)

	var prevKey [txhash.ColdKeySize]byte
	for i, e := range entries {
		wantSeq, known := wantSeqByKey[e.Key]
		require.True(t, known, "entry %d key %x is not one of the ingested fixture hashes", i, e.Key)
		require.Equal(t, wantSeq, e.Seq, "entry %d seq must equal the ledger it was ingested in", i)

		if i > 0 {
			require.LessOrEqual(t, bytes.Compare(prevKey[:], e.Key[:]), 0,
				"entries must be in non-decreasing key order")
		}
		prevKey = e.Key
	}
}

// ───────────────────────── canceled-context failure through the driver ─────────────────────────

// TestWriteColdChunk_CanceledContext asserts a worker that starts with an already
// canceled context (e.g. a sibling chunk failed and canceled the errgroup ctx)
// returns the cancellation error from the pre-build ctx check, and that the
// failed attempt still emits exactly one ColdChunkTotal.
func TestWriteColdChunk_CanceledContext(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	logger := testLogger()
	sink := &testSink{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rerr := WriteColdChunk(
		ctx, logger, chunkID, rawChunk(fullStream(t, chunkID, nil), chunkID),
		coldDirsAt(coldDir, chunkID), sink, Config{Ledgers: true},
	)
	require.ErrorIs(t, rerr, context.Canceled)
	require.Equal(t, 1, sink.coldChunkTotals, "a canceled chunk attempt still emits one ColdChunkTotal")
}

// ───────────────────────── Config validate / guard negatives (P2-g) ─────────────────────────

// TestWriteColdChunk_ConfigGuards covers the validate guard on the cold materializer:
// an empty Config (no data types enabled) is rejected. (The numChunks/chunkWorkers
// guards went away with the multi-chunk RunCold path.)
func TestWriteColdChunk_ConfigGuards(t *testing.T) {
	logger := testLogger()
	chunkID := chunk.ID(0)

	err := WriteColdChunk(context.Background(), logger, chunkID,
		rawChunk(fullStream(t, chunkID, nil), chunkID), coldDirsAt(t.TempDir(), chunkID), nil, Config{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "enables no data types")
}

// ───────────────────────── constructor-rollback metrics ─────────────────────────

// countCleanColdIngests counts recorded ColdIngest signals with a nil error.
func countCleanColdIngests(s *testSink) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.coldIngests {
		if c.err == nil {
			n++
		}
	}
	return n
}

// TestBuildColdIngesters_RollbackOneBuilt makes a LATER constructor (txhash) fail
// by planting a regular file at the txhash per-type directory, so the
// constructor's own MkdirAll fails. The earlier-built ledger ingester is rolled
// back via closeColdAll — which only closes it. Since Close no longer emits a
// per-ingester ColdIngest, a rolled-back ingester (built, never ingested or
// finalized) produces NO sample at all: no phantom success, no synthetic abort.
func TestBuildColdIngesters_RollbackOneBuilt(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	sink := &testSink{}

	// Plant a regular FILE where the txhash per-type directory must be
	// created: the ledger ingester builds first, then NewTxhashColdIngester
	// fails its bucket-dir MkdirAll.
	require.NoError(t, os.WriteFile(filepath.Join(coldDir, dataTypeTxhash), []byte("not a dir"), 0o644))

	_, err := buildColdIngesters(coldDirsAt(coldDir, chunkID), chunkID, sink, Config{Ledgers: true, Txhash: true})
	require.Error(t, err, "txhash constructor must fail on the planted file")

	// The ledger ingester was built then rolled back with no Ingest/Finalize, so
	// it emits nothing.
	require.Empty(t, sink.coldDataTypes(), "a rolled-back ingester emits no per-ingester ColdIngest")
}

// TestBuildColdIngesters_RollbackTwoBuilt makes the LAST constructor (events)
// fail AFTER both the ledger AND txhash ingesters were already built, so
// closeColdAll rolls back two ingesters. Same invariant at greater rollback
// depth: neither rolled-back ingester emits a per-ingester ColdIngest.
func TestBuildColdIngesters_RollbackTwoBuilt(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	sink := &testSink{}

	// Plant a directory at the events.pack path: the ledger and txhash
	// ingesters build first, then NewEventsColdIngester fails opening the
	// pack over the directory.
	packPath := filepath.Join(coldDir, dataTypeEvents, chunkID.BucketID(), eventstore.EventsPackName(chunkID))
	require.NoError(t, os.MkdirAll(packPath, 0o755))

	_, err := buildColdIngesters(coldDirsAt(coldDir, chunkID), chunkID, sink,
		Config{Ledgers: true, Txhash: true, Events: true})
	require.Error(t, err, "events constructor must fail on the planted directory")

	// Both the ledger and txhash ingesters were built then rolled back with no
	// Ingest/Finalize, so neither emits a per-ingester ColdIngest.
	require.Empty(t, sink.coldDataTypes(), "rolled-back ingesters emit no per-ingester ColdIngest")
}

// TestWriteColdChunk_ConstructorFailure_EmitsAggregate drives a constructor failure
// through WriteColdChunk (not buildColdIngesters directly) and asserts the chunk
// attempt still produces its single aggregate ColdChunkTotal — the invariant
// is one aggregate per chunk attempt, including pre-service failures.
func TestWriteColdChunk_ConstructorFailure_EmitsAggregate(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	logger := testLogger()
	sink := &testSink{}

	// Plant a regular file where the ledgers per-type subdir must be created.
	require.NoError(t, os.WriteFile(filepath.Join(coldDir, dataTypeLedgers), []byte("not a dir"), 0o644))

	err := WriteColdChunk(
		context.Background(), logger, chunkID, rawChunk(fullStream(t, chunkID, nil), chunkID),
		coldDirsAt(coldDir, chunkID), sink, Config{Ledgers: true},
	)
	require.Error(t, err)
	require.Equal(t, 1, sink.coldChunkTotals,
		"a constructor-failure chunk attempt still emits exactly one ColdChunkTotal")
	require.Zero(t, countCleanColdIngests(sink), "no clean ColdIngest on the rollback path")
}

// ───────────────────────── events Finish-then-WriteColdIndex failure ─────────────────────────

// TestEventsCold_FinishThenIndexFails_LeavesInertPack forces WriteColdIndex to
// fail AFTER writer.Finish has committed events.pack, by planting a directory
// where the index.hash file must be written (buildMPHF then hits EISDIR).
// Finalize must surface the error; the index-less events.pack stays on disk —
// without the orchestrator's completion record it is inert scratch (see the
// package doc's artifact model), and a retry's overwrite is the cleanup.
func TestEventsCold_FinishThenIndexFails_LeavesInertPack(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()

	ing, err := NewEventsColdIngester(filepath.Join(coldDir, chunkID.BucketID()), chunkID, nil)
	require.NoError(t, err)

	// Ingest one event-bearing ledger so the mirror is non-empty (an empty
	// build set would take the valid empty-index path instead of buildMPHF).
	rawEv, _, _ := marshalLCMWithEvent(t, first)
	require.NoError(t, ing.Ingest(context.Background(), first, xdr.LedgerCloseMetaView(rawEv)))

	// Plant a DIRECTORY where index.hash must be written → buildMPHF fails.
	bucketDir := filepath.Join(coldDir, chunkID.BucketID())
	indexHashPath := filepath.Join(bucketDir, eventstore.IndexHashName(chunkID))
	require.NoError(t, os.Mkdir(indexHashPath, 0o755))

	ferr := ing.Finalize(context.Background())
	require.Error(t, ferr, "Finalize must fail when WriteColdIndex fails")
	require.Contains(t, ferr.Error(), "WriteColdIndex")

	// The committed events.pack stays in place as inert scratch (Finish ran,
	// so the later Close does not drop it either).
	packPath := filepath.Join(bucketDir, eventstore.EventsPackName(chunkID))
	_, statErr := os.Stat(packPath)
	require.NoError(t, statErr, "the index-less events.pack stays on disk after WriteColdIndex failure")

	// Close is still safe/idempotent afterwards and does not remove the pack.
	require.NoError(t, ing.Close())
	_, statErr = os.Stat(packPath)
	require.NoError(t, statErr, "Close after a committed Finish must not drop the pack")
}

// TestEventsCold_FinalizeAfterFailedIngest_Refuses asserts the failed-Ingest
// latch: once an Ingest errors (here via a malformed view), Finalize must
// refuse rather than commit a pack+index whose mirror may be ahead of the
// offsets commit point.
func TestEventsCold_FinalizeAfterFailedIngest_Refuses(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()

	ing, err := NewEventsColdIngester(filepath.Join(coldDir, chunkID.BucketID()), chunkID, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, ing.Close()) }()

	bad := xdr.LedgerCloseMetaView([]byte{0x00, 0x01, 0x02})
	require.Error(t, ing.Ingest(context.Background(), chunkID.FirstLedger(), bad))

	ferr := ing.Finalize(context.Background())
	require.Error(t, ferr)
	require.Contains(t, ferr.Error(), "Finalize after failed Ingest")
}

// ───────────────────────── ColdService.Finalize first-error ─────────────────────────

// finalizeErrCold is a ColdIngester whose Finalize errors; it records whether
// Finalize/Close ran.
type finalizeErrCold struct {
	err       error
	finalized bool
	closed    bool
}

func (f *finalizeErrCold) Ingest(context.Context, uint32, xdr.LedgerCloseMetaView) error { return nil }
func (f *finalizeErrCold) Finalize(context.Context) error {
	f.finalized = true
	return f.err
}
func (f *finalizeErrCold) Close() error { f.closed = true; return nil }

// recordFinalizeCold is a ColdIngester that records it was finalized (no error).
type recordFinalizeCold struct {
	finalized bool
	closed    bool
}

func (r *recordFinalizeCold) Ingest(context.Context, uint32, xdr.LedgerCloseMetaView) error {
	return nil
}
func (r *recordFinalizeCold) Finalize(context.Context) error { r.finalized = true; return nil }
func (r *recordFinalizeCold) Close() error                   { r.closed = true; return nil }

// TestColdService_Finalize_FirstErrorStopsRemaining asserts ColdService.Finalize
// returns the first ingester's error and does NOT finalize (publish) the later
// ingesters — once a sibling failed, the rest are released (never finalized) by
// the caller's deferred Close, so a failed chunk never gains newly committed
// artifacts past the failure.
func TestColdService_Finalize_FirstErrorStopsRemaining(t *testing.T) {
	firstErr := errors.New("first finalize failure")
	failing := &finalizeErrCold{err: firstErr}
	later := &recordFinalizeCold{}

	service := NewColdService([]ColdIngester{failing, later}, &testSink{})
	ferr := service.Finalize(context.Background())

	require.ErrorIs(t, ferr, firstErr, "Finalize returns the first error")
	require.True(t, failing.finalized, "first ingester's Finalize ran (and failed)")
	require.False(t, later.finalized, "later ingester must NOT be finalized after a sibling failure")

	require.NoError(t, service.Close())
	require.True(t, later.closed, "later ingester is released via Close instead")
}

// ───────────────────────── drain overrun guard ─────────────────────────

// countingIngester counts Ingest calls; used to prove the overrun guard fires
// BEFORE the out-of-chunk ledger is handed to the ingesters.
type countingIngester struct{ ingested int }

func (c *countingIngester) Ingest(context.Context, uint32, xdr.LedgerCloseMetaView) error {
	c.ingested++
	return nil
}

// TestDrain_OverrunPastChunk asserts a stream that keeps yielding in order
// PAST the chunk's last ledger is rejected before the overrun ledger is
// ingested — not after the stream ends, by which point the extra ledgers
// would already be durably written on the hot path.
func TestDrain_OverrunPastChunk(t *testing.T) {
	chunkID := chunk.ID(0)
	ledgersInChunk := chunkID.LastLedger() - chunkID.FirstLedger() + 1
	// One ledger past the chunk, still in order.
	stream := &fakeStream{t: t, count: ledgersInChunk + 1}
	counter := &countingIngester{}

	err := drain(context.Background(), rawChunk(stream, chunkID), chunkID, counter)
	require.Error(t, err)
	require.Contains(t, err.Error(), "overrun")
	require.Equal(t, int(ledgersInChunk), counter.ingested,
		"the out-of-chunk ledger must not reach the ingesters")
}

// ───────────────────────── lazy-source / empty-stream failures ─────────────────────────

// lazyErrStream models a datastore-backed stream that is fully lazy: opening it
// succeeds (the SDK's buffered-storage stream is lazy), and the failure — bad
// config, missing objects, revoked credentials — only surfaces on the first
// RawLedgers pull.
type lazyErrStream struct{ err error }

func (s lazyErrStream) RawLedgers(
	context.Context, ledgerbackend.Range, ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		yield(nil, s.err)
	}
}

// TestWriteColdChunk_LazySourceFirstReadError covers the fully-lazy-source case: the
// iterator's first RawLedgers pull fails. The error surfaces from drain (after
// the cold ingesters were built); Finalize never runs, the deferred Close drops
// the ledger partial (Commit never ran), and the failed attempt still emits
// exactly one ColdChunkTotal.
func TestWriteColdChunk_LazySourceFirstReadError(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	logger := testLogger()
	sink := &testSink{}

	wantErr := errors.New("induced lazy-source failure (bad config / missing object)")
	err := WriteColdChunk(
		context.Background(), logger, chunkID, rawChunk(lazyErrStream{err: wantErr}, chunkID),
		coldDirsAt(coldDir, chunkID), sink, Config{Ledgers: true},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, wantErr)
	require.Contains(t, err.Error(), "RawLedgers", "the error surfaces from drain's stream pull")

	// Finalize never committed → no finalized pack (Close dropped the partial).
	path := packPath(filepath.Join(coldDir, dataTypeLedgers), chunkID)
	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "expected no cold pack at %s, stat err: %v", path, statErr)
	require.Equal(t, 1, sink.coldChunkTotals, "the failed attempt still emits its aggregate")
}

// TestWriteColdChunk_EmptyStream: a source whose stream yields nothing fails drain's
// post-loop completeness check, so Finalize never runs and the deferred Close
// drops the ledger partial — no finalized artifact is committed.
func TestWriteColdChunk_EmptyStream(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	logger := testLogger()
	sink := &testSink{}

	err := WriteColdChunk(
		context.Background(), logger, chunkID, rawChunk(&fakeStream{t: t, count: 0}, chunkID),
		coldDirsAt(coldDir, chunkID), sink, Config{Ledgers: true},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ended at", "the completeness check rejects the empty stream")

	path := packPath(filepath.Join(coldDir, dataTypeLedgers), chunkID)
	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "expected no cold pack at %s, stat err: %v", path, statErr)
	require.Equal(t, 1, sink.coldChunkTotals, "the failed attempt still emits its aggregate")
}

// TestColdService_FinalizeAbort_KeepsEarlierArtifact: when a LATER ingester's
// Finalize fails, the loop stops and the error is returned — but the artifact
// an earlier ingester already published in this attempt stays on disk. The
// orchestrator never records completion for the failed chunk, so the leftover
// is inert scratch (see the package doc's artifact model) and the retry's
// overwrite is the cleanup.
func TestColdService_FinalizeAbort_KeepsEarlierArtifact(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	sink := &testSink{}

	realLedger, err := NewLedgerColdIngester(packPath(filepath.Join(coldDir, dataTypeLedgers), chunkID), chunkID, sink)
	require.NoError(t, err)
	failErr := errors.New("induced finalize failure")
	failing := &finalizeErrCold{err: failErr}
	service := NewColdService([]ColdIngester{realLedger, failing}, sink)

	require.NoError(t, service.Ingest(context.Background(), chunkID.FirstLedger(), viewOf(t, chunkID.FirstLedger())))

	ferr := service.Finalize(context.Background())
	require.ErrorIs(t, ferr, failErr)
	require.True(t, failing.finalized, "the failing ingester's Finalize ran (and stopped the loop)")

	// The earlier ingester's already-published pack REMAINS on disk.
	path := packPath(filepath.Join(coldDir, dataTypeLedgers), chunkID)
	_, statErr := os.Stat(path)
	require.NoError(t, statErr, "the earlier ingester's published artifact stays on disk")

	require.NoError(t, service.Close())
	_, statErr = os.Stat(path)
	require.NoError(t, statErr, "Close after a committed Finalize must not drop the published pack")
}
