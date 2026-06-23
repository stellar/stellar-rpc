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
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/hotchunk"
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

// sourceOf wraps a single stream as a ChunkSource (every chunk gets it). For the
// cold driver this is safe in tests because each test uses one chunk.
func sourceOf(s ledgerbackend.LedgerStream) ChunkSource {
	return ChunkSourceFunc(func(chunk.ID) (ledgerbackend.LedgerStream, error) { return s, nil })
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

// TestLedgerHotIngester_Readback ingests one ledger via the hot ledger ingester
// (injected store) and reads the bytes back.
func TestLedgerHotIngester_Readback(t *testing.T) {
	seq := chunk.ID(0).FirstLedger()
	raw := marshalLCM(t, seq)
	dir := t.TempDir()
	logger := testLogger()

	store, err := ledger.OpenHotStore(dir, chunk.ID(0), logger)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	ing := NewLedgerHotIngester(store, nil)
	require.NoError(t, ing.Ingest(context.Background(), seq, xdr.LedgerCloseMetaView(raw)))

	got, err := store.GetLedgerRaw(seq)
	require.NoError(t, err)
	require.Equal(t, raw, got)
}

// TestTxhashHotIngester_Lookup ingests an event/tx-bearing ledger via the hot
// txhash ingester and looks the hash up.
func TestTxhashHotIngester_Lookup(t *testing.T) {
	seq := chunk.ID(0).FirstLedger()
	raw, hash, _ := marshalLCMWithEvent(t, seq)
	dir := t.TempDir()
	logger := testLogger()

	store, err := txhash.NewHotStore(dir, chunk.ID(0), logger)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	ing := NewTxhashHotIngester(store, nil)
	require.NoError(t, ing.Ingest(context.Background(), seq, xdr.LedgerCloseMetaView(raw)))

	got, err := store.Get(hash)
	require.NoError(t, err)
	require.Equal(t, seq, got)
}

// TestEventsHotIngester_Query ingests an event-bearing ledger via the hot events
// ingester and resolves the term.
func TestEventsHotIngester_Query(t *testing.T) {
	chunkID := chunk.ID(0)
	seq := chunkID.FirstLedger()
	raw, _, term := marshalLCMWithEvent(t, seq)
	dir := t.TempDir()
	logger := testLogger()

	store, err := eventstore.OpenHotStore(dir, chunkID, logger)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	ing := NewEventsHotIngester(store, nil)
	require.NoError(t, ing.Ingest(context.Background(), seq, xdr.LedgerCloseMetaView(raw)))

	bm, err := store.Lookup(context.Background(), term)
	require.NoError(t, err)
	require.NotNil(t, bm)
	require.Equal(t, uint64(1), bm.GetCardinality())
}

// TestLedgerColdIngester_Readback ingests one ledger via the cold ledger
// ingester, finalizes, and reads back through the cold reader.
func TestLedgerColdIngester_Readback(t *testing.T) {
	chunkID := chunk.ID(0)
	seq := chunkID.FirstLedger()
	raw := marshalLCM(t, seq)
	coldDir := t.TempDir()

	ing, err := NewLedgerColdIngester(coldDir, chunkID, nil)
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

	ing, err := NewTxhashColdIngester(coldDir, chunkID, nil)
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

	ing, err := NewEventsColdIngester(coldDir, chunkID, nil)
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

// TestEventsHotIngester_V0AsEmpty asserts the hot events ingester treats a V0
// LCM as a zero-event ledger (no error) rather than failing the range, and that
// the store records the empty ledger (its event count is unchanged).
func TestEventsHotIngester_V0AsEmpty(t *testing.T) {
	chunkID := chunk.ID(0)
	seq := chunkID.FirstLedger()
	dir := t.TempDir()
	logger := testLogger()

	store, err := eventstore.OpenHotStore(dir, chunkID, logger)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	ing := NewEventsHotIngester(store, nil)
	require.NoError(t, ing.Ingest(context.Background(), seq, xdr.LedgerCloseMetaView(marshalV0LCM(t, seq))),
		"V0 ledger must ingest as zero events, not error")

	cnt, err := store.EventCount()
	require.NoError(t, err)
	require.Equal(t, uint32(0), cnt, "V0 ledger contributes no events")
}

// TestEventsColdIngester_V0KeepsOffsetsContiguous ingests a V0 ledger followed by
// an event-bearing V2 ledger and asserts: the V0 ledger does not error, and the
// LedgerOffsets stay contiguous (both ledgers present, the event-bearing one's
// single event ID immediately follows the empty V0 ledger).
func TestEventsColdIngester_V0KeepsOffsetsContiguous(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()

	ing, err := NewEventsColdIngester(coldDir, chunkID, nil)
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

// TestRunCold_EventlessChunk_FullyReadable drives a full cold chunk of V0
// (pre-Soroban, eventless) ledgers with Events enabled — the common backfill
// case for early history. The whole chunk has zero contract events;
// eventstore.WriteColdIndex publishes a valid EMPTY index for it, so all
// three cold artifacts exist and the chunk is fully readable: a term-filtered
// Lookup resolves to "no matches" through the ordinary path instead of a
// missing-file error.
func TestRunCold_EventlessChunk_FullyReadable(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	logger := testLogger()
	sink := &testSink{}

	// Every ledger in the chunk is a V0 (pre-Soroban) ledger → zero events.
	require.NoError(t, RunCold(
		context.Background(), logger, sourceOf(fullStream(t, chunkID, marshalV0LCM)),
		coldDir, chunkID, 1, 1, sink, Config{Events: true},
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

// ───────────────────────── HotService tests ─────────────────────────

// TestHotService_AllTypes_OneAtomicBatch runs HotService over the SHARED
// multi-CF hot DB (decision (a)) for event/tx-bearing ledgers and reads each CF
// back through the DB's facades, asserting the aggregate HotLedgerTotal and the
// per-type HotIngest signals fired. Each ledger committed as ONE atomic synced
// WriteBatch across all CFs.
func TestHotService_AllTypes_OneAtomicBatch(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	logger := testLogger()

	db, err := hotchunk.Open(t.TempDir(), chunkID, logger)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	sink := &testSink{}
	service := NewHotService(db, hotchunk.Ingest{Ledgers: true, Txhash: true, Events: true}, sink)

	rawA, hashA, termA := marshalLCMWithEvent(t, first)
	rawB, hashB, _ := marshalLCMWithEvent(t, first+1)
	require.NoError(t, service.Ingest(context.Background(), first, xdr.LedgerCloseMetaView(rawA)))
	require.NoError(t, service.Ingest(context.Background(), first+1, xdr.LedgerCloseMetaView(rawB)))

	// Every CF retained the data (read through the shared DB's facades).
	gotRawA, err := db.Ledgers().GetLedgerRaw(first)
	require.NoError(t, err)
	require.Equal(t, rawA, gotRawA)
	gotA, err := db.Txhash().Get(hashA)
	require.NoError(t, err)
	require.Equal(t, first, gotA)
	gotB, err := db.Txhash().Get(hashB)
	require.NoError(t, err)
	require.Equal(t, first+1, gotB)
	bm, err := db.Events().Lookup(context.Background(), termA)
	require.NoError(t, err)
	require.Equal(t, uint64(2), bm.GetCardinality())

	// The single watermark advanced to the last committed ledger (every CF in
	// lockstep, decision (a)).
	maxSeq, ok, err := db.MaxCommittedSeq()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, first+1, maxSeq)

	// Aggregate + per-type signals.
	require.Equal(t, 2, sink.hotLedgerTotals, "one HotLedgerTotal per ledger")
	dt := sink.hotDataTypes()
	require.Equal(t, 2, dt[dataTypeLedgers])
	require.Equal(t, 2, dt[dataTypeTxhash])
	require.Equal(t, 2, dt[dataTypeEvents])
}

// TestHotService_EnabledSubset runs HotService with only ledgers enabled and
// asserts only that type's signal fires (txhash/events CFs untouched).
func TestHotService_EnabledSubset(t *testing.T) {
	seq := chunk.ID(0).FirstLedger()
	logger := testLogger()

	db, err := hotchunk.Open(t.TempDir(), chunk.ID(0), logger)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	sink := &testSink{}
	service := NewHotService(db, hotchunk.Ingest{Ledgers: true}, sink)
	require.NoError(t, service.Ingest(context.Background(), seq, viewOf(t, seq)))

	require.Equal(t, 1, sink.hotLedgerTotals)
	dt := sink.hotDataTypes()
	require.Equal(t, 1, dt[dataTypeLedgers])
	require.Zero(t, dt[dataTypeTxhash])
	require.Zero(t, dt[dataTypeEvents])
}

// ───────────────────────── ColdService tests ─────────────────────────

// TestColdService_Success drives ledger+txhash+events cold ingesters through a
// ColdService and asserts readback plus the metrics signals.
func TestColdService_Success(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()
	sink := &testSink{}

	ings, err := buildColdIngesters(coldDir, chunkID, sink, Config{Ledgers: true, Txhash: true, Events: true})
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

// TestColdService_FailurePath_NoArtifact uses a real ledger cold ingester plus a
// failing sibling: ColdService.Ingest returns the sibling's error, Finalize is
// not called, the deferred Close drops the partial ledger pack, and no finalized
// artifact remains. It also asserts the cold metrics still fire on this failure
// path: each real ingester emits exactly one ColdIngest and the service emits one
// aggregate ColdChunkTotal — driven from Close, since Finalize never ran.
func TestColdService_FailurePath_NoArtifact(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	sink := &testSink{}

	// Two real cold ingesters (ledger + events) plus a failing sibling, so we can
	// assert each real ingester emits its per-chunk ColdIngest from Close.
	realLedger, err := NewLedgerColdIngester(filepath.Join(coldDir, dataTypeLedgers), chunkID, sink)
	require.NoError(t, err)
	realEvents, err := NewEventsColdIngester(filepath.Join(coldDir, dataTypeEvents), chunkID, sink)
	require.NoError(t, err)
	failing := &failingCold{}
	service := NewColdService([]ColdIngester{realLedger, realEvents, failing}, sink)

	// First ledger: the real ingesters succeed, failing returns an error → the
	// sequential Ingest aborts the ledger with the sibling's error.
	err = service.Ingest(context.Background(), chunkID.FirstLedger(), viewOf(t, chunkID.FirstLedger()))
	require.ErrorIs(t, err, errFailingCold)
	require.False(t, failing.finalized, "Finalize must not run on the failure path")

	// Before Close, no cold metric has fired (emission is deferred to Close on the
	// failure path).
	require.Empty(t, sink.coldDataTypes(), "no ColdIngest before Close on failure path")
	require.Zero(t, sink.coldChunkTotals, "no ColdChunkTotal before Close on failure path")

	// Close drops partials and drives the deferred metric emissions.
	require.NoError(t, service.Close())
	require.True(t, failing.closed)

	// Each real ingester emitted exactly one ColdIngest; the aggregate fired once.
	cdt := sink.coldDataTypes()
	require.Equal(t, 1, cdt[dataTypeLedgers], "ledger cold ingester emits once on failure path")
	require.Equal(t, 1, cdt[dataTypeEvents], "events cold ingester emits once on failure path")
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
// a per-ingester error count and an aggregate duration sample. Emission happens
// exactly once (from Close), with the accumulated error carried.
func TestColdIngester_Failure_RecordsErrorMetric(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	sink := &testSink{}

	realLedger, err := NewLedgerColdIngester(filepath.Join(coldDir, dataTypeLedgers), chunkID, sink)
	require.NoError(t, err)
	service := NewColdService([]ColdIngester{realLedger}, sink)

	// An out-of-order seq makes the writer's own AppendLedger fail inside the
	// ingester's Ingest, so it records its firstErr. (drain would never feed
	// this — the test targets the ingester's metric path directly.)
	wrongSeq := chunkID.FirstLedger() + 5
	require.Error(t, service.Ingest(context.Background(), wrongSeq, viewOf(t, wrongSeq)))

	// Finalize is skipped on this path; Close drives the single emission.
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

// ───────────────────────── hot driver tests ─────────────────────────

// TestRunHot_AllTypes_Readback runs the RunHot driver with the injected SHARED
// hot DB (decision (a)) over event/tx-bearing ledgers and asserts every CF
// reads back. The short stream ends early so RunHot returns the completeness
// error after both ledgers are fully ingested.
func TestRunHot_AllTypes_Readback(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	logger := testLogger()

	db, err := hotchunk.Open(t.TempDir(), chunkID, logger)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

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

	stores := HotStores{HotDB: db}
	cfg := Config{Ledgers: true, Txhash: true, Events: true}

	err = RunHot(context.Background(), logger, sourceOf(stream), chunkID, stores, nil, cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ended at")

	gotRawA, err := db.Ledgers().GetLedgerRaw(evSeqA)
	require.NoError(t, err)
	require.Equal(t, rawA, gotRawA)

	gotA, err := db.Txhash().Get(hashA)
	require.NoError(t, err)
	require.Equal(t, evSeqA, gotA)
	gotB, err := db.Txhash().Get(hashB)
	require.NoError(t, err)
	require.Equal(t, evSeqB, gotB)

	bm, err := db.Events().Lookup(context.Background(), termA)
	require.NoError(t, err)
	require.NotNil(t, bm)
	require.Equal(t, uint64(2), bm.GetCardinality(), "both sentinel events share the term")
}

// TestRunHot_MissingStore asserts RunHot rejects an enabled type with a nil
// injected shared hot DB.
func TestRunHot_MissingStore(t *testing.T) {
	chunkID := chunk.ID(0)
	logger := testLogger()
	err := RunHot(context.Background(), logger, sourceOf(&fakeStream{t: t, count: 1}), chunkID,
		HotStores{}, nil, Config{Ledgers: true})
	require.Error(t, err)
	require.Contains(t, err.Error(), "HotStores.HotDB is nil")
}

// TestPackSource_RoundTrip exercises the production PackSource + packStream path
// end-to-end against a REAL cold ledger packfile, re-ingesting it via
// RunCold(NewPackSource(...)) and asserting the boundary bytes round-trip.
func TestPackSource_RoundTrip(t *testing.T) {
	chunkID := chunk.ID(0)
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()

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

	src := NewPackSource(srcDir)
	dstDir := t.TempDir()
	logger := testLogger()
	require.NoError(t, RunCold(
		context.Background(), logger, src, dstDir, chunkID, 1, 1, nil, Config{Ledgers: true},
	))

	cr, err := ledger.OpenColdReader(packPath(filepath.Join(dstDir, "ledgers"), chunkID))
	require.NoError(t, err)
	defer func() { require.NoError(t, cr.Close()) }()
	rawFirst, err := cr.GetLedgerRaw(first)
	require.NoError(t, err)
	require.Equal(t, marshalLCM(t, first), rawFirst)
	rawLast, err := cr.GetLedgerRaw(last)
	require.NoError(t, err)
	require.Equal(t, marshalLCM(t, last), rawLast)

	_, missErr := src.OpenStream(chunkID + 1)
	require.Error(t, missErr)
	require.Contains(t, missErr.Error(), "cold pack missing")
}

// TestPackStream_ObservesCtxCancellation pins the ChunkSource cancellation
// contract on the in-repo pack stream: once ctx is canceled, RawLedgers must
// yield the cancellation error instead of streaming on — drain relies on the
// stream for cancellation and does not poll ctx itself.
func TestPackStream_ObservesCtxCancellation(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()

	srcDir := t.TempDir()
	packFile := packPath(srcDir, chunkID)
	require.NoError(t, os.MkdirAll(filepath.Dir(packFile), 0o755))
	cw, err := ledger.NewColdWriter(packFile, first, ledger.ColdWriterOptions{})
	require.NoError(t, err)
	for seq := first; seq < first+3; seq++ {
		require.NoError(t, cw.AppendLedger(seq, marshalLCM(t, seq)))
	}
	require.NoError(t, cw.Commit())
	require.NoError(t, cw.Close())

	stream, err := NewPackSource(srcDir).OpenStream(chunkID)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	yielded := 0
	var gotErr error
	for _, serr := range stream.RawLedgers(ctx, ledgerbackend.BoundedRange(first, first+2)) {
		if serr != nil {
			gotErr = serr
			break
		}
		yielded++
		cancel() // cancel after the first ledger; the next step must error
	}
	require.ErrorIs(t, gotErr, context.Canceled)
	require.Equal(t, 1, yielded, "no ledger may be yielded after cancellation")
}

// ───────────────────────── cold driver tests ─────────────────────────

func TestRunCold_RoundTrip(t *testing.T) {
	chunkID := chunk.ID(0)
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
	stream := fullStream(t, chunkID, nil)

	coldDir := t.TempDir()
	logger := testLogger()
	sink := &testSink{}

	require.NoError(t, RunCold(
		context.Background(), logger, sourceOf(stream), coldDir, chunkID, 1, 1, sink, Config{Ledgers: true},
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

// TestRunCold_ShortStream_NoArtifact verifies a short stream makes RunCold error
// AND never produces a finalized cold artifact (completeness runs before
// Finalize, deferred Close drops the partial).
func TestRunCold_ShortStream_NoArtifact(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	logger := testLogger()

	short := &fakeStream{t: t, count: 3}
	err := RunCold(
		context.Background(), logger, sourceOf(short), coldDir, chunkID, 1, 1, nil, Config{Ledgers: true},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ended at")

	path := packPath(filepath.Join(coldDir, "ledgers"), chunkID)
	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "expected no cold artifact at %s, stat err: %v", path, statErr)
}

// customSource is a tiny in-test ChunkSource over in-memory LCMs, demonstrating
// that adding a backend requires only implementing the interface.
type customSource struct {
	t   *testing.T
	gen func(*testing.T, uint32) []byte
}

func (c customSource) OpenStream(chunkID chunk.ID) (ledgerbackend.LedgerStream, error) {
	return fullStream(c.t, chunkID, c.gen), nil
}

// TestRunCold_CustomSource_Extensibility runs the cold driver against a
// caller-defined ChunkSource and asserts success + readback.
func TestRunCold_CustomSource_Extensibility(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()
	logger := testLogger()

	src := customSource{t: t}
	require.NoError(t, RunCold(
		context.Background(), logger, src, coldDir, chunkID, 1, 1, nil, Config{Ledgers: true},
	))

	cr, err := ledger.OpenColdReader(packPath(filepath.Join(coldDir, "ledgers"), chunkID))
	require.NoError(t, err)
	defer func() { require.NoError(t, cr.Close()) }()
	raw, err := cr.GetLedgerRaw(first)
	require.NoError(t, err)
	require.Equal(t, marshalLCM(t, first), raw)
}

// TestRunCold_TxhashCold_Bin runs the cold txhash driver over a chunk whose
// sentinel ledgers carry one tx each and asserts the .bin entry count.
func TestRunCold_TxhashCold_Bin(t *testing.T) {
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

	require.NoError(t, RunCold(
		context.Background(), logger, customSource{t: t, gen: gen}, coldDir, chunkID, 1, 1, nil, Config{Txhash: true},
	))

	entries, err := txhash.ReadColdBin(txhashBinPath(filepath.Join(coldDir, dataTypeTxhash)))
	require.NoError(t, err)
	require.Len(t, entries, len(txSeqs))
}

// TestRunCold_EventsCold_Readback runs the cold events driver over a chunk whose
// sentinel ledgers carry one event each and resolves the term post-Finalize.
func TestRunCold_EventsCold_Readback(t *testing.T) {
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

	require.NoError(t, RunCold(
		context.Background(), logger, customSource{t: t, gen: gen}, coldDir, chunkID, 1, 1, nil, Config{Events: true},
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

// TestRunCold_OutOfOrderSeq_NoArtifact feeds a stream that yields a ledger out
// of expected order (the second ledger repeats the first's seq — right total
// count, wrong sequence). drain must reject it with the mismatch error before
// any Finalize, and leave no cold artifact behind.
func TestRunCold_OutOfOrderSeq_NoArtifact(t *testing.T) {
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
	err := RunCold(
		context.Background(), logger, sourceOf(stream), coldDir, chunkID, 1, 1, nil, Config{Ledgers: true},
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

	err := RunCold(
		context.Background(), logger, sourceOf(&seqStream{t: t, seqs: seqs}), coldDir, chunkID, 1, 1, nil,
		Config{Txhash: true},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "yielded ledger")

	binPath := txhashBinPath(filepath.Join(coldDir, dataTypeTxhash))
	_, statErr := os.Stat(binPath)
	require.True(t, os.IsNotExist(statErr), "expected no .bin at %s, stat err: %v", binPath, statErr)
}

// TestRunCold_DrainStreamError_NoArtifact exercises the drain mid-stream error
// path: the backend yields valid ledgers, then hands back (nil, err) at a seq in
// the middle of the chunk. drain must wrap the error with RawLedgers + the seq,
// short-circuit before Finalize (so no cold artifact is committed), and the
// deferred Close must drop the partial.
func TestRunCold_DrainStreamError_NoArtifact(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()
	logger := testLogger()

	failAt := first + 3
	wantErr := errors.New("induced mid-stream backend failure")
	stream := &errAtSeqStream{t: t, errAtSeq: failAt, err: wantErr}

	err := RunCold(
		context.Background(), logger, sourceOf(stream), coldDir, chunkID, 1, 1, nil, Config{Ledgers: true},
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

// ───────────────────────── HotService failure path (P1-c) ─────────────────────────

// TestHotService_IngestFailureStillEmitsTotal asserts a failed shared-DB ingest
// (here: a closed DB) returns the error and still emits exactly one
// HotLedgerTotal. Under decision (a) there is no fan-out to cancel — one atomic
// batch either commits or returns its error — so a single failure path replaces
// the old errgroup sibling-cancellation behavior.
func TestHotService_IngestFailureStillEmitsTotal(t *testing.T) {
	logger := testLogger()
	db, err := hotchunk.Open(t.TempDir(), chunk.ID(0), logger)
	require.NoError(t, err)
	require.NoError(t, db.Close()) // closed DB makes IngestLedger fail

	sink := &testSink{}
	service := NewHotService(db, hotchunk.Ingest{Ledgers: true, Txhash: true, Events: true}, sink)

	err = service.Ingest(context.Background(), chunk.ID(0).FirstLedger(), viewOf(t, chunk.ID(0).FirstLedger()))
	require.Error(t, err)
	require.Equal(t, 1, sink.hotLedgerTotals, "HotLedgerTotal fires exactly once even on failure")
}

// TestHotIngester_Failure_RecordsErrorMetric drives a REAL hot ingester
// (eventsHot, built via NewEventsHotIngester) with a malformed view so its own
// Ingest fails through the production hotMetrics emit path — unlike the
// failingHot/blockingHot stubs, which bypass hotMetrics entirely. Per #765 a
// failed hot Ingest must record exactly one HotIngest carrying a non-nil error
// for that data type. Mirrors the cold-side TestColdIngester_Failure_RecordsErrorMetric.
func TestHotIngester_Failure_RecordsErrorMetric(t *testing.T) {
	chunkID := chunk.ID(0)
	logger := testLogger()
	dir := t.TempDir()
	sink := &testSink{}

	store, err := eventstore.OpenHotStore(dir, chunkID, logger)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	ing := NewEventsHotIngester(store, sink)

	// A truncated/garbage view makes the event extraction fail inside the real
	// Ingest, so the deferred hotMetrics.emit reports the wrapped error.
	bad := xdr.LedgerCloseMetaView([]byte{0x00, 0x01, 0x02})
	require.Error(t, ing.Ingest(context.Background(), chunkID.FirstLedger(), bad))

	sink.mu.Lock()
	defer sink.mu.Unlock()
	require.Len(t, sink.hotIngests, 1, "exactly one HotIngest recorded")
	require.Equal(t, dataTypeEvents, sink.hotIngests[0].dataType)
	require.Error(t, sink.hotIngests[0].err, "the recorded HotIngest carries the ingest error")
}

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

	ing, err := NewTxhashColdIngester(coldDir, chunkID, nil)
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

// ───────────────────────── OpenStream failure through the driver (P1-e) ─────────────────────────

var errOpenStream = errors.New("induced OpenStream failure")

// erroringSource is a ChunkSource whose OpenStream always fails.
type erroringSource struct{}

func (erroringSource) OpenStream(chunk.ID) (ledgerbackend.LedgerStream, error) {
	return nil, errOpenStream
}

// TestRunCold_OpenStreamError wraps the open error with the chunk index, emits
// exactly one ColdChunkTotal (directly on the pre-build failure path, since the
// ColdService that normally owns it is not built when OpenStream fails), and
// leaves no pack.
func TestRunCold_OpenStreamError(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	logger := testLogger()
	sink := &testSink{}

	err := RunCold(
		context.Background(), logger, erroringSource{}, coldDir, chunkID, 1, 1, sink, Config{Ledgers: true},
	)
	require.ErrorIs(t, err, errOpenStream)
	require.Contains(t, err.Error(), "chunk 0:")

	require.Equal(t, 1, sink.coldChunkTotals, "exactly one ColdChunkTotal even when OpenStream fails")

	path := packPath(filepath.Join(coldDir, dataTypeLedgers), chunkID)
	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "expected no cold pack at %s, stat err: %v", path, statErr)
}

// TestRunCold_CanceledContext asserts a worker that starts with an already
// canceled context (e.g. a sibling chunk failed and canceled the errgroup ctx)
// returns the cancellation error from the pre-build ctx check, and that the
// failed attempt still emits exactly one ColdChunkTotal.
func TestRunCold_CanceledContext(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	logger := testLogger()
	sink := &testSink{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rerr := RunCold(
		ctx, logger, customSource{t: t}, coldDir, chunkID, 1, 1, sink, Config{Ledgers: true},
	)
	require.ErrorIs(t, rerr, context.Canceled)
	require.Equal(t, 1, sink.coldChunkTotals, "a canceled chunk attempt still emits one ColdChunkTotal")
}

// TestRunHot_OpenStreamError asserts RunHot surfaces the open error wrapped with
// the chunk index.
func TestRunHot_OpenStreamError(t *testing.T) {
	chunkID := chunk.ID(0)
	logger := testLogger()

	db, err := hotchunk.Open(t.TempDir(), chunkID, logger)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	err = RunHot(context.Background(), logger, erroringSource{}, chunkID,
		HotStores{HotDB: db}, nil, Config{Ledgers: true})
	require.ErrorIs(t, err, errOpenStream)
	require.Contains(t, err.Error(), "open stream for chunk 0")
}

// ───────────────────────── RunHot chunkID cross-check (P2-e) ─────────────────────────

// TestRunHot_ChunkIDMismatch asserts RunHot rejects an injected shared hot DB
// bound to a different chunk than the one being ingested, with a clear up-front
// error (rather than silently interleaving two chunks' data into one DB, or a
// later per-ledger out-of-range on the events CF). The shared DB is chunk-bound
// (decision (a)).
func TestRunHot_ChunkIDMismatch(t *testing.T) {
	ingestChunk := chunk.ID(1)
	storeChunk := chunk.ID(0)
	logger := testLogger()

	db, err := hotchunk.Open(t.TempDir(), storeChunk, logger)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	err = RunHot(context.Background(), logger, sourceOf(&fakeStream{t: t, count: 1}), ingestChunk,
		HotStores{HotDB: db}, nil, Config{Ledgers: true, Txhash: true, Events: true})
	require.Error(t, err)
	require.Contains(t, err.Error(), "bound to chunk 0")
	require.Contains(t, err.Error(), "RunHot chunk 1")
}

// ───────────────────────── Config validate / guard negatives (P2-g) ─────────────────────────

// TestRunCold_ConfigGuards covers the validate + numeric guards on the cold
// driver: empty Config, numChunks<1, chunkWorkers<1.
func TestRunCold_ConfigGuards(t *testing.T) {
	logger := testLogger()
	chunkID := chunk.ID(0)
	src := customSource{t: t}

	t.Run("empty-config", func(t *testing.T) {
		err := RunCold(context.Background(), logger, src, t.TempDir(), chunkID, 1, 1, nil, Config{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "enables no data types")
	})
	t.Run("zero-numChunks", func(t *testing.T) {
		err := RunCold(context.Background(), logger, src, t.TempDir(), chunkID, 0, 1, nil, Config{Ledgers: true})
		require.Error(t, err)
		require.Contains(t, err.Error(), "numChunks must be >= 1")
	})
	t.Run("zero-chunkWorkers", func(t *testing.T) {
		err := RunCold(context.Background(), logger, src, t.TempDir(), chunkID, 1, 0, nil, Config{Ledgers: true})
		require.Error(t, err)
		require.Contains(t, err.Error(), "chunkWorkers must be >= 1")
	})
}

// TestRunHot_EmptyConfig asserts the hot driver also rejects an empty Config.
func TestRunHot_EmptyConfig(t *testing.T) {
	err := RunHot(context.Background(), testLogger(), sourceOf(&fakeStream{t: t, count: 1}),
		chunk.ID(0), HotStores{}, nil, Config{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "enables no data types")
}

// ───────────────────────── multi-chunk RunCold (P2-g) ─────────────────────────

// TestRunCold_MultiChunk_OneFailing runs three chunks with two workers: two
// chunks have full streams and read back, while one chunk's source yields a
// short stream so RunCold returns an error naming that chunk index.
func TestRunCold_MultiChunk_OneFailing(t *testing.T) {
	startChunk := chunk.ID(0)
	const numChunks = 3
	badChunk := startChunk + 1
	coldDir := t.TempDir()
	logger := testLogger()

	src := ChunkSourceFunc(func(id chunk.ID) (ledgerbackend.LedgerStream, error) {
		if id == badChunk {
			// Short stream → completeness check fails for this chunk only.
			return &fakeStream{t: t, count: 1}, nil
		}
		return fullStream(t, id, nil), nil
	})

	err := RunCold(
		context.Background(), logger, src, coldDir, startChunk, numChunks, 2, nil, Config{Ledgers: true},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "chunk 1:", "error must name the failing chunk index")

	// The failing chunk must leave no pack. (The "good" chunks' packs are not
	// read back here: the errgroup cancels in-flight siblings on the first error,
	// so whether a given good chunk finalized is racy and not asserted — the
	// all-success fan-out below verifies concurrent readback deterministically.)
	badPath := packPath(filepath.Join(coldDir, dataTypeLedgers), badChunk)
	_, statErr := os.Stat(badPath)
	require.True(t, os.IsNotExist(statErr), "failing chunk must leave no pack")
}

// TestRunCold_MultiChunk_AllSuccess_ConcurrentReadback runs three chunks with two
// workers, all with full streams, and reads back EVERY chunk's ledger pack to
// verify the concurrent fan-out actually finalizes each chunk's artifact.
func TestRunCold_MultiChunk_AllSuccess_ConcurrentReadback(t *testing.T) {
	startChunk := chunk.ID(0)
	const numChunks = 3
	coldDir := t.TempDir()
	logger := testLogger()
	sink := &testSink{}

	src := ChunkSourceFunc(func(id chunk.ID) (ledgerbackend.LedgerStream, error) {
		return fullStream(t, id, nil), nil
	})

	require.NoError(t, RunCold(
		context.Background(), logger, src, coldDir, startChunk, numChunks, 2, sink, Config{Ledgers: true},
	))

	// Every chunk's pack must be readable at its boundaries.
	for i := range numChunks {
		chunkID := startChunk + chunk.ID(uint32(i))
		first, last := chunkID.FirstLedger(), chunkID.LastLedger()
		cr, err := ledger.OpenColdReader(packPath(filepath.Join(coldDir, dataTypeLedgers), chunkID))
		require.NoError(t, err, "chunk %d pack must be readable", uint32(chunkID))
		rawFirst, err := cr.GetLedgerRaw(first)
		require.NoError(t, err)
		require.Equal(t, marshalLCM(t, first), rawFirst)
		rawLast, err := cr.GetLedgerRaw(last)
		require.NoError(t, err)
		require.Equal(t, marshalLCM(t, last), rawLast)
		require.NoError(t, cr.Close())
	}

	require.Equal(t, numChunks, sink.coldChunkTotals, "one ColdChunkTotal per chunk")
	require.Equal(t, numChunks, sink.coldDataTypes()[dataTypeLedgers])
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

// TestBuildColdIngesters_RollbackNoPhantomMetric makes a LATER constructor
// (txhash) fail by planting a regular file at the txhash per-type directory,
// so the constructor's own MkdirAll fails. The earlier-built ledger ingester
// is rolled back via closeColdAll, which must NOT emit a phantom success
// ColdIngest — the recorded ledger metric (if any) must carry the abort
// error, never a clean (nil-err, 0-items) success.
func TestBuildColdIngesters_RollbackNoPhantomMetric(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	sink := &testSink{}

	// Plant a regular FILE where the txhash per-type directory must be
	// created: the ledger ingester builds first, then NewTxhashColdIngester
	// fails its bucket-dir MkdirAll.
	require.NoError(t, os.WriteFile(filepath.Join(coldDir, dataTypeTxhash), []byte("not a dir"), 0o644))

	_, err := buildColdIngesters(coldDir, chunkID, sink, Config{Ledgers: true, Txhash: true})
	require.Error(t, err, "txhash constructor must fail on the planted file")

	// The ledger ingester was built then rolled back. No phantom SUCCESS metric:
	// any recorded ledger ColdIngest must carry an error.
	cdt := sink.coldDataTypes()
	if cdt[dataTypeLedgers] > 0 {
		require.Equal(t, cdt[dataTypeLedgers], sink.coldErrorTypes()[dataTypeLedgers],
			"rolled-back ledger ingester must not emit a phantom success ColdIngest")
	}
	// And the success-only assertion: there must be zero clean (nil-err) cold
	// ingest signals recorded.
	require.Zero(t, countCleanColdIngests(sink), "no clean ColdIngest on the rollback path")
}

// TestBuildColdIngesters_RollbackLaterFailure_TxhashAborts makes the LAST
// constructor (events) fail AFTER both the ledger AND txhash ingesters were
// already built, so closeColdAll rolls back two ingesters. It asserts the txhash
// ingester (which DOES implement abortMetric) emits an error-carrying — not a
// clean-success — ColdIngest, complementing the ledger-only abort coverage above.
func TestBuildColdIngesters_RollbackLaterFailure_TxhashAborts(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	sink := &testSink{}

	// Plant a directory at the events.pack path: the ledger and txhash
	// ingesters build first, then NewEventsColdIngester fails opening the
	// pack over the directory.
	packPath := filepath.Join(coldDir, dataTypeEvents, chunkID.BucketID(), eventstore.EventsPackName(chunkID))
	require.NoError(t, os.MkdirAll(packPath, 0o755))

	_, err := buildColdIngesters(coldDir, chunkID, sink,
		Config{Ledgers: true, Txhash: true, Events: true})
	require.Error(t, err, "events constructor must fail on the planted directory")

	// The txhash ingester was built then rolled back: its recorded ColdIngest must
	// carry the abort error, never a clean success.
	cdt := sink.coldDataTypes()
	require.Equal(t, 1, cdt[dataTypeTxhash], "rolled-back txhash ingester emits one ColdIngest")
	require.Equal(t, 1, sink.coldErrorTypes()[dataTypeTxhash],
		"the rolled-back txhash ColdIngest must carry the abort error")

	// No phantom clean success on the rollback path for any ingester.
	require.Zero(t, countCleanColdIngests(sink), "no clean ColdIngest on the rollback path")
}

// TestRunCold_ConstructorFailure_EmitsAggregate drives a constructor failure
// through RunCold (not buildColdIngesters directly) and asserts the chunk
// attempt still produces its single aggregate ColdChunkTotal — the invariant
// is one aggregate per chunk attempt, including pre-service failures.
func TestRunCold_ConstructorFailure_EmitsAggregate(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	logger := testLogger()
	sink := &testSink{}

	// Plant a regular file where the ledgers per-type subdir must be created.
	require.NoError(t, os.WriteFile(filepath.Join(coldDir, dataTypeLedgers), []byte("not a dir"), 0o644))

	err := RunCold(
		context.Background(), logger, sourceOf(fullStream(t, chunkID, nil)),
		coldDir, chunkID, 1, 1, sink, Config{Ledgers: true},
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

	ing, err := NewEventsColdIngester(coldDir, chunkID, nil)
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

	ing, err := NewEventsColdIngester(coldDir, chunkID, nil)
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

	err := drain(context.Background(), stream, chunkID, counter)
	require.Error(t, err)
	require.Contains(t, err.Error(), "overrun")
	require.Equal(t, int(ledgersInChunk), counter.ingested,
		"the out-of-chunk ledger must not reach the ingesters")
}

// ───────────────────────── lazy-source / empty-stream failures ─────────────────────────

// lazyErrSource models a datastore-backed source: OpenStream succeeds (the
// SDK's buffered-storage stream is fully lazy), and the failure — bad config,
// missing objects, revoked credentials — only surfaces on the first
// RawLedgers pull.
type lazyErrSource struct{ err error }

func (s lazyErrSource) OpenStream(chunk.ID) (ledgerbackend.LedgerStream, error) {
	return lazyErrStream(s), nil
}

type lazyErrStream struct{ err error }

func (s lazyErrStream) RawLedgers(
	context.Context, ledgerbackend.Range, ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		yield(nil, s.err)
	}
}

// TestRunCold_LazySourceFirstReadError covers the fully-lazy-source case:
// OpenStream succeeds but the first RawLedgers pull fails. The error surfaces
// from drain (after the cold ingesters were built); Finalize never runs, the
// deferred Close drops the ledger partial (Commit never ran), and the failed
// attempt still emits exactly one ColdChunkTotal.
func TestRunCold_LazySourceFirstReadError(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	logger := testLogger()
	sink := &testSink{}

	wantErr := errors.New("induced lazy-source failure (bad config / missing object)")
	err := RunCold(
		context.Background(), logger, lazyErrSource{err: wantErr},
		coldDir, chunkID, 1, 1, sink, Config{Ledgers: true},
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

// TestRunCold_EmptyStream: a source whose stream yields nothing fails drain's
// post-loop completeness check, so Finalize never runs and the deferred Close
// drops the ledger partial — no finalized artifact is committed.
func TestRunCold_EmptyStream(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	logger := testLogger()
	sink := &testSink{}

	err := RunCold(
		context.Background(), logger, sourceOf(&fakeStream{t: t, count: 0}),
		coldDir, chunkID, 1, 1, sink, Config{Ledgers: true},
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

	realLedger, err := NewLedgerColdIngester(filepath.Join(coldDir, dataTypeLedgers), chunkID, sink)
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
