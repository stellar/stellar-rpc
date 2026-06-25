package ingest

import (
	"bytes"
	"context"
	"errors"
	"iter"
	"os"
	"path/filepath"
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

// coldDirsUnder derives a ColdDirs whose three roots are coldDir/<dataType> —
// the same layout the cold ingesters compose under, so a test can build the full
// enabled set with buildColdIngestersIn from one temp dir.
func coldDirsUnder(coldDir string) ColdDirs {
	return ColdDirs{
		Ledgers: filepath.Join(coldDir, dataTypeLedgers),
		Txhash:  filepath.Join(coldDir, dataTypeTxhash),
		Events:  filepath.Join(coldDir, dataTypeEvents),
	}
}

func TestColdService_Success(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()
	sink := &testSink{}

	ings, err := buildColdIngestersIn(coldDirsUnder(coldDir), chunkID, sink, Config{Ledgers: true, Txhash: true, Events: true})
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

// ───────────────────────── pack stream cancellation ─────────────────────────

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
