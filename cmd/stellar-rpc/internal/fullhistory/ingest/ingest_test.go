package ingest

import (
	"bytes"
	"context"
	"errors"
	"iter"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/fhtest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/eventstore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/txhash"
)

// testPassphrase is a network passphrase literal used only by the test fixtures
// (transaction-hash derivation); the package itself never hardcodes one.
const testPassphrase = "Public Global Stellar Network ; September 2015"

// ───────────────────────── test metric sink ─────────────────────────

type hotPhaseCall struct {
	phase hotchunk.Phase
	dur   time.Duration
	items int
	err   error
}

type coldCall struct {
	dataType string
	items    int
	err      error
}

type stageCall struct {
	dataType string
	stage    string
	items    int
}

type coldExtractCall struct {
	items int
	err   error
}

// testSink records every MetricSink call for assertions. Safe for concurrent
// use (the hot methods fire from the per-ledger ingestion goroutine).
type testSink struct {
	mu              sync.Mutex
	hotPhases       []hotPhaseCall
	coldIngests     []coldCall
	stages          []stageCall
	coldExtracts    []coldExtractCall // one entry per shared-walk ColdExtract call
	coldChunkTotals int
}

func (s *testSink) HotPhase(phase hotchunk.Phase, dur time.Duration, items int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hotPhases = append(s.hotPhases, hotPhaseCall{phase, dur, items, err})
}

func (s *testSink) ColdIngest(dataType string, _ time.Duration, items int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.coldIngests = append(s.coldIngests, coldCall{dataType, items, err})
}

func (s *testSink) ColdChunkTotal(time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.coldChunkTotals++
}

func (s *testSink) ColdExtract(_ time.Duration, items int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.coldExtracts = append(s.coldExtracts, coldExtractCall{items, err})
}

func (s *testSink) IngestStage(dataType, stage string, _ time.Duration, items int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stages = append(s.stages, stageCall{dataType, stage, items})
}

// stageCounts counts cold IngestStage calls keyed "dataType/stage".
func (s *testSink) stageCounts() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := map[string]int{}
	for _, c := range s.stages {
		m[c.dataType+"/"+c.stage]++
	}
	return m
}

// hotPhaseItems returns the items reported per hot phase, keyed by phase.
func (s *testSink) hotPhaseItems() map[hotchunk.Phase]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := map[hotchunk.Phase]int{}
	for _, c := range s.hotPhases {
		m[c.phase] += c.items
	}
	return m
}

// hotPhaseDurs returns the wall-clock reported per hot phase, keyed by phase.
func (s *testSink) hotPhaseDurs() map[hotchunk.Phase]time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := map[hotchunk.Phase]time.Duration{}
	for _, c := range s.hotPhases {
		m[c.phase] = c.dur
	}
	return m
}

// hotPhaseErr returns the phase that carried a non-nil error, or (0,false) if none.
func (s *testSink) hotPhaseErr() (hotchunk.Phase, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.hotPhases {
		if c.err != nil {
			return c.phase, true
		}
	}
	return 0, false
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
//
//nolint:unparam // chunk-general helper; every current caller uses chunk 0
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

// extractFor runs the shared ExtractLedgerEvents walk plus the close-time read
// over one raw ledger — what coldChunk.ingest hands the txhash/events writers.
// Tests that drive a single writer directly (no coldChunk) use it in place of
// the walk.
func extractFor(t *testing.T, raw []byte) ([]sdkingest.LedgerTransactionEvents, int64) {
	t.Helper()
	view := xdr.LedgerCloseMetaView(raw)
	txEvents, err := sdkingest.ExtractLedgerEvents(view)
	require.NoError(t, err)
	closedAt, err := view.LedgerCloseTime()
	require.NoError(t, err)
	return txEvents, closedAt
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

// ───────────────────────── per-writer unit tests ─────────────────────────

// TestLedgerColdWriter_Readback ingests one ledger via the cold ledger
// writer, finalizes, and reads back through the cold reader.
func TestLedgerColdWriter_Readback(t *testing.T) {
	chunkID := chunk.ID(0)
	seq := chunkID.FirstLedger()
	raw := marshalLCM(t, seq)
	coldDir := t.TempDir()

	ing, err := newLedgerCold(packPath(coldDir, chunkID), chunkID, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, ing.close()) }()

	require.NoError(t, ing.write(seq, raw))
	require.NoError(t, ing.finalize(context.Background()))

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

// TestTxhashColdWriter_Bin ingests two tx-bearing ledgers via the cold txhash
// writer, finalizes, and reads the .bin back through the store codec.
func TestTxhashColdWriter_Bin(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()

	ing, err := newTxhashCold(txhashBinPath(coldDir), chunkID, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, ing.close()) }()

	for _, seq := range []uint32{first, first + 1} {
		raw, _, _ := marshalLCMWithEvent(t, seq)
		txEvents, _ := extractFor(t, raw)
		require.NoError(t, ing.write(seq, txEvents))
	}
	require.NoError(t, ing.finalize(context.Background()))

	entries := fhtest.ReadColdBin(t, txhashBinPath(coldDir))
	require.Len(t, entries, 2)
}

// TestEventsColdWriter_Readback ingests two event-bearing ledgers via the cold
// events writer, finalizes, and resolves the term through the cold reader.
func TestEventsColdWriter_Readback(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()

	ing, err := newEventsCold(filepath.Join(coldDir, chunkID.BucketID()), chunkID, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, ing.close()) }()

	var term events.TermKey
	for _, seq := range []uint32{first, first + 1} {
		raw, _, tk := marshalLCMWithEvent(t, seq)
		term = tk
		txEvents, closedAt := extractFor(t, raw)
		require.NoError(t, ing.write(seq, closedAt, txEvents))
	}
	require.NoError(t, ing.finalize(context.Background()))

	bucketDir := filepath.Join(coldDir, chunkID.BucketID())
	cr, err := eventstore.OpenColdReader(chunkID, bucketDir, eventstore.ColdReaderOptions{})
	require.NoError(t, err)
	defer func() { require.NoError(t, cr.Close()) }()
	cnt, err := cr.EventCount()
	require.NoError(t, err)
	require.Equal(t, uint32(2), cnt)
	bms, err := cr.LookupKeys(context.Background(), []events.TermKey{term})
	require.NoError(t, err)
	require.NotNil(t, bms[0])
	require.Equal(t, uint64(2), bms[0].GetCardinality())
}

// ───────────────────────── V0 (pre-Soroban) events handling ─────────────────────────

// TestEventsColdWriter_V0KeepsOffsetsContiguous ingests a V0 ledger followed by
// an event-bearing V2 ledger and asserts: the V0 ledger does not error, and the
// LedgerOffsets stay contiguous (both ledgers present, the event-bearing one's
// single event ID immediately follows the empty V0 ledger).
func TestEventsColdWriter_V0KeepsOffsetsContiguous(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()

	ing, err := newEventsCold(filepath.Join(coldDir, chunkID.BucketID()), chunkID, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, ing.close()) }()

	// Ledger `first`: V0 → zero events, no error.
	v0Events, v0ClosedAt := extractFor(t, marshalV0LCM(t, first))
	require.NoError(t, ing.write(first, v0ClosedAt, v0Events))
	// Ledger `first+1`: one contract event.
	rawEv, _, term := marshalLCMWithEvent(t, first+1)
	evEvents, evClosedAt := extractFor(t, rawEv)
	require.NoError(t, ing.write(first+1, evClosedAt, evEvents))
	require.NoError(t, ing.finalize(context.Background()))

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
	bms, err := cr.LookupKeys(context.Background(), []events.TermKey{term})
	require.NoError(t, err)
	require.NotNil(t, bms[0])
	require.Equal(t, uint64(1), bms[0].GetCardinality())
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
	anyKey := events.ComputeTermKey([]byte("any"), events.FieldContractID)
	bms, lerr := cr.LookupKeys(context.Background(), []events.TermKey{anyKey})
	require.NoError(t, lerr)
	require.Nil(t, bms[0], "a term with no matching events misses cleanly (nil bitmap)")

	// Metrics still fired: one aggregate per-chunk, one (clean) per-ingester.
	require.Equal(t, 1, sink.coldChunkTotals, "ColdChunkTotal must fire for an eventless chunk")
	require.Equal(t, 1, sink.coldDataTypes()[dataTypeEvents], "one ColdIngest for events")
	require.Zero(t, sink.coldErrorTypes()[dataTypeEvents], "eventless chunk is not an error")
}

// ───────────────────────── coldChunk tests ─────────────────────────

// TestColdChunk_Success drives ledger+txhash+events cold writers through a
// coldChunk and asserts readback plus the metrics signals.
func TestColdChunk_Success(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()
	sink := &testSink{}

	cc, err := openColdChunk(
		coldDirsAt(coldDir, chunkID), chunkID, sink, Config{Ledgers: true, Txhash: true, Events: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, cc.close()) }()

	var term events.TermKey
	for _, seq := range []uint32{first, first + 1} {
		raw, _, tk := marshalLCMWithEvent(t, seq)
		term = tk
		require.NoError(t, cc.ingest(seq, xdr.LedgerCloseMetaView(raw)))
	}
	require.NoError(t, cc.finalize(context.Background()))

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
	bms, err := ecr.LookupKeys(context.Background(), []events.TermKey{term})
	require.NoError(t, err)
	require.Equal(t, uint64(2), bms[0].GetCardinality())

	// Txhash .bin count.
	binEntries := fhtest.ReadColdBin(t, txhashBinPath(filepath.Join(coldDir, dataTypeTxhash)))
	require.Len(t, binEntries, 2)

	// Metrics: one ColdIngest per data type, no errors. (The aggregate
	// ColdChunkTotal is WriteColdChunk's, not coldChunk's — see the driver tests.)
	require.Zero(t, sink.coldChunkTotals, "the aggregate belongs to WriteColdChunk, not coldChunk")
	cdt := sink.coldDataTypes()
	require.Equal(t, 1, cdt[dataTypeLedgers])
	require.Equal(t, 1, cdt[dataTypeTxhash])
	require.Equal(t, 1, cdt[dataTypeEvents])
	require.Empty(t, sink.coldErrorTypes(), "success path records no writer errors")

	// The shared per-ledger walk fires ONE ColdExtract per ledger — not one per
	// consuming type — and none carries an error on the success path.
	require.Len(t, sink.coldExtracts, 2, "one shared extract walk per ledger")
	for _, ce := range sink.coldExtracts {
		require.NoError(t, ce.err, "success path records no extract errors")
	}

	// Per-stage signals: the per-ledger stages fire once per (non-empty) ledger,
	// the per-chunk finalize stage once per writer. The exact map is asserted so
	// an unexpected stage emission (or a missing one) also fails — events emits
	// term_index/write for every ledger, and txhash does only its finalize sort +
	// .bin write (its cheap per-ledger accumulate folds into the ColdIngest
	// total).
	require.Equal(t, map[string]int{
		dataTypeLedgers + "/" + stageWrite:    2,
		dataTypeLedgers + "/" + stageFinalize: 1,
		dataTypeTxhash + "/" + stageFinalize:  1,
		dataTypeEvents + "/" + stageTermIndex: 2,
		dataTypeEvents + "/" + stageWrite:     2,
		dataTypeEvents + "/" + stageFinalize:  1,
	}, sink.stageCounts())

	// No double-emit: the deferred close (after this body) must not add a second
	// ColdIngest, since finalize already emitted.
	require.NoError(t, cc.close())
	require.Len(t, sink.coldIngests, 3, "close after finalize must not re-emit per-writer signals")
}

// TestColdChunk_MidChunkFailure_NoArtifact aborts a chunk mid-ingest: the first
// ledger lands in both writers, then the second ledger's shared extract walk
// fails on garbage LCM bytes — AFTER the ledgers writer already appended them
// (the raw append does not decode). finalize must not run (coldChunk contract);
// close drops the partial ledger pack, so no finalized artifact remains, and
// neither writer emits a per-writer ColdIngest: each ingested cleanly (no
// terminal error of its own) and never finalized — a chunk abandoned by the
// shared walk leaves no phantom sample.
func TestColdChunk_MidChunkFailure_NoArtifact(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()
	sink := &testSink{}

	cc, err := openColdChunk(coldDirsAt(coldDir, chunkID), chunkID, sink, Config{Ledgers: true, Events: true})
	require.NoError(t, err)

	require.NoError(t, cc.ingest(first, viewOf(t, first)))

	garbage := bytes.Repeat([]byte{0xff}, 16)
	err = cc.ingest(first+1, xdr.LedgerCloseMetaView(garbage))
	require.Error(t, err)
	require.Contains(t, err.Error(), "extract ledger events", "the shared walk rejects the garbage ledger")

	// The failed walk is metered: an error-carrying ColdExtract (its one metric —
	// no writer ran for that ledger), after the first ledger's clean one.
	require.Len(t, sink.coldExtracts, 2)
	require.NoError(t, sink.coldExtracts[0].err)
	require.Error(t, sink.coldExtracts[1].err, "the failed shared walk emits an error-carrying ColdExtract")

	// Nothing has emitted: both writers ingested cleanly (no terminal error of
	// their own) and never finalized.
	require.Empty(t, sink.coldDataTypes(), "no per-writer ColdIngest on the shared-walk failure path")

	// Close drops partials; still no per-writer sample.
	require.NoError(t, cc.close())
	require.Empty(t, sink.coldDataTypes(), "an abandoned chunk emits no per-writer ColdIngest")

	// No finalized ledger pack must exist.
	path := packPath(filepath.Join(coldDir, dataTypeLedgers), chunkID)
	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "expected no cold ledger artifact at %s, stat err: %v", path, statErr)
}

// TestColdWriter_Failure_RecordsErrorMetric drives a real cold ledger writer
// so its OWN write fails (recording firstErr), then close. The failure is an
// out-of-order seq: the per-chunk ColdWriter expects the chunk's first ledger,
// so AppendLedger rejects a later one. Per #765 a failed cold chunk must record
// a per-writer error count. A terminal write error emits the single per-writer
// ColdIngest right there (close never emits), so the error-carrying sample is
// present after ingest returns.
func TestColdWriter_Failure_RecordsErrorMetric(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	sink := &testSink{}

	cc, err := openColdChunk(coldDirsAt(coldDir, chunkID), chunkID, sink, Config{Ledgers: true})
	require.NoError(t, err)

	// An out-of-order seq makes the writer's own AppendLedger fail inside write,
	// so it records its firstErr and emits the error-carrying ColdIngest. (drain
	// would never feed this — the test targets the writer's metric path directly.)
	wrongSeq := chunkID.FirstLedger() + 5
	require.Error(t, cc.ingest(wrongSeq, viewOf(t, wrongSeq)))
	require.Equal(t, 1, sink.coldDataTypes()[dataTypeLedgers], "the failed write emits its ColdIngest immediately")

	// finalize is skipped on this path; close emits nothing more.
	require.NoError(t, cc.close())

	// Exactly one ColdIngest for ledgers, carrying the error.
	require.Equal(t, 1, sink.coldDataTypes()[dataTypeLedgers])
	require.Equal(t, 1, sink.coldErrorTypes()[dataTypeLedgers], "the recorded ColdIngest carries the write error")
}

// ───────────────────────── metrics sink tests ─────────────────────────

// TestPrometheusSink_Smoke asserts NewPrometheusSink registers without panic and
// its methods don't error.
func TestPrometheusSink_Smoke(t *testing.T) {
	reg := prometheus.NewRegistry()
	require.NotPanics(t, func() {
		sink := NewPrometheusSink(reg, "test")
		// The six hot per-ledger phases: extract/commit/apply carry no items, the
		// write phases carry per-type volume; the commit phase exercises the error
		// dimension.
		sink.HotPhase(hotchunk.PhaseExtract, time.Millisecond, 0, nil)
		sink.HotPhase(hotchunk.PhaseLedgers, time.Millisecond, 1, nil)
		sink.HotPhase(hotchunk.PhaseTxhash, time.Millisecond, 5, nil)
		sink.HotPhase(hotchunk.PhaseEvents, time.Millisecond, 3, nil)
		sink.HotPhase(hotchunk.PhaseCommit, time.Millisecond, 0, errors.New("induced commit failure"))
		sink.HotPhase(hotchunk.PhaseApply, time.Millisecond, 0, nil)
		sink.ColdIngest(dataTypeTxhash, time.Second, 100, nil)
		sink.ColdChunkTotal(time.Second)
		sink.ColdExtract(time.Millisecond, 3, nil)
		sink.ColdExtract(time.Millisecond, 0, errors.New("induced extract failure"))
		sink.IngestStage(dataTypeEvents, stageFinalize, time.Second, 0)
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
		context.Background(), logger, chunkID, rawChunk(stream, chunkID),
		coldDirsAt(coldDir, chunkID), sink, Config{Ledgers: true},
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

// eventRichLedger builds a multi-tx, multi-event ledger: tx0 with two op events,
// tx1 with BeforeAllTxs + AfterTx stage events straddling one op event, tx2 with
// no events. It exercises cross-tx and cross-stage cursor ordering, so the
// byte-identity check below is a real ordering test, not a single-event smoke.
func eventRichLedger(t *testing.T, seq uint32) []byte {
	t.Helper()
	tx0 := xdr.TransactionMeta{V: 4, V4: &xdr.TransactionMetaV4{
		Operations: []xdr.OperationMetaV2{{Events: []xdr.ContractEvent{
			buildContractEvent("a"), buildContractEvent("b"),
		}}},
	}}
	tx1 := xdr.TransactionMeta{V: 4, V4: &xdr.TransactionMetaV4{
		Events: []xdr.TransactionEvent{
			{Stage: xdr.TransactionEventStageTransactionEventStageBeforeAllTxs, Event: buildContractEvent("fee")},
			{Stage: xdr.TransactionEventStageTransactionEventStageAfterTx, Event: buildContractEvent("refund")},
		},
		Operations: []xdr.OperationMetaV2{{Events: []xdr.ContractEvent{buildContractEvent("c")}}},
	}}
	tx2 := xdr.TransactionMeta{V: 4, V4: &xdr.TransactionMetaV4{}}
	raw, err := buildLCM(t, seq, []xdr.TransactionMeta{tx0, tx1, tx2}).MarshalBinary()
	require.NoError(t, err)
	return raw
}

// TestWriteColdChunk_ByteIdentity_SharedWalk is the #836 invariant guard: the
// single-walk cold path must produce artifacts byte-identical to the old
// two-walk path. It drives the real WriteColdChunk entrypoint over a chunk with
// several rich sentinel ledgers and compares the artifacts to INDEPENDENT
// reference computations:
//
//   - txhash .bin vs. sorted, truncated ExtractTxHashes output. ExtractTxHashes
//     is the OLD txhash extractor — a separate SDK walk from the shared
//     ExtractLedgerEvents the ingester now reads — so an entry-by-entry match
//     proves the switch to `.Hash` off the shared walk changed no byte.
//   - events per-term bitmaps + count vs. an independent PayloadsFromLedgerEvents
//     shaping, with chunk-relative event IDs assigned in ingest (ascending-seq)
//     order — proving the event-ID assignment is unchanged.
//
//nolint:funlen // one invariant, two inline reference computations — splitting would obscure it
func TestWriteColdChunk_ByteIdentity_SharedWalk(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()

	// Rich sentinels scattered across the chunk; the rest are cheap zero-tx
	// ledgers (no hashes, no events). Capture each sentinel's raw for the
	// reference computations.
	sentinels := []uint32{first, first + 1, first + 7}
	raws := map[uint32][]byte{}
	gen := func(tt *testing.T, seq uint32) []byte {
		if slices.Contains(sentinels, seq) {
			raw := eventRichLedger(tt, seq)
			raws[seq] = raw
			return raw
		}
		return marshalLCM(tt, seq)
	}
	require.NoError(t, WriteColdChunk(
		context.Background(), testLogger(), chunkID, rawChunk(fullStream(t, chunkID, gen), chunkID),
		coldDirsAt(coldDir, chunkID), nil, Config{Ledgers: true, Txhash: true, Events: true},
	))

	// Reference #1 — txhash: sorted, truncated entries for every resolvable
	// hash (outer, plus a fee-bump's inner) over the sentinels.
	var wantEntries []txhash.ColdEntry
	for _, seq := range sentinels {
		txEvents, err := sdkingest.ExtractLedgerEvents(xdr.LedgerCloseMetaView(raws[seq]))
		require.NoError(t, err)
		for i := range txEvents {
			var ke txhash.ColdEntry
			copy(ke.Key[:], txEvents[i].Hash[:txhash.ColdKeySize])
			ke.Seq = seq
			wantEntries = append(wantEntries, ke)
			if txEvents[i].FeeBump {
				var ike txhash.ColdEntry
				copy(ike.Key[:], txEvents[i].InnerHash[:txhash.ColdKeySize])
				ike.Seq = seq
				wantEntries = append(wantEntries, ike)
			}
		}
	}
	slices.SortFunc(wantEntries, func(a, b txhash.ColdEntry) int { return bytes.Compare(a.Key[:], b.Key[:]) })
	gotEntries := fhtest.ReadColdBin(t, txhashBinPath(filepath.Join(coldDir, dataTypeTxhash)))
	require.Equal(t, wantEntries, gotEntries, "cold .bin must hold every resolvable hash, sorted and truncated")

	// Reference #2 — events: PayloadsFromLedgerEvents with chunk-relative IDs
	// assigned in ingest (ascending-seq) order, mapped to their term keys.
	wantTermIDs := map[events.TermKey][]uint32{}
	var nextID uint32
	for _, seq := range sentinels {
		view := xdr.LedgerCloseMetaView(raws[seq])
		txEvents, err := sdkingest.ExtractLedgerEvents(view)
		require.NoError(t, err)
		closedAt, err := view.LedgerCloseTime()
		require.NoError(t, err)
		payloads, err := events.PayloadsFromLedgerEvents(txEvents, seq, closedAt)
		require.NoError(t, err)
		for i := range payloads {
			keys, kerr := events.TermsForBytes(payloads[i].ContractEventBytes)
			require.NoError(t, kerr)
			for _, k := range keys {
				wantTermIDs[k] = append(wantTermIDs[k], nextID)
			}
			nextID++
		}
	}
	require.NotEmpty(t, wantTermIDs, "sentinels must carry events")

	ecr, err := eventstore.OpenColdReader(
		chunkID, filepath.Join(coldDir, dataTypeEvents, chunkID.BucketID()), eventstore.ColdReaderOptions{})
	require.NoError(t, err)
	defer func() { require.NoError(t, ecr.Close()) }()

	cnt, err := ecr.EventCount()
	require.NoError(t, err)
	require.Equal(t, nextID, cnt, "cold event count must match the reference payload count")

	terms := make([]events.TermKey, 0, len(wantTermIDs))
	for k := range wantTermIDs {
		terms = append(terms, k)
	}
	bms, err := ecr.LookupKeys(context.Background(), terms)
	require.NoError(t, err)
	for i, k := range terms {
		require.NotNil(t, bms[i], "term %d present in reference must resolve", i)
		require.Equal(t, wantTermIDs[k], bms[i].ToArray(),
			"cold term bitmap must carry the same event IDs as the shaping reference")
	}
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
		context.Background(), logger, chunkID, rawChunk(short, chunkID),
		coldDirsAt(coldDir, chunkID), nil, Config{Ledgers: true},
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

	entries := fhtest.ReadColdBin(t, txhashBinPath(filepath.Join(coldDir, dataTypeTxhash)))
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
	bms, err := cr.LookupKeys(context.Background(), []events.TermKey{term})
	require.NoError(t, err)
	require.NotNil(t, bms[0])
	require.Equal(t, uint64(len(evSeqs)), bms[0].GetCardinality())
}

// ───────────────────────── drain stream errors ─────────────────────────
//
// The per-seq order guard the shared cursor used to run in drain moved to the
// SOURCE (packStream reads positionally; hotLedgerStream key-checks its keyspace,
// see TestSource_RejectsGap; the SDK backends validate their own output), so drain
// keeps only its overrun + completeness checks on a local counter. The tests that
// fed an artificially mis-ordered stream to drain were deleted with the cursor.

// TestWriteColdChunk_DrainStreamError_NoArtifact exercises the drain mid-stream error
// path: the backend yields valid ledgers, then hands back (nil, err) at a seq in
// the middle of the chunk. drain must propagate the error (wrapped with the chunk),
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
		context.Background(), logger, chunkID, rawChunk(stream, chunkID),
		coldDirsAt(coldDir, chunkID), nil, Config{Ledgers: true},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, wantErr, "the backend error must propagate")
	require.Contains(t, err.Error(), "stream for chunk", "error wraps the drained chunk")

	// Finalize never ran → no finalized artifact; deferred Close dropped the partial.
	path := packPath(filepath.Join(coldDir, dataTypeLedgers), chunkID)
	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "expected no cold artifact at %s, stat err: %v", path, statErr)
}

// The txhash .bin codec itself — atomic publish, create/rename failure
// cleanup, layout, and the reader round-trip — is owned and tested by
// pkg/stores/txhash (cold_bin_test.go); these tests only cover the
// ingester-level behavior on top of it.

// ───────────────────────── hot service emission ─────────────────────────

func hotTestLogger() *supportlog.Entry {
	l := supportlog.New()
	l.SetLevel(logrus.ErrorLevel)
	return l
}

// TestHotService_EmitsEveryPhaseOnSuccess constructs a HotService over a real hot
// DB with a recording sink and asserts one successful ingest emits every phase
// once, the write phases carry per-type volume (extract/commit/apply carry none),
// and no phase carries an error.
func TestHotService_EmitsEveryPhaseOnSuccess(t *testing.T) {
	db, err := hotchunk.Open(t.TempDir(), chunk.ID(0), hotTestLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	sink := &testSink{}
	svc := NewHotService(db, sink)
	first := chunk.ID(0).FirstLedger()
	raw, _, _ := marshalLCMWithEvent(t, first) // one tx, one event
	require.NoError(t, svc.Ingest(context.Background(), first, xdr.LedgerCloseMetaView(raw)))

	require.Len(t, sink.hotPhases, int(hotchunk.NumPhases), "every phase emitted once on success")
	items := sink.hotPhaseItems()
	assert.Equal(t, 1, items[hotchunk.PhaseLedgers], "one ledger")
	assert.Equal(t, 1, items[hotchunk.PhaseTxhash], "one tx hash")
	assert.Equal(t, 1, items[hotchunk.PhaseEvents], "one event")
	assert.Zero(t, items[hotchunk.PhaseExtract], "extract carries no items")
	assert.Zero(t, items[hotchunk.PhaseCommit], "commit carries no items")
	assert.Zero(t, items[hotchunk.PhaseApply], "apply carries no items")
	_, hadErr := sink.hotPhaseErr()
	assert.False(t, hadErr, "success path carries no phase error")
}

// TestHotService_CommitErrorLandsOnCommitPhase asserts a commit failure (a closed
// DB) surfaces the error on the commit phase — by construction, not by a
// separately-maintained label — and emits no items on the failure path.
func TestHotService_CommitErrorLandsOnCommitPhase(t *testing.T) {
	db, err := hotchunk.Open(t.TempDir(), chunk.ID(0), hotTestLogger())
	require.NoError(t, err)
	require.NoError(t, db.Close()) // closed => the batch commit fails

	sink := &testSink{}
	svc := NewHotService(db, sink)
	first := chunk.ID(0).FirstLedger()
	raw, _, _ := marshalLCMWithEvent(t, first)
	require.Error(t, svc.Ingest(context.Background(), first, xdr.LedgerCloseMetaView(raw)))

	phase, hadErr := sink.hotPhaseErr()
	require.True(t, hadErr, "the failure must be reported on a phase")
	assert.Equal(t, hotchunk.PhaseCommit, phase, "a commit failure lands on the commit phase")
	for p, n := range sink.hotPhaseItems() {
		assert.Zero(t, n, "no items on the failure path (phase %v)", p)
	}
}

// TestHotService_ExtractFailureLandsOnExtractPhase asserts a decode failure (garbage
// LCM bytes) surfaces on the extract phase — the pre-batch walk where every decode
// failure lands by construction — and emits ONLY that phase (no batch was opened, so
// no later phase ran).
func TestHotService_ExtractFailureLandsOnExtractPhase(t *testing.T) {
	db, err := hotchunk.Open(t.TempDir(), chunk.ID(0), hotTestLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	sink := &testSink{}
	svc := NewHotService(db, sink)
	first := chunk.ID(0).FirstLedger()
	// Garbage bytes fail XDR decode in ExtractLedgerEvents, before any batch opens.
	garbage := bytes.Repeat([]byte{0xff}, 16)
	require.Error(t, svc.Ingest(context.Background(), first, xdr.LedgerCloseMetaView(garbage)))

	phase, hadErr := sink.hotPhaseErr()
	require.True(t, hadErr, "the decode failure must be reported on a phase")
	assert.Equal(t, hotchunk.PhaseExtract, phase, "a decode failure lands on the extract phase")
	require.Len(t, sink.hotPhases, 1, "a pre-batch decode failure emits only the extract phase")
}

// TestHotService_EventsQueueFailureLandsOnEventsPhase asserts an events-queue failure
// (a ledger that skips ahead of the chunk's next-expected seq) surfaces on the events
// phase — a queue-step failure AFTER extract/ledger/txhash ran. Because PhaseExtract is
// Failed's zero value, a NON-zero Failed is the discriminator that proves the phase was
// actually assigned rather than defaulted.
func TestHotService_EventsQueueFailureLandsOnEventsPhase(t *testing.T) {
	db, err := hotchunk.Open(t.TempDir(), chunk.ID(0), hotTestLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	sink := &testSink{}
	svc := NewHotService(db, sink)
	first := chunk.ID(0).FirstLedger()
	// A valid LCM but a seq that skips ahead: the events facade expects the empty
	// chunk's first ledger and rejects first+5 as out of order (ErrLedgerOutOfOrder).
	// The ledger + txhash queue steps do not sequence-check, so they run first.
	raw, _, _ := marshalLCMWithEvent(t, first+5)
	require.Error(t, svc.Ingest(context.Background(), first+5, xdr.LedgerCloseMetaView(raw)))

	phase, hadErr := sink.hotPhaseErr()
	require.True(t, hadErr, "the queue failure must be reported on a phase")
	assert.Equal(t, hotchunk.PhaseEvents, phase, "an events-queue failure lands on the events phase")
	assert.NotEqual(t, hotchunk.PhaseExtract, phase, "a non-zero Failed proves it was assigned, not defaulted")
	// Phases [extract, ledgers, txhash, events] emitted; commit/apply never ran.
	assert.Len(t, sink.hotPhases, int(hotchunk.PhaseEvents)+1)
}

// TestHotService_FailedPhaseCarriesPartialDuration asserts that a failed ledger ingest
// stamps the failed phase's PARTIAL wall-clock before returning (not a zero sample), and
// that completed earlier phases carry theirs too. Uses the same out-of-order failure as
// above, whose failed phase is the events queue.
func TestHotService_FailedPhaseCarriesPartialDuration(t *testing.T) {
	db, err := hotchunk.Open(t.TempDir(), chunk.ID(0), hotTestLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	sink := &testSink{}
	svc := NewHotService(db, sink)
	first := chunk.ID(0).FirstLedger()
	raw, _, _ := marshalLCMWithEvent(t, first+5)
	require.Error(t, svc.Ingest(context.Background(), first+5, xdr.LedgerCloseMetaView(raw)))

	durs := sink.hotPhaseDurs()
	assert.Positive(t, durs[hotchunk.PhaseExtract], "a completed earlier phase carries its wall-clock")
	assert.Positive(t, durs[hotchunk.PhaseEvents], "the failed phase stamps its partial duration before returning")
}

// ───────────────────────── cold txhash .bin content (P1-d) ─────────────────────────

// TestTxhashColdWriter_BinContent ingests two tx-bearing ledgers, finalizes,
// then reads the .bin back through the store codec and asserts the contract
// the deferred streamhash builder relies on: each key == the fixture tx hash
// truncated to txhash.ColdKeySize (pinned to streamhash.MinKeySize by the
// codec), each seq == the ledger it was ingested in, and entries are in
// non-decreasing key order.
func TestTxhashColdWriter_BinContent(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()

	ing, err := newTxhashCold(txhashBinPath(coldDir), chunkID, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, ing.close()) }()

	// Capture each fixture hash + the seq it was ingested in.
	wantSeqByKey := map[[txhash.ColdKeySize]byte]uint32{}
	for _, seq := range []uint32{first, first + 1} {
		raw, hash, _ := marshalLCMWithEvent(t, seq)
		var key [txhash.ColdKeySize]byte
		copy(key[:], hash[:txhash.ColdKeySize])
		wantSeqByKey[key] = seq
		txEvents, _ := extractFor(t, raw)
		require.NoError(t, ing.write(seq, txEvents))
	}
	require.NoError(t, ing.finalize(context.Background()))

	entries := fhtest.ReadColdBin(t, txhashBinPath(coldDir))
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

// TestOpenColdChunk_RollbackOneBuilt makes a LATER writer's open (txhash) fail
// by planting a regular file at the txhash per-type directory, so its own
// MkdirAll fails. The earlier-opened ledger writer is rolled back — closed
// only. Since close never emits a per-writer ColdIngest, a rolled-back writer
// (opened, never ingested or finalized) produces NO sample at all: no phantom
// success, no synthetic abort.
func TestOpenColdChunk_RollbackOneBuilt(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	sink := &testSink{}

	// Plant a regular FILE where the txhash per-type directory must be
	// created: the ledger writer opens first, then newTxhashCold fails its
	// bucket-dir MkdirAll.
	require.NoError(t, os.WriteFile(filepath.Join(coldDir, dataTypeTxhash), []byte("not a dir"), 0o644))

	_, err := openColdChunk(coldDirsAt(coldDir, chunkID), chunkID, sink, Config{Ledgers: true, Txhash: true})
	require.Error(t, err, "the txhash open must fail on the planted file")

	// The ledger writer was opened then rolled back with no write/finalize, so
	// it emits nothing.
	require.Empty(t, sink.coldDataTypes(), "a rolled-back writer emits no per-writer ColdIngest")
}

// TestOpenColdChunk_RollbackTwoBuilt makes the LAST writer's open (events)
// fail AFTER both the ledger AND txhash writers were already opened, so the
// rollback closes two writers. Same invariant at greater rollback depth:
// neither rolled-back writer emits a per-writer ColdIngest.
func TestOpenColdChunk_RollbackTwoBuilt(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	sink := &testSink{}

	// Plant a directory at the events.pack path: the ledger and txhash
	// writers open first, then newEventsCold fails opening the pack over
	// the directory.
	packPath := filepath.Join(coldDir, dataTypeEvents, chunkID.BucketID(), eventstore.EventsPackName(chunkID))
	require.NoError(t, os.MkdirAll(packPath, 0o755))

	_, err := openColdChunk(coldDirsAt(coldDir, chunkID), chunkID, sink,
		Config{Ledgers: true, Txhash: true, Events: true})
	require.Error(t, err, "the events open must fail on the planted directory")

	// Both the ledger and txhash writers were opened then rolled back with no
	// write/finalize, so neither emits a per-writer ColdIngest.
	require.Empty(t, sink.coldDataTypes(), "rolled-back writers emit no per-writer ColdIngest")
}

// TestWriteColdChunk_ConstructorFailure_EmitsAggregate drives a constructor failure
// through WriteColdChunk (not openColdChunk directly) and asserts the chunk
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

	ing, err := newEventsCold(filepath.Join(coldDir, chunkID.BucketID()), chunkID, nil)
	require.NoError(t, err)

	// Ingest one event-bearing ledger so the mirror is non-empty, exercising a
	// real (non-empty) MPHF build.
	rawEv, _, _ := marshalLCMWithEvent(t, first)
	txEvents, closedAt := extractFor(t, rawEv)
	require.NoError(t, ing.write(first, closedAt, txEvents))

	// Plant a DIRECTORY where index.hash must be written → buildMPHF fails.
	bucketDir := filepath.Join(coldDir, chunkID.BucketID())
	indexHashPath := filepath.Join(bucketDir, eventstore.IndexHashName(chunkID))
	require.NoError(t, os.Mkdir(indexHashPath, 0o755))

	ferr := ing.finalize(context.Background())
	require.Error(t, ferr, "Finalize must fail when WriteColdIndex fails")
	require.Contains(t, ferr.Error(), "WriteColdIndex")

	// The committed events.pack stays in place as inert scratch (Finish ran,
	// so the later Close does not drop it either).
	packPath := filepath.Join(bucketDir, eventstore.EventsPackName(chunkID))
	_, statErr := os.Stat(packPath)
	require.NoError(t, statErr, "the index-less events.pack stays on disk after WriteColdIndex failure")

	// Close is still safe/idempotent afterwards and does not remove the pack.
	require.NoError(t, ing.close())
	_, statErr = os.Stat(packPath)
	require.NoError(t, statErr, "close after a committed Finish must not drop the pack")
}

// TestEventsCold_FinalizeAfterFailedIngest_Refuses asserts the failed-write
// latch: once a write errors (here shaping a malformed TransactionEvent),
// finalize must refuse rather than commit a pack+index whose mirror may be ahead
// of the offsets commit point.
func TestEventsCold_FinalizeAfterFailedIngest_Refuses(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()

	ing, err := newEventsCold(filepath.Join(coldDir, chunkID.BucketID()), chunkID, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, ing.close()) }()

	// A garbage top-level TransactionEvent fails PayloadsFromLedgerEvents' Stage
	// decode inside write — the walk itself is coldChunk's job, so the
	// per-writer failure is a shaping error over the shared walk's output.
	bad := []sdkingest.LedgerTransactionEvents{{TransactionEvents: [][]byte{{0xff, 0xff}}}}
	require.Error(t, ing.write(chunkID.FirstLedger(), 0, bad))

	ferr := ing.finalize(context.Background())
	require.Error(t, ferr)
	require.Contains(t, ferr.Error(), "finalize after failed write")
}

// ───────────────────────── coldChunk.finalize first-error ─────────────────────────

// TestColdChunk_Finalize_FirstErrorStopsRemaining asserts coldChunk.finalize
// stops at the first writer's error and does NOT finalize (publish) the later
// writers — once a sibling failed, the rest are released (never finalized) by
// the caller's deferred close, so a failed chunk never gains newly committed
// artifacts past the failure. It also asserts the flip side: the artifact an
// EARLIER writer already committed in this attempt stays on disk as inert
// scratch (see the package doc's artifact model). All three writers are real:
// the txhash finalize is made to fail by planting a directory at its .bin
// path, after the ledgers finalize already committed its pack and before the
// events finalize would publish its index.
func TestColdChunk_Finalize_FirstErrorStopsRemaining(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()
	sink := &testSink{}

	cc, err := openColdChunk(coldDirsAt(coldDir, chunkID), chunkID, sink,
		Config{Ledgers: true, Txhash: true, Events: true})
	require.NoError(t, err)

	raw, _, _ := marshalLCMWithEvent(t, first)
	require.NoError(t, cc.ingest(first, xdr.LedgerCloseMetaView(raw)))

	// Plant a DIRECTORY at the .bin path: txhash's finalize (WriteColdBin's
	// os.Create) fails after the ledgers finalize already ran.
	binPath := txhashBinPath(filepath.Join(coldDir, dataTypeTxhash))
	require.NoError(t, os.Mkdir(binPath, 0o755))

	ferr := cc.finalize(context.Background())
	require.Error(t, ferr, "finalize must surface the txhash failure")

	// The earlier writer's already-committed pack REMAINS on disk.
	path := packPath(filepath.Join(coldDir, dataTypeLedgers), chunkID)
	_, statErr := os.Stat(path)
	require.NoError(t, statErr, "the earlier writer's published artifact stays on disk")

	// The later (events) writer was never finalized: no index artifacts.
	bucketDir := filepath.Join(coldDir, dataTypeEvents, chunkID.BucketID())
	_, statErr = os.Stat(filepath.Join(bucketDir, eventstore.IndexPackName(chunkID)))
	require.True(t, os.IsNotExist(statErr), "a later writer must NOT be finalized after a sibling failure")

	require.NoError(t, cc.close())
	_, statErr = os.Stat(path)
	require.NoError(t, statErr, "close after a committed finalize must not drop the published pack")
}

// ───────────────────────── drain overrun guard ─────────────────────────

// TestDrain_OverrunPastChunk asserts a stream that keeps yielding in order
// PAST the chunk's last ledger is rejected before the overrun ledger is
// ingested — not after the stream ends, by which point the extra ledgers
// would already be durably written on the hot path. The per-ledger write-stage
// count is the proof: the ledgers writer emits one per accepted ledger.
func TestDrain_OverrunPastChunk(t *testing.T) {
	chunkID := chunk.ID(0)
	ledgersInChunk := chunkID.LastLedger() - chunkID.FirstLedger() + 1
	// One ledger past the chunk, still in order.
	stream := &fakeStream{t: t, count: ledgersInChunk + 1}
	sink := &testSink{}
	cc, err := openColdChunk(coldDirsAt(t.TempDir(), chunkID), chunkID, sink, Config{Ledgers: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, cc.close()) }()

	err = drain(rawChunk(stream, chunkID), chunkID, cc)
	require.Error(t, err)
	require.Contains(t, err.Error(), "overrun")
	require.Equal(t, int(ledgersInChunk), sink.stageCounts()[dataTypeLedgers+"/"+stageWrite],
		"the out-of-chunk ledger must not reach the writers")
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
	require.Contains(t, err.Error(), "stream for chunk", "the error surfaces from drain's stream pull")

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

// The earlier-artifact-stays-on-disk invariant when a LATER writer's finalize
// fails is covered by TestColdChunk_Finalize_FirstErrorStopsRemaining above.

// buildLCMWithFeeBump builds a single-transaction V2 LCM whose transaction is a
// fee-bump with a real txFEE_BUMP_INNER_SUCCESS result, returning the outer and
// inner hashes.
func buildLCMWithFeeBump(t *testing.T, seq uint32) (xdr.LedgerCloseMeta, [32]byte, [32]byte) {
	t.Helper()
	inner := xdr.Transaction{
		SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
		Ext:           xdr.TransactionExt{V: 1, SorobanData: &xdr.SorobanTransactionData{}},
	}
	envelope := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTxFeeBump,
		FeeBump: &xdr.FeeBumpTransactionEnvelope{Tx: xdr.FeeBumpTransaction{
			Fee:       999,
			FeeSource: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
			InnerTx: xdr.FeeBumpTransactionInnerTx{
				Type: xdr.EnvelopeTypeEnvelopeTypeTx,
				V1:   &xdr.TransactionV1Envelope{Tx: inner},
			},
		}},
	}
	outerHash, err := network.HashTransactionInEnvelope(envelope, testPassphrase)
	require.NoError(t, err)
	innerHash, err := network.HashTransactionInEnvelope(xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx, V1: envelope.FeeBump.Tx.InnerTx.V1,
	}, testPassphrase)
	require.NoError(t, err)

	ops := []xdr.OperationResult{}
	result := xdr.TransactionResult{FeeCharged: 200, Result: xdr.TransactionResultResult{
		Code: xdr.TransactionResultCodeTxFeeBumpInnerSuccess,
		InnerResultPair: &xdr.InnerTransactionResultPair{
			TransactionHash: innerHash,
			Result: xdr.InnerTransactionResult{Result: xdr.InnerTransactionResultResult{
				Code: xdr.TransactionResultCodeTxSuccess, Results: &ops,
			}},
		},
	}}
	comp := []xdr.TxSetComponent{{
		Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
		TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
			Txs: []xdr.TransactionEnvelope{envelope},
		},
	}}
	lcm := xdr.LedgerCloseMeta{V: 2, V2: &xdr.LedgerCloseMetaV2{
		LedgerHeader: xdr.LedgerHeaderHistoryEntry{Header: xdr.LedgerHeader{
			ScpValue: xdr.StellarValue{CloseTime: xdr.TimePoint(0)}, LedgerSeq: xdr.Uint32(seq),
		}},
		TxSet: xdr.GeneralizedTransactionSet{V: 1, V1TxSet: &xdr.TransactionSetV1{
			Phases: []xdr.TransactionPhase{{V: 0, V0Components: &comp}},
		}},
		TxProcessing: []xdr.TransactionResultMetaV1{{
			TxApplyProcessing: xdr.TransactionMeta{V: 3, V3: &xdr.TransactionMetaV3{}},
			Result:            xdr.TransactionResultPair{TransactionHash: outerHash, Result: result},
		}},
	}}
	return lcm, outerHash, innerHash
}

// TestTxhashColdWriter_FeeBumpBothHashes: a fee-bump ledger contributes two
// .bin entries — the outer and the inner hash — both mapping to the same seq,
// so the cold index resolves the transaction by either.
func TestTxhashColdWriter_FeeBumpBothHashes(t *testing.T) {
	chunkID := chunk.ID(0)
	seq := chunkID.FirstLedger()
	coldDir := t.TempDir()

	ing, err := newTxhashCold(txhashBinPath(coldDir), chunkID, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, ing.close()) }()

	lcm, outerHash, innerHash := buildLCMWithFeeBump(t, seq)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	txEvents, _ := extractFor(t, raw)
	require.NoError(t, ing.write(seq, txEvents))
	require.NoError(t, ing.finalize(context.Background()))

	entries := fhtest.ReadColdBin(t, txhashBinPath(coldDir))
	require.Len(t, entries, 2)
	keys := make([][]byte, 0, len(entries))
	for i := range entries {
		require.Equal(t, seq, entries[i].Seq)
		keys = append(keys, entries[i].Key[:])
	}
	assert.Contains(t, keys, outerHash[:txhash.ColdKeySize])
	assert.Contains(t, keys, innerHash[:txhash.ColdKeySize])
}
