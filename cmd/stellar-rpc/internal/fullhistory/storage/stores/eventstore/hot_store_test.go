package eventstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"iter"
	"path/filepath"
	"sync"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores"
)

// silentLogger returns a logger whose output is buffered into an
// internal sink so tests stay quiet. Debug-level so logged callers
// don't suppress output and mask a future logging regression.
func silentLogger() *supportlog.Entry {
	var buf bytes.Buffer
	log := supportlog.New()
	log.SetLevel(logrus.DebugLevel)
	log.SetOutput(&buf)
	return log
}

// hotStoreHarness bundles a HotStore + the root data directory used
// by tests. The store is registered for cleanup.
type hotStoreHarness struct {
	dataDir string
	store   *HotStore
	raw     *rocksdb.Store
}

// openHotStoreForTest opens a fresh per-Chunk hot DB for chunkID
// under a single t.TempDir() and returns a HotStore wrapping it. The
// DB auto-closes on test cleanup.
//
//nolint:unparam // chunkID kept as a param so tests can vary it; today every caller uses 0
func openHotStoreForTest(t *testing.T, chunkID chunk.ID) *hotStoreHarness {
	t.Helper()
	dir := t.TempDir()

	hot, raw := openHotStoreForTestAt(t, dir, chunkID)
	return &hotStoreHarness{dataDir: dir, store: hot, raw: raw}
}

func openHotStoreForTestAt(t *testing.T, dir string, chunkID chunk.ID) (*HotStore, *rocksdb.Store) {
	t.Helper()
	hot, raw, err := tryOpenHotStoreForTest(t, dir, chunkID)
	require.NoError(t, err)
	return hot, raw
}

func tryOpenHotStoreForTest(t *testing.T, dir string, chunkID chunk.ID) (*HotStore, *rocksdb.Store, error) {
	t.Helper()
	raw := openRawHotChunkForTest(t, dir, chunkID)
	hot, err := NewWithStore(raw, chunkID)
	if err != nil {
		_ = raw.Close()
		return nil, nil, err
	}
	t.Cleanup(func() { _ = raw.Close() })
	return hot, raw, nil
}

func openRawHotChunkForTest(t *testing.T, dir string, chunkID chunk.ID) *rocksdb.Store {
	t.Helper()
	raw, err := rocksdb.New(rocksdb.Config{
		Path:           filepath.Join(dir, chunkID.String()),
		ColumnFamilies: CFNames(),
		Logger:         silentLogger(),
		PerCFOptions:   CFOptions(),
	})
	require.NoError(t, err)
	return raw
}

func makePayload(symbol string) (events.Payload, []events.TermKey) {
	var cid xdr.ContractId
	cid[0] = 0xab
	sym := xdr.ScSymbol(symbol)
	ev := xdr.ContractEvent{
		ContractId: &cid,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: []xdr.ScVal{{Type: xdr.ScValTypeScvSymbol, Sym: &sym}},
				Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
			},
		},
	}
	evBytes, err := ev.MarshalBinary()
	if err != nil {
		panic(err) // hardcoded test fixture; an error here is a test bug
	}
	keys, err := events.TermsForBytes(evBytes)
	if err != nil {
		panic(err)
	}
	p := events.Payload{
		TxHash:             xdr.Hash{0xde, 0xad},
		TxIdx:              1,
		ContractEventBytes: evBytes,
	}
	return p, keys
}

// eventOf decodes a Payload's ContractEventBytes back into the struct for
// fixtures and assertions. Payloads carry only the raw XDR; a decode
// failure means a corrupt fixture, so it panics.
func eventOf(p events.Payload) xdr.ContractEvent {
	var ev xdr.ContractEvent
	if err := ev.UnmarshalBinary(p.ContractEventBytes); err != nil {
		panic(err)
	}
	return ev
}

// dataSym returns a Payload's ScVal Data symbol. The eventstore read path
// yields Payloads carrying only raw event XDR (ContractEventBytes), so
// assertions decode it back to a ContractEvent first.
func dataSym(t *testing.T, p events.Payload) string {
	t.Helper()
	return string(*eventOf(p).Body.V0.Data.Sym)
}

func TestHotStore_FreshChunkHasEmptyState(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)

	assert.Equal(t, chunkID, h.store.ChunkID())
	assert.Equal(t, uint32(0), mustEventCount(t, h.store))
	assert.Equal(t, chunkID.FirstLedger(), mustOffsets(t, h.store).StartLedger())
}

func TestHotStore_IngestLedgerWritesAllCFs(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)

	p, keys := makePayload("transfer")
	require.NoError(t, ingestLedgerEvents(h.store, 2, []events.Payload{p}))

	// events_data row exists.
	got, found, err := h.store.chunkStore.Get(DataCF, encodeDataKey(0))
	require.NoError(t, err)
	require.True(t, found)
	var decoded events.Payload
	require.NoError(t, decoded.Unmarshal(got))
	assert.Equal(t, p.TxHash, decoded.TxHash)

	// events_index row per term.
	for _, key := range keys {
		_, found, err := h.store.chunkStore.Get(IndexCF, encodeIndexKey(key, 0))
		require.NoError(t, err)
		assert.True(t, found, "missing index row for term %x", key)
	}

	// events_offsets: cumulative = 1.
	offVal, found, err := h.store.chunkStore.Get(OffsetsCF, encodeOffsetKey(2))
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, uint32(1), binary.BigEndian.Uint32(offVal))

	// In-memory mirror sees the term.
	bm := lookupOne(t, h.store, keys[0])
	require.NotNil(t, bm)
	assert.True(t, bm.Contains(0))

	assert.Equal(t, uint32(1), mustEventCount(t, h.store))
}

func TestHotStore_EventIDsAreMonotonic(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)
	first := chunkID.FirstLedger()

	p1, _ := makePayload("a")
	p2, _ := makePayload("b")
	require.NoError(t, ingestLedgerEvents(h.store, first, []events.Payload{p1, p2}))

	p3, _ := makePayload("c")
	require.NoError(t, ingestLedgerEvents(h.store, first+1, []events.Payload{p3}))

	for id := range uint32(3) {
		_, found, err := h.store.chunkStore.Get(DataCF, encodeDataKey(id))
		require.NoError(t, err)
		assert.True(t, found, "missing event id %d", id)
	}
	assert.Equal(t, uint32(3), mustEventCount(t, h.store))
}

func TestHotStore_EmptyLedgerStillWritesOffsetsAndState(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)

	require.NoError(t, ingestLedgerEvents(h.store, 2, nil))

	val, found, err := h.store.chunkStore.Get(OffsetsCF, encodeOffsetKey(2))
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, uint32(0), binary.BigEndian.Uint32(val))
}

func TestHotStore_LookupReturnsImmutableSnapshot(t *testing.T) {
	// Pins the dense-mode contract: HotStore.LookupKeys returns
	// immutable snapshots of the live mirror. Writers (IngestLedgerEvents)
	// publish new snapshots via atomic.Pointer COW; the pointer
	// previously returned by LookupKeys is never mutated. A subsequent
	// IngestLedgerEvents must NOT affect a previously-returned
	// bitmap pointer — callers can safely retain the pointer
	// across writes.
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)

	p, keys := makePayload("snapshot")
	// Promote to dense mode so we exercise the bm.Load path (sparse
	// mode allocates a fresh bitmap per Get).
	for i := range uint32(70) {
		require.NoError(t, ingestLedgerEvents(h.store, 2+i, []events.Payload{p}))
	}

	first := lookupOne(t, h.store, keys[0])
	cardBefore := first.GetCardinality()

	// New ingest publishes a new snapshot. The old pointer must
	// remain unchanged (it's the previous snapshot).
	require.NoError(t, ingestLedgerEvents(h.store, 72, []events.Payload{p}))

	assert.Equal(t, cardBefore, first.GetCardinality(),
		"prior LookupKeys result must be an immutable snapshot — later IngestLedgerEvents must not mutate it")

	second := lookupOne(t, h.store, keys[0])
	assert.Equal(t, cardBefore+1, second.GetCardinality(),
		"subsequent LookupKeys must observe the new snapshot")
}

func TestHotStore_FetchEventsRoundTrip(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)

	p1, _ := makePayload("a")
	p2, _ := makePayload("b")
	require.NoError(t, ingestLedgerEvents(h.store, 2, []events.Payload{p1, p2}))
	p3, _ := makePayload("c")
	require.NoError(t, ingestLedgerEvents(h.store, 3, []events.Payload{p3}))

	fetched, err := h.store.FetchEvents(context.Background(), []uint32{0, 1, 2})
	require.NoError(t, err)
	require.Len(t, fetched, 3)
	assert.Equal(t, "a", dataSym(t, fetched[0]))
	assert.Equal(t, "b", dataSym(t, fetched[1]))
	assert.Equal(t, "c", dataSym(t, fetched[2]))
}

func TestHotStore_FetchEventsErrorsOnMissingID(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)
	p, _ := makePayload("only")
	require.NoError(t, ingestLedgerEvents(h.store, 2, []events.Payload{p}))

	_, err := h.store.FetchEvents(context.Background(), []uint32{99})
	assert.Error(t, err)
}

// TestHotStore_FetchEventsLargeBatch exercises the BatchMultiGet
// path with enough keys to defeat any per-iteration optimization
// the wrapper might hide. The serial-Get implementation passed this
// trivially; the batched implementation must produce the same
// positional alignment across ~hundreds of keys.
func TestHotStore_FetchEventsLargeBatch(t *testing.T) {
	const chunkID = chunk.ID(0)
	const n = 256
	h := openHotStoreForTest(t, chunkID)

	payloads := make([]events.Payload, n)
	for i := range n {
		p, _ := makePayload(fmt.Sprintf("evt-%03d", i))
		payloads[i] = p
	}
	require.NoError(t, ingestLedgerEvents(h.store, 2, payloads))

	ids := make([]uint32, n)
	for i := range n {
		ids[i] = uint32(i)
	}
	fetched, err := h.store.FetchEvents(context.Background(), ids)
	require.NoError(t, err)
	require.Len(t, fetched, n)
	for i := range n {
		expected := fmt.Sprintf("evt-%03d", i)
		assert.Equal(t, expected, dataSym(t, fetched[i]),
			"position %d", i)
	}
}

// TestHotStore_FetchEventsHonorsContext pins that a pre-canceled
// context is observed before the first Get fires — FetchEvents
// returns context.Canceled, not a partial slice. The serial Get loop
// checks ctx.Err() at the top of every iteration; with a non-empty
// input the very first iteration tickles the check.
func TestHotStore_FetchEventsHonorsContext(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)
	p, _ := makePayload("only")
	require.NoError(t, ingestLedgerEvents(h.store, 2, []events.Payload{p}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := h.store.FetchEvents(ctx, []uint32{0})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestHotStore_FetchEventsRejectsUnsortedInput pins the sorted-input
// precondition: unsorted positions are rejected up front with
// ErrUnsortedEventIDs before any I/O — mirrors the cold-side
// behavior. Covers both out-of-order and duplicate.
func TestHotStore_FetchEventsRejectsUnsortedInput(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)
	p1, _ := makePayload("a")
	p1.LedgerSequence = 2
	p2, _ := makePayload("b")
	p2.LedgerSequence = 2
	p3, _ := makePayload("c")
	p3.LedgerSequence = 2
	require.NoError(t, ingestLedgerEvents(h.store, 2, []events.Payload{p1, p2, p3}))

	_, err := h.store.FetchEvents(context.Background(), []uint32{2, 0})
	require.ErrorIs(t, err, ErrUnsortedEventIDs, "out-of-order input must error")

	_, err = h.store.FetchEvents(context.Background(), []uint32{0, 0})
	require.ErrorIs(t, err, ErrUnsortedEventIDs, "duplicate input must error")
}

func TestHotStore_AllStreamsInEventIDOrder(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)
	p1, _ := makePayload("a")
	p1.LedgerSequence = 2
	p2, _ := makePayload("b")
	p2.LedgerSequence = 2
	require.NoError(t, ingestLedgerEvents(h.store, 2, []events.Payload{p1, p2}))
	p3, _ := makePayload("c")
	p3.LedgerSequence = 3
	require.NoError(t, ingestLedgerEvents(h.store, 3, []events.Payload{p3}))

	got := make([]string, 0, 3)
	gotLedgers := make([]uint32, 0, 3)
	for p, err := range h.store.All(context.Background()) {
		require.NoError(t, err)
		got = append(got, dataSym(t, p))
		gotLedgers = append(gotLedgers, p.LedgerSequence)
	}
	assert.Equal(t, []string{"a", "b", "c"}, got)
	assert.Equal(t, []uint32{2, 2, 3}, gotLedgers)
}

func TestHotStore_AllEmptyChunkYieldsNothing(t *testing.T) {
	h := openHotStoreForTest(t, 0)
	var count int
	for _, err := range h.store.All(context.Background()) {
		require.NoError(t, err)
		count++
	}
	assert.Zero(t, count)
}

func TestHotStore_CloseRejectsWrites(t *testing.T) {
	h := openHotStoreForTest(t, 0)
	require.NoError(t, h.raw.Close())
	err := ingestLedgerEvents(h.store, 2, nil)
	assert.ErrorIs(t, err, stores.ErrStoreClosed)
}

// TestHotStore_PostCloseReadsError pins the contract that read methods
// fail loudly after Close. Pre-fix: LookupKeys only touched the in-memory
// mirror and returned the cached bitmaps silently, even after Close had
// released chunkStore — the only "this store is gone" signal callers
// got was when they tried to FetchEvents and hit a closed RocksDB.
func TestHotStore_PostCloseReadsError(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)

	p, keys := makePayload("seed")
	require.NoError(t, ingestLedgerEvents(h.store, chunkID.FirstLedger(), []events.Payload{p}))
	require.NoError(t, h.raw.Close())

	// LookupKeys must error rather than silently returning cached bitmaps.
	bms, err := h.store.LookupKeys(context.Background(), []events.TermKey{keys[0]})
	assert.Nil(t, bms)
	require.ErrorIs(t, err, stores.ErrStoreClosed)

	// FetchEvents returns ErrStoreClosed.
	_, err = h.store.FetchEvents(context.Background(), []uint32{0})
	require.ErrorIs(t, err, stores.ErrStoreClosed)

	// All iterator yields ErrStoreClosed on first step.
	require.ErrorIs(t, firstIterError(h.store.All(context.Background())), stores.ErrStoreClosed)

	// Post-Close: ChunkID still works (constructor param);
	// EventCount and Offsets return ErrStoreClosed.
	assert.Equal(t, chunkID, h.store.ChunkID())
	_, err = h.store.EventCount()
	require.ErrorIs(t, err, stores.ErrStoreClosed)
	_, err = h.store.Offsets()
	require.ErrorIs(t, err, stores.ErrStoreClosed)
}

// TestHotStore_IngestLedgerEvents_DuplicateLedgerErrors pins the sequencing
// contract after the staging collapse (#30): re-ingesting an already-committed
// ledger is NOT a silent no-op — it is a mis-sequencing error (ErrLedgerOutOfOrder)
// that leaves state untouched (Store.Batch discards the WriteBatch on the error).
// Under decision (a) the ingestion loop always resumes at MaxCommittedSeq+1 and
// the shared cursor validates contiguity, so a duplicate can only mean a broken
// source — an error, never silent tolerance.
func TestHotStore_IngestLedgerEvents_DuplicateLedgerErrors(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)
	first := chunkID.FirstLedger()

	p1, _ := makePayload("a")
	require.NoError(t, ingestLedgerEvents(h.store, first, []events.Payload{p1}))

	countBefore := mustEventCount(t, h.store)

	// Re-ingesting the same ledger errors (expected is now first+1).
	p2, _ := makePayload("b")
	err := ingestLedgerEvents(h.store, first, []events.Payload{p2})
	require.ErrorIs(t, err, ErrLedgerOutOfOrder, "a re-delivered committed ledger must error, not no-op")

	assert.Equal(t, countBefore, mustEventCount(t, h.store), "event count must not advance on the rejected ingest")

	// The original ledger's event is untouched, and the rejected batch committed
	// nothing (Store.Batch discards the WriteBatch on the callback error).
	got, err := h.store.FetchEvents(context.Background(), []uint32{0})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "a", dataSym(t, got[0]), "original event must survive the rejected re-ingest")

	// The rejected payload must not reach the mirror. makePayload emits
	// [contractID, topic0, ...]; contractID is shared across symbols
	// (hardcoded 0xab), so we check topic0 (index 1), which is symbol-specific.
	_, secondKeys := makePayload("b")
	require.GreaterOrEqual(t, len(secondKeys), 2, "test fixture expected to have a topic0 term")
	assert.Nil(t, lookupOne(t, h.store, secondKeys[1]),
		"the rejected payload's topic0 term must not appear in the mirror")
}

// TestHotStore_IngestLedgerEvents_RejectsLedgerGap pins the contract
// that skipping a ledger errors out without committing.
func TestHotStore_IngestLedgerEvents_RejectsLedgerGap(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)
	first := chunkID.FirstLedger()

	p1, _ := makePayload("a")
	require.NoError(t, ingestLedgerEvents(h.store, first, []events.Payload{p1}))

	countBefore := mustEventCount(t, h.store)

	// Skip first+1; jump directly to first+2.
	p2, _ := makePayload("c")
	err := ingestLedgerEvents(h.store, first+2, []events.Payload{p2})
	require.ErrorIs(t, err, ErrLedgerOutOfOrder)

	assert.Equal(t, countBefore, mustEventCount(t, h.store))
}

// TestHotStore_IngestLedgerEvents_RejectsOutOfRangeLedger pins the
// contract that a ledger outside [FirstLedger, LastLedger] is rejected.
func TestHotStore_IngestLedgerEvents_RejectsOutOfRangeLedger(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)

	p, _ := makePayload("a")

	// Below range (chunk 0's FirstLedger is FirstLedgerSeq == 2).
	err := ingestLedgerEvents(h.store, 1, []events.Payload{p})
	require.ErrorIs(t, err, ErrLedgerOutOfRange, "ledger below chunk range")

	// Above range — well past chunk 0's LastLedger.
	err = ingestLedgerEvents(h.store, chunkID.LastLedger()+1, []events.Payload{p})
	require.ErrorIs(t, err, ErrLedgerOutOfRange, "ledger above chunk range")

	// State must be unchanged after both rejections.
	assert.Equal(t, uint32(0), mustEventCount(t, h.store))
}

func TestHotStore_CloseIsIdempotent(t *testing.T) {
	h := openHotStoreForTest(t, 0)
	require.NoError(t, h.raw.Close())
	assert.NoError(t, h.raw.Close())
}

func TestHotStore_ReopenRecoversState(t *testing.T) {
	// Open + ingest + close + reopen + ingest. Recovery should
	// reconstruct the mirror + offsets so the second writer assigns
	// event IDs continuing from where the first left off.
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	hot1, raw1 := openHotStoreForTestAt(t, dir, chunkID)
	p1, _ := makePayload("before")
	require.NoError(t, ingestLedgerEvents(hot1, 2, []events.Payload{p1}))
	require.NoError(t, raw1.Close())

	hot2, _ := openHotStoreForTestAt(t, dir, chunkID)

	assert.Equal(t, uint32(1), mustEventCount(t, hot2), "warmup recovered offsets")

	p2, _ := makePayload("after")
	require.NoError(t, ingestLedgerEvents(hot2, 3, []events.Payload{p2}))
	assert.Equal(t, uint32(2), mustEventCount(t, hot2))
}

func TestHotStore_SatisfiesReader(t *testing.T) {
	// Compile-time guard already enforces this via the package-level
	// var declaration; this test demonstrates callers can hold a
	// *HotStore as a Reader for the freeze path and query coordinator.
	h := openHotStoreForTest(t, 0)
	var r Reader = h.store
	assert.Equal(t, chunk.ID(0), r.ChunkID())
}

func TestHotStore_ConcurrentIngestAndLookup(t *testing.T) {
	// Smoke test under -race: drive ingest on one goroutine and
	// Lookup on another for a few hundred iterations. Catches
	// missing locks at the IngestLedgerEvents / Lookup boundary.
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)

	p, keys := makePayload("concurrent")
	const N = 200

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range uint32(N) {
			if err := ingestLedgerEvents(h.store, 2+i, []events.Payload{p}); err != nil {
				t.Errorf("ingest %d: %v", i, err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for range N {
			// A miss during the race window (writer hasn't ingested
			// yet) is a nil bitmap, not an error — any error is a bug.
			if _, err := h.store.LookupKeys(context.Background(), keys[:1]); err != nil {
				t.Errorf("lookup: %v", err)
				return
			}
		}
	}()
	wg.Wait()
	assert.Equal(t, uint32(N), mustEventCount(t, h.store))
}

// fetchRangePayloads fully drains FetchRange into a slice for tests
// that want to compare against a known sequence. Shared with
// cold_reader_test.go via the package.
func fetchRangePayloads(t *testing.T, r Reader, start, count uint32) ([]events.Payload, error) {
	t.Helper()
	var out []events.Payload
	var firstErr error
	for p, err := range r.FetchRange(context.Background(), start, count) {
		if err != nil {
			firstErr = err
			break
		}
		// FetchRange yields borrowed Payloads (ContractEventBytes valid
		// only for the step); clone to retain past the loop.
		p.ContractEventBytes = bytes.Clone(p.ContractEventBytes)
		out = append(out, p)
	}
	return out, firstErr
}

// firstIterError returns the first non-nil error yielded by seq,
// or nil if the sequence finishes without one. Used by tests that
// pin "after Close, this iterator yields stores.ErrStoreClosed and
// stops" — the iterator returns immediately after the error yield,
// so the helper doesn't walk a long sequence in practice. Shared
// with cold_reader_test.go.
func firstIterError(seq iter.Seq2[events.Payload, error]) error {
	for _, err := range seq {
		if err != nil {
			return err
		}
	}
	return nil
}

// lookupOne resolves a single term through the batched LookupKeys API
// and returns its bitmap (nil on a clean miss). Shared by the
// hot/cold/query tests that assert on one term at a time. It requires
// LookupKeys to succeed, so closed/corrupt-path tests must call
// LookupKeys directly and assert on the error.
func lookupOne(t *testing.T, r Reader, key events.TermKey) *roaring.Bitmap {
	t.Helper()
	bms, err := r.LookupKeys(context.Background(), []events.TermKey{key})
	require.NoError(t, err)
	require.Len(t, bms, 1)
	return bms[0]
}

func TestHotStore_FetchRangeMidRange(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)
	first := chunkID.FirstLedger()

	payloads := make([]events.Payload, 5)
	for i := range payloads {
		p, _ := makePayload(fmt.Sprintf("evt-%d", i))
		payloads[i] = p
	}
	require.NoError(t, ingestLedgerEvents(h.store, first, payloads))

	got, err := fetchRangePayloads(t, h.store, 1, 3)
	require.NoError(t, err)
	require.Len(t, got, 3)
	for i, p := range got {
		want := fmt.Sprintf("evt-%d", i+1)
		assert.Equal(t, want, dataSym(t, p))
	}
}

func TestHotStore_FetchRangeZeroCountYieldsNothing(t *testing.T) {
	h := openHotStoreForTest(t, 0)
	got, err := fetchRangePayloads(t, h.store, 0, 0)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestHotStore_FetchRangeOutOfBoundsErrors(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)
	p, _ := makePayload("only")
	require.NoError(t, ingestLedgerEvents(h.store, chunkID.FirstLedger(), []events.Payload{p}))

	_, err := fetchRangePayloads(t, h.store, 0, 2) // count > EventCount
	require.Error(t, err)
	_, err = fetchRangePayloads(t, h.store, 1, 1) // start at EventCount
	require.Error(t, err)
}

func TestHotStore_FetchRangePostCloseYieldsErrClosed(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)
	require.NoError(t, h.raw.Close())

	require.ErrorIs(t, firstIterError(h.store.FetchRange(context.Background(), 0, 1)), stores.ErrStoreClosed)
}

func TestHotStore_AllMatchesFetchRange(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)
	first := chunkID.FirstLedger()
	payloads := make([]events.Payload, 4)
	for i := range payloads {
		p, _ := makePayload(fmt.Sprintf("e%d", i))
		payloads[i] = p
	}
	require.NoError(t, ingestLedgerEvents(h.store, first, payloads))

	allSyms := make([]string, 0, len(payloads))
	for p, err := range h.store.All(context.Background()) {
		require.NoError(t, err)
		allSyms = append(allSyms, dataSym(t, p))
	}
	got, err := fetchRangePayloads(t, h.store, 0, uint32(len(payloads)))
	require.NoError(t, err)
	rangeSyms := make([]string, 0, len(got))
	for _, p := range got {
		rangeSyms = append(rangeSyms, dataSym(t, p))
	}
	assert.Equal(t, allSyms, rangeSyms)
}

// mustEventCount asserts r.EventCount() succeeds and returns the
// count. Reduces test-side boilerplate around the (uint32, error)
// signature; failures bail out cleanly via t.Fatal.
func mustEventCount(t *testing.T, r Reader) uint32 {
	t.Helper()
	c, err := r.EventCount()
	require.NoError(t, err)
	return c
}

// mustOffsets asserts r.Offsets() succeeds and returns the offsets.
func mustOffsets(t *testing.T, r Reader) *events.LedgerOffsets {
	t.Helper()
	o, err := r.Offsets()
	require.NoError(t, err)
	require.NotNil(t, o)
	return o
}

// ingestLedgerEvents commits one ledger's events through IngestLedgerToBatch in
// a test-owned batch and runs the post-commit apply hook — the production
// write shape, reduced to a test seeding call.
func ingestLedgerEvents(h *HotStore, ledgerSeq uint32, payloads []events.Payload) error {
	if h.chunkStore.IsClosed() {
		return stores.ErrStoreClosed
	}
	var apply func()
	if err := h.chunkStore.Batch(func(b *rocksdb.BatchWriter) error {
		a, aerr := h.IngestLedgerToBatch(b, ledgerSeq, payloads)
		apply = a
		return aerr
	}); err != nil {
		return err
	}
	if apply != nil {
		apply()
	}
	return nil
}
