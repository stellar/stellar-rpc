package eventstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
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
}

// openHotStoreForTest opens a fresh per-Chunk hot DB for chunkID
// under a single t.TempDir() and returns a HotStore wrapping it. The
// DB auto-closes on test cleanup.
//
//nolint:unparam // chunkID kept as a param so tests can vary it; today every caller uses 0
func openHotStoreForTest(t *testing.T, chunkID chunk.ID) *hotStoreHarness {
	t.Helper()
	dir := t.TempDir()

	hot, err := OpenHotStore(dir, chunkID, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = hot.Close() })

	return &hotStoreHarness{dataDir: dir, store: hot}
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
	p := events.Payload{
		TxHash:        xdr.Hash{0xde, 0xad},
		TxIdx:         1,
		EventIdx:      0,
		ContractEvent: ev,
	}
	keys, err := events.TermsFor(ev)
	if err != nil {
		panic(err) // hardcoded test fixture; an error here is a test bug
	}
	return p, keys
}

func TestOpenHotStore_RequiresDataDirAndLogger(t *testing.T) {
	dir := t.TempDir()

	_, err := OpenHotStore("", 0, silentLogger())
	require.Error(t, err, "missing dataDir")

	_, err = OpenHotStore(dir, 0, nil)
	require.Error(t, err, "missing logger")
}

func TestHotStore_FreshChunkHasEmptyState(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)

	assert.Equal(t, chunkID, h.store.ChunkID())
	assert.Equal(t, uint32(0), h.store.EventCount())
	assert.Equal(t, uint32(0), h.store.NextEventID())
	assert.Equal(t, chunkID.FirstLedger(), h.store.Offsets().StartLedger())
}

func TestHotStore_IngestLedgerWritesAllCFs(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)

	p, keys := makePayload("transfer")
	require.NoError(t, h.store.IngestLedgerEvents(2, []events.Payload{p}))

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
	bm, err := h.store.Lookup(keys[0])
	require.NoError(t, err)
	require.NotNil(t, bm)
	assert.True(t, bm.Contains(0))

	assert.Equal(t, uint32(1), h.store.NextEventID())
}

func TestHotStore_EventIDsAreMonotonic(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)
	first := chunkID.FirstLedger()

	p1, _ := makePayload("a")
	p2, _ := makePayload("b")
	require.NoError(t, h.store.IngestLedgerEvents(first, []events.Payload{p1, p2}))

	p3, _ := makePayload("c")
	require.NoError(t, h.store.IngestLedgerEvents(first+1, []events.Payload{p3}))

	for id := range uint32(3) {
		_, found, err := h.store.chunkStore.Get(DataCF, encodeDataKey(id))
		require.NoError(t, err)
		assert.True(t, found, "missing event id %d", id)
	}
	assert.Equal(t, uint32(3), h.store.NextEventID())
}

func TestHotStore_EmptyLedgerStillWritesOffsetsAndState(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)

	require.NoError(t, h.store.IngestLedgerEvents(2, nil))

	val, found, err := h.store.chunkStore.Get(OffsetsCF, encodeOffsetKey(2))
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, uint32(0), binary.BigEndian.Uint32(val))
}

func TestHotStore_LookupReturnsCloneNotLive(t *testing.T) {
	// Mutating a Lookup result must not bleed back into the mirror.
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)

	p, keys := makePayload("clone-me")
	// Promote to a roaring bitmap so events.MemBitmaps takes the clone-on-Get
	// path (sparse term entries return a different shape).
	for i := range uint32(70) {
		require.NoError(t, h.store.IngestLedgerEvents(2+i, []events.Payload{p}))
	}

	first, err := h.store.Lookup(keys[0])
	require.NoError(t, err)
	first.Add(999_999)

	second, err := h.store.Lookup(keys[0])
	require.NoError(t, err)
	assert.False(t, second.Contains(999_999), "Lookup must clone, not leak the live bitmap")
}

func TestHotStore_FetchEventsRoundTrip(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)

	p1, _ := makePayload("a")
	p2, _ := makePayload("b")
	require.NoError(t, h.store.IngestLedgerEvents(2, []events.Payload{p1, p2}))
	p3, _ := makePayload("c")
	require.NoError(t, h.store.IngestLedgerEvents(3, []events.Payload{p3}))

	var fetched []events.Payload
	for p, err := range h.store.FetchEvents([]uint32{0, 1, 2}) {
		require.NoError(t, err)
		fetched = append(fetched, p)
	}
	require.Len(t, fetched, 3)
	assert.Equal(t, "a", string(*fetched[0].ContractEvent.Body.V0.Data.Sym))
	assert.Equal(t, "b", string(*fetched[1].ContractEvent.Body.V0.Data.Sym))
	assert.Equal(t, "c", string(*fetched[2].ContractEvent.Body.V0.Data.Sym))
}

func TestHotStore_FetchEventsPreservesOrder(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)
	p1, _ := makePayload("a")
	p2, _ := makePayload("b")
	p3, _ := makePayload("c")
	require.NoError(t, h.store.IngestLedgerEvents(2, []events.Payload{p1, p2, p3}))

	got := make([]string, 0, 3)
	for p, err := range h.store.FetchEvents([]uint32{2, 0, 1}) {
		require.NoError(t, err)
		got = append(got, string(*p.ContractEvent.Body.V0.Data.Sym))
	}
	assert.Equal(t, []string{"c", "a", "b"}, got)
}

func TestHotStore_FetchEventsErrorsOnMissingID(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)
	p, _ := makePayload("only")
	require.NoError(t, h.store.IngestLedgerEvents(2, []events.Payload{p}))

	var sawErr bool
	for _, err := range h.store.FetchEvents([]uint32{99}) {
		if err != nil {
			sawErr = true
		}
	}
	assert.True(t, sawErr)
}

func TestHotStore_AllStreamsInEventIDOrder(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)
	p1, _ := makePayload("a")
	p1.LedgerSequence = 2
	p2, _ := makePayload("b")
	p2.LedgerSequence = 2
	require.NoError(t, h.store.IngestLedgerEvents(2, []events.Payload{p1, p2}))
	p3, _ := makePayload("c")
	p3.LedgerSequence = 3
	require.NoError(t, h.store.IngestLedgerEvents(3, []events.Payload{p3}))

	got := make([]string, 0, 3)
	gotLedgers := make([]uint32, 0, 3)
	for p, err := range h.store.All() {
		require.NoError(t, err)
		got = append(got, string(*p.ContractEvent.Body.V0.Data.Sym))
		gotLedgers = append(gotLedgers, p.LedgerSequence)
	}
	assert.Equal(t, []string{"a", "b", "c"}, got)
	assert.Equal(t, []uint32{2, 2, 3}, gotLedgers)
}

func TestHotStore_AllEmptyChunkYieldsNothing(t *testing.T) {
	h := openHotStoreForTest(t, 0)
	var count int
	for _, err := range h.store.All() {
		require.NoError(t, err)
		count++
	}
	assert.Zero(t, count)
}

func TestHotStore_CloseRejectsWrites(t *testing.T) {
	h := openHotStoreForTest(t, 0)
	require.NoError(t, h.store.Close())
	err := h.store.IngestLedgerEvents(2, nil)
	assert.ErrorIs(t, err, ErrClosed)
}

// TestHotStore_PostCloseReadsError pins the contract that read methods
// fail loudly after Close. Pre-fix: Lookup only touched the in-memory
// mirror and returned the cached bitmap silently, even after Close had
// released chunkStore — the only "this store is gone" signal callers
// got was when they tried to FetchEvents and hit a closed RocksDB.
func TestHotStore_PostCloseReadsError(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)

	p, keys := makePayload("seed")
	require.NoError(t, h.store.IngestLedgerEvents(chunkID.FirstLedger(), []events.Payload{p}))
	require.NoError(t, h.store.Close())

	// Lookup must error rather than silently returning the cached bitmap.
	bm, err := h.store.Lookup(keys[0])
	assert.Nil(t, bm)
	require.ErrorIs(t, err, ErrClosed)

	// FetchEvents iterator yields ErrClosed on first step.
	var sawErr error
	for _, e := range h.store.FetchEvents([]uint32{0}) {
		if e != nil {
			sawErr = e
			break
		}
	}
	require.ErrorIs(t, sawErr, ErrClosed)

	// All iterator yields ErrClosed on first step.
	sawErr = nil
	for _, e := range h.store.All() {
		if e != nil {
			sawErr = e
			break
		}
	}
	require.ErrorIs(t, sawErr, ErrClosed)

	// Metadata accessors survive Close: same contract as ColdReader.
	assert.Equal(t, chunkID, h.store.ChunkID())
	assert.Equal(t, uint32(1), h.store.EventCount())
	assert.NotNil(t, h.store.Offsets())
}

// TestHotStore_IngestLedgerEvents_RejectsDuplicateLedger pins the
// contract that ingesting the same ledger twice errors out cleanly,
// without committing the second batch. Pre-fix this silently wrote a
// second event row, advanced eventID, and clobbered the cumulative
// offset for that ledger.
func TestHotStore_IngestLedgerEvents_RejectsDuplicateLedger(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)
	first := chunkID.FirstLedger()

	p1, _ := makePayload("a")
	require.NoError(t, h.store.IngestLedgerEvents(first, []events.Payload{p1}))

	countBefore := h.store.EventCount()
	nextBefore := h.store.NextEventID()

	// Second ingest of the same ledger must fail and leave state untouched.
	p2, _ := makePayload("b")
	err := h.store.IngestLedgerEvents(first, []events.Payload{p2})
	require.ErrorIs(t, err, ErrLedgerOutOfOrder)

	assert.Equal(t, countBefore, h.store.EventCount(), "EventCount must not advance on rejected duplicate ingest")
	assert.Equal(t, nextBefore, h.store.NextEventID(), "NextEventID must not advance on rejected duplicate ingest")

	// Term mirror must not gain any entry from the rejected payload.
	// makePayload emits [contractID, topic0, ...]; contractID is
	// shared across all makePayload symbols (hardcoded 0xab), so we
	// check topic0 (index 1) which is symbol-specific.
	_, secondKeys := makePayload("b")
	require.GreaterOrEqual(t, len(secondKeys), 2, "test fixture expected to have a topic0 term")
	bm, lookupErr := h.store.Lookup(secondKeys[1])
	require.ErrorIs(t, lookupErr, ErrTermNotFound,
		"second payload's topic0 term must not appear in the mirror after rejection")
	assert.Nil(t, bm)
}

// TestHotStore_IngestLedgerEvents_RejectsLedgerGap pins the contract
// that skipping a ledger errors out without committing.
func TestHotStore_IngestLedgerEvents_RejectsLedgerGap(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)
	first := chunkID.FirstLedger()

	p1, _ := makePayload("a")
	require.NoError(t, h.store.IngestLedgerEvents(first, []events.Payload{p1}))

	countBefore := h.store.EventCount()
	nextBefore := h.store.NextEventID()

	// Skip first+1; jump directly to first+2.
	p2, _ := makePayload("c")
	err := h.store.IngestLedgerEvents(first+2, []events.Payload{p2})
	require.ErrorIs(t, err, ErrLedgerOutOfOrder)

	assert.Equal(t, countBefore, h.store.EventCount())
	assert.Equal(t, nextBefore, h.store.NextEventID())
}

// TestHotStore_IngestLedgerEvents_RejectsOutOfRangeLedger pins the
// contract that a ledger outside [FirstLedger, LastLedger] is rejected.
func TestHotStore_IngestLedgerEvents_RejectsOutOfRangeLedger(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)

	p, _ := makePayload("a")

	// Below range (chunk 0's FirstLedger is FirstLedgerSeq == 2).
	err := h.store.IngestLedgerEvents(1, []events.Payload{p})
	require.ErrorIs(t, err, ErrLedgerOutOfRange, "ledger below chunk range")

	// Above range — well past chunk 0's LastLedger.
	err = h.store.IngestLedgerEvents(chunkID.LastLedger()+1, []events.Payload{p})
	require.ErrorIs(t, err, ErrLedgerOutOfRange, "ledger above chunk range")

	// State must be unchanged after both rejections.
	assert.Equal(t, uint32(0), h.store.EventCount())
	assert.Equal(t, uint32(0), h.store.NextEventID())
}

func TestHotStore_CloseIsIdempotent(t *testing.T) {
	h := openHotStoreForTest(t, 0)
	require.NoError(t, h.store.Close())
	assert.NoError(t, h.store.Close())
}

func TestHotStore_ReopenRecoversState(t *testing.T) {
	// Open + ingest + close + reopen + ingest. Recovery should
	// reconstruct the mirror + offsets so the second writer assigns
	// event IDs continuing from where the first left off.
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	hot1, err := OpenHotStore(dir, chunkID, silentLogger())
	require.NoError(t, err)
	p1, _ := makePayload("before")
	require.NoError(t, hot1.IngestLedgerEvents(2, []events.Payload{p1}))
	require.NoError(t, hot1.Close())

	hot2, err := OpenHotStore(dir, chunkID, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = hot2.Close() })

	assert.Equal(t, uint32(1), hot2.NextEventID(), "warmup recovered offsets")

	p2, _ := makePayload("after")
	require.NoError(t, hot2.IngestLedgerEvents(3, []events.Payload{p2}))
	assert.Equal(t, uint32(2), hot2.NextEventID())
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
			if err := h.store.IngestLedgerEvents(2+i, []events.Payload{p}); err != nil {
				t.Errorf("ingest %d: %v", i, err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for range N {
			// ErrTermNotFound is expected during the race window
			// where the writer goroutine hasn't ingested yet.
			if _, err := h.store.Lookup(keys[0]); err != nil && !errors.Is(err, ErrTermNotFound) {
				t.Errorf("lookup: %v", err)
				return
			}
		}
	}()
	wg.Wait()
	assert.Equal(t, uint32(N), h.store.NextEventID())
}
