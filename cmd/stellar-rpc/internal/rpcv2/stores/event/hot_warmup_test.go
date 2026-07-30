package event

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
)

// These tests exercise the (unexported) warmup() function indirectly
// through NewWithStore over an explicitly opened RocksDB store. They
// document the "fresh chunk → empty caches", "ingested chunk →
// reconstructed caches" contract.

func TestWarmup_FreshChunkProducesEmptyMirrorsViaNewWithStore(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)

	// A fresh mirror is empty: probing any term is a clean miss
	// (nil bitmap, no error).
	bm, err := h.store.mirror.Get(events.ComputeTermKey([]byte("any"), events.FieldContractID))
	require.NoError(t, err)
	assert.Nil(t, bm)
	assert.Zero(t, h.store.offsets.LedgerCount())
	assert.Equal(t, uint32(0), h.store.offsets.TotalEvents())
	assert.Equal(t, chunkID.FirstLedger(), h.store.offsets.StartLedger())
}

func TestWarmup_RebuildsMirrorFromIngestedRows(t *testing.T) {
	// Ingest into one HotStore, close, reopen; the reopened store's
	// mirror must hold the same terms (warmup replayed them from
	// events_index). One-directional by design: every seeded term
	// must survive with the right cardinality, but a phantom extra
	// term in the reopened mirror would pass unnoticed —
	// ConcurrentBitmaps deliberately has no term enumeration to
	// check the other direction with.
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	hot1, raw1 := openHotStoreForTestAt(t, dir, chunkID)
	p1, _ := makePayload("alpha")
	p2, _ := makePayload("beta")
	require.NoError(t, ingestLedgerEvents(hot1, 2, []events.Payload{p1, p2}))

	// Derive the terms each seeded payload contributes (the same
	// events.TermsForBytes the ingest path indexed by), then snapshot
	// their pre-close cardinalities straight from the live mirror.
	var seededKeys []events.TermKey
	for _, p := range []events.Payload{p1, p2} {
		keys, err := events.TermsForBytes(p.ContractEventBytes)
		require.NoError(t, err)
		seededKeys = append(seededKeys, keys...)
	}
	expected := make(map[events.TermKey]uint64)
	for _, k := range seededKeys {
		bm, err := hot1.mirror.Get(k)
		require.NoError(t, err)
		require.NotNil(t, bm, "seeded term missing from the pre-close mirror")
		expected[k] = bm.GetCardinality()
	}
	require.NoError(t, raw1.Close())

	// Reopen — warmup replays events_index into a fresh mirror.
	hot2, _ := openHotStoreForTestAt(t, dir, chunkID)

	got := make(map[events.TermKey]uint64)
	for k := range expected {
		bm, err := hot2.mirror.Get(k)
		require.NoError(t, err)
		require.NotNil(t, bm, "seeded term missing from the reopened mirror")
		got[k] = bm.GetCardinality()
	}
	assert.Equal(t, expected, got)
}

func TestWarmup_RestoresEventIDsForRepeatedTerm(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	hot1, raw1 := openHotStoreForTestAt(t, dir, chunkID)
	p1, _ := makePayload("shared")
	p2, _ := makePayload("shared")
	p3, _ := makePayload("shared")
	require.NoError(t, ingestLedgerEvents(hot1, 2, []events.Payload{p1, p2, p3}))
	require.NoError(t, raw1.Close())

	hot2, _ := openHotStoreForTestAt(t, dir, chunkID)

	contractTermKey := events.ComputeTermKey(eventOf(p1).ContractId[:], events.FieldContractID)
	bm := lookupOne(t, hot2, contractTermKey)
	require.NotNil(t, bm)
	assert.Equal(t, uint64(3), bm.GetCardinality())
	for id := range uint32(3) {
		assert.True(t, bm.Contains(id), "warmup missed event id %d", id)
	}
}

func TestWarmup_OffsetsReconstructedAcrossLedgers(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	hot1, raw1 := openHotStoreForTestAt(t, dir, chunkID)
	p1, _ := makePayload("a")
	p2, _ := makePayload("b")
	require.NoError(t, ingestLedgerEvents(hot1, 2, []events.Payload{p1, p2}))
	p3, _ := makePayload("c")
	require.NoError(t, ingestLedgerEvents(hot1, 3, []events.Payload{p3}))
	require.NoError(t, raw1.Close())

	hot2, _ := openHotStoreForTestAt(t, dir, chunkID)

	assert.Equal(t, uint32(3), mustEventCount(t, hot2))

	start, end, err := mustOffsets(t, hot2).EventIDs(2)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), start)
	assert.Equal(t, uint32(2), end)

	start, end, err = mustOffsets(t, hot2).EventIDs(3)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), start)
	assert.Equal(t, uint32(3), end)
}

// corruptHotChunk reopens chunkID's raw per-chunk DB (bypassing warmup),
// applies mutate, and closes it — used to inject on-disk inconsistencies
// that warmup's verifyChunkConsistency must reject. The HotStore for
// chunkID must already be closed so the LOCK is free.
//
//nolint:unparam // chunkID kept as a param for call-site clarity; today every caller uses 0
func corruptHotChunk(t *testing.T, dir string, chunkID chunk.ID, mutate func(raw *rocksdb.Store)) {
	t.Helper()
	raw := openRawHotChunkForTest(t, dir, chunkID)
	defer func() { require.NoError(t, raw.Close()) }() // release LOCK even if mutate fails
	mutate(raw)
}

func TestWarmup_RejectsDataEventBeyondOffsets(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	hot1, raw1 := openHotStoreForTestAt(t, dir, chunkID)
	p1, _ := makePayload("a")
	p2, _ := makePayload("b")
	require.NoError(t, ingestLedgerEvents(hot1, 2, []events.Payload{p1, p2})) // total = 2
	require.NoError(t, raw1.Close())

	// An orphan data row well beyond total (id 7, total = 2): proves the
	// check catches any id >= total, not just one past the boundary.
	corruptHotChunk(t, dir, chunkID, func(raw *rocksdb.Store) {
		require.NoError(t, raw.Put(DataCF, encodeDataKey(7), []byte("orphan")))
	})

	_, _, err := tryOpenHotStoreForTest(t, dir, chunkID)
	// Branch-specific substring: every corruption shares "corrupt chunk",
	// so assert the data-orphan message to prove this branch fired.
	require.ErrorContains(t, err, "data present at id >= committed count")
}

func TestWarmup_RejectsOffsetsGap(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	hot1, raw1 := openHotStoreForTestAt(t, dir, chunkID)
	for _, seq := range []uint32{2, 3, 4} {
		p, _ := makePayload("x")
		require.NoError(t, ingestLedgerEvents(hot1, seq, []events.Payload{p}))
	}
	require.NoError(t, raw1.Close())

	// Drop ledger 3's offset row: warmup then iterates 2, 4 and must
	// reject the gap. This is the sequence check that moved out of
	// ConcurrentLedgerOffsets.Append into warmupOffsets' trust boundary.
	corruptHotChunk(t, dir, chunkID, func(raw *rocksdb.Store) {
		require.NoError(t, raw.Delete(OffsetsCF, encodeOffsetKey(3)))
	})

	_, _, err := tryOpenHotStoreForTest(t, dir, chunkID)
	require.ErrorContains(t, err, "expected ledger 3, got 4")
}

func TestWarmup_RejectsOffsetsOverflow(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	hot1, raw1 := openHotStoreForTestAt(t, dir, chunkID)
	for _, seq := range []uint32{2, 3} {
		p, _ := makePayload("x")
		require.NoError(t, ingestLedgerEvents(hot1, seq, []events.Payload{p}))
	}
	require.NoError(t, raw1.Close())

	// Overwrite the offset rows with counts that sum past uint32: warmup
	// must reject the cumulative overflow rather than silently wrapping.
	corruptHotChunk(t, dir, chunkID, func(raw *rocksdb.Store) {
		require.NoError(t, raw.Put(OffsetsCF, encodeOffsetKey(2), encodeLedgerEventCount(3_000_000_000)))
		require.NoError(t, raw.Put(OffsetsCF, encodeOffsetKey(3), encodeLedgerEventCount(2_000_000_000)))
	})

	_, _, err := tryOpenHotStoreForTest(t, dir, chunkID)
	require.ErrorContains(t, err, "cumulative event count overflow")
}

func TestWarmup_RejectsOrphanInEmptyChunk(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	_, raw1 := openHotStoreForTestAt(t, dir, chunkID)
	require.NoError(t, raw1.Close()) // total = 0, nothing committed

	// A data row in a chunk that committed nothing: total == 0, so the
	// tail Get is skipped and the orphan scan must fire from id 0.
	corruptHotChunk(t, dir, chunkID, func(raw *rocksdb.Store) {
		require.NoError(t, raw.Put(DataCF, encodeDataKey(0), []byte("orphan")))
	})

	_, _, err := tryOpenHotStoreForTest(t, dir, chunkID)
	require.ErrorContains(t, err, "data present at id >= committed count 0")
}

func TestWarmup_RejectsMissingTailDataEvent(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	hot1, raw1 := openHotStoreForTestAt(t, dir, chunkID)
	p1, _ := makePayload("a")
	p2, _ := makePayload("b")
	require.NoError(t, ingestLedgerEvents(hot1, 2, []events.Payload{p1, p2})) // total = 2
	require.NoError(t, raw1.Close())

	// Drop the last data row (event id total-1 == 1) while offsets still
	// count 2.
	corruptHotChunk(t, dir, chunkID, func(raw *rocksdb.Store) {
		require.NoError(t, raw.Delete(DataCF, encodeDataKey(1)))
	})

	_, _, err := tryOpenHotStoreForTest(t, dir, chunkID)
	require.ErrorContains(t, err, "missing from data")
}

func TestWarmup_RejectsIndexBeyondCommitted(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	hot1, raw1 := openHotStoreForTestAt(t, dir, chunkID)
	p1, _ := makePayload("a")
	p2, _ := makePayload("b")
	require.NoError(t, ingestLedgerEvents(hot1, 2, []events.Payload{p1, p2})) // total = 2
	require.NoError(t, raw1.Close())

	// An index row at exactly total (id 2): the tightest "beyond
	// committed" case, pinning the > (not >=) bound — valid ids are 0..1.
	corruptHotChunk(t, dir, chunkID, func(raw *rocksdb.Store) {
		var term events.TermKey
		term[0] = 0x99
		require.NoError(t, raw.Put(IndexCF, encodeIndexKey(term, 2), nil))
	})

	_, _, err := tryOpenHotStoreForTest(t, dir, chunkID)
	require.ErrorContains(t, err, "index references event 2 but only 2 committed")
}

func TestWarmup_OffsetsHandleEmptyTrailingLedger(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	hot1, raw1 := openHotStoreForTestAt(t, dir, chunkID)
	p, _ := makePayload("only")
	require.NoError(t, ingestLedgerEvents(hot1, 2, []events.Payload{p}))
	require.NoError(t, ingestLedgerEvents(hot1, 3, nil))
	require.NoError(t, raw1.Close())

	hot2, _ := openHotStoreForTestAt(t, dir, chunkID)

	assert.Equal(t, uint32(1), mustEventCount(t, hot2))
	assert.Equal(t, 2, mustOffsets(t, hot2).LedgerCount())

	start, end, err := mustOffsets(t, hot2).EventIDs(3)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), start)
	assert.Equal(t, uint32(1), end, "empty ledger reports zero-width range")
}

// TestWarmup_RejectsIndexRowAtMaxUint32 pins the tripwire against its
// wrap-adversarial input: an index row at eventID == MaxUint32. With a
// uint32 upper bound, max+1 wraps to 0 and slips past the > total check
// — the exact tamper the tripwire exists to catch; the bound is uint64.
func TestWarmup_RejectsIndexRowAtMaxUint32(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	hot1, raw1 := openHotStoreForTestAt(t, dir, chunkID)
	p1, _ := makePayload("a")
	require.NoError(t, ingestLedgerEvents(hot1, 2, []events.Payload{p1})) // total = 1
	require.NoError(t, raw1.Close())

	corruptHotChunk(t, dir, chunkID, func(raw *rocksdb.Store) {
		var term events.TermKey
		term[0] = 0x99
		require.NoError(t, raw.Put(IndexCF, encodeIndexKey(term, math.MaxUint32), nil))
	})

	_, _, err := tryOpenHotStoreForTest(t, dir, chunkID)
	require.ErrorContains(t, err,
		fmt.Sprintf("index references event %d but only 1 committed", uint32(math.MaxUint32)))
}
