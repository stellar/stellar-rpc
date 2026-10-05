package event

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
)

// These tests exercise the (unexported) warmup() function indirectly
// through NewWithStore over an explicitly opened RocksDB store. They
// document the "fresh chunk → empty caches", "ingested chunk →
// reconstructed caches" contract.

func TestWarmup_FreshChunkProducesEmptyStateViaNewWithStore(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)

	// A fresh index is empty: probing any term is a clean miss, an empty
	// bitmap rather than nil.
	bm := lookupOne(t, h.store, ComputeTermKey([]byte("any"), FieldContractID))
	require.NotNil(t, bm)
	assert.True(t, bm.IsEmpty())
	assert.Zero(t, h.store.offsets.LedgerCount())
	assert.Equal(t, uint32(0), h.store.offsets.TotalEvents())
	assert.Equal(t, chunkID.FirstLedger(), h.store.offsets.StartLedger())
}

func TestWarmup_OffsetsReconstructedAcrossLedgers(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	hot1, raw1 := openHotStoreForTestAt(t, dir, chunkID)
	p1, _ := makePayload("a")
	p2, _ := makePayload("b")
	require.NoError(t, ingestLedgerEvents(hot1, 2, []Payload{p1, p2}))
	p3, _ := makePayload("c")
	require.NoError(t, ingestLedgerEvents(hot1, 3, []Payload{p3}))
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
	require.NoError(t, ingestLedgerEvents(hot1, 2, []Payload{p1, p2})) // total = 2
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
		require.NoError(t, ingestLedgerEvents(hot1, seq, []Payload{p}))
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
		require.NoError(t, ingestLedgerEvents(hot1, seq, []Payload{p}))
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

// A data row the offsets count must be there: the last one is checked
// directly, and the unsealed ones as warmup indexes them again.
func TestWarmup_RejectsMissingDataEvent(t *testing.T) {
	for _, tc := range []struct {
		name    string
		missing uint32
		want    string
	}{
		{"the last event", 1, "missing from data"},
		{"an earlier unsealed event", 0, "where event 0 belongs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const chunkID = chunk.ID(0)
			dir := t.TempDir()

			hot1, raw1 := openHotStoreForTestAt(t, dir, chunkID)
			p1, _ := makePayload("a")
			p2, _ := makePayload("b")
			require.NoError(t, ingestLedgerEvents(hot1, 2, []Payload{p1, p2})) // total = 2
			require.NoError(t, raw1.Close())

			corruptHotChunk(t, dir, chunkID, func(raw *rocksdb.Store) {
				require.NoError(t, raw.Delete(DataCF, encodeDataKey(tc.missing)))
			})

			_, _, err := tryOpenHotStoreForTest(t, dir, chunkID)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestWarmup_OffsetsHandleEmptyTrailingLedger(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	hot1, raw1 := openHotStoreForTestAt(t, dir, chunkID)
	p, _ := makePayload("only")
	require.NoError(t, ingestLedgerEvents(hot1, 2, []Payload{p}))
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

func TestWarmup_RejectsSealedSlabBeyondOffsets(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	hot1, raw1 := openHotStoreForTestAt(t, dir, chunkID)
	p1, _ := makePayload("a")
	require.NoError(t, ingestLedgerEvents(hot1, 2, []Payload{p1})) // total = 1
	require.NoError(t, raw1.Close())

	corruptHotChunk(t, dir, chunkID, func(raw *rocksdb.Store) {
		require.NoError(t, raw.Put(IndexCF, hotIndexKey(0, TermKey{}), nil))
	})

	_, _, err := tryOpenHotStoreForTest(t, dir, chunkID)
	require.ErrorContains(t, err, "1 index slabs sealed but only 1 events committed")
}
