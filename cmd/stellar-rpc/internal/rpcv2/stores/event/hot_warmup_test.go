package event

import (
	"context"
	"fmt"
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
	bm := lookupOne(t, h.store, NewTerm(FieldContractID, []byte("any")))
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
// that warmup must reject. The HotStore for
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
	require.ErrorContains(t, err, "8 events sealed but only 2 committed")
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

	corruptHotChunk(t, dir, chunkID, func(raw *rocksdb.Store) {
		require.NoError(t, raw.Put(DataCF, encodeDataKey(0), []byte("orphan")))
	})

	_, _, err := tryOpenHotStoreForTest(t, dir, chunkID)
	require.ErrorContains(t, err, "1 events sealed but only 0 committed")
}

// ingestSealing ingests ledgers of two events each into a store whose data
// windows hold two ledgers, waits for the seals, and closes it. It returns
// the payloads in id order.
func ingestSealing(t *testing.T, dir string, chunkID chunk.ID, ledgers int) []Payload {
	t.Helper()
	hot, raw, err := tryOpenHotStoreSealing(t, dir, chunkID, 2)
	require.NoError(t, err)
	all := make([]Payload, 0, 2*ledgers)
	for i := range ledgers {
		seq := chunkID.FirstLedger() + uint32(i)
		payloads := make([]Payload, 0, 2)
		for j := range 2 {
			p, _ := makePayload(fmt.Sprintf("s%d_%d", i, j))
			p.LedgerSequence = seq
			payloads = append(payloads, p)
		}
		require.NoError(t, ingestLedgerEvents(hot, seq, payloads))
		all = append(all, payloads...)
	}
	require.NoError(t, hot.data.unsealed.Wait())
	require.NoError(t, raw.Close())
	return all
}

// A reopen finds every event: the sealed windows in DataCF, the rest rebuilt
// from the ledgers. Fetches read across the two.
func TestWarmup_RebuildsUnsealedEventsFromLedgers(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()
	all := ingestSealing(t, dir, chunkID, 7)

	hot, raw, err := tryOpenHotStoreSealing(t, dir, chunkID, 2)
	require.NoError(t, err)
	sealed, err := sealedEvents(raw)
	require.NoError(t, err)
	require.Positive(t, sealed)
	first, _ := hot.data.inMemory()
	require.Less(t, first, uint32(len(all)), "some events are only in memory")
	require.Equal(t, uint32(len(all)), mustEventCount(t, hot))

	ctx := context.Background()
	var ids []uint32
	var id uint32
	for p, err := range hot.All(ctx) {
		require.NoError(t, err)
		require.Equal(t, all[id].ContractEventBytes, p.ContractEventBytes)
		require.Equal(t, all[id].LedgerSequence, p.LedgerSequence)
		ids = append(ids, id)
		id++
	}
	require.Len(t, ids, len(all))
	got, err := hot.FetchEvents(ctx, ids)
	require.NoError(t, err)
	for i := range got {
		require.Equal(t, all[i].ContractEventBytes, got[i].ContractEventBytes)
	}
	id = first - 1
	for p, err := range hot.FetchRange(ctx, first-1, 3) {
		require.NoError(t, err)
		require.Equal(t, all[id].ContractEventBytes, p.ContractEventBytes)
		id++
	}
	require.Equal(t, first+2, id)
	for i, p := range all {
		terms, err := TermsForBytes(p.ContractEventBytes)
		require.NoError(t, err)
		require.True(t, lookupOne(t, hot, terms[len(terms)-1]).Contains(uint32(i)))
	}

	// The next ledger goes on from the rebuilt state.
	p, _ := makePayload("next")
	require.NoError(t, ingestLedgerEvents(hot, mustOffsets(t, hot).EndLedger(), []Payload{p}))
	got, err = hot.FetchEvents(ctx, []uint32{uint32(len(all))})
	require.NoError(t, err)
	require.Equal(t, p.ContractEventBytes, got[0].ContractEventBytes)
}

// A ledger without events still seals a window sealLedgers old, so an open
// never replays more than two windows of ledgers.
func TestWarmup_EventlessLedgerSealsAnOldWindow(t *testing.T) {
	const chunkID = chunk.ID(0)
	hot, raw, err := tryOpenHotStoreSealing(t, t.TempDir(), chunkID, 2)
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	p, _ := makePayload("only")
	require.NoError(t, ingestLedgerEvents(hot, chunkID.FirstLedger(), []Payload{p}))
	require.NoError(t, ingestLedgerEvents(hot, chunkID.FirstLedger()+1, nil))
	require.NoError(t, ingestLedgerEvents(hot, chunkID.FirstLedger()+2, nil))
	require.NoError(t, hot.data.unsealed.Wait())
	sealed, err := sealedEvents(raw)
	require.NoError(t, err)
	require.Equal(t, uint32(1), sealed)
}

// The sealed events must be dense and end on a ledger boundary, and a
// rebuilt ledger must yield the event count that was committed for it.
func TestWarmup_RejectsInconsistentEventData(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(raw *rocksdb.Store)
		want   string
	}{
		{"a sealed event is missing", func(raw *rocksdb.Store) {
			require.NoError(t, raw.Delete(DataCF, encodeDataKey(1)))
		}, "where event 1 belongs"},
		{"the sealed events end inside a ledger", func(raw *rocksdb.Store) {
			last, _, err := raw.LastKey(DataCF)
			require.NoError(t, err)
			require.NoError(t, raw.Delete(DataCF, last))
		}, "ends inside ledger"},
		{"a ledger yields another event count", func(raw *rocksdb.Store) {
			last, _, err := raw.LastKey(testLedgersCF)
			require.NoError(t, err)
			require.NoError(t, raw.Put(testLedgersCF, last, nil))
		}, "0 events derived, 2 committed"},
		{"an unsealed ledger is missing", func(raw *rocksdb.Store) {
			last, _, err := raw.LastKey(testLedgersCF)
			require.NoError(t, err)
			require.NoError(t, raw.Delete(testLedgersCF, last))
		}, "the ledgers end at"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const chunkID = chunk.ID(0)
			dir := t.TempDir()
			ingestSealing(t, dir, chunkID, 7)
			corruptHotChunk(t, dir, chunkID, tc.mutate)
			_, _, err := tryOpenHotStoreSealing(t, dir, chunkID, 2)
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
