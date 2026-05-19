package eventstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// These tests exercise the (unexported) warmup() function indirectly
// through OpenHotStore, which is the only production caller. They
// document the "fresh chunk → empty caches", "ingested chunk →
// reconstructed caches" contract.

func TestWarmup_FreshChunkProducesEmptyMirrorsViaOpenHotStore(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)

	// The mirror is open (ingest can still happen); use Len rather
	// than All to inspect it, since All requires a closed index.
	assert.Equal(t, int64(0), h.store.mirror.Len())
	assert.Zero(t, h.store.offsets.LedgerCount())
	assert.Equal(t, uint32(0), h.store.offsets.TotalEvents())
	assert.Equal(t, chunkID.FirstLedger(), h.store.offsets.StartLedger())
}

func TestWarmup_RebuildsMirrorFromIngestedRows(t *testing.T) {
	// Ingest into one HotStore, close, reopen; the reopened store's
	// mirror must hold the same terms (warmup replayed them from
	// events_index).
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	hot1, err := OpenHotStore(dir, chunkID, silentLogger())
	require.NoError(t, err)
	p1, _ := makePayload("alpha")
	p2, _ := makePayload("beta")
	require.NoError(t, hot1.IngestLedgerEvents(2, []events.Payload{p1, p2}))

	// Snapshot the mirror state before close. All takes an RLock on
	// the live mirror, so iteration is safe without any explicit
	// close step.
	expected := make(map[events.TermKey]uint64)
	for term, bm := range hot1.mirror.All() {
		expected[term] = bm.GetCardinality()
	}
	require.NoError(t, hot1.Close())

	// Reopen — warmup replays events_index into a fresh mirror.
	hot2, err := OpenHotStore(dir, chunkID, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = hot2.Close() })

	got := make(map[events.TermKey]uint64)
	for term, bm := range hot2.mirror.All() {
		got[term] = bm.GetCardinality()
	}
	assert.Equal(t, expected, got)
}

func TestWarmup_RestoresEventIDsForRepeatedTerm(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	hot1, err := OpenHotStore(dir, chunkID, silentLogger())
	require.NoError(t, err)
	p1, _ := makePayload("shared")
	p2, _ := makePayload("shared")
	p3, _ := makePayload("shared")
	require.NoError(t, hot1.IngestLedgerEvents(2, []events.Payload{p1, p2, p3}))
	require.NoError(t, hot1.Close())

	hot2, err := OpenHotStore(dir, chunkID, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = hot2.Close() })

	contractTermKey := events.ComputeTermKey(p1.ContractEvent.ContractId[:], events.FieldContractID)
	bm, err := hot2.Lookup(contractTermKey)
	require.NoError(t, err)
	require.NotNil(t, bm)
	assert.Equal(t, uint64(3), bm.GetCardinality())
	for id := range uint32(3) {
		assert.True(t, bm.Contains(id), "warmup missed event id %d", id)
	}
}

func TestWarmup_OffsetsReconstructedAcrossLedgers(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	hot1, err := OpenHotStore(dir, chunkID, silentLogger())
	require.NoError(t, err)
	p1, _ := makePayload("a")
	p2, _ := makePayload("b")
	require.NoError(t, hot1.IngestLedgerEvents(2, []events.Payload{p1, p2}))
	p3, _ := makePayload("c")
	require.NoError(t, hot1.IngestLedgerEvents(3, []events.Payload{p3}))
	require.NoError(t, hot1.Close())

	hot2, err := OpenHotStore(dir, chunkID, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = hot2.Close() })

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

func TestWarmup_OffsetsHandleEmptyTrailingLedger(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	hot1, err := OpenHotStore(dir, chunkID, silentLogger())
	require.NoError(t, err)
	p, _ := makePayload("only")
	require.NoError(t, hot1.IngestLedgerEvents(2, []events.Payload{p}))
	require.NoError(t, hot1.IngestLedgerEvents(3, nil))
	require.NoError(t, hot1.Close())

	hot2, err := OpenHotStore(dir, chunkID, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = hot2.Close() })

	assert.Equal(t, uint32(1), mustEventCount(t, hot2))
	assert.Equal(t, 2, mustOffsets(t, hot2).LedgerCount())

	start, end, err := mustOffsets(t, hot2).EventIDs(3)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), start)
	assert.Equal(t, uint32(1), end, "empty ledger reports zero-width range")
}
