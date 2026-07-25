package event

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
)

func decodeAll(t *testing.T, val []byte) map[events.TermKey][]uint32 {
	t.Helper()
	out := map[events.TermKey][]uint32{}
	require.NoError(t, decodePackedIndexRow(val, func(term events.TermKey, ids []uint32) {
		out[term] = append([]uint32(nil), ids...)
	}))
	return out
}

func TestPackedIndexRow_RoundTrip(t *testing.T) {
	var t1, t2 events.TermKey
	t1[0] = 0x01
	t2[0] = 0x02
	in := map[events.TermKey][]uint32{
		t1: {0, 1, 2, 500},
		// Non-contiguous, large absolute IDs exercise multi-byte varints.
		t2: {7, 1 << 20, 1<<20 + 1},
	}
	assert.Equal(t, in, decodeAll(t, appendPackedIndexRow(nil, in)))
}

func TestPackedIndexRow_RejectsCorruption(t *testing.T) {
	var term events.TermKey
	term[0] = 0xaa
	good := appendPackedIndexRow(nil, map[events.TermKey][]uint32{term: {3, 4}})

	nop := func(events.TermKey, []uint32) {}
	// Truncated term header.
	require.ErrorContains(t, decodePackedIndexRow(good[:10], nop), "trailing bytes")
	// id count exceeding remaining bytes (truncate the ID area).
	require.ErrorContains(t, decodePackedIndexRow(good[:len(good)-1], nop), "exceeds")
	// Zero delta = duplicate ID within a term.
	var dup []byte
	dup = append(dup, term[:]...)
	dup = append(dup, 2 /* count */, 3 /* first id */, 0 /* zero delta */)
	require.ErrorContains(t, decodePackedIndexRow(dup, nop), "zero delta")
}

// TestWarmup_MixedLegacyAndPackedRows simulates a mid-chunk daemon upgrade:
// ledger 2's index rows are in the legacy per-(term,event) format (as a
// pre-upgrade daemon left them), ledger 3's arrive packed. Warmup must merge
// both formats into one mirror — including a term that spans the formats.
func TestWarmup_MixedLegacyAndPackedRows(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	// Ingest ledger 2 (event 0) with the current packed writer.
	hot1, raw1 := openHotStoreForTestAt(t, dir, chunkID)
	p1, keys1 := makePayload("alpha")
	require.NoError(t, ingestLedgerEvents(hot1, 2, []events.Payload{p1}))
	require.NoError(t, raw1.Close())

	// Rewrite ledger 2's index into the LEGACY format, exactly as an old
	// daemon would have written it.
	corruptHotChunk(t, dir, chunkID, func(raw *rocksdb.Store) {
		val, found, err := raw.Get(IndexCF, encodePackedIndexKey(2))
		require.NoError(t, err)
		require.True(t, found)
		require.NoError(t, raw.Delete(IndexCF, encodePackedIndexKey(2)))
		require.NoError(t, decodePackedIndexRow(val, func(term events.TermKey, ids []uint32) {
			for _, id := range ids {
				require.NoError(t, raw.Put(IndexCF, encodeIndexKey(term, id), nil))
			}
		}))
	})

	// Reopen (warmup: legacy-only) and ingest ledger 3 (event 1) packed.
	hot2, raw2 := openHotStoreForTestAt(t, dir, chunkID)
	p2, keys2 := makePayload("beta")
	require.NoError(t, ingestLedgerEvents(hot2, 3, []events.Payload{p2}))
	require.NoError(t, raw2.Close())

	// Final warmup sees both formats in one CF.
	hot3, _ := openHotStoreForTestAt(t, dir, chunkID)
	assert.Equal(t, uint32(2), mustEventCount(t, hot3))

	// Per-payload symbol terms: one event each, from their own format.
	assert.True(t, lookupOne(t, hot3, keys1[len(keys1)-1]).Contains(0))
	assert.True(t, lookupOne(t, hot3, keys2[len(keys2)-1]).Contains(1))

	// The shared contract-ID term (same contract in both payloads) must hold
	// BOTH events — one arrived via a legacy row, one via a packed row.
	shared := lookupOne(t, hot3, keys1[0])
	require.NotNil(t, shared)
	assert.True(t, shared.Contains(0) && shared.Contains(1),
		"cross-format merge lost an event for the shared term")
}
