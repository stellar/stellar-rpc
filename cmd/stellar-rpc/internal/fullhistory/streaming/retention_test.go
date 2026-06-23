package streaming

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ---------------------------------------------------------------------------
// Reader retention contract (retention.go): a seq below the floor is not-found
// regardless of on-disk state. These are pure-arithmetic unit tests; the
// straddling-window scenario below ties the gate to real on-disk artifacts.
// ---------------------------------------------------------------------------

func TestRetentionGate_AdmitsAtAndAboveFloor(t *testing.T) {
	// through = chunk 100's last ledger, retain 10 chunks ⇒ floor = chunk 91's
	// first ledger (effectiveRetentionFloor: 100-10+1 = 91).
	through := chunk.ID(100).LastLedger()
	gate := NewRetentionGate(through, 10, 0)
	require.Equal(t, chunk.ID(91).FirstLedger(), gate.Floor())

	tests := []struct {
		name string
		seq  uint32
		want bool
	}{
		{"one below the floor => not-found", gate.Floor() - 1, false},
		{"exactly the floor => admitted", gate.Floor(), true},
		{"floor chunk's last ledger => admitted", chunk.ID(91).LastLedger(), true},
		{"well above the floor => admitted", chunk.ID(100).FirstLedger(), true},
		{"genesis (far below) => not-found", chunk.FirstLedgerSeq, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, gate.Admits(tc.seq))
			// The free function and the gate agree (one definition).
			assert.Equal(t, tc.want, seqWithinRetention(tc.seq, through, 10, 0))
		})
	}
}

// Shortening retention raises the floor immediately in the gate — no per-chunk
// state to migrate. The SAME (through, earliest) with a smaller retentionChunks
// yields a higher floor, so seqs that were admitted become not-found at once.
func TestRetentionGate_ShorteningRaisesFloorImmediately(t *testing.T) {
	through := chunk.ID(100).LastLedger()

	wide := NewRetentionGate(through, 50, 0)   // floor = chunk 51
	narrow := NewRetentionGate(through, 10, 0) // floor = chunk 91
	require.Equal(t, chunk.ID(51).FirstLedger(), wide.Floor())
	require.Equal(t, chunk.ID(91).FirstLedger(), narrow.Floor())

	// A seq in chunk 60: inside the wide window, below the narrowed floor.
	seq := chunk.ID(60).FirstLedger()
	assert.True(t, wide.Admits(seq), "in range under the wide retention")
	assert.False(t, narrow.Admits(seq), "shortening retention makes it not-found at once")
}

// ChunkBelowFloor: a chunk wholly below the floor is past retention; one
// straddling it is not.
func TestRetentionGate_ChunkBelowFloor(t *testing.T) {
	// through = chunk 11's last ledger, retain 4 chunks ⇒ floor = chunk 8's first
	// ledger (11-4+1 = 8).
	through := chunk.ID(11).LastLedger()
	gate := NewRetentionGate(through, 4, 0)
	require.Equal(t, chunk.ID(8).FirstLedger(), gate.Floor())

	// Chunk 7 is below the floor; chunk 8 is the floor chunk.
	assert.True(t, gate.ChunkBelowFloor(7))
	assert.False(t, gate.ChunkBelowFloor(8))
}

// ---------------------------------------------------------------------------
// Scenario: a chunk STRADDLING the floor serves in-range seqs and not-found
// below. The reader gate makes below-floor reads not-found regardless of what
// is on disk, while the in-range tail still serves. Only chunks WHOLLY below the
// floor are swept by the prune scan; a straddling chunk's frozen ledger artifact
// survives.
// ---------------------------------------------------------------------------

func TestReaderRetention_StraddlingFloorServesInRangeNotBelow(t *testing.T) {
	cat, _ := testCatalog(t)

	// Chunks 0..3 have their ledger + events artifacts frozen, written when the
	// floor sat at genesis.
	for c := chunk.ID(0); c <= 3; c++ {
		freezeKinds(t, cat, c, KindLedgers, KindEvents)
		writeArtifact(t, cat.layout.LedgerPackPath(c))
	}

	// The floor later rose to chunk 2 (its first ledger): chunks 0,1 below it,
	// chunks 2,3 in range.
	through := chunk.ID(3).LastLedger()
	// Pick retentionChunks so the sliding floor lands on chunk 2:
	// lastCompleteChunkAt(through)=3, floor chunk = 3-retention+1 = 2 ⇒ retention=2.
	gate := NewRetentionGate(through, 2, 0)
	require.Equal(t, chunk.ID(2).FirstLedger(), gate.Floor(), "the floor lands at chunk 2")

	// A seq in chunk 2 or 3 (in range) is admitted; a seq in chunk 0 or 1 is
	// not-found regardless of the file still being on disk.
	assert.True(t, gate.Admits(chunk.ID(2).FirstLedger()), "floor chunk: in range")
	assert.True(t, gate.Admits(chunk.ID(3).LastLedger()), "above the floor: in range")
	assert.False(t, gate.Admits(chunk.ID(1).LastLedger()), "below the floor: not-found")
	assert.False(t, gate.Admits(chunk.ID(0).FirstLedger()), "below the floor: not-found")

	// The prune scan sweeps only the WHOLLY-below-floor chunks 0,1; chunks 2,3
	// survive — exactly the data the gate admits.
	cfg, _ := lifecycleTestConfig(t, cat, 2)
	pops, err := eligiblePruneOps(cfg, cat, through)
	require.NoError(t, err)
	for _, op := range pops {
		require.NoError(t, op())
	}

	for c := chunk.ID(0); c <= 1; c++ {
		ledgers, serr := cat.State(c, KindLedgers)
		require.NoError(t, serr)
		assert.Equal(t, State(""), ledgers, "below-floor chunk %s pruned", c)
	}
	for c := chunk.ID(2); c <= 3; c++ {
		ledgers, serr := cat.State(c, KindLedgers)
		require.NoError(t, serr)
		assert.Equal(t, StateFrozen, ledgers, "in-range chunk %s survives", c)
	}
	assertQuiescent(t, cfg, cat, through)
}

// ---------------------------------------------------------------------------
// Scenario: retention SHORTENING prunes the newly-out-of-range chunks
// immediately. The prune scan reads the floor live from (through,
// RetentionChunks), so a smaller RetentionChunks raises the floor and the next
// tick sweeps the chunks that just fell past it — keys and files alike.
// ---------------------------------------------------------------------------

func TestReaderRetention_ShorteningPrunesNewlyOutOfRangeChunks(t *testing.T) {
	cat, _ := testCatalog(t)

	// Chunks 0..5 fully frozen (ledgers + events), with a real .pack on disk. Live
	// chunk 6 (positional ⇒ through = chunk 5's last).
	for c := chunk.ID(0); c <= 5; c++ {
		freezeKinds(t, cat, c, KindLedgers, KindEvents)
		writeArtifact(t, cat.layout.LedgerPackPath(c))
	}
	live := openLiveHotDB(t, cat, 6)
	t.Cleanup(func() { _ = live.Close() })

	through, err := deriveCompleteThrough(cat)
	require.NoError(t, err)
	require.Equal(t, chunk.ID(5).LastLedger(), through)

	// Under wide retention (5 chunks) the floor would be chunk 1's first ledger,
	// so only chunk 0 would be past it — documenting the pre-shortening floor.
	require.Equal(t, chunk.ID(1).FirstLedger(),
		effectiveRetentionFloor(through, 5, 0), "the wide-retention floor is chunk 1")

	// Now SHORTEN retention to 2 chunks: floor = chunk 4's first ledger. Chunks
	// 0..3 are now past retention and must be swept on the next tick.
	cfg, rec := lifecycleTestConfig(t, cat, 2)
	require.Equal(t, chunk.ID(4).FirstLedger(),
		effectiveRetentionFloor(through, 2, 0), "shortening raised the floor to chunk 4")

	runTickForCatalog(context.Background(), t, cfg, cat)
	require.False(t, rec.fired(), "a shortening prune tick never aborts: %v", rec.last.Load())

	// Chunks 0..3 (newly out of range) are gone — keys and files.
	for c := chunk.ID(0); c <= 3; c++ {
		ledgers, serr := cat.State(c, KindLedgers)
		require.NoError(t, serr)
		assert.Equal(t, State(""), ledgers, "chunk %s key swept by the shortened floor", c)
		assert.NoFileExists(t, cat.layout.LedgerPackPath(c), "chunk %s pack swept", c)
	}
	// Chunks 4,5 (the new retention window) survive.
	for c := chunk.ID(4); c <= 5; c++ {
		ledgers, serr := cat.State(c, KindLedgers)
		require.NoError(t, serr)
		assert.Equal(t, StateFrozen, ledgers, "chunk %s within the shortened retention survives", c)
		assert.FileExists(t, cat.layout.LedgerPackPath(c))
	}

	assertQuiescent(t, cfg, cat, through)
}
