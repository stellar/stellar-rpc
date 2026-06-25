package streaming

import (
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

// WindowBelowFloor / ChunkBelowFloor: a window or chunk wholly below the floor
// is past retention; one straddling it is not.
func TestRetentionGate_WindowAndChunkBelowFloor(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4) // windows: 0=[0,3], 1=[4,7], 2=[8,11]
	wins := cat.Windows()

	// through = chunk 11's last ledger, retain 4 chunks ⇒ floor = chunk 8's first
	// ledger (11-4+1 = 8). Window 2 starts at the floor.
	through := chunk.ID(11).LastLedger()
	gate := NewRetentionGate(through, 4, 0)
	require.Equal(t, chunk.ID(8).FirstLedger(), gate.Floor())

	// Window 0 ([0,3]) and window 1 ([4,7]) are wholly below the floor (chunk 8);
	// window 2 ([8,11]) is the floor window — at it, not below.
	assert.True(t, gate.WindowBelowFloor(0, wins))
	assert.True(t, gate.WindowBelowFloor(1, wins))
	assert.False(t, gate.WindowBelowFloor(2, wins))

	// Chunk 7 is below the floor; chunk 8 is the floor chunk.
	assert.True(t, gate.ChunkBelowFloor(7))
	assert.False(t, gate.ChunkBelowFloor(8))
}

// retention_chunks = 0 means "full history": the sliding floor is disabled, so
// the gate pins at earliest_ledger (never below genesis) and does NOT move with
// `through`. This also exercises the earliest-wins branch the other tests miss
// (they all pass earliest=0, below genesis, so the sliding floor always wins).
func TestRetentionGate_FullHistoryPinsAtEarliest(t *testing.T) {
	through := chunk.ID(100).LastLedger()

	// earliest above genesis: the fixed floor wins; no sliding floor applies.
	earliest := chunk.ID(50).FirstLedger()
	gate := NewRetentionGate(through, 0, earliest)
	require.Equal(t, earliest, gate.Floor(), "retention_chunks=0 pins the floor at earliest_ledger")
	assert.False(t, gate.Admits(earliest-1), "below earliest => not-found")
	assert.True(t, gate.Admits(earliest), "earliest is in range")

	// earliest = genesis: full history from the start of the chain.
	atGenesis := NewRetentionGate(through, 0, chunk.FirstLedgerSeq)
	require.Equal(t, chunk.ID(0).FirstLedger(), atGenesis.Floor(), "genesis floor under full history")
	assert.True(t, atGenesis.Admits(chunk.FirstLedgerSeq), "genesis is in range under full history")

	// The full-history floor is independent of `through`: a much higher tip does
	// not raise it (there is no sliding window to slide).
	higher := NewRetentionGate(chunk.ID(1_000).LastLedger(), 0, earliest)
	require.Equal(t, earliest, higher.Floor(), "full-history floor does not move with the tip")
}

// A young store — or a retention_chunks larger than the history that exists —
// must clamp the sliding floor to chunk 0 / genesis, not underflow the signed
// chunk arithmetic into a giant uint32 floor that would hide all data.
func TestRetentionGate_YoungStoreClampsToGenesis(t *testing.T) {
	// Only 4 complete chunks exist (0..3) but we ask to retain 1000: the sliding
	// floor 3-1000+1 = -996 must clamp to chunk 0, not wrap.
	gate := NewRetentionGate(chunk.ID(3).LastLedger(), 1000, 0)
	require.Equal(t, chunk.ID(0).FirstLedger(), gate.Floor(), "clamp lands exactly at genesis")

	assert.True(t, gate.Admits(chunk.FirstLedgerSeq), "genesis admitted — nothing clamped below it")
	assert.False(t, gate.ChunkBelowFloor(0), "chunk 0 is at the floor, not below it")
}
