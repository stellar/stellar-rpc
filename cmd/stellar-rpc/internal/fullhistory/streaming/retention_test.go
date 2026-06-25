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
