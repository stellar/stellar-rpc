package chunk

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// LastLedgerOf — sentinel-safe signed->ledger map.
//
// ALIASING TRAP: a guard-less impl wraps -1 to exactly PreGenesisLedger anyway
// (MaxUint32+1 overflows to 0), so a -1-only test is blind to a dropped guard.
// The -2/-100 rows are the load-bearing ones (they wrap to large, distinct values
// the guard must squash).
// ---------------------------------------------------------------------------

func TestChunkLastLedger(t *testing.T) {
	tests := []struct {
		name string
		in   int64
		want uint32
	}{
		{"pre-genesis sentinel -1 => FirstLedgerSeq-1, not MaxUint32 (aliases the wrap)", -1, PreGenesisLedger},
		{"sentinel -2 does NOT alias the wrap (guard-less would yield 4294957297)", -2, PreGenesisLedger},
		{"deeply negative still pre-genesis", -100, PreGenesisLedger},
		{"chunk 0 last ledger", 0, ID(0).LastLedger()},
		{"chunk 5 last ledger", 5, ID(5).LastLedger()},
	}
	require.Equal(t, uint32(1), PreGenesisLedger, "FirstLedgerSeq-1 == 1 (the doc's chunkLastLedger(-1))")
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, LastLedgerOf(tc.in))
		})
	}

	// Assert the aliasing trap directly so the comment above can't rot: -1 wraps to
	// PreGenesisLedger, -2 does not. Computed from chunk arithmetic, not hardcoded.
	guardlessWrap := func(c int64) uint32 {
		return ID(uint32(c)).LastLedger()
	}
	require.Equal(t, PreGenesisLedger, guardlessWrap(-1),
		"-1 aliases PreGenesisLedger under the wrap — the coincidence this test must not rely on")
	require.NotEqual(t, PreGenesisLedger, guardlessWrap(-2),
		"-2 must NOT alias — proving the guard (not a coincidence) is what makes LastLedgerOf(-2) safe")
}

// SignedIDOfLedger maps a ledger to its containing chunk, signed so a sub-genesis
// ledger yields -1 rather than panicking.
func TestChunkIDOfLedger(t *testing.T) {
	require.Equal(t, int64(-1), SignedIDOfLedger(FirstLedgerSeq-1), "sub-genesis => -1 sentinel")
	require.Equal(t, int64(0), SignedIDOfLedger(FirstLedgerSeq), "genesis => chunk 0")
	require.Equal(t, int64(0), SignedIDOfLedger(ID(0).LastLedger()), "chunk 0's last ledger => chunk 0")
	require.Equal(t, int64(1), SignedIDOfLedger(ID(1).FirstLedger()), "chunk 1's first ledger => chunk 1")
}
