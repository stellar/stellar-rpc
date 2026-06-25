package fullhistory

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ---------------------------------------------------------------------------
// progress derivation test helpers.
// ---------------------------------------------------------------------------

// makeChunkDurable freezes ledgers+events+txhash for a chunk — the durable state
// highestDurableChunk counts.
func makeChunkDurable(t *testing.T, cat *catalog.Catalog, c chunk.ID) {
	t.Helper()
	freezeKinds(t, cat, c, geometry.KindLedgers, geometry.KindEvents, geometry.KindTxHash)
}

// ---------------------------------------------------------------------------
// completeThrough — sentinel-safe signed->ledger map.
//
// ALIASING TRAP: a guard-less impl wraps -1 to exactly preGenesisLedger anyway
// (MaxUint32+1 overflows to 0), so a -1-only test is blind to a dropped guard.
// The -2/-100 rows are the load-bearing ones (they wrap to large, distinct values
// the guard must squash).
// ---------------------------------------------------------------------------

func TestCompleteThrough(t *testing.T) {
	tests := []struct {
		name string
		in   int64
		want uint32
	}{
		{"pre-genesis sentinel -1 => FirstLedgerSeq-1, not MaxUint32 (ALIASES the wrap; see trap above)", -1, preGenesisLedger},
		{"sentinel -2 does NOT alias the wrap (guard-less would yield 4294957297)", -2, preGenesisLedger},
		{"deeply negative still pre-genesis", -100, preGenesisLedger},
		{"chunk 0 last ledger", 0, chunk.ID(0).LastLedger()},
		{"chunk 5 last ledger", 5, chunk.ID(5).LastLedger()},
	}
	require.Equal(t, uint32(1), preGenesisLedger, "FirstLedgerSeq-1 == 1 (the doc's chunkLastLedger(-1))")
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, completeThrough(tc.in))
		})
	}

	// Assert the aliasing trap directly so the comment above can't rot: -1 wraps to
	// preGenesisLedger, -2 does not. Computed from chunk arithmetic, not hardcoded.
	guardlessWrap := func(c int64) uint32 {
		return chunk.ID(uint32(c)).LastLedger() //nolint:gosec // deliberate wrap to model a guard-less impl
	}
	require.Equal(t, preGenesisLedger, guardlessWrap(-1),
		"-1 aliases preGenesisLedger under the wrap — the coincidence this test must not rely on")
	require.NotEqual(t, preGenesisLedger, guardlessWrap(-2),
		"-2 must NOT alias — proving the guard (not a coincidence) is what makes completeThrough(-2) safe")
}

// ---------------------------------------------------------------------------
// lastCommittedLedger — chunk-granularity bound, pure catalog read.
// ---------------------------------------------------------------------------

func TestLastCommittedLedger(t *testing.T) {
	t.Run("fresh store => pre-genesis sentinel, never MaxUint32", func(t *testing.T) {
		// Every term is -1; the signed domain must yield FirstLedgerSeq-1, not wrap.
		cat, _ := testCatalog(t)
		got, err := lastCommittedLedger(cat)
		require.NoError(t, err)
		require.Equal(t, preGenesisLedger, got)
	})

	t.Run("cold term leads: highest fully-durable chunk", func(t *testing.T) {
		cat, _ := testCatalog(t)
		makeChunkDurable(t, cat, 0)
		makeChunkDurable(t, cat, 1)
		makeChunkDurable(t, cat, 2)
		got, err := lastCommittedLedger(cat)
		require.NoError(t, err)
		require.Equal(t, chunk.ID(2).LastLedger(), got)
	})

	t.Run("incompletely-frozen tip degrades the bound (ledgers frozen, events freezing)", func(t *testing.T) {
		cat, _ := testCatalog(t)
		makeChunkDurable(t, cat, 0)
		makeChunkDurable(t, cat, 1)
		// Chunk 2 mid-freeze (events only "freezing") must NOT count: bound stays at 1.
		freezeKinds(t, cat, 2, geometry.KindLedgers, geometry.KindTxHash)
		require.NoError(t, cat.MarkChunkFreezing(2, geometry.KindEvents))
		got, err := lastCommittedLedger(cat)
		require.NoError(t, err)
		require.Equal(t, chunk.ID(1).LastLedger(), got)
	})

	t.Run("txhash satisfied by a frozen index coverage (post-finalization demote)", func(t *testing.T) {
		cat, _ := testCatalog(t)
		// Chunk 7: txhash demoted but a frozen index coverage spans it ⇒ still durable.
		freezeKinds(t, cat, 7, geometry.KindLedgers, geometry.KindEvents)
		freezeCoverage(t, cat, cat.TxHashIndexLayout().TxHashIndexID(7), 0, 999) // window 0 covers chunk 7
		got, err := lastCommittedLedger(cat)
		require.NoError(t, err)
		require.Equal(t, chunk.ID(7).LastLedger(), got)
	})

	t.Run("chunk NOT covered by any frozen index and no frozen txhash does not count", func(t *testing.T) {
		cat, _ := testCatalog(t)
		makeChunkDurable(t, cat, 0)
		// Chunk 1: ledgers+events frozen, no txhash, no covering index.
		freezeKinds(t, cat, 1, geometry.KindLedgers, geometry.KindEvents)
		got, err := lastCommittedLedger(cat)
		require.NoError(t, err)
		require.Equal(t, chunk.ID(0).LastLedger(), got, "chunk 1 not durable; bound stays at chunk 0")
	})

	t.Run("earliest pin floor leads when above the cold term", func(t *testing.T) {
		cat, _ := testCatalog(t)
		// Floor pinned mid-chain, no chunks durable, no hot keys.
		const floor = 50000
		require.NoError(t, cat.PinEarliestLedger(floor))
		got, err := lastCommittedLedger(cat)
		require.NoError(t, err)
		require.Equal(t, uint32(floor-1), got)
	})

	t.Run("earliest pin == genesis (2) does not underflow", func(t *testing.T) {
		cat, _ := testCatalog(t)
		require.NoError(t, cat.PinEarliestLedger(chunk.FirstLedgerSeq))
		got, err := lastCommittedLedger(cat)
		require.NoError(t, err)
		require.Equal(t, preGenesisLedger, got, "earliest 2 - 1 = 1, not MaxUint32")
	})

	t.Run("max of the cold term and the earliest floor", func(t *testing.T) {
		cat, _ := testCatalog(t)
		makeChunkDurable(t, cat, 3) // cold => chunk 3 last ledger (the higher term)
		require.NoError(t, cat.PinEarliestLedger(2))
		got, err := lastCommittedLedger(cat)
		require.NoError(t, err)
		require.Equal(t, chunk.ID(3).LastLedger(), got)
	})
}
