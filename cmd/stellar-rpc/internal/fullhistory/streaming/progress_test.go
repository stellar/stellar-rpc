package streaming

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/geometry"
)

// ---------------------------------------------------------------------------
// progress derivation test helpers.
// ---------------------------------------------------------------------------

// makeChunkDurable flips ledgers + events + txhash to frozen for a chunk — the
// pendingArtifacts-empty state highestDurableChunk counts.
func makeChunkDurable(t *testing.T, cat *catalog.Catalog, c chunk.ID) {
	t.Helper()
	freezeKinds(t, cat, c, geometry.KindLedgers, geometry.KindEvents, geometry.KindTxHash)
}

// ---------------------------------------------------------------------------
// completeThrough — the sentinel-safe signed->ledger map. Proves the
// pre-genesis sentinel resolves to FirstLedgerSeq-1 (=1), NOT a uint32 wrap.
//
// THE ALIASING TRAP this test exists to catch: a guard-less completeThrough
// (chunk.ID(uint32(c)).LastLedger() with no `c<0` branch) does NOT fail on the
// production sentinel -1, because chunk.ID(uint32(-1)=MaxUint32).LastLedger()
// computes (MaxUint32+1)*LedgersPerChunk+FirstLedgerSeq-1, whose (MaxUint32+1)
// overflows uint32 to 0 — yielding exactly 1 == preGenesisLedger. So a -1-only
// test would pass even with the guard removed. Every OTHER negative input wraps
// to a large, distinct value (e.g. -2 => 4294957297), so the guard is only
// actually exercised by a negative sentinel that is NOT -1. The -2 and -100
// rows below are the load-bearing underflow guards; -1 alone is decorative.
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

	// The aliasing trap, asserted directly so the comment above cannot rot: the
	// production sentinel -1 wraps to exactly preGenesisLedger (which is why a
	// -1-only test is blind to a dropped guard), while -2 wraps to a large,
	// distinct value that the guard must squash. Computed from chunk arithmetic,
	// not hardcoded, so it tracks LedgersPerChunk/FirstLedgerSeq.
	guardlessWrap := func(c int64) uint32 {
		return chunk.ID(uint32(c)).LastLedger() //nolint:gosec // deliberate wrap to model a guard-less impl
	}
	require.Equal(t, preGenesisLedger, guardlessWrap(-1),
		"-1 aliases preGenesisLedger under the wrap — the coincidence this test must not rely on")
	require.NotEqual(t, preGenesisLedger, guardlessWrap(-2),
		"-2 must NOT alias — proving the guard (not a coincidence) is what makes completeThrough(-2) safe")
}

// ---------------------------------------------------------------------------
// deriveCompleteThrough — chunk-granularity bound, pure catalog read.
// ---------------------------------------------------------------------------

func TestDeriveCompleteThrough(t *testing.T) {
	t.Run("fresh store => pre-genesis sentinel, never MaxUint32", func(t *testing.T) {
		// No durable chunk, no hot key, no earliest pin: every term is -1.
		// A naive uint32 impl (chunkLastLedger(ID(-1)) / earliest-1) would wrap
		// to MaxUint32 here; the signed domain must yield FirstLedgerSeq-1.
		cat, _ := testCatalog(t)
		got, err := deriveCompleteThrough(cat)
		require.NoError(t, err)
		require.Equal(t, preGenesisLedger, got)
	})

	t.Run("cold term leads: highest fully-durable chunk", func(t *testing.T) {
		cat, _ := testCatalog(t)
		makeChunkDurable(t, cat, 0)
		makeChunkDurable(t, cat, 1)
		makeChunkDurable(t, cat, 2)
		got, err := deriveCompleteThrough(cat)
		require.NoError(t, err)
		require.Equal(t, chunk.ID(2).LastLedger(), got)
	})

	t.Run("incompletely-frozen tip degrades the bound (ledgers frozen, events freezing)", func(t *testing.T) {
		cat, _ := testCatalog(t)
		makeChunkDurable(t, cat, 0)
		makeChunkDurable(t, cat, 1)
		// Chunk 2: ledgers frozen but events only "freezing" — a mid-freeze crash.
		// It must NOT count: bound stays at chunk 1.
		freezeKinds(t, cat, 2, geometry.KindLedgers, geometry.KindTxHash)
		require.NoError(t, cat.MarkChunkFreezing(2, geometry.KindEvents))
		got, err := deriveCompleteThrough(cat)
		require.NoError(t, err)
		require.Equal(t, chunk.ID(1).LastLedger(), got)
	})

	t.Run("txhash satisfied by a frozen index coverage (post-finalization demote)", func(t *testing.T) {
		cat, _ := testCatalog(t)
		// Chunk 7: ledgers+events frozen, but txhash NOT frozen (demoted) — instead a
		// frozen index coverage spans it. It must still count as durable.
		freezeKinds(t, cat, 7, geometry.KindLedgers, geometry.KindEvents)
		freezeCoverage(t, cat, cat.TxHashIndexLayout().TxHashIndexID(7), 0, 999) // window 0 covers chunk 7
		got, err := deriveCompleteThrough(cat)
		require.NoError(t, err)
		require.Equal(t, chunk.ID(7).LastLedger(), got)
	})

	t.Run("chunk NOT covered by any frozen index and no frozen txhash does not count", func(t *testing.T) {
		cat, _ := testCatalog(t)
		makeChunkDurable(t, cat, 0)
		// Chunk 1: ledgers+events frozen, no txhash, no covering frozen index.
		freezeKinds(t, cat, 1, geometry.KindLedgers, geometry.KindEvents)
		got, err := deriveCompleteThrough(cat)
		require.NoError(t, err)
		require.Equal(t, chunk.ID(0).LastLedger(), got, "chunk 1 not durable; bound stays at chunk 0")
	})

	t.Run("earliest pin floor leads when above the cold term", func(t *testing.T) {
		cat, _ := testCatalog(t)
		// Floor pinned mid-chain, no chunks durable, no hot keys.
		const floor = 50000
		require.NoError(t, cat.PinLayout(testCPI, floor))
		got, err := deriveCompleteThrough(cat)
		require.NoError(t, err)
		require.Equal(t, uint32(floor-1), got)
	})

	t.Run("earliest pin == genesis (2) does not underflow", func(t *testing.T) {
		cat, _ := testCatalog(t)
		require.NoError(t, cat.PinLayout(testCPI, chunk.FirstLedgerSeq))
		got, err := deriveCompleteThrough(cat)
		require.NoError(t, err)
		require.Equal(t, preGenesisLedger, got, "earliest 2 - 1 = 1, not MaxUint32")
	})

	t.Run("max of the cold term and the earliest floor", func(t *testing.T) {
		cat, _ := testCatalog(t)
		makeChunkDurable(t, cat, 3) // cold => chunk 3 last ledger (the higher term)
		require.NoError(t, cat.PinLayout(testCPI, 2))
		got, err := deriveCompleteThrough(cat)
		require.NoError(t, err)
		require.Equal(t, chunk.ID(3).LastLedger(), got)
	})
}
