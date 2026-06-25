package lifecycle

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
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

// makeHotDir creates the on-disk hot dir for a chunk so deriveWatermark's
// per-ready-key dir-existence loop sees it present.
func makeHotDir(t *testing.T, cat *catalog.Catalog, c chunk.ID) {
	t.Helper()
	require.NoError(t, os.MkdirAll(cat.Layout().HotChunkPath(c), 0o755))
}

// readyHot marks a chunk's hot key "ready" AND creates its dir, the production
// pairing deriveWatermark expects (a ready key whose dir is missing is loss).
func readyHot(t *testing.T, cat *catalog.Catalog, c chunk.ID) {
	t.Helper()
	require.NoError(t, cat.PutHotTransient(c))
	require.NoError(t, cat.FlipHotReady(c))
	makeHotDir(t, cat, c)
}

// ---------------------------------------------------------------------------
// CompleteThrough — sentinel-safe signed->ledger map.
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
		{"pre-genesis sentinel -1 => FirstLedgerSeq-1, not MaxUint32 (aliases the wrap)", -1, preGenesisLedger},
		{"sentinel -2 does NOT alias the wrap (guard-less would yield 4294957297)", -2, preGenesisLedger},
		{"deeply negative still pre-genesis", -100, preGenesisLedger},
		{"chunk 0 last ledger", 0, chunk.ID(0).LastLedger()},
		{"chunk 5 last ledger", 5, chunk.ID(5).LastLedger()},
	}
	require.Equal(t, uint32(1), preGenesisLedger, "FirstLedgerSeq-1 == 1 (the doc's chunkLastLedger(-1))")
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, CompleteThrough(tc.in))
		})
	}

	// Assert the aliasing trap directly so the comment above can't rot: -1 wraps to
	// preGenesisLedger, -2 does not. Computed from chunk arithmetic, not hardcoded.
	guardlessWrap := func(c int64) uint32 {
		return chunk.ID(uint32(c)).LastLedger()
	}
	require.Equal(t, preGenesisLedger, guardlessWrap(-1),
		"-1 aliases preGenesisLedger under the wrap — the coincidence this test must not rely on")
	require.NotEqual(t, preGenesisLedger, guardlessWrap(-2),
		"-2 must NOT alias — proving the guard (not a coincidence) is what makes CompleteThrough(-2) safe")
}

// ---------------------------------------------------------------------------
// LastCommittedLedger — chunk-granularity bound, pure catalog read.
// ---------------------------------------------------------------------------

func TestLastCommittedLedger(t *testing.T) {
	t.Run("fresh store => pre-genesis sentinel, never MaxUint32", func(t *testing.T) {
		// Every term is -1; the signed domain must yield FirstLedgerSeq-1, not wrap.
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
		// Chunk 2 mid-freeze (events only "freezing") must NOT count: bound stays at 1.
		freezeKinds(t, cat, 2, geometry.KindLedgers, geometry.KindTxHash)
		require.NoError(t, cat.MarkChunkFreezing(2, geometry.KindEvents))
		got, err := deriveCompleteThrough(cat)
		require.NoError(t, err)
		require.Equal(t, chunk.ID(1).LastLedger(), got)
	})

	t.Run("txhash satisfied by a frozen index coverage (post-finalization demote)", func(t *testing.T) {
		cat, _ := testCatalog(t)
		// Chunk 7: txhash demoted but a frozen index coverage spans it ⇒ still durable.
		freezeKinds(t, cat, 7, geometry.KindLedgers, geometry.KindEvents)
		freezeCoverage(t, cat, cat.TxHashIndexLayout().TxHashIndexID(7), 0, 999) // window 0 covers chunk 7
		got, err := deriveCompleteThrough(cat)
		require.NoError(t, err)
		require.Equal(t, chunk.ID(7).LastLedger(), got)
	})

	t.Run("chunk NOT covered by any frozen index and no frozen txhash does not count", func(t *testing.T) {
		cat, _ := testCatalog(t)
		makeChunkDurable(t, cat, 0)
		// Chunk 1: ledgers+events frozen, no txhash, no covering index.
		freezeKinds(t, cat, 1, geometry.KindLedgers, geometry.KindEvents)
		got, err := deriveCompleteThrough(cat)
		require.NoError(t, err)
		require.Equal(t, chunk.ID(0).LastLedger(), got, "chunk 1 not durable; bound stays at chunk 0")
	})

	t.Run("positional term leads in steady state: everything below the live chunk", func(t *testing.T) {
		cat, _ := testCatalog(t)
		// No cold artifacts yet (steady state: chunks complete before cold exists).
		// Ready hot keys 3,4,5 => live chunk is 5 => everything below 5 complete.
		readyHot(t, cat, 3)
		readyHot(t, cat, 4)
		readyHot(t, cat, 5)
		got, err := deriveCompleteThrough(cat)
		require.NoError(t, err)
		require.Equal(t, chunk.ID(4).LastLedger(), got, "max ready (5) - 1 = chunk 4's last ledger")
	})

	t.Run("transient hot key does NOT advance the positional term", func(t *testing.T) {
		cat, _ := testCatalog(t)
		readyHot(t, cat, 3)
		// A transient key above the highest ready one must be excluded.
		require.NoError(t, cat.PutHotTransient(9))
		got, err := deriveCompleteThrough(cat)
		require.NoError(t, err)
		require.Equal(t, chunk.ID(2).LastLedger(), got, "max READY (3) - 1, ignoring transient 9")
	})

	t.Run("live chunk 0 => positional term is pre-genesis, NOT MaxUint32", func(t *testing.T) {
		// The exact uint32-underflow trap: max ready = 0, so 0-1 must be the
		// pre-genesis sentinel, not ID(4294967295).LastLedger().
		cat, _ := testCatalog(t)
		readyHot(t, cat, 0)
		got, err := deriveCompleteThrough(cat)
		require.NoError(t, err)
		require.Equal(t, preGenesisLedger, got)
	})

	t.Run("earliest pin floor leads when above cold/positional terms", func(t *testing.T) {
		cat, _ := testCatalog(t)
		// Floor pinned mid-chain, no chunks durable, no hot keys.
		const floor = 50000
		require.NoError(t, cat.PinEarliestLedger(floor))
		got, err := deriveCompleteThrough(cat)
		require.NoError(t, err)
		require.Equal(t, uint32(floor-1), got)
	})

	t.Run("earliest pin == genesis (2) does not underflow", func(t *testing.T) {
		cat, _ := testCatalog(t)
		require.NoError(t, cat.PinEarliestLedger(chunk.FirstLedgerSeq))
		got, err := deriveCompleteThrough(cat)
		require.NoError(t, err)
		require.Equal(t, preGenesisLedger, got, "earliest 2 - 1 = 1, not MaxUint32")
	})

	t.Run("max of all three terms", func(t *testing.T) {
		cat, _ := testCatalog(t)
		makeChunkDurable(t, cat, 0) // cold => chunk 0 last ledger
		readyHot(t, cat, 4)         // positional => chunk 3 last ledger (highest)
		require.NoError(t, cat.PinEarliestLedger(2))
		got, err := deriveCompleteThrough(cat)
		require.NoError(t, err)
		require.Equal(t, chunk.ID(3).LastLedger(), got)
	})
}

// ---------------------------------------------------------------------------
// deriveWatermark — deriveCompleteThrough + one refinement read + the
// per-ready-key dir-existence fatal loop.
// ---------------------------------------------------------------------------

func TestDeriveWatermark(t *testing.T) {
	t.Run("no ready hot keys => equals deriveCompleteThrough, no open", func(t *testing.T) {
		cat, _ := testCatalog(t)
		makeChunkDurable(t, cat, 0)
		probe := &fakeHotProbe{} // would error if opened with ok=false under "ready", but none ready
		got, err := deriveWatermark(cat, probe)
		require.NoError(t, err)
		require.Equal(t, chunk.ID(0).LastLedger(), got)
	})

	t.Run("sub-chunk precision: refinement reads mid-chunk seq inside the live chunk", func(t *testing.T) {
		cat, _ := testCatalog(t)
		readyHot(t, cat, 5) // live chunk 5; positional term = chunk 4 last ledger
		midLive := chunk.ID(5).FirstLedger() + 123
		probe := &fakeHotProbe{ok: true, chunk: &fakeHotChunk{maxSeq: midLive, present: true}}
		got, err := deriveWatermark(cat, probe)
		require.NoError(t, err)
		require.Equal(t, midLive, got, "refined to the live chunk's committed seq")
	})

	t.Run("boundary-crash under-count recovered by refinement", func(t *testing.T) {
		// Live chunk crashed at a boundary and was demoted to "transient": the
		// highest READY key is the just-completed predecessor (chunk 4), whose
		// completion no key advertises (positional term = chunk 3). The refinement
		// opens chunk 4 and reads its full committed seq = chunk 4's last ledger,
		// recovering the frontier the positional term under-counted.
		cat, _ := testCatalog(t)
		readyHot(t, cat, 4)
		require.NoError(t, cat.PutHotTransient(5)) // the crashed live chunk
		require.Equal(t, chunk.ID(3).LastLedger(), mustDeriveCompleteThrough(t, cat),
			"positional term alone under-counts to chunk 3")

		chunk4Last := chunk.ID(4).LastLedger()
		probe := &fakeHotProbe{ok: true, chunk: &fakeHotChunk{maxSeq: chunk4Last, present: true}}
		got, err := deriveWatermark(cat, probe)
		require.NoError(t, err)
		require.Equal(t, chunk4Last, got, "refinement recovers the chunk-4 frontier")
	})

	t.Run("count-only-ready: an empty refinement DB falls back to deriveCompleteThrough", func(t *testing.T) {
		cat, _ := testCatalog(t)
		makeChunkDurable(t, cat, 0)
		readyHot(t, cat, 3) // positional => chunk 2 last ledger
		// DB present but empty (present=false): no refinement, w stays positional.
		probe := &fakeHotProbe{ok: true, chunk: &fakeHotChunk{present: false}}
		got, err := deriveWatermark(cat, probe)
		require.NoError(t, err)
		require.Equal(t, chunk.ID(2).LastLedger(), got)
	})

	t.Run("refinement only RAISES the bound, never lowers it", func(t *testing.T) {
		cat, _ := testCatalog(t)
		makeChunkDurable(t, cat, 0)
		makeChunkDurable(t, cat, 1)
		makeChunkDurable(t, cat, 2) // cold term => chunk 2 last ledger
		readyHot(t, cat, 3)         // positional => chunk 2 last ledger
		// Live DB reports a seq below the cold bound (e.g. just opened); max wins.
		probe := &fakeHotProbe{ok: true, chunk: &fakeHotChunk{maxSeq: 5, present: true}}
		got, err := deriveWatermark(cat, probe)
		require.NoError(t, err)
		require.Equal(t, chunk.ID(2).LastLedger(), got)
	})

	t.Run("LAZY loss (item R2-6): only the highest ready chunk is opened; a lower"+
		" ready key's missing dir is NOT eagerly flagged", func(t *testing.T) {
		cat, _ := testCatalog(t)
		// Two ready keys; the LOWER one's dir is missing. Under the design's lazy
		// detection (no eager all-ready-keys scan) only the HIGHEST ready chunk is
		// opened, so the lower key's missing dir is not surfaced here — it surfaces
		// later, when ingestion/discard reaches that chunk via openHotTierForChunk.
		require.NoError(t, cat.PutHotTransient(2))
		require.NoError(t, cat.FlipHotReady(2)) // ready key 2, NO dir (not opened here)
		readyHot(t, cat, 5)                     // highest ready key 5 WITH dir (opened)
		probe := &fakeHotProbe{ok: true, chunk: &fakeHotChunk{maxSeq: 10, present: true}}
		got, err := deriveWatermark(cat, probe)
		require.NoError(t, err)
		require.Equal(t, uint32(10), got, "refined to the highest ready chunk's seq")
	})

	t.Run("fatal: a ready HIGHEST chunk whose dir is missing (lazy loss on open)", func(t *testing.T) {
		cat, _ := testCatalog(t)
		// The highest ready chunk's dir is missing: the one open the derivation
		// performs surfaces the loss as backfill.ErrHotVolumeLost with recovery guidance.
		require.NoError(t, cat.PutHotTransient(5))
		require.NoError(t, cat.FlipHotReady(5)) // ready key 5, NO dir
		probe := &fakeHotProbe{ok: false}       // OpenHotChunk reports dir absent
		_, err := deriveWatermark(cat, probe)
		require.Error(t, err)
		require.ErrorIs(t, err, backfill.ErrHotVolumeLost)
		require.Contains(t, err.Error(), "00000005")
	})

	t.Run("fatal: refinement open error on the highest ready chunk", func(t *testing.T) {
		cat, _ := testCatalog(t)
		readyHot(t, cat, 3) // dir present
		probe := &fakeHotProbe{openErr: errors.New("rocksdb LOCK held")}
		_, err := deriveWatermark(cat, probe)
		require.Error(t, err)
		require.ErrorIs(t, err, backfill.ErrHotVolumeLost)
	})

	t.Run("fatal: refinement read error", func(t *testing.T) {
		cat, _ := testCatalog(t)
		readyHot(t, cat, 3)
		probe := &fakeHotProbe{ok: true, chunk: &fakeHotChunk{maxErr: errors.New("corrupt")}}
		_, err := deriveWatermark(cat, probe)
		require.Error(t, err)
		require.ErrorIs(t, err, backfill.ErrHotVolumeLost)
	})

	t.Run("live chunk 0 ready, empty DB => pre-genesis, no underflow", func(t *testing.T) {
		cat, _ := testCatalog(t)
		readyHot(t, cat, 0)
		probe := &fakeHotProbe{ok: true, chunk: &fakeHotChunk{present: false}}
		got, err := deriveWatermark(cat, probe)
		require.NoError(t, err)
		require.Equal(t, preGenesisLedger, got)
	})
}

func mustDeriveCompleteThrough(t *testing.T, cat *catalog.Catalog) uint32 {
	t.Helper()
	got, err := deriveCompleteThrough(cat)
	require.NoError(t, err)
	return got
}
