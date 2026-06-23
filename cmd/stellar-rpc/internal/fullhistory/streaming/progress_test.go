package streaming

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ---------------------------------------------------------------------------
// progress derivation test helpers.
// ---------------------------------------------------------------------------

// makeChunkDurable flips ledgers to frozen for a chunk — the
// pendingArtifacts-empty state highestDurableChunk counts.
func makeChunkDurable(t *testing.T, cat *Catalog, c chunk.ID) {
	t.Helper()
	freezeKinds(t, cat, c, KindLedgers)
}

// makeHotDir creates the on-disk hot dir for a chunk so deriveWatermark's
// per-ready-key dir-existence loop sees it present.
func makeHotDir(t *testing.T, cat *Catalog, c chunk.ID) {
	t.Helper()
	require.NoError(t, os.MkdirAll(cat.layout.HotChunkPath(c), 0o755))
}

// readyHot marks a chunk's hot key "ready" AND creates its dir, the production
// pairing deriveWatermark expects (a ready key whose dir is missing is loss).
func readyHot(t *testing.T, cat *Catalog, c chunk.ID) {
	t.Helper()
	require.NoError(t, cat.PutHotTransient(c))
	require.NoError(t, cat.FlipHotReady(c))
	makeHotDir(t, cat, c)
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

	t.Run("incompletely-frozen tip degrades the bound (ledgers freezing, not frozen)", func(t *testing.T) {
		cat, _ := testCatalog(t)
		makeChunkDurable(t, cat, 0)
		makeChunkDurable(t, cat, 1)
		// Chunk 2: ledgers only "freezing" — a mid-freeze crash. It must NOT
		// count: bound stays at chunk 1.
		require.NoError(t, cat.MarkChunkFreezing(2, KindLedgers))
		got, err := deriveCompleteThrough(cat)
		require.NoError(t, err)
		require.Equal(t, chunk.ID(1).LastLedger(), got)
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
		require.NoError(t, cat.PutEarliestLedger(floor))
		got, err := deriveCompleteThrough(cat)
		require.NoError(t, err)
		require.Equal(t, uint32(floor-1), got)
	})

	t.Run("earliest pin == genesis (2) does not underflow", func(t *testing.T) {
		cat, _ := testCatalog(t)
		require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))
		got, err := deriveCompleteThrough(cat)
		require.NoError(t, err)
		require.Equal(t, preGenesisLedger, got, "earliest 2 - 1 = 1, not MaxUint32")
	})

	t.Run("max of all three terms", func(t *testing.T) {
		cat, _ := testCatalog(t)
		makeChunkDurable(t, cat, 0) // cold => chunk 0 last ledger
		readyHot(t, cat, 4)         // positional => chunk 3 last ledger (highest)
		require.NoError(t, cat.PutEarliestLedger(2))
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
		// performs surfaces the loss as ErrHotVolumeLost with recovery guidance.
		require.NoError(t, cat.PutHotTransient(5))
		require.NoError(t, cat.FlipHotReady(5)) // ready key 5, NO dir
		probe := &fakeHotProbe{ok: false}       // OpenHotChunk reports dir absent
		_, err := deriveWatermark(cat, probe)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrHotVolumeLost)
		require.Contains(t, err.Error(), "00000005")
	})

	t.Run("fatal: refinement open error on the highest ready chunk", func(t *testing.T) {
		cat, _ := testCatalog(t)
		readyHot(t, cat, 3) // dir present
		probe := &fakeHotProbe{openErr: errors.New("rocksdb LOCK held")}
		_, err := deriveWatermark(cat, probe)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrHotVolumeLost)
	})

	t.Run("fatal: refinement read error", func(t *testing.T) {
		cat, _ := testCatalog(t)
		readyHot(t, cat, 3)
		probe := &fakeHotProbe{ok: true, chunk: &fakeHotChunk{maxErr: errors.New("corrupt")}}
		_, err := deriveWatermark(cat, probe)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrHotVolumeLost)
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

func mustDeriveCompleteThrough(t *testing.T, cat *Catalog) uint32 {
	t.Helper()
	got, err := deriveCompleteThrough(cat)
	require.NoError(t, err)
	return got
}
