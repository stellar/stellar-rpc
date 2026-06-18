package streaming

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// testBuildConfig wires a BuildConfig over the test catalog with a silent
// logger. Small windows let tests cover whole windows with a handful of chunks.
func testBuildConfig(cat *Catalog) BuildConfig {
	return BuildConfig{Catalog: cat, Logger: silentLogger()}
}

// smallWindowCatalog builds a test catalog whose windows are cpi chunks wide, so
// a "terminal" (full-window) build needs only a few chunks. Returns the catalog
// and the artifact root.
func smallWindowCatalog(t *testing.T, cpi uint32) (*Catalog, string) {
	t.Helper()
	cat, root := testCatalog(t)
	w, err := NewWindows(cpi)
	require.NoError(t, err)
	cat.windows = w
	return cat, root
}

// txEntry is a (full 32-byte tx hash, ledger seq) pair a test wants resolvable
// through the cold index.
type txEntry struct {
	hash [32]byte
	seq  uint32
}

// hashAt returns a deterministic 32-byte tx hash for a test tag.
func hashAt(tag uint64) [32]byte {
	var seed [8]byte
	binary.BigEndian.PutUint64(seed[:], tag)
	return sha256.Sum256(seed[:])
}

// freezeChunkBin writes a real sorted .bin for chunkID holding entries, fsyncs
// it, and flips chunk:{c}:txhash to "frozen" through the one-write protocol —
// the exact state buildTxhashIndex's precondition demands. Each entry's seq must
// fall in the chunk's ledger range; the helper assigns seqs the caller chose.
// Returns the entries (so the test can later assert each resolves to its seq).
func freezeChunkBin(t *testing.T, cat *Catalog, chunkID chunk.ID, entries []txEntry) {
	t.Helper()

	cold := make([]txhash.ColdEntry, len(entries))
	for i, e := range entries {
		require.GreaterOrEqual(t, e.seq, chunkID.FirstLedger(), "seq in chunk range")
		require.LessOrEqual(t, e.seq, chunkID.LastLedger(), "seq in chunk range")
		var key [txhash.ColdKeySize]byte
		copy(key[:], e.hash[:txhash.ColdKeySize])
		cold[i] = txhash.ColdEntry{Key: key, Seq: e.seq}
	}
	// WriteColdBin writes entries verbatim; they must be sorted lex by key.
	sort.Slice(cold, func(i, j int) bool {
		return string(cold[i].Key[:]) < string(cold[j].Key[:])
	})

	path := cat.layout.TxHashBinPath(chunkID)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, cat.MarkChunkFreezing(chunkID, KindTxHash))
	require.NoError(t, txhash.WriteColdBin(path, cold))
	require.NoError(t, barrierNewFile(path, true))
	require.NoError(t, cat.FlipChunkFrozen(chunkID, KindTxHash))
}

// seqIn returns a ledger seq inside chunkID's range, offset within the chunk.
func seqIn(chunkID chunk.ID, offset uint32) uint32 {
	return chunkID.FirstLedger() + offset
}

// assertCoverageQueryable opens the window's unique frozen coverage's .idx and
// asserts every (hash, seq) resolves and an unseen hash misses.
func assertCoverageQueryable(t *testing.T, cat *Catalog, w WindowID, want []txEntry) {
	t.Helper()
	frozen, ok, err := cat.FrozenCoverage(w)
	require.NoError(t, err)
	require.True(t, ok, "window %s must have a frozen coverage", w)

	reader, err := txhash.OpenColdReader(cat.layout.IndexFilePath(frozen))
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	for _, e := range want {
		got, gerr := reader.Get(e.hash)
		require.NoError(t, gerr, "hash %x must resolve", e.hash[:4])
		require.Equal(t, e.seq, got, "hash %x resolves to its seq", e.hash[:4])
	}

	// An unseen hash misses (the fingerprint rejects ~255/256; this one is well
	// outside the build set).
	_, miss := reader.Get(hashAt(0xDEADBEEF))
	require.ErrorIs(t, miss, stores.ErrNotFound)
}

// ---------------------------------------------------------------------------
// Happy path: build a coverage from synthetic .bin runs; assert the .idx is
// queryable and the catalog coverage is unique + frozen.
// ---------------------------------------------------------------------------

func TestBuildTxhashIndex_BuildsQueryableCoverage(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4) // window 0 = chunks [0,3]
	cfg := testBuildConfig(cat)

	// Two chunks, each with a couple of entries.
	e0a := txEntry{hashAt(1), seqIn(0, 5)}
	e0b := txEntry{hashAt(2), seqIn(0, 9000)}
	e1a := txEntry{hashAt(3), seqIn(1, 1)}
	freezeChunkBin(t, cat, 0, []txEntry{e0a, e0b})
	freezeChunkBin(t, cat, 1, []txEntry{e1a})

	// Non-terminal build [0,1] (hi 1 < window-last 3).
	require.NoError(t, buildTxhashIndex(context.Background(), 0, 0, 1, cfg))

	// Exactly one frozen coverage, covering [0,1].
	frozen, ok, err := cat.FrozenCoverage(0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, chunk.ID(0), frozen.Lo)
	require.Equal(t, chunk.ID(1), frozen.Hi)
	require.Equal(t, StateFrozen, frozen.State)

	// Only one coverage key in the window (no debris).
	keys, err := cat.IndexKeys(0)
	require.NoError(t, err)
	require.Len(t, keys, 1)

	// Non-terminal: .bin inputs stay frozen (window still filling).
	for _, c := range []chunk.ID{0, 1} {
		s, serr := cat.State(c, KindTxHash)
		require.NoError(t, serr)
		require.Equal(t, StateFrozen, s)
	}

	// The .idx resolves every entry.
	require.FileExists(t, cat.layout.IndexFilePath(frozen))
	assertCoverageQueryable(t, cat, 0, []txEntry{e0a, e0b, e1a})
}

// ---------------------------------------------------------------------------
// Rolling case: hi advances by one each boundary; the predecessor is demoted
// AND swept; exactly one frozen coverage exists at every instant.
// ---------------------------------------------------------------------------

func TestBuildThenSweep_RollingPredecessorDemotedAndSwept(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 10) // window 0 = chunks [0,9]
	cfg := testBuildConfig(cat)

	var all []txEntry
	for c := chunk.ID(0); c <= 4; c++ {
		e := txEntry{hashAt(uint64(100 + c)), seqIn(c, 7)}
		freezeChunkBin(t, cat, c, []txEntry{e})
		all = append(all, e)
	}

	var prevPath string
	for hi := chunk.ID(0); hi <= 4; hi++ {
		require.NoError(t, buildThenSweep(context.Background(), IndexBuild{Window: 0, Lo: 0, Hi: hi}, cfg))

		// Exactly one frozen coverage at this instant, covering [0,hi].
		frozen, ok, err := cat.FrozenCoverage(0)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, chunk.ID(0), frozen.Lo)
		require.Equal(t, hi, frozen.Hi)

		// Exactly ONE coverage key remains — the predecessor was demoted and the
		// eager sweep removed it (key + file).
		keys, err := cat.IndexKeys(0)
		require.NoError(t, err)
		require.Len(t, keys, 1, "exactly one coverage key after the eager sweep")
		require.Equal(t, frozen.Key, keys[0].Key)
		require.Equal(t, StateFrozen, keys[0].State)

		// The predecessor file is gone.
		if prevPath != "" {
			require.NoFileExists(t, prevPath)
		}
		prevPath = cat.layout.IndexFilePath(frozen)
		require.FileExists(t, prevPath)

		// Non-terminal (hi < 9): inputs stay frozen.
		for c := chunk.ID(0); c <= hi; c++ {
			s, serr := cat.State(c, KindTxHash)
			require.NoError(t, serr)
			require.Equal(t, StateFrozen, s)
		}
	}

	// The final coverage resolves every entry rolled in.
	assertCoverageQueryable(t, cat, 0, all)
}

// ---------------------------------------------------------------------------
// Terminal case: a full-window build demotes AND sweeps every in-window txhash
// key (the .bin inputs), and leaves exactly one frozen full-window coverage.
// ---------------------------------------------------------------------------

func TestBuildThenSweep_TerminalDemotesAndSweepsAllInputs(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4) // window 0 = chunks [0,3]
	cfg := testBuildConfig(cat)

	var all []txEntry
	for c := chunk.ID(0); c <= 3; c++ {
		e := txEntry{hashAt(uint64(200 + c)), seqIn(c, 11)}
		freezeChunkBin(t, cat, c, []txEntry{e})
		all = append(all, e)
	}
	// A non-txhash key in the window must survive the terminal sweep.
	require.NoError(t, cat.MarkChunkFreezing(2, KindLFS))
	require.NoError(t, cat.FlipChunkFrozen(2, KindLFS))

	// Terminal build [0,3]: hi == window-last 3.
	require.NoError(t, buildThenSweep(context.Background(), IndexBuild{Window: 0, Lo: 0, Hi: 3}, cfg))

	// Frozen full-window coverage.
	frozen, ok, err := cat.FrozenCoverage(0)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, cat.windows.IsTerminalCoverage(frozen))
	require.Equal(t, chunk.ID(3), frozen.Hi)

	// Every in-window txhash key was demoted AND swept: key absent => .bin gone.
	for c := chunk.ID(0); c <= 3; c++ {
		s, serr := cat.State(c, KindTxHash)
		require.NoError(t, serr)
		require.Equal(t, State(""), s, "chunk %s txhash key swept", c)
		require.NoFileExists(t, cat.layout.TxHashBinPath(c))
	}
	// The lfs key (and file would be) untouched.
	lfs, err := cat.State(2, KindLFS)
	require.NoError(t, err)
	require.Equal(t, StateFrozen, lfs)

	// The terminal .idx still resolves every entry after the input sweep.
	assertCoverageQueryable(t, cat, 0, all)
}

// ---------------------------------------------------------------------------
// Skip case: if the window's unique frozen coverage already equals [lo,hi], the
// build returns early — no precondition demand on .bin inputs (load-bearing for
// re-scheduled finalized windows whose inputs the sweep deleted).
// ---------------------------------------------------------------------------

func TestBuildTxhashIndex_SkipsWhenCoverageAlreadyFrozen(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4)
	cfg := testBuildConfig(cat)

	e := txEntry{hashAt(300), seqIn(0, 3)}
	freezeChunkBin(t, cat, 0, []txEntry{e})
	freezeChunkBin(t, cat, 1, []txEntry{{hashAt(301), seqIn(1, 4)}})

	// First build [0,1].
	require.NoError(t, buildTxhashIndex(context.Background(), 0, 0, 1, cfg))
	frozen, ok, err := cat.FrozenCoverage(0)
	require.NoError(t, err)
	require.True(t, ok)
	idxPath := cat.layout.IndexFilePath(frozen)
	before, err := os.Stat(idxPath)
	require.NoError(t, err)

	// Now demote the .bin inputs to "pruning" — simulating a finalized window
	// whose inputs the sweep is about to remove. A second build of the SAME
	// coverage must SKIP (never demand the now-non-frozen inputs).
	require.NoError(t, cat.store.Put(chunkKey(0, KindTxHash), string(StatePruning)))
	require.NoError(t, cat.store.Put(chunkKey(1, KindTxHash), string(StatePruning)))

	require.NoError(t, buildTxhashIndex(context.Background(), 0, 0, 1, cfg),
		"skip check must precede the precondition")

	// The .idx was not rewritten (same file, untouched).
	after, err := os.Stat(idxPath)
	require.NoError(t, err)
	require.Equal(t, before.ModTime(), after.ModTime(), "skipped build must not rewrite the .idx")

	// Still exactly one frozen coverage.
	keys, err := cat.IndexKeys(0)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, StateFrozen, keys[0].State)
}

// ---------------------------------------------------------------------------
// Loud precondition: a chunk in [lo,hi] whose .bin is not frozen aborts the
// build BEFORE any key is touched — no coverage key is left behind.
// ---------------------------------------------------------------------------

func TestBuildTxhashIndex_PreconditionFailsLoudly(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4)
	cfg := testBuildConfig(cat)

	// Chunk 0 frozen, chunk 1 absent (never produced).
	freezeChunkBin(t, cat, 0, []txEntry{{hashAt(400), seqIn(0, 1)}})

	err := buildTxhashIndex(context.Background(), 0, 0, 1, cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "precondition violated")
	require.Contains(t, err.Error(), "chunk 00000001")

	// No coverage key was written (the precondition precedes the mark).
	keys, err := cat.IndexKeys(0)
	require.NoError(t, err)
	require.Empty(t, keys, "a precondition failure must not leave a coverage key")
	require.NoFileExists(t, cat.layout.IndexFilePath(IndexCoverage{Window: 0, Lo: 0, Hi: 1}))

	// A "freezing" (in-progress) input is also not "frozen" => still aborts.
	require.NoError(t, cat.MarkChunkFreezing(1, KindTxHash))
	err = buildTxhashIndex(context.Background(), 0, 0, 1, cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "precondition violated")
}

// ---------------------------------------------------------------------------
// §7.6 crash matrix — three rows, each converging on a re-run.
// ---------------------------------------------------------------------------

// Row "after step 2, mid step 3": coverage key "freezing", file partial/complete,
// predecessor still the unique frozen coverage. A re-run of the same coverage
// re-marks and rewrites wholesale, converging on a single frozen coverage.
func TestBuildCrashMatrix_AfterMarkBeforeCommit(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 10)
	cfg := testBuildConfig(cat)

	for c := chunk.ID(0); c <= 2; c++ {
		freezeChunkBin(t, cat, c, []txEntry{{hashAt(uint64(500 + c)), seqIn(c, 2)}})
	}

	// Land a predecessor coverage [0,1] first.
	require.NoError(t, buildTxhashIndex(context.Background(), 0, 0, 1, cfg))
	predFrozen, ok, err := cat.FrozenCoverage(0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, chunk.ID(1), predFrozen.Hi)

	// "Crash" the next build [0,2] right after the mark (before the commit) by
	// observing state in the afterIndexMark hook, then aborting via a panic the
	// test recovers — simulating process death between step 2 and step 4.
	cat.hooks.afterIndexMark = func() {
		// At this instant: new key "freezing", predecessor still the unique frozen
		// coverage (no two-frozen window).
		frozen, fok, ferr := cat.FrozenCoverage(0)
		require.NoError(t, ferr)
		require.True(t, fok)
		require.Equal(t, predFrozen.Key, frozen.Key, "predecessor still the unique frozen coverage")
		v, vok, verr := cat.Get(indexKey(0, 0, 2))
		require.NoError(t, verr)
		require.True(t, vok)
		require.Equal(t, string(StateFreezing), v, "new coverage marked freezing")
		panic("crash after mark")
	}
	require.PanicsWithValue(t, "crash after mark", func() {
		_ = buildTxhashIndex(context.Background(), 0, 0, 2, cfg)
	})
	cat.hooks.afterIndexMark = nil

	// Durable state after the "crash": predecessor [0,1] frozen, [0,2] "freezing"
	// debris.
	keys, err := cat.IndexKeys(0)
	require.NoError(t, err)
	states := map[string]State{}
	for _, k := range keys {
		states[k.Key] = k.State
	}
	require.Equal(t, StateFrozen, states[indexKey(0, 0, 1)])
	require.Equal(t, StateFreezing, states[indexKey(0, 0, 2)])

	// Recovery: re-run the build of [0,2]. It re-marks (idempotent overwrite),
	// rewrites the .idx, and commits — converging on a single frozen coverage.
	require.NoError(t, buildThenSweep(context.Background(), IndexBuild{Window: 0, Lo: 0, Hi: 2}, cfg))
	frozen, ok, err := cat.FrozenCoverage(0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, chunk.ID(2), frozen.Hi)
	// The predecessor [0,1] was demoted by the commit and swept eagerly.
	keys, err = cat.IndexKeys(0)
	require.NoError(t, err)
	require.Len(t, keys, 1, "exactly one coverage after recovery")
	require.Equal(t, indexKey(0, 0, 2), keys[0].Key)
	assertCoverageQueryable(t, cat, 0, []txEntry{{hashAt(500), seqIn(0, 2)}, {hashAt(501), seqIn(1, 2)}, {hashAt(502), seqIn(2, 2)}})
}

// Row "after step 4, before the eager sweep": the commit batch landed (new
// coverage frozen + live, predecessor "pruning", terminal inputs "pruning") but
// the sweeps did not run. Re-running buildThenSweep finishes the sweeps.
func TestBuildCrashMatrix_AfterCommitBeforeSweep(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4) // window 0 = chunks [0,3]
	cfg := testBuildConfig(cat)

	for c := chunk.ID(0); c <= 3; c++ {
		freezeChunkBin(t, cat, c, []txEntry{{hashAt(uint64(600 + c)), seqIn(c, 3)}})
	}
	// A predecessor [0,2] so the commit has a coverage to demote too.
	require.NoError(t, buildTxhashIndex(context.Background(), 0, 0, 2, cfg))
	predPath := cat.layout.IndexFilePath(IndexCoverage{Window: 0, Lo: 0, Hi: 2})

	// "Crash" the terminal build [0,3] right after the commit, before the sweeps.
	cat.hooks.afterCommitBeforeSweep = func() {
		// New coverage frozen + live; predecessor and inputs "pruning" sweep work.
		frozen, fok, ferr := cat.FrozenCoverage(0)
		require.NoError(t, ferr)
		require.True(t, fok)
		require.Equal(t, chunk.ID(3), frozen.Hi)
		v, _, _ := cat.Get(indexKey(0, 0, 2))
		require.Equal(t, string(StatePruning), v, "predecessor demoted, not yet swept")
		for c := chunk.ID(0); c <= 3; c++ {
			s, _ := cat.State(c, KindTxHash)
			require.Equal(t, StatePruning, s, "input demoted, not yet swept")
		}
		panic("crash after commit")
	}
	require.PanicsWithValue(t, "crash after commit", func() {
		_ = buildThenSweep(context.Background(), IndexBuild{Window: 0, Lo: 0, Hi: 3}, cfg)
	})
	cat.hooks.afterCommitBeforeSweep = nil

	// The predecessor file and the .bin inputs are still on disk (sweeps didn't
	// run), but their keys are "pruning".
	require.FileExists(t, predPath)
	for c := chunk.ID(0); c <= 3; c++ {
		require.FileExists(t, cat.layout.TxHashBinPath(c))
	}

	// Recovery: re-run buildThenSweep for [0,3]. buildTxhashIndex SKIPS (already
	// frozen) and the eager sweeps finish the demoted predecessor + inputs.
	require.NoError(t, buildThenSweep(context.Background(), IndexBuild{Window: 0, Lo: 0, Hi: 3}, cfg))
	require.NoFileExists(t, predPath)
	for c := chunk.ID(0); c <= 3; c++ {
		require.NoFileExists(t, cat.layout.TxHashBinPath(c))
		s, serr := cat.State(c, KindTxHash)
		require.NoError(t, serr)
		require.Equal(t, State(""), s)
	}
	keys, err := cat.IndexKeys(0)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, StateFrozen, keys[0].State)
}

// Row "mid-sweep": a "pruning" key whose durable unlink completed but whose key
// delete didn't. The sweep re-runs; key absent => file gone. Driven through the
// real SweepChunkArtifacts via buildThenSweep's beforeKeyDelete hook.
func TestBuildCrashMatrix_MidSweepReRuns(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4)
	cfg := testBuildConfig(cat)

	for c := chunk.ID(0); c <= 3; c++ {
		freezeChunkBin(t, cat, c, []txEntry{{hashAt(uint64(700 + c)), seqIn(c, 4)}})
	}

	// "Crash" mid-sweep: inside SweepChunkArtifacts, after the durable unlink and
	// before the key-delete batch. The files are already gone here; the keys are
	// not. Panic to simulate process death at that exact instant.
	cat.hooks.beforeKeyDelete = func() {
		for c := chunk.ID(0); c <= 3; c++ {
			require.NoFileExists(t, cat.layout.TxHashBinPath(c), "unlink durable before key delete")
		}
		panic("crash mid-sweep")
	}
	require.PanicsWithValue(t, "crash mid-sweep", func() {
		_ = buildThenSweep(context.Background(), IndexBuild{Window: 0, Lo: 0, Hi: 3}, cfg)
	})
	cat.hooks.beforeKeyDelete = nil

	// The terminal commit landed (coverage frozen), the input .bin files are gone,
	// but their keys survive as "pruning" — the mid-sweep leftover the next run
	// finishes.
	frozen, ok, err := cat.FrozenCoverage(0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, chunk.ID(3), frozen.Hi)
	pruningLeft := 0
	for c := chunk.ID(0); c <= 3; c++ {
		require.NoFileExists(t, cat.layout.TxHashBinPath(c))
		s, serr := cat.State(c, KindTxHash)
		require.NoError(t, serr)
		require.Equal(t, StatePruning, s, "key outlives the durable unlink")
		pruningLeft++
	}
	require.Equal(t, 4, pruningLeft)

	// Recovery: re-run buildThenSweep. The build skips (frozen) and the sweep
	// re-runs over the surviving "pruning" keys, converging on key absent.
	require.NoError(t, buildThenSweep(context.Background(), IndexBuild{Window: 0, Lo: 0, Hi: 3}, cfg))
	for c := chunk.ID(0); c <= 3; c++ {
		s, serr := cat.State(c, KindTxHash)
		require.NoError(t, serr)
		require.Equal(t, State(""), s, "mid-sweep leftover finished on re-run")
	}
	assertCoverageQueryable(t, cat, 0, []txEntry{{hashAt(700), seqIn(0, 4)}})
}

// ---------------------------------------------------------------------------
// Config validation + lo>hi guard.
// ---------------------------------------------------------------------------

func TestBuildConfigValidation(t *testing.T) {
	cat, _ := testCatalog(t)
	require.Error(t, buildTxhashIndex(context.Background(), 0, 0, 0, BuildConfig{Logger: silentLogger()}))
	require.Error(t, buildTxhashIndex(context.Background(), 0, 0, 0, BuildConfig{Catalog: cat}))
	// lo > hi is a programmer error surfaced loudly.
	require.Error(t, buildTxhashIndex(context.Background(), 0, 5, 1, testBuildConfig(cat)))
}
