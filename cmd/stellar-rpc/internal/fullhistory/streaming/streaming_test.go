package streaming

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
)

const testCPI = 1000 // chunks_per_txhash_index for tests (the default)

func silentLogger() *supportlog.Entry {
	var buf bytes.Buffer
	log := supportlog.New()
	log.SetLevel(logrus.DebugLevel)
	log.SetOutput(&buf)
	return log
}

// testCatalog builds a Catalog over a real metastore.Store on a temp dir plus
// a temp artifact dir (the Layout root). Returns the catalog and the artifact
// root so tests can assert against real files on disk.
func testCatalog(t *testing.T) (*Catalog, string) {
	t.Helper()
	metaDir := t.TempDir()
	artifactRoot := t.TempDir()

	store, err := metastore.New(filepath.Join(metaDir, "rocksdb"), silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	windows, err := NewWindows(testCPI)
	require.NoError(t, err)

	return NewCatalog(store, NewLayout(artifactRoot), windows), artifactRoot
}

// writeArtifact materializes a placeholder file at path (creating parents) so a
// sweep has something real to unlink.
func writeArtifact(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte("artifact"), 0o644))
}

// ---------------------------------------------------------------------------
// Window arithmetic.
// ---------------------------------------------------------------------------

func TestNewWindows_Validation(t *testing.T) {
	_, err := NewWindows(0)
	require.Error(t, err)

	_, err = NewWindows(MaxChunksPerTxhashIndex + 1)
	require.Error(t, err)

	w, err := NewWindows(MaxChunksPerTxhashIndex)
	require.NoError(t, err)
	require.Equal(t, MaxChunksPerTxhashIndex, w.ChunksPerIndex())
}

func TestWindowArithmetic(t *testing.T) {
	w, err := NewWindows(1000)
	require.NoError(t, err)

	tests := []struct {
		name              string
		chunkID           chunk.ID
		wantWindow        WindowID
		wantFirst, wantHi chunk.ID
	}{
		{"first chunk of window 0", 0, 0, 0, 999},
		{"mid window 0", 500, 0, 0, 999},
		{"last chunk of window 0", 999, 0, 0, 999},
		{"first chunk of window 1", 1000, 1, 1000, 1999},
		{"the doc's example chunk 5350", 5350, 5, 5000, 5999},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wantWindow, w.WindowID(tc.chunkID))
			require.Equal(t, tc.wantFirst, w.FirstChunk(tc.wantWindow))
			require.Equal(t, tc.wantHi, w.LastChunk(tc.wantWindow))
			require.Equal(t, uint32(1000), w.ChunksIn())
		})
	}
}

func TestIsTerminalCoverage(t *testing.T) {
	w, err := NewWindows(1000)
	require.NoError(t, err)

	// hi == window's last chunk => terminal.
	require.True(t, w.IsTerminalCoverage(IndexCoverage{Window: 5, Lo: 5100, Hi: 5999}))
	// hi below the last chunk => not terminal (still filling).
	require.False(t, w.IsTerminalCoverage(IndexCoverage{Window: 5, Lo: 5100, Hi: 5349}))
}

// ---------------------------------------------------------------------------
// Key <-> path bijection, both directions.
// ---------------------------------------------------------------------------

func TestKeyConstructorsMatchSpec(t *testing.T) {
	require.Equal(t, "chunk:00005350:lfs", chunkKey(5350, KindLFS))
	require.Equal(t, "chunk:00005350:events", chunkKey(5350, KindEvents))
	require.Equal(t, "chunk:00005350:txhash", chunkKey(5350, KindTxHash))
	require.Equal(t, "hot:chunk:00005350", hotChunkKey(5350))
	require.Equal(t, "index:00000005:00005100:00005349", indexKey(5, 5100, 5349))
}

func TestChunkKeyBijection(t *testing.T) {
	for _, kind := range AllKinds() {
		for _, id := range []chunk.ID{0, 1, 999, 1000, 5350, chunk.ID(MaxChunksPerTxhashIndex)} {
			key := chunkKey(id, kind)
			gotID, gotKind, ok := parseChunkKey(key)
			require.True(t, ok, "parse %q", key)
			require.Equal(t, id, gotID)
			require.Equal(t, kind, gotKind)
		}
	}
}

func TestHotKeyBijection(t *testing.T) {
	for _, id := range []chunk.ID{0, 7, 5350} {
		key := hotChunkKey(id)
		got, ok := parseHotChunkKey(key)
		require.True(t, ok)
		require.Equal(t, id, got)
	}
}

func TestIndexKeyBijection(t *testing.T) {
	cov := IndexCoverage{Window: 5, Lo: 5100, Hi: 5349}
	key := indexKey(cov.Window, cov.Lo, cov.Hi)
	got, ok := parseIndexKey(key)
	require.True(t, ok)
	require.Equal(t, cov.Window, got.Window)
	require.Equal(t, cov.Lo, got.Lo)
	require.Equal(t, cov.Hi, got.Hi)
	require.Equal(t, key, got.Key)
}

func TestKeyToPathBijection(t *testing.T) {
	l := NewLayout("/data")

	// The doc's directory-layout examples.
	require.Equal(t, "/data/ledgers/00005/00005350.pack", l.LedgerPackPath(5350))
	require.Equal(t, "/data/txhash/raw/00005/00005350.bin", l.TxHashBinPath(5350))
	require.Equal(t, []string{
		"/data/events/00005/00005350-events.pack",
		"/data/events/00005/00005350-index.pack",
		"/data/events/00005/00005350-index.hash",
	}, l.EventsPaths(5350))
	require.Equal(t, "/data/hot/00005350", l.HotChunkPath(5350))

	cov := IndexCoverage{Window: 5, Lo: 5100, Hi: 5349}
	require.Equal(t, "/data/txhash/index/00000005", l.IndexWindowDir(cov.Window))
	require.Equal(t, "/data/txhash/index/00000005/00005100-00005349.idx", l.IndexFilePath(cov))
}

func TestParseRejectsMalformed(t *testing.T) {
	bad := []string{
		"chunk:5350:lfs",          // not 8-digit padded
		"chunk:00005350:bogus",    // unknown kind
		"chunk:00005350",          // missing kind
		"hot:chunk:5350",          // not padded
		"index:00000005:00005100", // too few segments
		"index:5:5100:5349",       // not padded
		"unrelated:key",           // wrong family
	}
	for _, key := range bad {
		_, _, okChunk := parseChunkKey(key)
		_, okHot := parseHotChunkKey(key)
		_, okIdx := parseIndexKey(key)
		require.False(t, okChunk && okHot && okIdx, "expected %q to be rejected by all parsers", key)
	}
	// Specific rejections.
	_, _, ok := parseChunkKey("chunk:00005350:bogus")
	require.False(t, ok)
	_, ok2 := parseIndexKey("index:00000005:00005349:00005100") // lo > hi
	require.False(t, ok2)
}

func TestIndexKeyPanicsOnLoGreaterThanHi(t *testing.T) {
	require.Panics(t, func() { indexKey(5, 5349, 5100) })
}

// ---------------------------------------------------------------------------
// Round-trip every key family through the real metastore.
// ---------------------------------------------------------------------------

func TestRoundTripChunkKeys(t *testing.T) {
	cat, _ := testCatalog(t)

	for _, kind := range AllKinds() {
		state, err := cat.State(42, kind)
		require.NoError(t, err)
		require.Equal(t, State(""), state, "absent key reads as empty State")
	}

	require.NoError(t, cat.MarkChunkFreezing(42, AllKinds()...))
	for _, kind := range AllKinds() {
		state, err := cat.State(42, kind)
		require.NoError(t, err)
		require.Equal(t, StateFreezing, state)
	}

	require.NoError(t, cat.FlipChunkFrozen(42, AllKinds()...))
	for _, kind := range AllKinds() {
		state, err := cat.State(42, kind)
		require.NoError(t, err)
		require.Equal(t, StateFrozen, state)
	}
}

func TestRoundTripHotKeys(t *testing.T) {
	cat, _ := testCatalog(t)

	state, err := cat.HotState(7)
	require.NoError(t, err)
	require.Equal(t, HotState(""), state)

	require.NoError(t, cat.PutHotTransient(7))
	state, err = cat.HotState(7)
	require.NoError(t, err)
	require.Equal(t, HotTransient, state)

	require.NoError(t, cat.FlipHotReady(7))
	state, err = cat.HotState(7)
	require.NoError(t, err)
	require.Equal(t, HotReady, state)

	require.NoError(t, cat.DeleteHotKey(7))
	state, err = cat.HotState(7)
	require.NoError(t, err)
	require.Equal(t, HotState(""), state)
	// Idempotent on a missing key.
	require.NoError(t, cat.DeleteHotKey(7))
}

func TestRoundTripIndexKey(t *testing.T) {
	cat, _ := testCatalog(t)

	cov, err := cat.MarkIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	require.Equal(t, StateFreezing, cov.State)

	keys, err := cat.IndexKeys(5)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, StateFreezing, keys[0].State)
	require.Equal(t, chunk.ID(5100), keys[0].Lo)
	require.Equal(t, chunk.ID(5349), keys[0].Hi)
}

func TestConfigPins(t *testing.T) {
	cat, _ := testCatalog(t)

	_, ok, err := cat.EarliestLedger()
	require.NoError(t, err)
	require.False(t, ok, "pristine store has no earliest_ledger pin")

	require.NoError(t, cat.PutEarliestLedger(2))
	el, ok, err := cat.EarliestLedger()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(2), el)

	_, ok, err = cat.ChunksPerTxhashIndex()
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, cat.PutChunksPerTxhashIndex(testCPI))
	cpi, ok, err := cat.ChunksPerTxhashIndex()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(testCPI), cpi)
}

// ---------------------------------------------------------------------------
// Scans: HotChunkKeys (value-blind) vs ReadyHotChunkKeys (ready-only).
// ---------------------------------------------------------------------------

func TestHotChunkKeysValueBlindVsReadyOnly(t *testing.T) {
	cat, _ := testCatalog(t)

	require.NoError(t, cat.PutHotTransient(3))
	require.NoError(t, cat.FlipHotReady(5))
	require.NoError(t, cat.PutHotTransient(9))
	require.NoError(t, cat.FlipHotReady(12))

	all, err := cat.HotChunkKeys()
	require.NoError(t, err)
	require.Equal(t, []chunk.ID{3, 5, 9, 12}, all, "value-blind: every hot key")

	ready, err := cat.ReadyHotChunkKeys()
	require.NoError(t, err)
	require.Equal(t, []chunk.ID{5, 12}, ready, "ready-only excludes transient")
}

func TestChunkArtifactKeys(t *testing.T) {
	cat, _ := testCatalog(t)

	require.NoError(t, cat.MarkChunkFreezing(1, KindLFS))
	require.NoError(t, cat.FlipChunkFrozen(2, KindEvents))

	refs, err := cat.ChunkArtifactKeys()
	require.NoError(t, err)
	require.Len(t, refs, 2)
	// Sorted by key: chunk:00000001:lfs before chunk:00000002:events.
	require.Equal(t, ArtifactRef{Chunk: 1, Kind: KindLFS, State: StateFreezing}, refs[0])
	require.Equal(t, ArtifactRef{Chunk: 2, Kind: KindEvents, State: StateFrozen}, refs[1])
}

// ---------------------------------------------------------------------------
// frozenCoverage: uniqueness + none-case.
// ---------------------------------------------------------------------------

func TestFrozenCoverageNone(t *testing.T) {
	cat, _ := testCatalog(t)

	_, ok, err := cat.FrozenCoverage(5)
	require.NoError(t, err)
	require.False(t, ok, "no coverage at all")

	// A "freezing" coverage is not frozen.
	_, err = cat.MarkIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	_, ok, err = cat.FrozenCoverage(5)
	require.NoError(t, err)
	require.False(t, ok, "freezing is not frozen")
}

func TestFrozenCoverageUnique(t *testing.T) {
	cat, _ := testCatalog(t)

	cov, err := cat.MarkIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	require.NoError(t, cat.CommitIndex(cov))

	got, ok, err := cat.FrozenCoverage(5)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, chunk.ID(5100), got.Lo)
	require.Equal(t, chunk.ID(5349), got.Hi)
}

func TestFrozenCoverageDetectsTwoFrozen(t *testing.T) {
	cat, _ := testCatalog(t)

	// Force the invariant-violating state directly through the store: two
	// frozen coverages in one window. FrozenCoverage must detect it, not pick
	// one.
	require.NoError(t, cat.store.Put(indexKey(5, 5100, 5349), string(StateFrozen)))
	require.NoError(t, cat.store.Put(indexKey(5, 5100, 5350), string(StateFrozen)))

	_, _, err := cat.FrozenCoverage(5)
	require.Error(t, err)
	require.Contains(t, err.Error(), "uniqueness invariant violated")
}

// ---------------------------------------------------------------------------
// Index commit batch atomicity: promote + demote + terminal land together.
// ---------------------------------------------------------------------------

func TestCommitIndexPromoteAndDemote(t *testing.T) {
	cat, _ := testCatalog(t)

	// First coverage [5100,5349] becomes frozen.
	cov1, err := cat.MarkIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	require.NoError(t, cat.CommitIndex(cov1))

	// Next boundary: [5100,5350]. Commit promotes it and demotes [5100,5349].
	cov2, err := cat.MarkIndexFreezing(5, 5100, 5350)
	require.NoError(t, err)
	require.NoError(t, cat.CommitIndex(cov2))

	// Exactly one frozen coverage — the new one.
	frozen, ok, err := cat.FrozenCoverage(5)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, chunk.ID(5350), frozen.Hi)

	// The predecessor is now "pruning".
	keys, err := cat.IndexKeys(5)
	require.NoError(t, err)
	states := map[string]State{}
	for _, k := range keys {
		states[k.Key] = k.State
	}
	require.Equal(t, StatePruning, states[indexKey(5, 5100, 5349)])
	require.Equal(t, StateFrozen, states[indexKey(5, 5100, 5350)])
}

func TestCommitIndexTerminalDemotesTxhashKeys(t *testing.T) {
	cat, _ := testCatalog(t)

	// Window 0 (chunks 0..999). Mark a few chunks' .bin frozen.
	for _, c := range []chunk.ID{0, 1, 500, 999} {
		require.NoError(t, cat.MarkChunkFreezing(c, KindTxHash))
		require.NoError(t, cat.FlipChunkFrozen(c, KindTxHash))
	}
	// A non-txhash key in the window must NOT be demoted.
	require.NoError(t, cat.FlipChunkFrozen(500, KindLFS))

	// Terminal build covers the whole window [0,999] => hi == last chunk.
	cov, err := cat.MarkIndexFreezing(0, 0, 999)
	require.NoError(t, err)
	require.True(t, cat.windows.IsTerminalCoverage(cov))
	require.NoError(t, cat.CommitIndex(cov))

	// Every present txhash key in the window demoted to "pruning".
	for _, c := range []chunk.ID{0, 1, 500, 999} {
		s, err := cat.State(c, KindTxHash)
		require.NoError(t, err)
		require.Equal(t, StatePruning, s, "chunk %d txhash", c)
	}
	// The lfs key is untouched.
	lfs, err := cat.State(500, KindLFS)
	require.NoError(t, err)
	require.Equal(t, StateFrozen, lfs)

	// And the index coverage is frozen.
	frozen, ok, err := cat.FrozenCoverage(0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, chunk.ID(999), frozen.Hi)
}

func TestCommitIndexNonTerminalLeavesTxhashKeys(t *testing.T) {
	cat, _ := testCatalog(t)

	require.NoError(t, cat.MarkChunkFreezing(0, KindTxHash))
	require.NoError(t, cat.FlipChunkFrozen(0, KindTxHash))

	// Non-terminal: hi (5) < window's last chunk (999).
	cov, err := cat.MarkIndexFreezing(0, 0, 5)
	require.NoError(t, err)
	require.False(t, cat.windows.IsTerminalCoverage(cov))
	require.NoError(t, cat.CommitIndex(cov))

	// txhash key NOT demoted — the window is still filling.
	s, err := cat.State(0, KindTxHash)
	require.NoError(t, err)
	require.Equal(t, StateFrozen, s)
}

// CommitIndex's finalization is one atomic batch: promote-new + demote-prev (+
// demote terminal txhash keys) land together or not at all. We prove it by
// fault-injecting a failure INSIDE the batch callback (which makes metastore
// drop the whole batch) and then asserting NOTHING the batch would have written
// is observable: the predecessor is still the unique frozen coverage, the new
// coverage is still "freezing", and the in-window txhash keys are still frozen.
// Rewriting CommitIndex as separate non-atomic Puts would leave some of those
// writes durable here and fail this test.
func TestCommitIndexBatchIsAtomic(t *testing.T) {
	cat, _ := testCatalog(t)

	// Predecessor [0,499] frozen.
	prev, err := cat.MarkIndexFreezing(0, 0, 499)
	require.NoError(t, err)
	require.NoError(t, cat.CommitIndex(prev))

	// A terminal txhash input that a successful terminal commit would demote.
	require.NoError(t, cat.MarkChunkFreezing(0, KindTxHash))
	require.NoError(t, cat.FlipChunkFrozen(0, KindTxHash))

	// The new TERMINAL coverage [0,999] — exercises all three batch puts at once.
	cov, err := cat.MarkIndexFreezing(0, 0, 999)
	require.NoError(t, err)
	require.True(t, cat.windows.IsTerminalCoverage(cov))

	// Fail the batch from inside its callback: metastore drops the whole batch.
	cat.hooks.failCommitBatch = func() bool { return true }
	err = cat.CommitIndex(cov)
	require.Error(t, err, "CommitIndex must surface the injected batch failure")
	cat.hooks.failCommitBatch = nil

	// All-or-nothing: the failed batch wrote NOTHING.
	// (1) The predecessor is still the window's unique frozen coverage.
	frozen, ok, err := cat.FrozenCoverage(0)
	require.NoError(t, err, "must not observe two frozen coverages")
	require.True(t, ok)
	require.Equal(t, chunk.ID(499), frozen.Hi, "predecessor still the unique frozen coverage")
	// (2) The new coverage is still merely "freezing" (its promote did not land).
	v, ok, err := cat.Get(cov.Key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, string(StateFreezing), v)
	// (3) The terminal txhash input was not demoted.
	s, err := cat.State(0, KindTxHash)
	require.NoError(t, err)
	require.Equal(t, StateFrozen, s)

	// And a clean re-commit (no fault) lands the whole batch.
	require.NoError(t, cat.CommitIndex(cov))
	frozen, ok, err = cat.FrozenCoverage(0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, chunk.ID(999), frozen.Hi)
	prevState, ok, err := cat.Get(prev.Key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, string(StatePruning), prevState)
	s, err = cat.State(0, KindTxHash)
	require.NoError(t, err)
	require.Equal(t, StatePruning, s)
}

// CommitIndex is documented crash-safe to re-run on the same coverage (the
// hasPrev && prev.Key == cov.Key branch in protocol.go): a re-commit of an
// already-landed batch must be a no-op overwrite, leaving exactly one frozen
// coverage and nothing demoted against itself. This exercises that branch,
// which no other test touched.
func TestCommitIndexReCommitIsIdempotent(t *testing.T) {
	cat, _ := testCatalog(t)

	cov, err := cat.MarkIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	require.NoError(t, cat.CommitIndex(cov))

	// Second commit on the SAME coverage: the predecessor IS cov, so the demote
	// branch is skipped and the promote is an idempotent overwrite.
	require.NoError(t, cat.CommitIndex(cov))

	// Exactly one frozen coverage remains, and it is cov — not demoted against
	// itself, no debris.
	keys, err := cat.IndexKeys(5)
	require.NoError(t, err)
	require.Len(t, keys, 1, "exactly one coverage key in the window")
	require.Equal(t, cov.Key, keys[0].Key)
	require.Equal(t, StateFrozen, keys[0].State, "re-commit must leave it frozen, not pruning")

	frozen, ok, err := cat.FrozenCoverage(5)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, chunk.ID(5349), frozen.Hi)
}

// ---------------------------------------------------------------------------
// Sweeps: the two deletion bodies.
// ---------------------------------------------------------------------------

func TestSweepChunkArtifacts(t *testing.T) {
	cat, root := testCatalog(t)
	_ = root

	// Set up a frozen lfs + frozen events for chunk 3, with real files.
	lfsPath := cat.layout.LedgerPackPath(3)
	writeArtifact(t, lfsPath)
	require.NoError(t, cat.MarkChunkFreezing(3, KindLFS))
	require.NoError(t, cat.FlipChunkFrozen(3, KindLFS))

	eventsPaths := cat.layout.EventsPaths(3)
	for _, p := range eventsPaths {
		writeArtifact(t, p)
	}
	require.NoError(t, cat.MarkChunkFreezing(3, KindEvents))
	require.NoError(t, cat.FlipChunkFrozen(3, KindEvents))

	refs := []ArtifactRef{
		{Chunk: 3, Kind: KindLFS, State: StateFrozen},
		{Chunk: 3, Kind: KindEvents, State: StateFrozen},
	}
	require.NoError(t, cat.SweepChunkArtifacts(refs))

	// Files gone.
	require.NoFileExists(t, lfsPath)
	for _, p := range eventsPaths {
		require.NoFileExists(t, p)
	}
	// Keys gone (key absent => file gone).
	for _, kind := range []Kind{KindLFS, KindEvents} {
		s, err := cat.State(3, kind)
		require.NoError(t, err)
		require.Equal(t, State(""), s)
	}
}

func TestSweepChunkArtifactsIdempotentOnMissingFiles(t *testing.T) {
	cat, _ := testCatalog(t)

	// Key present, file never written (a "pruning" leftover whose file is
	// already gone).
	require.NoError(t, cat.store.Put(chunkKey(8, KindLFS), string(StatePruning)))
	require.NoError(t, cat.SweepChunkArtifacts([]ArtifactRef{
		{Chunk: 8, Kind: KindLFS, State: StatePruning},
	}))
	s, err := cat.State(8, KindLFS)
	require.NoError(t, err)
	require.Equal(t, State(""), s)
}

func TestSweepIndexKey(t *testing.T) {
	cat, _ := testCatalog(t)

	cov, err := cat.MarkIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	idxPath := cat.layout.IndexFilePath(cov)
	writeArtifact(t, idxPath)
	require.NoError(t, cat.CommitIndex(cov))

	// Re-read as frozen for the sweep.
	frozen, ok, err := cat.FrozenCoverage(5)
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, cat.SweepIndexKey(frozen))

	require.NoFileExists(t, idxPath)
	keys, err := cat.IndexKeys(5)
	require.NoError(t, err)
	require.Empty(t, keys, "key absent => file gone")
}

func TestSweepIndexKeyFreezingDebris(t *testing.T) {
	cat, _ := testCatalog(t)

	// A crashed attempt: "freezing" key with a partial file.
	cov, err := cat.MarkIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	idxPath := cat.layout.IndexFilePath(cov)
	writeArtifact(t, idxPath)

	require.NoError(t, cat.SweepIndexKey(cov))
	require.NoFileExists(t, idxPath)
	keys, err := cat.IndexKeys(5)
	require.NoError(t, err)
	require.Empty(t, keys)
}

// ---------------------------------------------------------------------------
// CRASH-SAFETY tests — interpose at the two dangerous instants and assert both
// invariants: (A) every file on disk has its meta key; (B) key absent => file
// gone.
// ---------------------------------------------------------------------------

// assertEveryFileHasKey walks every artifact file under root and asserts a
// non-empty meta-store key exists for it (Design invariant: "every key
// precedes its file"). This is INV-3's disk->meta direction.
func assertEveryFileHasKey(t *testing.T, cat *Catalog, root string) {
	t.Helper()
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		require.NoError(t, err)
		if info.IsDir() {
			return nil
		}
		key, present := keyForArtifactFile(t, cat, path)
		require.True(t, present, "file %q has no resolvable meta key", path)
		ok, err := cat.Has(key)
		require.NoError(t, err)
		require.True(t, ok, "file %q on disk but key %q absent", path, key)
		return nil
	})
}

// keyForArtifactFile maps an on-disk artifact path back to its meta-store key
// by inverting the Layout bijection. Returns present=false for paths outside
// the artifact tree (e.g. the meta rocksdb dir, which lives elsewhere here).
func keyForArtifactFile(t *testing.T, cat *Catalog, path string) (string, bool) {
	t.Helper()

	// Index file: txhash/index/{w}/{lo}-{hi}.idx
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	if filepath.Ext(base) == ".idx" {
		w, errW := parsePadded(filepath.Base(dir))
		require.NoError(t, errW)
		name := strings.TrimSuffix(base, ".idx")
		loStr, hiStr, found := strings.Cut(name, "-")
		require.True(t, found, "bad idx name %q", base)
		lo, errLo := parsePadded(loStr)
		require.NoError(t, errLo)
		hi, errHi := parsePadded(hiStr)
		require.NoError(t, errHi)
		return indexKey(WindowID(w), chunk.ID(lo), chunk.ID(hi)), true
	}

	// Per-chunk files: identify by reconstructing each kind's path for the
	// chunk id embedded in the filename (the leading 8-digit stem, before any
	// "-events"/".pack"/".bin" suffix).
	stem, _, _ := strings.Cut(base, ".")
	stem, _, _ = strings.Cut(stem, "-")
	cid, errC := parsePadded(stem)
	require.NoError(t, errC)
	c := chunk.ID(cid)
	for _, kind := range AllKinds() {
		if slices.Contains(cat.layout.ArtifactPaths(c, kind), path) {
			return chunkKey(c, kind), true
		}
	}
	return "", false
}

// Crash instant (i): file written but key not yet flipped to "frozen".
//
// Reproduces the mark-then-write protocol stopped after barrierNewFile but
// before FlipChunkFrozen / CommitIndex. The key is "freezing", the file is on
// disk. INV-3 disk->meta must still hold: the file is reachable from its key.
func TestCrashSafety_FileWrittenKeyNotFlipped(t *testing.T) {
	cat, root := testCatalog(t)

	// Per-chunk: mark freezing, write+barrier the file, then "crash" before the
	// flip.
	require.NoError(t, cat.MarkChunkFreezing(4, KindLFS))
	lfsPath := cat.layout.LedgerPackPath(4)
	writeArtifact(t, lfsPath)
	require.NoError(t, barrierNewFile(lfsPath, true))
	// <-- crash here: no FlipChunkFrozen.

	// Index: mark freezing, write+barrier the file, "crash" before CommitIndex.
	cov, err := cat.MarkIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	idxPath := cat.layout.IndexFilePath(cov)
	writeArtifact(t, idxPath)
	require.NoError(t, barrierNewFile(idxPath, true))
	// <-- crash here: no CommitIndex.

	// INV-3 (disk -> meta): every file on disk has its key.
	assertEveryFileHasKey(t, cat, root)

	// The keys are observable as "freezing" — the recovery signal.
	s, err := cat.State(4, KindLFS)
	require.NoError(t, err)
	require.Equal(t, StateFreezing, s)

	keys, err := cat.IndexKeys(5)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, StateFreezing, keys[0].State)

	// Recovery for the index "freezing" debris is the sweep: delete file + key.
	require.NoError(t, cat.SweepIndexKey(keys[0]))
	require.NoFileExists(t, idxPath)
	// And after the sweep, INV-3 still holds for what remains.
	assertEveryFileHasKey(t, cat, root)
}

// Crash instant (ii): inside the REAL sweep, between the durable unlink and the
// key delete.
//
// Earlier this test hand-replayed the sweep steps and stopped before the final
// delete — which stays green no matter how SweepChunkArtifacts orders its own
// steps, because the test never runs that code. We now fire a hook from INSIDE
// SweepChunkArtifacts at the exact instant after unlink+fsync and before the
// key-delete batch, and assert the EXIT-side invariant there: file gone =>
// key still present. If the key delete were reordered ahead of the unlink, the
// file would still be on disk when the hook fires and the in-hook assertion
// fails. (Verified by experiment: moving the delete batch above the unlink loop
// turns this test red.)
func TestCrashSafety_SweepUnlinkDurableKeyNotDeleted(t *testing.T) {
	cat, root := testCatalog(t)

	// A frozen lfs (one file) + frozen events (three files) for chunk 6.
	lfsPath := cat.layout.LedgerPackPath(6)
	writeArtifact(t, lfsPath)
	require.NoError(t, cat.MarkChunkFreezing(6, KindLFS))
	require.NoError(t, cat.FlipChunkFrozen(6, KindLFS))

	eventsPaths := cat.layout.EventsPaths(6)
	for _, p := range eventsPaths {
		writeArtifact(t, p)
	}
	require.NoError(t, cat.MarkChunkFreezing(6, KindEvents))
	require.NoError(t, cat.FlipChunkFrozen(6, KindEvents))

	refs := []ArtifactRef{
		{Chunk: 6, Kind: KindLFS, State: StateFrozen},
		{Chunk: 6, Kind: KindEvents, State: StateFrozen},
	}
	allPaths := append([]string{lfsPath}, eventsPaths...)

	// The hook fires once, between the durable unlink and the key delete.
	fired := false
	cat.hooks.beforeKeyDelete = func() {
		fired = true
		for _, p := range allPaths {
			require.NoFileExists(t, p, "EXIT invariant: file must be unlinked before its key is deleted")
		}
		// ...and the keys must still be present (they are about to be deleted).
		for _, ref := range refs {
			ok, err := cat.Has(ref.Key())
			require.NoError(t, err)
			require.True(t, ok, "key %q must still exist at the pre-delete instant", ref.Key())
		}
	}

	require.NoError(t, cat.SweepChunkArtifacts(refs))
	require.True(t, fired, "beforeKeyDelete hook must have fired inside SweepChunkArtifacts")

	// After the sweep both invariants hold globally.
	assertEveryFileHasKey(t, cat, root) // (A), vacuous — files gone
	for _, ref := range refs {          // (B) key absent => file gone
		s, err := cat.State(ref.Chunk, ref.Kind)
		require.NoError(t, err)
		require.Equal(t, State(""), s)
	}
	for _, p := range allPaths {
		require.NoFileExists(t, p)
	}
}

// Index-side twin of the EXIT-invariant test: fire INSIDE SweepIndexKey, between
// the durable unlink and the key delete, and assert file-gone => key-present.
func TestCrashSafety_SweepIndexUnlinkDurableKeyNotDeleted(t *testing.T) {
	cat, root := testCatalog(t)

	cov, err := cat.MarkIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	idxPath := cat.layout.IndexFilePath(cov)
	writeArtifact(t, idxPath)
	require.NoError(t, cat.CommitIndex(cov))

	frozen, ok, err := cat.FrozenCoverage(5)
	require.NoError(t, err)
	require.True(t, ok)

	fired := false
	cat.hooks.beforeKeyDelete = func() {
		fired = true
		require.NoFileExists(t, idxPath, "EXIT invariant: idx file must be unlinked before its key is deleted")
		ok, err := cat.Has(frozen.Key)
		require.NoError(t, err)
		require.True(t, ok, "coverage key must still exist at the pre-delete instant")
	}

	require.NoError(t, cat.SweepIndexKey(frozen))
	require.True(t, fired, "beforeKeyDelete hook must have fired inside SweepIndexKey")

	require.NoFileExists(t, idxPath)
	keys, err := cat.IndexKeys(5)
	require.NoError(t, err)
	require.Empty(t, keys)
	assertEveryFileHasKey(t, cat, root)
}

// Never-unlink-under-a-frozen-key, asserted at the instant it matters: fire
// INSIDE SweepIndexKey between the frozen->pruning demote and the unlink, and
// require the durable value to be "pruning" — never "frozen". If the demote
// were dropped (or moved after the unlink), the value here would still be
// "frozen" and this fails. The same hook also confirms the file is still on
// disk at this instant (the demote precedes any unlink).
func TestSweepIndex_NeverUnlinksUnderFrozenKey(t *testing.T) {
	cat, _ := testCatalog(t)

	cov, err := cat.MarkIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	idxPath := cat.layout.IndexFilePath(cov)
	writeArtifact(t, idxPath)
	require.NoError(t, cat.CommitIndex(cov))

	frozen, ok, err := cat.FrozenCoverage(5)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, StateFrozen, frozen.State)

	fired := false
	cat.hooks.beforeUnlink = func() {
		fired = true
		v, ok, err := cat.Get(frozen.Key)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, string(StatePruning), v,
			"value at the pre-unlink instant must be pruning, never frozen")
		require.FileExists(t, idxPath, "file must still be on disk before the unlink")
	}

	require.NoError(t, cat.SweepIndexKey(frozen))
	require.True(t, fired, "beforeUnlink hook must have fired inside SweepIndexKey")

	require.NoFileExists(t, idxPath)
	keys, err := cat.IndexKeys(5)
	require.NoError(t, err)
	require.Empty(t, keys)
}

// Per-chunk twin of the never-unlink-under-frozen-key assertion: fire INSIDE
// SweepChunkArtifacts between the demote batch and the unlinks; every "frozen"
// ref must read "pruning" by then. Dropping the demote batch leaves them
// "frozen" here and this fails.
func TestSweepChunk_NeverUnlinksUnderFrozenKey(t *testing.T) {
	cat, _ := testCatalog(t)

	lfsPath := cat.layout.LedgerPackPath(6)
	writeArtifact(t, lfsPath)
	require.NoError(t, cat.MarkChunkFreezing(6, KindLFS))
	require.NoError(t, cat.FlipChunkFrozen(6, KindLFS))

	ref := ArtifactRef{Chunk: 6, Kind: KindLFS, State: StateFrozen}

	fired := false
	cat.hooks.beforeUnlink = func() {
		fired = true
		v, ok, err := cat.Get(ref.Key())
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, string(StatePruning), v,
			"value at the pre-unlink instant must be pruning, never frozen")
		require.FileExists(t, lfsPath, "file must still be on disk before the unlink")
	}

	require.NoError(t, cat.SweepChunkArtifacts([]ArtifactRef{ref}))
	require.True(t, fired, "beforeUnlink hook must have fired inside SweepChunkArtifacts")

	require.NoFileExists(t, lfsPath)
	s, err := cat.State(6, KindLFS)
	require.NoError(t, err)
	require.Equal(t, State(""), s)
}

func TestSweepEmptyRefsNoop(t *testing.T) {
	cat, _ := testCatalog(t)
	require.NoError(t, cat.SweepChunkArtifacts(nil))
}

func TestMarkRequiresKinds(t *testing.T) {
	cat, _ := testCatalog(t)
	require.Error(t, cat.MarkChunkFreezing(1))
	require.Error(t, cat.FlipChunkFrozen(1))
}

func TestGetHasMissReturnsCleanly(t *testing.T) {
	cat, _ := testCatalog(t)
	_, ok, err := cat.Get("nope")
	require.NoError(t, err)
	require.False(t, ok)
	has, err := cat.Has("nope")
	require.NoError(t, err)
	require.False(t, has)
}
