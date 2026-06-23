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

	return NewCatalog(store, NewLayout(artifactRoot)), artifactRoot
}

// writeArtifact materializes a placeholder file at path (creating parents) so a
// sweep has something real to unlink.
func writeArtifact(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte("artifact"), 0o644))
}

// ---------------------------------------------------------------------------
// Key <-> path bijection, both directions.
// ---------------------------------------------------------------------------

func TestKeyConstructorsMatchSpec(t *testing.T) {
	require.Equal(t, "chunk:00005350:ledgers", chunkKey(5350, KindLedgers))
	require.Equal(t, "hot:chunk:00005350", hotChunkKey(5350))
}

func TestChunkKeyBijection(t *testing.T) {
	for _, kind := range AllKinds() {
		for _, id := range []chunk.ID{0, 1, 999, 1000, 5350} {
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

func TestKeyToPathBijection(t *testing.T) {
	l := NewLayout("/data")

	// The doc's directory-layout examples.
	require.Equal(t, "/data/ledgers/00005/00005350.pack", l.LedgerPackPath(5350))
	require.Equal(t, "/data/hot/00005350", l.HotChunkPath(5350))
}

func TestParseRejectsMalformed(t *testing.T) {
	bad := []string{
		"chunk:5350:ledgers",   // not 8-digit padded
		"chunk:00005350:bogus", // unknown kind
		"chunk:00005350",       // missing kind
		"hot:chunk:5350",       // not padded
		"unrelated:key",        // wrong family
	}
	for _, key := range bad {
		_, _, okChunk := parseChunkKey(key)
		_, okHot := parseHotChunkKey(key)
		require.False(t, okChunk && okHot, "expected %q to be rejected by all parsers", key)
	}
	// Specific rejection.
	_, _, ok := parseChunkKey("chunk:00005350:bogus")
	require.False(t, ok)
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

	require.NoError(t, cat.MarkChunkFreezing(1, KindLedgers))
	require.NoError(t, cat.FlipChunkFrozen(2, KindLedgers))

	refs, err := cat.ChunkArtifactKeys()
	require.NoError(t, err)
	require.Len(t, refs, 2)
	// Sorted by key: chunk:00000001 before chunk:00000002.
	require.Equal(t, ArtifactRef{Chunk: 1, Kind: KindLedgers, State: StateFreezing}, refs[0])
	require.Equal(t, ArtifactRef{Chunk: 2, Kind: KindLedgers, State: StateFrozen}, refs[1])
}

// ---------------------------------------------------------------------------
// Sweep: the deletion body.
// ---------------------------------------------------------------------------

func TestSweepChunkArtifacts(t *testing.T) {
	cat, _ := testCatalog(t)

	// Set up a frozen ledgers for chunk 3, with a real file.
	lfsPath := cat.layout.LedgerPackPath(3)
	writeArtifact(t, lfsPath)
	require.NoError(t, cat.MarkChunkFreezing(3, KindLedgers))
	require.NoError(t, cat.FlipChunkFrozen(3, KindLedgers))

	refs := []ArtifactRef{
		{Chunk: 3, Kind: KindLedgers, State: StateFrozen},
	}
	require.NoError(t, cat.SweepChunkArtifacts(refs))

	// File gone.
	require.NoFileExists(t, lfsPath)
	// Key gone (key absent => file gone).
	s, err := cat.State(3, KindLedgers)
	require.NoError(t, err)
	require.Equal(t, State(""), s)
}

func TestSweepChunkArtifactsIdempotentOnMissingFiles(t *testing.T) {
	cat, _ := testCatalog(t)

	// Key present, file never written (a "pruning" leftover whose file is
	// already gone).
	require.NoError(t, cat.store.Put(chunkKey(8, KindLedgers), string(StatePruning)))
	require.NoError(t, cat.SweepChunkArtifacts([]ArtifactRef{
		{Chunk: 8, Kind: KindLedgers, State: StatePruning},
	}))
	s, err := cat.State(8, KindLedgers)
	require.NoError(t, err)
	require.Equal(t, State(""), s)
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

	// Per-chunk files: identify by reconstructing each kind's path for the
	// chunk id embedded in the filename (the leading 8-digit stem, before any
	// ".pack" suffix).
	base := filepath.Base(path)
	stem, _, _ := strings.Cut(base, ".")
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
// before FlipChunkFrozen. The key is "freezing", the file is on disk. INV-3
// disk->meta must still hold: the file is reachable from its key.
func TestCrashSafety_FileWrittenKeyNotFlipped(t *testing.T) {
	cat, root := testCatalog(t)

	// Per-chunk: mark freezing, write+barrier the file, then "crash" before the
	// flip.
	require.NoError(t, cat.MarkChunkFreezing(4, KindLedgers))
	lfsPath := cat.layout.LedgerPackPath(4)
	writeArtifact(t, lfsPath)
	require.NoError(t, barrierNewFile(lfsPath, true))
	// <-- crash here: no FlipChunkFrozen.

	// INV-3 (disk -> meta): every file on disk has its key.
	assertEveryFileHasKey(t, cat, root)

	// The key is observable as "freezing" — the recovery signal.
	s, err := cat.State(4, KindLedgers)
	require.NoError(t, err)
	require.Equal(t, StateFreezing, s)
}

// Crash instant (ii): inside the REAL sweep, between the durable unlink and the
// key delete.
//
// We fire a hook from INSIDE SweepChunkArtifacts at the exact instant after
// unlink+fsync and before the key-delete batch, and assert the EXIT-side
// invariant there: file gone => key still present. If the key delete were
// reordered ahead of the unlink, the file would still be on disk when the hook
// fires and the in-hook assertion fails.
func TestCrashSafety_SweepUnlinkDurableKeyNotDeleted(t *testing.T) {
	cat, root := testCatalog(t)

	// A frozen ledgers (one file) for chunk 6.
	lfsPath := cat.layout.LedgerPackPath(6)
	writeArtifact(t, lfsPath)
	require.NoError(t, cat.MarkChunkFreezing(6, KindLedgers))
	require.NoError(t, cat.FlipChunkFrozen(6, KindLedgers))

	refs := []ArtifactRef{
		{Chunk: 6, Kind: KindLedgers, State: StateFrozen},
	}
	allPaths := []string{lfsPath}

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

// Per-chunk never-unlink-under-frozen-key assertion: fire INSIDE
// SweepChunkArtifacts between the demote batch and the unlinks; every "frozen"
// ref must read "pruning" by then. Dropping the demote batch leaves them
// "frozen" here and this fails.
func TestSweepChunk_NeverUnlinksUnderFrozenKey(t *testing.T) {
	cat, _ := testCatalog(t)

	lfsPath := cat.layout.LedgerPackPath(6)
	writeArtifact(t, lfsPath)
	require.NoError(t, cat.MarkChunkFreezing(6, KindLedgers))
	require.NoError(t, cat.FlipChunkFrozen(6, KindLedgers))

	ref := ArtifactRef{Chunk: 6, Kind: KindLedgers, State: StateFrozen}

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
	s, err := cat.State(6, KindLedgers)
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
