package backfill

import (
	"context"
	"errors"
	"iter"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/fhtest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/ledger"
)

// ---------------------------------------------------------------------------
// LCM fixtures + fakes (a ledger stream and a Backend).
// ---------------------------------------------------------------------------

// fullChunkStream is an in-memory ledgerbackend.LedgerStream yielding every ledger
// in [from, to] from a per-seq LCM generator. It models a source (a pack or a
// backend) that has the whole requested range.
type fullChunkStream struct {
	t   *testing.T
	gen func(*testing.T, uint32) []byte
}

var _ ledgerbackend.LedgerStream = (*fullChunkStream)(nil)

func (s *fullChunkStream) RawLedgers(
	_ context.Context, r ledgerbackend.Range, _ ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		for seq := r.From(); seq <= r.To(); seq++ {
			if !yield(s.gen(s.t, seq), nil) {
				return
			}
		}
	}
}

// fakeBackend is an in-memory Backend: a full-range ledger stream plus a
// configurable frontier Tip. rawCalls counts RawLedgers invocations so a test can
// assert whether backfillSource actually read from the bulk backend (vs the pack).
type fakeBackend struct {
	t        *testing.T
	gen      func(*testing.T, uint32) []byte
	tip      uint32
	tipErr   error
	tipFn    func(context.Context) (uint32, error) // overrides tip/tipErr when set
	rawCalls atomic.Int32
}

var _ Backend = (*fakeBackend)(nil)

func (b *fakeBackend) RawLedgers(
	_ context.Context, r ledgerbackend.Range, _ ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	b.rawCalls.Add(1)
	return func(yield func([]byte, error) bool) {
		for seq := r.From(); seq <= r.To(); seq++ {
			if !yield(b.gen(b.t, seq), nil) {
				return
			}
		}
	}
}

func (b *fakeBackend) Tip(ctx context.Context) (uint32, error) {
	if b.tipFn != nil {
		return b.tipFn(ctx)
	}
	return b.tip, b.tipErr
}

// zeroTxBackend is a Backend whose tip covers any chunk (so the coverage wait
// passes immediately) and whose stream yields zero-tx LCMs.
func zeroTxBackend(t *testing.T) *fakeBackend {
	return &fakeBackend{t: t, gen: fhtest.ZeroTxLCMBytes, tip: math.MaxUint32}
}

// ---------------------------------------------------------------------------
// process config helper.
// ---------------------------------------------------------------------------

func testProcessConfig(t *testing.T, cat *catalog.Catalog) ProcessConfig {
	t.Helper()
	return ProcessConfig{
		Catalog: cat,
		Logger:  silentLogger(),
		Sink:    ingest.NopSink{},
	}
}

// ---------------------------------------------------------------------------
// processChunk — produces the three artifacts and flips the keys to frozen.
// ---------------------------------------------------------------------------

func TestProcessChunk_ProducesAllArtifactsAndFreezes(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat)
	cfg.Backend = zeroTxBackend(t)

	chunkID := chunk.ID(0)
	require.NoError(t, processChunk(context.Background(), chunkID, catalog.AllArtifacts(), cfg))

	// All three catalog keys flipped to frozen (verified via Phase A Catalog).
	for _, kind := range geometry.AllKinds() {
		state, err := cat.State(chunkID, kind)
		require.NoError(t, err)
		require.Equal(t, geometry.StateFrozen, state, "kind %s should be frozen", kind)
	}

	// All three artifacts exist on disk at their canonical Layout paths.
	layout := cat.Layout()
	require.FileExists(t, layout.LedgerPackPath(chunkID))
	require.FileExists(t, layout.TxHashBinPath(chunkID))
	for _, p := range layout.EventsPaths(chunkID) {
		require.FileExists(t, p)
	}

	// The .bin is readable as a sorted run (rule 5) — exercises the merged
	// txhash cold writer's output via its reader.
	entries := fhtest.ReadColdBin(t, layout.TxHashBinPath(chunkID))
	require.Empty(t, entries, "zero-tx chunk yields an empty sorted .bin")

	// The pack is a valid cold ledger pack covering the whole chunk.
	cr, err := ledger.OpenColdReader(layout.LedgerPackPath(chunkID))
	require.NoError(t, err)
	defer func() { _ = cr.Close() }()
	last, err := cr.LastSeq()
	require.NoError(t, err)
	require.Equal(t, chunkID.LastLedger(), last)
}

func TestProcessChunk_SubsetOfKinds(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat)
	cfg.Backend = zeroTxBackend(t)

	chunkID := chunk.ID(3)
	// Request only events + txhash; ledgers stays absent.
	set := catalog.NewArtifactSet(geometry.KindEvents, geometry.KindTxHash)
	require.NoError(t, processChunk(context.Background(), chunkID, set, cfg))

	eState, _ := cat.State(chunkID, geometry.KindEvents)
	tState, _ := cat.State(chunkID, geometry.KindTxHash)
	lState, _ := cat.State(chunkID, geometry.KindLedgers)
	require.Equal(t, geometry.StateFrozen, eState)
	require.Equal(t, geometry.StateFrozen, tState)
	require.Equal(t, geometry.State(""), lState, "ledgers was not requested — key stays absent")

	layout := cat.Layout()
	require.NoFileExists(t, layout.LedgerPackPath(chunkID))
	require.FileExists(t, layout.TxHashBinPath(chunkID))
}

// ---------------------------------------------------------------------------
// Idempotency: a frozen kind self-skips.
// ---------------------------------------------------------------------------

func TestProcessChunk_IdempotentSkipWhenFrozen(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat)
	backend := zeroTxBackend(t)
	cfg.Backend = backend

	chunkID := chunk.ID(0)
	require.NoError(t, processChunk(context.Background(), chunkID, catalog.AllArtifacts(), cfg))
	readsAfterFirst := backend.rawCalls.Load()
	require.Equal(t, int32(1), readsAfterFirst, "first pass reads the backend once")

	// Second pass: every kind is frozen, so processChunk returns without resolving
	// or reading any source.
	require.NoError(t, processChunk(context.Background(), chunkID, catalog.AllArtifacts(), cfg))
	require.Equal(t, readsAfterFirst, backend.rawCalls.Load(),
		"a fully-frozen chunk must not re-read the source")
}

// ---------------------------------------------------------------------------
// Crash recovery: a "freezing" key (partial crash) is re-materialized.
// ---------------------------------------------------------------------------

func TestProcessChunk_RematerializesAfterFreezingCrash(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat)
	cfg.Backend = zeroTxBackend(t)

	chunkID := chunk.ID(0)
	layout := cat.Layout()

	// Simulate a crash mid-freeze: the keys are "freezing" and a stale/partial
	// pack file exists at the canonical path.
	require.NoError(t, cat.MarkChunkFreezing(chunkID, geometry.AllKinds()...))
	require.NoError(t, os.MkdirAll(filepath.Dir(layout.LedgerPackPath(chunkID)), 0o755))
	require.NoError(t, os.WriteFile(layout.LedgerPackPath(chunkID), []byte("PARTIAL-GARBAGE"), 0o644))

	// Re-run: a "freezing" key triggers re-materialization (rule 1), overwriting
	// the partial at the canonical path.
	require.NoError(t, processChunk(context.Background(), chunkID, catalog.AllArtifacts(), cfg))

	for _, kind := range geometry.AllKinds() {
		state, err := cat.State(chunkID, kind)
		require.NoError(t, err)
		require.Equal(t, geometry.StateFrozen, state)
	}
	// The partial garbage was overwritten with a real pack.
	cr, err := ledger.OpenColdReader(layout.LedgerPackPath(chunkID))
	require.NoError(t, err)
	defer func() { _ = cr.Close() }()
	last, err := cr.LastSeq()
	require.NoError(t, err)
	require.Equal(t, chunkID.LastLedger(), last)
}

// ---------------------------------------------------------------------------
// One-write-protocol ORDERING (mark-then-write, write-then-flip): the entry-
// and exit-side invariants once asserted from INSIDE processChunk via the
// afterMarkFreezing / afterBarrier fault-injection hooks. Those in-method
// observation points were dropped with the rest of the crash hooks (see #817);
// the ordering they guarded is now covered structurally — MarkChunkFreezing /
// FlipChunkFrozen by the catalog protocol tests, the file-written-key-not-
// flipped instant by catalog's TestCrashSafety_FileWrittenKeyNotFlipped, and
// the recovery side by TestProcessChunk_RematerializesAfterFreezingCrash /
// TestProcessChunk_MixedPartialFrozenReusesPack below. Exhaustive per-write
// crash coverage is the job of the fault-injection harness tracked in #823.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Mixed partial-frozen recovery: a chunk with ledgers already "frozen" (real
// .pack on disk), events left "freezing" by a crash, and txhash never started.
// One processChunk pass over ALL kinds must: self-skip ledgers, re-materialize
// events, produce txhash — AND source the ledgers from the frozen .pack
// (re-derivation without a download), so the bulk backend is never read. This
// is the pack-reuse-under-partial-frozen path the resolver leans on in recovery.
// ---------------------------------------------------------------------------

func TestProcessChunk_MixedPartialFrozenReusesPack(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat)
	bulk := zeroTxBackend(t)
	cfg.Backend = bulk

	chunkID := chunk.ID(0)
	layout := cat.Layout()

	// Ledgers already frozen with a real pack on disk.
	require.NoError(t, cat.MarkChunkFreezing(chunkID, geometry.KindLedgers))
	require.NoError(t, os.MkdirAll(filepath.Dir(layout.LedgerPackPath(chunkID)), 0o755))
	writeRealPack(t, cat, chunkID)
	require.NoError(t, cat.FlipChunkFrozen(chunkID, geometry.KindLedgers))

	// Events left "freezing" by a crash mid-freeze; txhash never started.
	require.NoError(t, cat.MarkChunkFreezing(chunkID, geometry.KindEvents))

	require.NoError(t, processChunk(context.Background(), chunkID, catalog.AllArtifacts(), cfg))

	// The ledger source was the frozen .pack, not the bulk backend.
	require.Equal(t, int32(0), bulk.rawCalls.Load(),
		"a partial-frozen chunk with a present pack must re-derive from it, not the bulk backend")

	// Every kind ends frozen; the re-materialized artifacts exist on disk.
	for _, kind := range geometry.AllKinds() {
		state, err := cat.State(chunkID, kind)
		require.NoError(t, err)
		require.Equal(t, geometry.StateFrozen, state, "kind %s should end frozen", kind)
	}
	require.FileExists(t, layout.TxHashBinPath(chunkID))
	for _, p := range layout.EventsPaths(chunkID) {
		require.FileExists(t, p)
	}
}

// ---------------------------------------------------------------------------
// backfillSource preference order.
// ---------------------------------------------------------------------------

func TestBackfillSource_PrefersFrozenPackWhenLFSNotRequested(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat)

	chunkID := chunk.ID(0)
	layout := cat.Layout()
	// Frozen ledgers with a real pack on disk; ledgers is NOT requested.
	require.NoError(t, cat.MarkChunkFreezing(chunkID, geometry.KindLedgers))
	require.NoError(t, os.MkdirAll(filepath.Dir(layout.LedgerPackPath(chunkID)), 0o755))
	writeRealPack(t, cat, chunkID)
	require.NoError(t, cat.FlipChunkFrozen(chunkID, geometry.KindLedgers))

	// bulk configured but should not be used.
	bulk := zeroTxBackend(t)
	cfg.Backend = bulk

	set := catalog.NewArtifactSet(geometry.KindEvents, geometry.KindTxHash) // ledgers NOT requested
	src, closeSrc, err := backfillSource(context.Background(), chunkID, set, cfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, closeSrc()) }()
	// It is a pack stream (re-derivation without download); the bulk backend was
	// not consulted.
	require.IsType(t, ledger.NewPackStream(""), src)
	require.Equal(t, int32(0), bulk.rawCalls.Load())
}

func TestBackfillSource_DoesNotUsePackWhenLFSRequested(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat)

	chunkID := chunk.ID(0)
	layout := cat.Layout()
	require.NoError(t, cat.MarkChunkFreezing(chunkID, geometry.KindLedgers))
	require.NoError(t, os.MkdirAll(filepath.Dir(layout.LedgerPackPath(chunkID)), 0o755))
	writeRealPack(t, cat, chunkID)
	require.NoError(t, cat.FlipChunkFrozen(chunkID, geometry.KindLedgers))

	bulk := zeroTxBackend(t)
	cfg.Backend = bulk

	// ledgers IS requested — the pack branch is skipped (circular), so it goes to
	// the bulk backend (whose tip covers the chunk, so the wait passes).
	src, closeSrc, err := backfillSource(context.Background(), chunkID, catalog.AllArtifacts(), cfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, closeSrc()) }()
	require.Same(t, bulk, src)
}

// TestBackfillSource_BulkCoverageErrorAborts: a coverage failure from the bulk
// backend (here, a fatal tip-query error) aborts source resolution before any
// "freezing" key is marked — backfillSource surfaces it directly.
func TestBackfillSource_BulkCoverageErrorAborts(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat)

	chunkID := chunk.ID(0)
	cfg.Backend = &fakeBackend{t: t, gen: fhtest.ZeroTxLCMBytes, tipErr: errors.New("boom")}

	_, _, err := backfillSource(context.Background(), chunkID, catalog.AllArtifacts(), cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "backend tip query")
}

func TestBackfillSource_NoBackendConfigured(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat)
	cfg.Backend = nil

	_, _, err := backfillSource(context.Background(), chunk.ID(0), catalog.AllArtifacts(), cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no bulk backend")
}

// ---------------------------------------------------------------------------
// waitForCoverage — the coverage-poll over a Backend's Tip.
// ---------------------------------------------------------------------------

// TestWaitForCoverage_CoveredReturnsNil: a tip already at/above the target returns
// nil on the first poll.
func TestWaitForCoverage_CoveredReturnsNil(t *testing.T) {
	b := &fakeBackend{tip: 100}
	require.NoError(t, waitForCoverage(context.Background(), b, 100, time.Millisecond, time.Second))
}

// TestWaitForCoverage_TimeoutReturnsSentinel: when the tip keeps succeeding but
// never reaches the target, the wait must stop at timeout and return
// ErrBackendCoverageTimeout — the sentinel callers classify lag with.
func TestWaitForCoverage_TimeoutReturnsSentinel(t *testing.T) {
	b := &fakeBackend{tip: 1} // always below the target

	done := make(chan error, 1)
	go func() { done <- waitForCoverage(context.Background(), b, 100, time.Millisecond, 50*time.Millisecond) }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrBackendCoverageTimeout)
	case <-time.After(5 * time.Second):
		t.Fatal("waitForCoverage did not return at timeout")
	}
}

// TestWaitForCoverage_TipQueryFatal: a tip-query error is fatal — the wait does not
// retry a broken backend and returns the wrapped error quickly.
func TestWaitForCoverage_TipQueryFatal(t *testing.T) {
	b := &fakeBackend{tipErr: errors.New("boom")}

	done := make(chan error, 1)
	go func() { done <- waitForCoverage(context.Background(), b, 100, time.Millisecond, 5*time.Second) }()
	select {
	case err := <-done:
		require.Error(t, err)
		require.Contains(t, err.Error(), "backend tip query")
	case <-time.After(5 * time.Second):
		t.Fatal("waitForCoverage did not fail fast on a tip-query error")
	}
}

// TestWaitForCoverage_TipBoundedByDeadline: a tip query that blocks until its
// context is canceled must not outlast the timeout, even when the parent ctx
// carries no deadline of its own.
func TestWaitForCoverage_TipBoundedByDeadline(t *testing.T) {
	b := &fakeBackend{tipFn: func(ctx context.Context) (uint32, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	}}

	done := make(chan error, 1)
	go func() { done <- waitForCoverage(context.Background(), b, 100, time.Millisecond, 100*time.Millisecond) }()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("waitForCoverage did not return — tip query not bounded by the deadline")
	}
}

// writeRealPack writes a valid cold ledger pack for chunkID at its canonical
// Layout path by driving the cold ledger materializer over a zero-tx stream.
func writeRealPack(t *testing.T, cat *catalog.Catalog, chunkID chunk.ID) {
	t.Helper()
	stream := &fullChunkStream{t: t, gen: fhtest.ZeroTxLCMBytes}
	raw := stream.RawLedgers(context.Background(),
		ledgerbackend.BoundedRange(chunkID.FirstLedger(), chunkID.LastLedger()))
	dirs := ingest.ColdDirs{LedgerPack: cat.Layout().LedgerPackPath(chunkID)}
	require.NoError(t, ingest.WriteColdChunk(
		context.Background(), silentLogger(), chunkID, raw, dirs,
		ingest.NopSink{}, ingest.Config{Ledgers: true}))
	require.FileExists(t, cat.Layout().LedgerPackPath(chunkID))
}
