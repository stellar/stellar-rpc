package backfill

import (
	"context"
	"iter"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// ---------------------------------------------------------------------------
// LCM fixtures + fake ChunkSource.
// ---------------------------------------------------------------------------

// zeroTxLCMBytes builds the wire bytes of a minimal valid zero-transaction V2
// LedgerCloseMeta for seq. Zero-tx keeps the per-ledger work trivial so a full
// 10,000-ledger chunk pass stays fast in tests.
func zeroTxLCMBytes(t *testing.T, seq uint32) []byte {
	t.Helper()
	lcm := xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(0)},
					LedgerSeq: xdr.Uint32(seq),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{Phases: nil},
			},
			TxProcessing: nil,
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}

// fullChunkStream is an in-memory ledgerbackend.LedgerStream yielding every
// ledger in [from, to] from a per-seq LCM generator. It models a backend (or a
// pack) that has the whole requested range.
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

// countingChunkSource wraps a stream factory and counts OpenStream calls, so a
// test can assert which preference branch backfillSource picked.
type countingChunkSource struct {
	opens atomic.Int32
	make  func(chunk.ID) (ledgerbackend.LedgerStream, error)
}

func (c *countingChunkSource) OpenStream(id chunk.ID) (ledgerbackend.LedgerStream, error) {
	c.opens.Add(1)
	return c.make(id)
}

func zeroTxBackend(t *testing.T) *countingChunkSource {
	return &countingChunkSource{
		make: func(chunk.ID) (ledgerbackend.LedgerStream, error) {
			return &fullChunkStream{t: t, gen: zeroTxLCMBytes}, nil
		},
	}
}

// ---------------------------------------------------------------------------
// fake BackendWaiter.
// ---------------------------------------------------------------------------

type fakeWaiter struct {
	err error
}

func (w *fakeWaiter) WaitForCoverage(context.Context, uint32) error {
	return w.err
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
	cat, root := testCatalog(t)
	cfg := testProcessConfig(t, cat)
	backend := zeroTxBackend(t)
	cfg.Backend = backend
	cfg.BackendWaiter = &fakeWaiter{}

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
	entries, err := txhash.ReadColdBin(layout.TxHashBinPath(chunkID))
	require.NoError(t, err)
	require.Empty(t, entries, "zero-tx chunk yields an empty sorted .bin")

	// The pack is a valid cold ledger pack covering the whole chunk.
	cr, err := ledger.OpenColdReader(layout.LedgerPackPath(chunkID))
	require.NoError(t, err)
	defer func() { _ = cr.Close() }()
	last, err := cr.LastSeq()
	require.NoError(t, err)
	require.Equal(t, chunkID.LastLedger(), last)
	_ = root
}

func TestProcessChunk_SubsetOfKinds(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat)
	cfg.Backend = zeroTxBackend(t)
	cfg.BackendWaiter = &fakeWaiter{}

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
	cfg.BackendWaiter = &fakeWaiter{}

	chunkID := chunk.ID(0)
	require.NoError(t, processChunk(context.Background(), chunkID, catalog.AllArtifacts(), cfg))
	opensAfterFirst := backend.opens.Load()
	require.Equal(t, int32(1), opensAfterFirst, "first pass opens the backend once")

	// Second pass: every kind is frozen, so processChunk returns without opening
	// any source.
	require.NoError(t, processChunk(context.Background(), chunkID, catalog.AllArtifacts(), cfg))
	require.Equal(t, opensAfterFirst, backend.opens.Load(),
		"a fully-frozen chunk must not re-open the source")
}

// ---------------------------------------------------------------------------
// Crash recovery: a "freezing" key (partial crash) is re-materialized.
// ---------------------------------------------------------------------------

func TestProcessChunk_RematerializesAfterFreezingCrash(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat)
	cfg.Backend = zeroTxBackend(t)
	cfg.BackendWaiter = &fakeWaiter{}

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
// (re-derivation without a download), so the bulk backend is never opened. This
// is the pack-reuse-under-partial-frozen path the resolver leans on in recovery.
// ---------------------------------------------------------------------------

func TestProcessChunk_MixedPartialFrozenReusesPack(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat)
	bulk := zeroTxBackend(t)
	cfg.Backend = bulk
	cfg.BackendWaiter = &fakeWaiter{}

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
	require.Equal(t, int32(0), bulk.opens.Load(),
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

	// hot not ready; bulk configured but should not be used.
	bulk := zeroTxBackend(t)
	cfg.Backend = bulk
	cfg.BackendWaiter = &fakeWaiter{}

	set := catalog.NewArtifactSet(geometry.KindEvents, geometry.KindTxHash) // ledgers NOT requested
	src, closeSrc, err := backfillSource(context.Background(), chunkID, set, cfg)
	require.NoError(t, err)
	require.NoError(t, closeSrc())
	// It is a pack source (re-derivation without download); the bulk backend was
	// not consulted.
	require.IsType(t, ingest.NewPackSource(""), src)
	require.Equal(t, int32(0), bulk.opens.Load())
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
	cfg.BackendWaiter = &fakeWaiter{}

	// ledgers IS requested — the pack branch is skipped (circular), so it goes to bulk.
	src, closeSrc, err := backfillSource(context.Background(), chunkID, catalog.AllArtifacts(), cfg)
	require.NoError(t, err)
	require.NoError(t, closeSrc())
	require.Same(t, ingest.ChunkSource(bulk), src)
}

func TestBackfillSource_BulkWaitTimeoutFatal(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat)

	chunkID := chunk.ID(0)
	cfg.Backend = zeroTxBackend(t)
	cfg.BackendWaiter = &fakeWaiter{err: ErrBackendCoverageTimeout}

	_, _, err := backfillSource(context.Background(), chunkID, catalog.AllArtifacts(), cfg)
	require.ErrorIs(t, err, ErrBackendCoverageTimeout)
}

func TestBackfillSource_NoBackendConfigured(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat)
	cfg.Backend = nil

	_, _, err := backfillSource(context.Background(), chunk.ID(0), catalog.AllArtifacts(), cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no bulk backend")
}

func TestBackfillSource_BulkBackendRequiresWaiter(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat)
	cfg.Backend = zeroTxBackend(t)
	cfg.BackendWaiter = nil // backend set, waiter missing — the bulk branch must reject it

	_, _, err := backfillSource(context.Background(), chunk.ID(0), catalog.AllArtifacts(), cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no BackendWaiter")
}

// TestPollingBackendWaiter_TipBoundedByDeadline: a tip query that blocks until its
// context is canceled must not outlast Timeout, even when the parent ctx carries
// no deadline of its own.
func TestPollingBackendWaiter_TipBoundedByDeadline(t *testing.T) {
	w := NewPollingBackendWaiter(func(ctx context.Context) (uint32, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	}, time.Millisecond, 100*time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- w.WaitForCoverage(context.Background(), 100) }()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForCoverage did not return — tip query not bounded by the deadline")
	}
}

// TestPollingBackendWaiter_TimeoutReturnsSentinel: when the tip query keeps
// succeeding but the tip never reaches the target, the waiter must stop at Timeout
// and return ErrBackendCoverageTimeout — the sentinel callers classify lag with.
func TestPollingBackendWaiter_TimeoutReturnsSentinel(t *testing.T) {
	w := NewPollingBackendWaiter(func(context.Context) (uint32, error) {
		return 1, nil // always below the target
	}, time.Millisecond, 50*time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- w.WaitForCoverage(context.Background(), 100) }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrBackendCoverageTimeout)
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForCoverage did not return at Timeout")
	}
}

// writeRealPack writes a valid cold ledger pack for chunkID at its canonical
// Layout path by driving the merged cold ledger ingester over a zero-tx stream.
func writeRealPack(t *testing.T, cat *catalog.Catalog, chunkID chunk.ID) {
	t.Helper()
	src := &countingChunkSource{
		make: func(chunk.ID) (ledgerbackend.LedgerStream, error) {
			return &fullChunkStream{t: t, gen: zeroTxLCMBytes}, nil
		},
	}
	dirs := ingest.ColdDirs{Ledgers: cat.Layout().LedgersRoot()}
	require.NoError(t, ingest.RunColdChunk(
		context.Background(), silentLogger(), src, dirs, chunkID,
		ingest.NopSink{}, ingest.Config{Ledgers: true}))
	require.FileExists(t, cat.Layout().LedgerPackPath(chunkID))
}
