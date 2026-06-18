package streaming

import (
	"context"
	"errors"
	"iter"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/hotchunk"
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
// pack) that has the whole requested range. counter (optional) records the
// number of OpenStream-driven ledgers pulled so a test can assert a source was
// (or was not) used.
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
// fake HotProbe / HotChunk.
// ---------------------------------------------------------------------------

type fakeHotChunk struct {
	maxSeq   uint32
	present  bool
	maxErr   error
	source   ingest.ChunkSource
	closedTo *atomic.Int32
}

func (h *fakeHotChunk) MaxCommittedSeq() (uint32, bool, error) {
	return h.maxSeq, h.present, h.maxErr
}
func (h *fakeHotChunk) Source() ingest.ChunkSource { return h.source }
func (h *fakeHotChunk) Close() error {
	if h.closedTo != nil {
		h.closedTo.Add(1)
	}
	return nil
}

type fakeHotProbe struct {
	chunk    *fakeHotChunk
	ok       bool
	openErr  error
	openedTo *atomic.Int32
}

func (p *fakeHotProbe) OpenHotChunk(chunk.ID) (HotChunk, bool, error) {
	if p.openedTo != nil {
		p.openedTo.Add(1)
	}
	if p.openErr != nil {
		return nil, false, p.openErr
	}
	if !p.ok {
		return nil, false, nil
	}
	return p.chunk, true, nil
}

// ---------------------------------------------------------------------------
// fake BackendWaiter.
// ---------------------------------------------------------------------------

type fakeWaiter struct {
	err    error
	called atomic.Int32
}

func (w *fakeWaiter) WaitForCoverage(context.Context, uint32) error {
	w.called.Add(1)
	return w.err
}

// ---------------------------------------------------------------------------
// process config helper.
// ---------------------------------------------------------------------------

func testProcessConfig(t *testing.T, cat *Catalog) ProcessConfig {
	t.Helper()
	return ProcessConfig{
		Catalog:  cat,
		Logger:   silentLogger(),
		Sink:     ingest.NopSink{},
		HotProbe: &fakeHotProbe{}, // not "ready" by default; tests override
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
	require.NoError(t, processChunk(context.Background(), chunkID, AllArtifacts(), cfg))

	// All three catalog keys flipped to frozen (verified via Phase A Catalog).
	for _, kind := range AllKinds() {
		state, err := cat.State(chunkID, kind)
		require.NoError(t, err)
		require.Equal(t, StateFrozen, state, "kind %s should be frozen", kind)
	}

	// All three artifacts exist on disk at their canonical Layout paths.
	require.FileExists(t, cat.layout.LedgerPackPath(chunkID))
	require.FileExists(t, cat.layout.TxHashBinPath(chunkID))
	for _, p := range cat.layout.EventsPaths(chunkID) {
		require.FileExists(t, p)
	}

	// The .bin is readable as a sorted run (rule 5) — exercises the merged
	// txhash cold writer's output via its reader.
	entries, err := txhash.ReadColdBin(cat.layout.TxHashBinPath(chunkID))
	require.NoError(t, err)
	require.Empty(t, entries, "zero-tx chunk yields an empty sorted .bin")

	// The pack is a valid cold ledger pack covering the whole chunk.
	cr, err := ledger.OpenColdReader(cat.layout.LedgerPackPath(chunkID))
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
	set := NewArtifactSet(KindEvents, KindTxHash)
	require.NoError(t, processChunk(context.Background(), chunkID, set, cfg))

	eState, _ := cat.State(chunkID, KindEvents)
	tState, _ := cat.State(chunkID, KindTxHash)
	lState, _ := cat.State(chunkID, KindLedgers)
	require.Equal(t, StateFrozen, eState)
	require.Equal(t, StateFrozen, tState)
	require.Equal(t, State(""), lState, "ledgers was not requested — key stays absent")

	require.NoFileExists(t, cat.layout.LedgerPackPath(chunkID))
	require.FileExists(t, cat.layout.TxHashBinPath(chunkID))
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
	require.NoError(t, processChunk(context.Background(), chunkID, AllArtifacts(), cfg))
	opensAfterFirst := backend.opens.Load()
	require.Equal(t, int32(1), opensAfterFirst, "first pass opens the backend once")

	// Second pass: every kind is frozen, so processChunk returns without opening
	// any source.
	require.NoError(t, processChunk(context.Background(), chunkID, AllArtifacts(), cfg))
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

	// Simulate a crash mid-freeze: the keys are "freezing" and a stale/partial
	// pack file exists at the canonical path.
	require.NoError(t, cat.MarkChunkFreezing(chunkID, AllKinds()...))
	require.NoError(t, os.MkdirAll(filepath.Dir(cat.layout.LedgerPackPath(chunkID)), 0o755))
	require.NoError(t, os.WriteFile(cat.layout.LedgerPackPath(chunkID), []byte("PARTIAL-GARBAGE"), 0o644))

	// Re-run: a "freezing" key triggers re-materialization (rule 1), overwriting
	// the partial at the canonical path.
	require.NoError(t, processChunk(context.Background(), chunkID, AllArtifacts(), cfg))

	for _, kind := range AllKinds() {
		state, err := cat.State(chunkID, kind)
		require.NoError(t, err)
		require.Equal(t, StateFrozen, state)
	}
	// The partial garbage was overwritten with a real pack.
	cr, err := ledger.OpenColdReader(cat.layout.LedgerPackPath(chunkID))
	require.NoError(t, err)
	defer func() { _ = cr.Close() }()
	last, err := cr.LastSeq()
	require.NoError(t, err)
	require.Equal(t, chunkID.LastLedger(), last)
}

// ---------------------------------------------------------------------------
// Mark-then-write ORDERING: the core one-write-protocol invariant. At the
// instant after MarkChunkFreezing and before any file I/O, every requested kind
// must read "freezing" and no artifact file may exist yet. The afterMarkFreezing
// crash hook (hooks.go) observes that exact instant from INSIDE processChunk, so
// dropping the mark (keys would be absent) or reordering the write ahead of it
// (a file would exist) is caught — neither could ship green.
// ---------------------------------------------------------------------------

func TestProcessChunk_MarksFreezingBeforeWrite(t *testing.T) {
	for _, tc := range []struct {
		name      string
		artifacts ArtifactSet
	}{
		{"all kinds", AllArtifacts()},
		{"events+txhash subset", NewArtifactSet(KindEvents, KindTxHash)},
		{"ledgers only", NewArtifactSet(KindLedgers)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cat, _ := testCatalog(t)
			cfg := testProcessConfig(t, cat)
			cfg.Backend = zeroTxBackend(t)
			cfg.BackendWaiter = &fakeWaiter{}

			chunkID := chunk.ID(0)
			requested := tc.artifacts.Kinds()

			var fired bool
			cat.hooks.afterMarkFreezing = func() {
				fired = true
				// (1) Every requested kind reads "freezing" at the mark instant.
				// Dropping MarkChunkFreezing would leave these absent (empty State).
				for _, kind := range requested {
					state, err := cat.State(chunkID, kind)
					require.NoError(t, err)
					require.Equal(t, StateFreezing, state,
						"kind %s must be 'freezing' before any I/O", kind)
				}
				// (2) No artifact file exists yet. Reordering the write ahead of the
				// mark (or writing without marking) would leave a file present here.
				for _, kind := range requested {
					for _, p := range cat.layout.ArtifactPaths(chunkID, kind) {
						require.NoFileExists(t, p,
							"no %s artifact file may exist at the mark instant", kind)
					}
				}
			}

			require.NoError(t, processChunk(context.Background(), chunkID, tc.artifacts, cfg))
			require.True(t, fired, "afterMarkFreezing hook must have fired inside processChunk")

			// And the freeze still completes: every requested kind ends "frozen".
			for _, kind := range requested {
				state, err := cat.State(chunkID, kind)
				require.NoError(t, err)
				require.Equal(t, StateFrozen, state)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// backfillSource preference order.
// ---------------------------------------------------------------------------

func TestBackfillSource_PrefersCompleteHotTier(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat)

	chunkID := chunk.ID(0)
	// Mark the hot key "ready" and wire a complete hot tier (max committed seq
	// reaches the chunk's last ledger).
	require.NoError(t, cat.FlipHotReady(chunkID))
	hotBackend := zeroTxBackend(t)
	var closed atomic.Int32
	cfg.HotProbe = &fakeHotProbe{
		ok: true,
		chunk: &fakeHotChunk{
			maxSeq:   chunkID.LastLedger(),
			present:  true,
			source:   hotBackend,
			closedTo: &closed,
		},
	}
	// A bulk backend is configured but must NOT be used.
	bulk := zeroTxBackend(t)
	cfg.Backend = bulk
	cfg.BackendWaiter = &fakeWaiter{}

	src, closeSrc, err := backfillSource(context.Background(), chunkID, AllArtifacts(), cfg)
	require.NoError(t, err)
	require.Same(t, ingest.ChunkSource(hotBackend), src)
	require.NoError(t, closeSrc())
	require.Equal(t, int32(1), closed.Load(), "the closer releases the opened hot tier")
	require.Equal(t, int32(0), bulk.opens.Load(), "the bulk backend was not consulted")
}

func TestBackfillSource_WatermarkGate_IncompleteFallsThrough(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat)

	chunkID := chunk.ID(0)
	require.NoError(t, cat.FlipHotReady(chunkID))
	var closed atomic.Int32
	// maxSeq is ONE BELOW the chunk's last ledger — i.e. the single DB's
	// watermark has not reached completeness even though it is present. Under
	// decision (a) every CF advances together, so a watermark short of the last
	// ledger means the chunk is genuinely unfinished. It is staleness, not loss:
	// fall through.
	cfg.HotProbe = &fakeHotProbe{
		ok: true,
		chunk: &fakeHotChunk{
			maxSeq:   chunkID.LastLedger() - 1,
			present:  true,
			closedTo: &closed,
		},
	}
	bulk := zeroTxBackend(t)
	cfg.Backend = bulk
	cfg.BackendWaiter = &fakeWaiter{}

	src, closeSrc, err := backfillSource(context.Background(), chunkID, AllArtifacts(), cfg)
	require.NoError(t, err)
	require.Same(t, ingest.ChunkSource(bulk), src, "incomplete hot tier falls through to bulk")
	require.NoError(t, closeSrc())
	require.GreaterOrEqual(t, closed.Load(), int32(1), "the incomplete hot tier was closed on fall-through")
}

func TestBackfillSource_LossIsFatal(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat)

	chunkID := chunk.ID(0)
	require.NoError(t, cat.FlipHotReady(chunkID))
	// "ready" key but the probe reports the dir absent (ok=false) — case-4 loss.
	cfg.HotProbe = &fakeHotProbe{ok: false}
	cfg.Backend = zeroTxBackend(t)
	cfg.BackendWaiter = &fakeWaiter{}

	_, _, err := backfillSource(context.Background(), chunkID, AllArtifacts(), cfg)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHotVolumeLost)
}

func TestBackfillSource_LossOnOpenError(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat)

	chunkID := chunk.ID(0)
	require.NoError(t, cat.FlipHotReady(chunkID))
	cfg.HotProbe = &fakeHotProbe{openErr: errors.New("cannot open hot dir")}
	cfg.Backend = zeroTxBackend(t)
	cfg.BackendWaiter = &fakeWaiter{}

	_, _, err := backfillSource(context.Background(), chunkID, AllArtifacts(), cfg)
	require.ErrorIs(t, err, ErrHotVolumeLost)
}

func TestBackfillSource_PrefersFrozenPackWhenLFSNotRequested(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat)

	chunkID := chunk.ID(0)
	// Frozen ledgers with a real pack on disk; ledgers is NOT requested.
	require.NoError(t, cat.MarkChunkFreezing(chunkID, KindLedgers))
	require.NoError(t, os.MkdirAll(filepath.Dir(cat.layout.LedgerPackPath(chunkID)), 0o755))
	writeRealPack(t, cat, chunkID)
	require.NoError(t, cat.FlipChunkFrozen(chunkID, KindLedgers))

	// hot not ready; bulk configured but should not be used.
	bulk := zeroTxBackend(t)
	cfg.Backend = bulk
	cfg.BackendWaiter = &fakeWaiter{}

	set := NewArtifactSet(KindEvents, KindTxHash) // ledgers NOT requested
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
	require.NoError(t, cat.MarkChunkFreezing(chunkID, KindLedgers))
	require.NoError(t, os.MkdirAll(filepath.Dir(cat.layout.LedgerPackPath(chunkID)), 0o755))
	writeRealPack(t, cat, chunkID)
	require.NoError(t, cat.FlipChunkFrozen(chunkID, KindLedgers))

	bulk := zeroTxBackend(t)
	cfg.Backend = bulk
	cfg.BackendWaiter = &fakeWaiter{}

	// ledgers IS requested — the pack branch is skipped (circular), so it goes to bulk.
	src, closeSrc, err := backfillSource(context.Background(), chunkID, AllArtifacts(), cfg)
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

	_, _, err := backfillSource(context.Background(), chunkID, AllArtifacts(), cfg)
	require.ErrorIs(t, err, ErrBackendCoverageTimeout)
}

func TestBackfillSource_NoBackendConfigured(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat)
	cfg.Backend = nil

	_, _, err := backfillSource(context.Background(), chunk.ID(0), AllArtifacts(), cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no bulk backend")
}

// writeRealPack writes a valid cold ledger pack for chunkID at its canonical
// Layout path by driving the merged cold ledger ingester over a zero-tx stream.
func writeRealPack(t *testing.T, cat *Catalog, chunkID chunk.ID) {
	t.Helper()
	src := &countingChunkSource{
		make: func(chunk.ID) (ledgerbackend.LedgerStream, error) {
			return &fullChunkStream{t: t, gen: zeroTxLCMBytes}, nil
		},
	}
	dirs := ingest.ColdDirs{Ledgers: cat.layout.LedgersRoot()}
	require.NoError(t, ingest.RunColdChunk(
		context.Background(), silentLogger(), src, dirs, chunkID,
		ingest.NopSink{}, ingest.Config{Ledgers: true}))
	require.FileExists(t, cat.layout.LedgerPackPath(chunkID))
}

// ---------------------------------------------------------------------------
// Real hot probe: single-watermark completeness over the shared multi-CF
// RocksDB hot DB (decision (a)).
// ---------------------------------------------------------------------------

func TestRocksHotProbe_SingleWatermark_CompleteVsStale(t *testing.T) {
	hotRoot := t.TempDir()
	chunkID := chunk.ID(0)
	chunkDir := filepath.Join(hotRoot, chunkID.String())

	// Ingest a SHORT prefix of the chunk into the shared hot DB (one atomic
	// batch per ledger across all CFs), so the single watermark is well below
	// the chunk's last ledger (stale).
	stalePrefix := chunkID.FirstLedger() + 4
	ingestHotPrefix(t, chunkDir, chunkID, stalePrefix)

	probe := NewRocksHotProbe(func(c chunk.ID) string {
		return filepath.Join(hotRoot, c.String())
	}, silentLogger())

	hot, ok, err := probe.OpenHotChunk(chunkID)
	require.NoError(t, err)
	require.True(t, ok)
	defer func() { _ = hot.Close() }()

	maxSeq, present, err := hot.MaxCommittedSeq()
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, stalePrefix, maxSeq, "the single watermark equals the last committed ledger")
	require.Less(t, maxSeq, chunkID.LastLedger(), "a stale prefix is not complete")
}

func TestRocksHotProbe_AbsentDirIsNotOpened(t *testing.T) {
	hotRoot := t.TempDir()
	probe := NewRocksHotProbe(func(c chunk.ID) string {
		return filepath.Join(hotRoot, c.String())
	}, silentLogger())
	_, ok, err := probe.OpenHotChunk(chunk.ID(7))
	require.NoError(t, err)
	require.False(t, ok, "an absent hot dir reports ok=false (loss when key is ready)")
}

// ingestHotPrefix writes ledgers [chunk.First, throughSeq] into the chunk's
// SHARED multi-CF hot DB via hotchunk.IngestLedger — one atomic synced
// WriteBatch per ledger across all CFs (decision (a)) — then closes it so the
// probe can reopen it.
func ingestHotPrefix(t *testing.T, chunkDir string, chunkID chunk.ID, throughSeq uint32) {
	t.Helper()
	require.NoError(t, os.MkdirAll(chunkDir, 0o755))

	db, err := hotchunk.Open(chunkDir, chunkID, silentLogger())
	require.NoError(t, err)

	cfg := hotchunk.Ingest{Ledgers: true, Txhash: true, Events: true}
	for seq := chunkID.FirstLedger(); seq <= throughSeq; seq++ {
		lcm := xdr.LedgerCloseMetaView(zeroTxLCMBytes(t, seq))
		_, err := db.IngestLedger(seq, lcm, cfg)
		require.NoError(t, err)
	}
	require.NoError(t, db.Close())
}
