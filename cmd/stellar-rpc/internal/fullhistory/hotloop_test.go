package fullhistory

import (
	"context"
	"errors"
	"iter"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/fhtest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/latencytrack"
)

// ---------------------------------------------------------------------------
// fakeCoreStream — an injectable ledgerbackend.LedgerStream the ingestion loop
// consumes (the design's raw captive-core stream). RawLedgers yields programmed
// frames contiguously from the range's From(); once it runs past the last
// programmed seq it either blocks until ctx is canceled (a live tip stream that
// only ends on shutdown) or yields endErr (a crashed backend). It records the
// FIRST seq it was asked for (the loop's resume point) and a per-seq consideration
// count so a test can wait for the loop to reach the blocking pull.
// ---------------------------------------------------------------------------

type fakeCoreStream struct {
	frames     map[uint32][]byte // seq -> raw LCM bytes
	blockOnCtx bool              // past the last frame, block until ctx.Done
	endClean   bool              // past the last frame, return with NO error (a graceful stream end)
	endErr     error             // past the last frame, yield this (when not blocking)
	yieldErrAt uint32            // if non-zero, yield errAt at this seq instead of bytes
	errAt      error

	calls     atomic.Int32 // seqs yielded by the stream
	firstSeen atomic.Uint32
	sawFirst  atomic.Bool
}

var _ ledgerbackend.LedgerStream = (*fakeCoreStream)(nil)

func (s *fakeCoreStream) RawLedgers(
	ctx context.Context, r ledgerbackend.Range, _ ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		if s.sawFirst.CompareAndSwap(false, true) {
			s.firstSeen.Store(r.From())
		}
		for seq := r.From(); ; seq++ {
			s.calls.Add(1)
			if ctx.Err() != nil {
				yield(nil, ctx.Err())
				return
			}
			if s.yieldErrAt != 0 && seq == s.yieldErrAt {
				yield(nil, s.errAt)
				return
			}
			if raw, ok := s.frames[seq]; ok {
				if !yield(raw, nil) {
					return
				}
				continue
			}
			// Past the programmed frames.
			if s.blockOnCtx {
				<-ctx.Done()
				yield(nil, ctx.Err())
				return
			}
			if s.endClean {
				return // graceful stream end: exhaust with no error and no ctx cancel
			}
			if s.endErr != nil {
				yield(nil, s.endErr)
				return
			}
			yield(nil, errors.New("fakeCoreStream: no frame for seq"))
			return
		}
	}
}

// streamForSeqs builds a fakeCoreStream with zero-tx LCM frames for [from,to].
func streamForSeqs(t *testing.T, from, to uint32) *fakeCoreStream {
	t.Helper()
	s := &fakeCoreStream{frames: map[uint32][]byte{}}
	for seq := from; seq <= to; seq++ {
		s.frames[seq] = fhtest.ZeroTxLCMBytes(t, seq)
	}
	return s
}

// recordingBoundary is a test boundaryPublisher capturing the completed chunk ids
// the loop publishes at each boundary, so a test can assert the handoff without
// wiring a real lifecycle Loop.
type recordingBoundary struct {
	mu  sync.Mutex
	ids []chunk.ID
}

func (r *recordingBoundary) Publish(c chunk.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ids = append(r.ids, c)
}

func (r *recordingBoundary) list() []chunk.ID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]chunk.ID(nil), r.ids...)
}

// loopConfig builds an ingestionLoopConfig for a test: the stream + resume point +
// a recording boundary, and opens the resume chunk's hot DB the way run() does now
// (the loop takes ownership and closes it). The test must hold no other handle on
// that dir while the loop runs (a second read-write open would contend the LOCK).
func loopConfig(
	t *testing.T, stream ledgerbackend.LedgerStream, cat *catalog.Catalog, resume uint32,
) (ingestionLoopConfig, *recordingBoundary) {
	t.Helper()
	rec := &recordingBoundary{}
	db, err := openHotDBForChunk(cat, chunk.IDFromLedger(resume), silentLogger())
	require.NoError(t, err)
	return ingestionLoopConfig{
		Stream:   stream,
		Resume:   resume,
		HotDB:    db,
		Catalog:  cat,
		Boundary: rec,
		Logger:   silentLogger(),
	}, rec
}

// impliedResume is the resume point a hot DB's durable last committed ledger implies — one past
// its last committed ledger, or the chunk's first ledger when empty. Production no
// longer derives this in the loop (it trusts the resume run() passes it), but tests
// still assert that a restart's durable last committed ledger matches what startup would derive.
func impliedResume(t *testing.T, db *hotchunk.DB) uint32 {
	t.Helper()
	maxSeq, ok, err := db.MaxCommittedSeq()
	require.NoError(t, err)
	if !ok {
		return db.ChunkID().FirstLedger()
	}
	return maxSeq + 1
}

// openLiveHotDB opens (and brackets ready) the live hot DB for a chunk via the
// production opener, returning the handle and the catalog it lives under.
func openLiveHotDB(t *testing.T, cat *catalog.Catalog, c chunk.ID) *hotchunk.DB {
	t.Helper()
	db, err := openHotDBForChunk(cat, c, silentLogger())
	require.NoError(t, err)
	return db
}

// seedLastCommitted commits real zero-tx LCMs for [FirstLedger, seq] into chunk c's
// hot DB through the production IngestLedger path (the events CF requires strict
// ledger contiguity from the chunk's first ledger), then CLOSES the handle —
// leaving the chunk "ready" on disk with NO open handle, so the loop can open it
// itself. Returns the resume point (seq+1) a boundary test drives the loop from.
// Seeding a near-full chunk costs one synced commit per ledger, so its callers run
// t.Parallel().
func seedLastCommitted(t *testing.T, cat *catalog.Catalog, c chunk.ID, seq uint32) uint32 {
	t.Helper()
	db := openLiveHotDB(t, cat, c)
	for s := c.FirstLedger(); s <= seq; s++ {
		_, err := db.IngestLedger(s, fhtest.ZeroTxLCMBytes(t, s))
		require.NoError(t, err)
	}
	require.NoError(t, db.Close())
	return seq + 1
}

// ---------------------------------------------------------------------------
// openHotDBForChunk — the bracket's open end.
// ---------------------------------------------------------------------------

// TestOpenHotTier_CreatesBracketAndDir: a fresh open writes the dir and flips
// the key "ready"; the returned DB is empty (resume at FirstLedger).
func TestOpenHotTier_CreatesBracketAndDir(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(3)

	db, err := openHotDBForChunk(cat, c, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	state, err := cat.HotState(c)
	require.NoError(t, err)
	assert.Equal(t, geometry.HotReady, state, "open flips the key ready")

	_, statErr := os.Stat(cat.Layout().HotChunkPath(c))
	require.NoError(t, statErr, "the dir exists")

	assert.Equal(t, c.FirstLedger(), impliedResume(t, db), "an empty resume DB resumes at the chunk's first ledger")
}

// TestOpenHotTier_ReadyButDirMissingFailsOpen: a "ready" key whose DB is gone
// FAILS the must-exist open (never auto-healed into a fresh empty DB). The error
// is ordinary/restartable — no sentinel.
func TestOpenHotTier_ReadyButDirMissingFailsOpen(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(5)
	require.NoError(t, cat.PutHotTransient(c))
	require.NoError(t, cat.FlipHotReady(c)) // key says ready, but no dir created

	_, err := openHotDBForChunk(cat, c, silentLogger())
	require.Error(t, err)
}

// TestOpenHotTier_TransientRecreatesFresh: a "transient" key (crashed
// create/discard) is recovered by wiping any leftover and recreating.
func TestOpenHotTier_TransientRecreatesFresh(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(2)
	require.NoError(t, cat.PutHotTransient(c)) // a crash left a transient key

	db, err := openHotDBForChunk(cat, c, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	state, err := cat.HotState(c)
	require.NoError(t, err)
	assert.Equal(t, geometry.HotReady, state)
}

// ---------------------------------------------------------------------------
// runIngestionLoop — atomic landing.
// ---------------------------------------------------------------------------

// TestRunIngestionLoop_LedgerLandsAcrossAllCFs: polling a short contiguous
// prefix lands each ledger atomically across the ledgers, txhash, and events
// CFs — the single committed frontier advances to the last committed seq, and every CF
// is readable. The getter then errs (backend crash), which the loop returns.
func TestRunIngestionLoop_LedgerLandsAcrossAllCFs(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	first := c.FirstLedger()

	// A short contiguous prefix from the chunk's first ledger (events require
	// strict contiguity from FirstLedger), then the stream runs dry and errs. The
	// loop opens the empty chunk 0 itself and resumes at its first ledger.
	stream := streamForSeqs(t, first, first+2)
	stream.endErr = errors.New("backend crashed")
	cfg, _ := loopConfig(t, stream, cat, first)

	err := runIngestionLoop(context.Background(), cfg)
	require.Error(t, err, "stream ran past the prefix and errored")

	// Reopen the (loop-closed) DB and assert every CF advanced together.
	reopened, err := hotchunk.Open(cat.Layout().HotChunkPath(c), c, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	maxSeq, ok, err := reopened.MaxCommittedSeq()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, first+2, maxSeq, "the single committed frontier is the last committed seq")

	raw, err := reopened.Ledgers().GetLedgerRaw(first + 2)
	require.NoError(t, err)
	assert.NotEmpty(t, raw)
	assert.Equal(t, uint32(0), eventCount(t, reopened.Events()), "zero-tx ledgers carry no events")
}

// TestRunIngestionLoop_RecordsPerLedgerLatency: each committed ledger records one
// sample into the ingest.read / ingest.write / ingest.e2e series. The failed final
// pull (the stream's endErr) records nothing.
func TestRunIngestionLoop_RecordsPerLedgerLatency(t *testing.T) {
	cat, _ := testCatalog(t)
	first := chunk.ID(0).FirstLedger()

	stream := streamForSeqs(t, first, first+2)
	stream.endErr = errors.New("end")
	cfg, _ := loopConfig(t, stream, cat, first)
	cfg.Latency = new(latencytrack.Set)

	require.Error(t, runIngestionLoop(context.Background(), cfg))

	all := cfg.Latency.SnapshotAll()
	for _, series := range []string{latSeriesIngestRead, latSeriesIngestWrite, latSeriesIngestE2E} {
		assert.Equal(t, uint64(3), all[series].Count, "series %s: one sample per committed ledger", series)
	}
	assert.Positive(t, all[latSeriesIngestWrite].Max, "a synced commit takes measurable time")
	assert.GreaterOrEqual(t, all[latSeriesIngestE2E].Max, all[latSeriesIngestWrite].Max,
		"e2e is read + write per ledger")
}

// TestRunIngestionLoop_LastCommittedGaugeAdvancesPerLedger: the ingestion loop owns
// the last-committed gauge and sets it once per COMMITTED ledger (mid-chunk included),
// not a single chunk-aligned value. Asserted by the exact ordered sequence of gauge
// values a short prefix produces.
func TestRunIngestionLoop_LastCommittedGaugeAdvancesPerLedger(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	first := c.FirstLedger()

	stream := streamForSeqs(t, first, first+2)
	stream.endErr = errors.New("end")
	cfg, _ := loopConfig(t, stream, cat, first)
	rec := newRecordingMetrics()
	cfg.Metrics = rec

	require.Error(t, runIngestionLoop(context.Background(), cfg))

	assert.Equal(t, []uint32{first, first + 1, first + 2}, rec.lastCommittedSeq(),
		"the loop sets the last-committed gauge per committed ledger, not chunk-aligned")
}

// ---------------------------------------------------------------------------
// runIngestionLoop — boundary notifications carry the completed chunk id.
// ---------------------------------------------------------------------------

// TestRunIngestionLoop_BoundaryNotifiesCompletedChunk: crossing the chunk 0 -> 1
// boundary publishes chunk 0 to the lifecycle. The last committed seq is seeded just below
// the boundary so the stream crosses it in one step.
func TestRunIngestionLoop_BoundaryNotifiesCompletedChunk(t *testing.T) {
	t.Parallel() // seeds a near-full chunk (one synced commit per ledger)
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	c1 := c + 1
	resume := seedLastCommitted(t, cat, c, c.LastLedger()-1) // == c.LastLedger()

	stream := &fakeCoreStream{frames: map[uint32][]byte{
		c.LastLedger():   fhtest.ZeroTxLCMBytes(t, c.LastLedger()),   // boundary 0->1
		c1.FirstLedger(): fhtest.ZeroTxLCMBytes(t, c1.FirstLedger()), // a ledger in chunk 1
	}, endErr: errors.New("end")}
	cfg, rec := loopConfig(t, stream, cat, resume)

	done := make(chan error, 1)
	go func() {
		done <- runIngestionLoop(context.Background(), cfg)
	}()

	select {
	case err := <-done:
		require.Error(t, err, "stream ran dry")
	case <-time.After(10 * time.Second):
		t.Fatal("ingestion loop deadlocked")
	}

	assert.Equal(t, []chunk.ID{c}, rec.list(), "the completed chunk id was published at the boundary")
}

// ---------------------------------------------------------------------------
// runIngestionLoop — handoff fence: close-before-next-key, publish-after-open.
// ---------------------------------------------------------------------------

// fencePublisher verifies the loop's HANDOFF FENCE from inside Publish. At each
// boundary it checks, for the just-closed chunk c: (1) c's write handle was released
// BEFORE the publish — a read-WRITE OpenExisting on c's path takes the RocksDB LOCK,
// which would fail if the writer still held it; and (2) the NEXT chunk's hot key is
// already "ready" (its DB was opened before publish). Outcomes are recorded per
// boundary for the test to assert after the loop.
type fencePublisher struct {
	cat *catalog.Catalog

	mu             sync.Mutex
	published      []chunk.ID
	closedReleased []bool // per boundary: the closed chunk's write LOCK was free
	nextReady      []bool // per boundary: the next chunk's hot key was already ready
}

func (p *fencePublisher) Publish(c chunk.ID) {
	// (1) The closed chunk's write handle must be released: OpenExisting is read-write
	// and takes the LOCK, so it succeeds only if the loop already closed the handle.
	db, err := hotchunk.OpenExisting(p.cat.Layout().HotChunkPath(c), c, silentLogger())
	released := err == nil
	if db != nil {
		_ = db.Close()
	}
	// (2) The next chunk must already be open+ready before this publish fires.
	st, herr := p.cat.HotState(c + 1)
	ready := herr == nil && st == geometry.HotReady

	p.mu.Lock()
	defer p.mu.Unlock()
	p.published = append(p.published, c)
	p.closedReleased = append(p.closedReleased, released)
	p.nextReady = append(p.nextReady, ready)
}

// TestRunIngestionLoop_HandoffFenceClosesBeforeNextKey pins the boundary handoff order:
// the just-closed chunk's write handle is released before the completed chunk is
// published, and the next chunk's hot key is already "ready" at publish time.
func TestRunIngestionLoop_HandoffFenceClosesBeforeNextKey(t *testing.T) {
	t.Parallel() // seeds a near-full chunk (one synced commit per ledger)
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	c1 := c + 1
	resume := seedLastCommitted(t, cat, c, c.LastLedger()-1) // == c.LastLedger()

	stream := &fakeCoreStream{frames: map[uint32][]byte{
		c.LastLedger():   fhtest.ZeroTxLCMBytes(t, c.LastLedger()),   // boundary 0->1
		c1.FirstLedger(): fhtest.ZeroTxLCMBytes(t, c1.FirstLedger()), // a ledger in chunk 1
	}, endErr: errors.New("end")}

	// Build the loop config manually so the boundary publisher is our fence checker.
	db, err := openHotDBForChunk(cat, chunk.IDFromLedger(resume), silentLogger())
	require.NoError(t, err)
	fence := &fencePublisher{cat: cat}
	cfg := ingestionLoopConfig{
		Stream: stream, Resume: resume, HotDB: db, Catalog: cat, Boundary: fence, Logger: silentLogger(),
	}

	require.Error(t, runIngestionLoop(context.Background(), cfg), "stream ran dry")

	require.Equal(t, []chunk.ID{c}, fence.published, "the completed chunk was published at the boundary")
	require.Equal(t, []bool{true}, fence.closedReleased, "the closed chunk's write LOCK was released before publish")
	require.Equal(t, []bool{true}, fence.nextReady, "the next chunk's hot key was ready before publish")
}

// ---------------------------------------------------------------------------
// runIngestionLoop — clean shutdown vs crash (classified at the daemon top
// level: ctx-canceled return is clean, any other error is restartable).
// ---------------------------------------------------------------------------

// TestRunIngestionLoop_CtxCancelReturnsCtxErr: a ctx cancellation while the stream
// is blocking on the tip makes RawLedgers yield ctx.Err(); the loop returns that
// (the daemon top level classifies a ctx-canceled return as a clean shutdown).
func TestRunIngestionLoop_CtxCancelReturnsCtxErr(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	first := c.FirstLedger()

	stream := streamForSeqs(t, first, first+1)
	stream.blockOnCtx = true // after the frames, behave like a live tip stream
	cfg, _ := loopConfig(t, stream, cat, first)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- runIngestionLoop(ctx, cfg)
	}()

	require.Eventually(t, func() bool {
		return stream.calls.Load() >= 3 // ingested 2 frames, blocked on the 3rd
	}, 5*time.Second, 5*time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled, "the loop surfaces the ctx-canceled stream error")
	case <-time.After(10 * time.Second):
		t.Fatal("ingestion loop did not stop on ctx cancellation")
	}
}

// TestRunIngestionLoop_CleanStreamEndIsError: an ingestion stream that EXHAUSTS with no
// error and no ctx cancellation (a graceful end the daemon never expects from captive
// core) surfaces as a non-nil restartable error — never a nil return, which g.Wait would
// read as a clean shutdown and silently stop ingesting.
func TestRunIngestionLoop_CleanStreamEndIsError(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	first := c.FirstLedger()

	stream := streamForSeqs(t, first, first+2)
	stream.endClean = true // exhaust cleanly past the prefix (no error, no ctx cancel)
	cfg, _ := loopConfig(t, stream, cat, first)

	err := runIngestionLoop(context.Background(), cfg)
	require.Error(t, err, "a graceful stream end must surface as an error, not nil")
	require.Contains(t, err.Error(), "ended unexpectedly")
	require.NotErrorIs(t, err, context.Canceled, "a clean stream end is restartable, not a clean shutdown")
}

// TestRunIngestionLoop_StreamErrorReturnsError: a stream error (not a shutdown)
// propagates as a restartable failure.
func TestRunIngestionLoop_StreamErrorReturnsError(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	first := c.FirstLedger()

	boom := errors.New("backend exploded")
	stream := streamForSeqs(t, first, first)
	stream.yieldErrAt = first + 1
	stream.errAt = boom
	cfg, _ := loopConfig(t, stream, cat, first)

	err := runIngestionLoop(context.Background(), cfg)
	require.Error(t, err)
	require.ErrorIs(t, err, boom)
}

// ---------------------------------------------------------------------------
// runIngestionLoop — restart resumes idempotently from the derived last-committed.
// ---------------------------------------------------------------------------

// TestRunIngestionLoop_RestartResumesFromLastCommitted: after a first run commits a
// prefix and exits, a second run over a FRESH open of the SAME hot dir resumes at
// last-committed+1 (asserted via the FIRST seq the stream is asked for) — the stream
// range starts at the derived resume, and the final last-committed is exactly the last
// delivered seq.
func TestRunIngestionLoop_RestartResumesFromLastCommitted(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	first := c.FirstLedger()

	// First run: loopConfig opens empty chunk 0 (resumes at first), the loop commits
	// [first, first+2], then the stream errs.
	stream1 := streamForSeqs(t, first, first+2)
	stream1.endErr = errors.New("end")
	cfg1, _ := loopConfig(t, stream1, cat, first)
	err := runIngestionLoop(context.Background(), cfg1)
	require.Error(t, err)
	assert.Equal(t, first, stream1.firstSeen.Load(), "first run resumed at the chunk's first ledger")

	// The durable last-committed ledger now implies resume first+3 — exactly what
	// startup would derive on restart. Close the handle before the loop reopens the dir.
	db2, err := openHotDBForChunk(cat, c, silentLogger())
	require.NoError(t, err)
	resume := impliedResume(t, db2)
	assert.Equal(t, first+3, resume, "restart resumes one past the durable last-committed ledger")
	require.NoError(t, db2.Close())

	// Second run resumes at the derived last-committed ledger and commits two more ledgers.
	stream2 := streamForSeqs(t, first+3, first+5)
	stream2.endErr = errors.New("end")
	cfg2, _ := loopConfig(t, stream2, cat, resume)
	err = runIngestionLoop(context.Background(), cfg2)
	require.Error(t, err)
	assert.Equal(t, first+3, stream2.firstSeen.Load(), "second run resumed at last-committed+1")

	reopened, err := hotchunk.Open(cat.Layout().HotChunkPath(c), c, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	maxSeq, ok, err := reopened.MaxCommittedSeq()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, first+5, maxSeq)
}

// eventCount reads the hot events store's committed event count, failing the
// test on the (close-only) error the Reader contract allows.
func eventCount(t *testing.T, r interface{ EventCount() (uint32, error) }) uint32 {
	t.Helper()
	n, err := r.EventCount()
	require.NoError(t, err)
	return n
}
