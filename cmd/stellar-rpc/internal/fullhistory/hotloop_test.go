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
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/hotchunk"
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
	endErr     error             // past the last frame, yield this (when not blocking)
	yieldErrAt uint32            // if non-zero, yield errAt at this seq instead of bytes
	errAt      error

	calls     atomic.Int32 // seqs considered (mirrors the old per-GetLedger count)
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
		s.frames[seq] = zeroTxLCMBytes(t, seq)
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

// loopConfig builds an ingestionLoopConfig for a test: the stream + hot DB + a
// recording boundary, with Resume derived from the DB (the value the loop asserts).
func loopConfig(t *testing.T, stream ledgerbackend.LedgerStream, db *hotchunk.DB, cat *catalog.Catalog) (ingestionLoopConfig, *recordingBoundary) {
	t.Helper()
	resume, err := nextIngestLedger(db)
	require.NoError(t, err)
	rec := &recordingBoundary{}
	return ingestionLoopConfig{
		Stream:   stream,
		Resume:   resume,
		HotDB:    db,
		Catalog:  cat,
		Boundary: rec,
		Logger:   silentLogger(),
	}, rec
}

// openLiveHotDB opens (and brackets ready) the live hot DB for a chunk via the
// production opener, returning the handle and the catalog it lives under.
func openLiveHotDB(t *testing.T, cat *catalog.Catalog, c chunk.ID) *hotchunk.DB {
	t.Helper()
	db, err := openHotDBForChunk(cat, c, silentLogger())
	require.NoError(t, err)
	return db
}

// seedWatermark advances a chunk's hot DB to a last-committed ledger of seq so
// the indexed poll resumes at seq+1, letting a boundary test drive the loop over
// only the last ledger or two of a chunk. It ingests a real zero-tx LCM for
// every ledger up to seq through the production IngestLedger path (the events
// CF requires strict ledger contiguity from the chunk's first ledger). The
// returned DB is the (re-opened, ready) live handle the loop then owns. Seeding
// a near-full chunk costs one synced commit per ledger, so its callers run
// t.Parallel().
func seedWatermark(t *testing.T, cat *catalog.Catalog, c chunk.ID, seq uint32) *hotchunk.DB {
	t.Helper()
	db := openLiveHotDB(t, cat, c)
	for s := c.FirstLedger(); s <= seq; s++ {
		_, err := db.IngestLedger(s, zeroTxLCMBytes(t, s))
		require.NoError(t, err)
	}
	require.NoError(t, db.Close())
	reopened, err := openHotDBForChunk(cat, c, silentLogger())
	require.NoError(t, err)
	return reopened
}

// drainLifecycle counts how many chunk ids the buffered lifecycle channel
// delivered after the loop returned (the loop is done, so no send races this).
func drainLifecycle(ch chan chunk.ID) []chunk.ID {
	var got []chunk.ID
	for {
		select {
		case c := <-ch:
			got = append(got, c)
		default:
			return got
		}
	}
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

	resume, err := nextIngestLedger(db)
	require.NoError(t, err)
	assert.Equal(t, c.FirstLedger(), resume, "an empty resume DB resumes at the chunk's first ledger")
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
// CFs — the single watermark advances to the last committed seq, and every CF
// is readable. The getter then errs (backend crash), which the loop returns.
func TestRunIngestionLoop_LedgerLandsAcrossAllCFs(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	first := c.FirstLedger()
	db := openLiveHotDB(t, cat, c)

	// A short contiguous prefix from the chunk's first ledger (events require
	// strict contiguity from FirstLedger), then the stream runs dry and errs.
	stream := streamForSeqs(t, first, first+2)
	stream.endErr = errors.New("backend crashed")
	cfg, _ := loopConfig(t, stream, db, cat)

	err := runIngestionLoop(context.Background(), cfg)
	require.Error(t, err, "stream ran past the prefix and errored")

	// Reopen the (loop-closed) DB and assert every CF advanced together.
	reopened, err := hotchunk.Open(cat.Layout().HotChunkPath(c), c, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	maxSeq, ok, err := reopened.MaxCommittedSeq()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, first+2, maxSeq, "the single watermark is the last committed seq")

	raw, err := reopened.Ledgers().GetLedgerRaw(first + 2)
	require.NoError(t, err)
	assert.NotEmpty(t, raw)
	assert.Equal(t, uint32(0), eventCount(t, reopened.Events()), "zero-tx ledgers carry no events")
}

// ---------------------------------------------------------------------------
// runIngestionLoop — boundary notifications carry the completed chunk id.
// ---------------------------------------------------------------------------

// TestRunIngestionLoop_BoundaryNotifiesCompletedChunk: crossing the chunk 0 -> 1
// boundary publishes chunk 0 to the lifecycle. The watermark is seeded just below
// the boundary so the stream crosses it in one step.
func TestRunIngestionLoop_BoundaryNotifiesCompletedChunk(t *testing.T) {
	t.Parallel() // seeds a near-full chunk (one synced commit per ledger)
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	c1 := c + 1
	db := seedWatermark(t, cat, c, c.LastLedger()-1)

	stream := &fakeCoreStream{frames: map[uint32][]byte{
		c.LastLedger():   zeroTxLCMBytes(t, c.LastLedger()),   // boundary 0->1
		c1.FirstLedger(): zeroTxLCMBytes(t, c1.FirstLedger()), // a ledger in chunk 1
	}, endErr: errors.New("end")}
	cfg, rec := loopConfig(t, stream, db, cat)

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
	db := openLiveHotDB(t, cat, c)

	stream := streamForSeqs(t, first, first+1)
	stream.blockOnCtx = true // after the frames, behave like a live tip stream
	cfg, _ := loopConfig(t, stream, db, cat)
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

// TestRunIngestionLoop_StreamErrorReturnsError: a stream error (not a shutdown)
// propagates as a restartable failure.
func TestRunIngestionLoop_StreamErrorReturnsError(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	first := c.FirstLedger()
	db := openLiveHotDB(t, cat, c)

	boom := errors.New("backend exploded")
	stream := streamForSeqs(t, first, first)
	stream.yieldErrAt = first + 1
	stream.errAt = boom
	cfg, _ := loopConfig(t, stream, db, cat)

	err := runIngestionLoop(context.Background(), cfg)
	require.Error(t, err)
	require.ErrorIs(t, err, boom)
}

// ---------------------------------------------------------------------------
// runIngestionLoop — restart resumes idempotently from the derived watermark.
// ---------------------------------------------------------------------------

// TestRunIngestionLoop_RestartResumesFromWatermark: after a first run commits a
// prefix and exits, a second run over a FRESH open of the SAME hot dir resumes at
// watermark+1 (asserted via the FIRST seq the stream is asked for) — the stream
// range starts at the derived resume, and the final watermark is exactly the last
// delivered seq.
func TestRunIngestionLoop_RestartResumesFromWatermark(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	first := c.FirstLedger()

	// First run: commit [first, first+2], then the stream errs.
	db1 := openLiveHotDB(t, cat, c)
	stream1 := streamForSeqs(t, first, first+2)
	stream1.endErr = errors.New("end")
	cfg1, _ := loopConfig(t, stream1, db1, cat)
	err := runIngestionLoop(context.Background(), cfg1)
	require.Error(t, err)
	assert.Equal(t, first, stream1.firstSeen.Load(), "first run resumed at the chunk's first ledger")

	// Restart: re-open the live DB the way startup would. The resume point must
	// be watermark+1.
	db2, err := openHotDBForChunk(cat, c, silentLogger())
	require.NoError(t, err)
	resume, err := nextIngestLedger(db2)
	require.NoError(t, err)
	assert.Equal(t, first+3, resume, "restart resumes one past the durable watermark")

	// Second run resumes at watermark+1 and commits two more ledgers.
	stream2 := streamForSeqs(t, first+3, first+5)
	stream2.endErr = errors.New("end")
	cfg2, _ := loopConfig(t, stream2, db2, cat)
	err = runIngestionLoop(context.Background(), cfg2)
	require.Error(t, err)
	assert.Equal(t, first+3, stream2.firstSeen.Load(), "second run resumed at watermark+1")

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
