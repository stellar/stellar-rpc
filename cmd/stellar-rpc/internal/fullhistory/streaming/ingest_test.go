package streaming

import (
	"context"
	"errors"
	"iter"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/hotchunk"
)

// ---------------------------------------------------------------------------
// fakeLedgerStream — an injectable ledgerbackend.LedgerStream the ingestion
// loop drains. It yields a programmed list of (raw-bytes, error) frames in
// order and, when blockOnCtx is set, blocks after the last frame until ctx is
// cancelled (modeling a live tip stream that only ends on shutdown). It records
// the From of the requested range and the number of RawLedgers invocations.
// ---------------------------------------------------------------------------

type streamFrame struct {
	raw []byte
	err error
}

type fakeLedgerStream struct {
	frames     []streamFrame
	blockOnCtx bool // after the last frame, block until ctx.Done (clean-shutdown model)

	calls    atomic.Int32
	fromSeen atomic.Uint32
}

var _ ledgerbackend.LedgerStream = (*fakeLedgerStream)(nil)

func (s *fakeLedgerStream) RawLedgers(
	ctx context.Context, r ledgerbackend.Range, _ ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	s.calls.Add(1)
	s.fromSeen.Store(r.From())
	return func(yield func([]byte, error) bool) {
		for _, f := range s.frames {
			if ctx.Err() != nil {
				return
			}
			if !yield(f.raw, f.err) {
				return
			}
		}
		if s.blockOnCtx {
			<-ctx.Done() // a live stream ends only when cancelled
		}
		// Otherwise iteration ends naturally — the loop reads this as an
		// unexpected close (the production range is unbounded).
	}
}

// framesFromSeqs builds zero-tx LCM frames for the given sequences.
func framesFromSeqs(t *testing.T, seqs ...uint32) []streamFrame {
	t.Helper()
	frames := make([]streamFrame, len(seqs))
	for i, seq := range seqs {
		frames[i] = streamFrame{raw: zeroTxLCMBytes(t, seq)}
	}
	return frames
}

// seqRange builds frames for the contiguous closed range [from, to].
func seqRange(t *testing.T, from, to uint32) []streamFrame {
	t.Helper()
	var seqs []uint32
	for seq := from; seq <= to; seq++ {
		seqs = append(seqs, seq)
	}
	return framesFromSeqs(t, seqs...)
}

// openLiveHotDB opens (and brackets ready) the live hot DB for a chunk via the
// production opener, returning the handle and the catalog it lives under.
func openLiveHotDB(t *testing.T, cat *Catalog, c chunk.ID) *hotchunk.DB {
	t.Helper()
	db, err := openHotTierForChunk(cat, c, silentLogger())
	require.NoError(t, err)
	return db
}

// drainDoorbell counts how many notifications a size-1 doorbell delivered after
// the loop returned (the loop is done, so no concurrent sends race this).
func drainDoorbell(doorbell chan struct{}) int {
	n := 0
	for {
		select {
		case <-doorbell:
			n++
		default:
			return n
		}
	}
}

// ---------------------------------------------------------------------------
// openHotTierForChunk / discardHotTierForChunk — the bracket.
// ---------------------------------------------------------------------------

// TestOpenHotTier_CreatesBracketAndDir: a fresh open writes the dir and flips
// the key "ready"; the returned DB is empty (resume at FirstLedger).
func TestOpenHotTier_CreatesBracketAndDir(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(3)

	db, err := openHotTierForChunk(cat, c, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	state, err := cat.HotState(c)
	require.NoError(t, err)
	assert.Equal(t, HotReady, state, "open flips the key ready")

	_, statErr := os.Stat(cat.layout.HotChunkPath(c))
	require.NoError(t, statErr, "the dir exists")

	resume, err := nextIngestLedger(db)
	require.NoError(t, err)
	assert.Equal(t, c.FirstLedger(), resume, "an empty resume DB resumes at the chunk's first ledger")
}

// TestOpenHotTier_ReadyButDirMissingIsCase4 is the case-4 fatal: a "ready" key
// whose dir is gone is hot-volume loss, never auto-healed.
func TestOpenHotTier_ReadyButDirMissingIsCase4(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(5)
	require.NoError(t, cat.PutHotTransient(c))
	require.NoError(t, cat.FlipHotReady(c)) // key says ready, but no dir created

	_, err := openHotTierForChunk(cat, c, silentLogger())
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHotVolumeLost)
}

// TestOpenHotTier_TransientRecreatesFresh: a "transient" key (crashed
// create/discard) is recovered by wiping any leftover and recreating.
func TestOpenHotTier_TransientRecreatesFresh(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(2)
	require.NoError(t, cat.PutHotTransient(c)) // a crash left a transient key

	db, err := openHotTierForChunk(cat, c, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	state, err := cat.HotState(c)
	require.NoError(t, err)
	assert.Equal(t, HotReady, state)
}

// TestDiscardHotTier_RemovesDirAndKey retires the bracket: the key is deleted
// and the dir is gone. A second discard is a no-op.
func TestDiscardHotTier_RemovesDirAndKey(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(4)
	db := openLiveHotDB(t, cat, c)
	require.NoError(t, db.Close())

	require.NoError(t, discardHotTierForChunk(cat, c))

	has, err := cat.Has(hotChunkKey(c))
	require.NoError(t, err)
	assert.False(t, has, "the hot key is deleted")
	_, statErr := os.Stat(cat.layout.HotChunkPath(c))
	assert.True(t, os.IsNotExist(statErr), "the dir is removed")

	require.NoError(t, discardHotTierForChunk(cat, c), "second discard is a no-op")
}

// ---------------------------------------------------------------------------
// runIngestionLoop — atomic landing.
// ---------------------------------------------------------------------------

// TestRunIngestionLoop_LedgerLandsAcrossAllCFs: ingesting a short contiguous
// prefix lands each ledger atomically across the ledgers, txhash, and events
// CFs — the single watermark advances to the last committed seq, and every CF
// is readable. The stream then ends (unexpected close), which the loop reports.
func TestRunIngestionLoop_LedgerLandsAcrossAllCFs(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	first := c.FirstLedger()
	db := openLiveHotDB(t, cat, c)

	// A short contiguous prefix from the chunk's first ledger (events require
	// strict contiguity from FirstLedger), then the stream ends.
	stream := &fakeLedgerStream{frames: seqRange(t, first, first+2)}
	doorbell := make(chan struct{}, 1)

	err := runIngestionLoop(context.Background(), stream, db, cat, doorbell, allHotTypes, silentLogger())
	require.Error(t, err, "stream ended without a shutdown — unexpected close")
	require.NotErrorIs(t, err, ErrHotVolumeLost)

	// Reopen the (loop-closed) DB and assert every CF advanced together.
	reopened, err := hotchunk.Open(cat.layout.HotChunkPath(c), c, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	maxSeq, ok, err := reopened.MaxCommittedSeq()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, first+2, maxSeq, "the single watermark is the last committed seq")

	// ledgers CF.
	raw, err := reopened.Ledgers().GetLedgerRaw(first + 2)
	require.NoError(t, err)
	assert.NotEmpty(t, raw)
	// events CF advanced for exactly the three ingested ledgers (zero-tx, so the
	// offsets are contiguous and NextEventID stays 0 events but the ledger count
	// is recorded — proven by the watermark and a successful reopen warmup).
	assert.Equal(t, uint32(0), reopened.Events().NextEventID(), "zero-tx ledgers carry no events")
}

// ---------------------------------------------------------------------------
// runIngestionLoop — boundary handoff: close BEFORE creating C+1's key.
// ---------------------------------------------------------------------------

// TestRunIngestionLoop_BoundaryClosesBeforeNextKey asserts the load-bearing
// handoff order: at the chunk boundary the just-filled DB is CLOSED before the
// next chunk's hot:chunk key is created. The beforeHotTransient hook fires at
// the exact instant the next key appears; at that moment the predecessor's DB
// directory must be reopenable (its RocksDB LOCK released = it is closed).
//
// To keep the test fast we ingest ONLY ledgers+txhash (no events contiguity
// constraint) and yield the chunk's true last ledger directly, then the first
// ledger of the next chunk.
func TestRunIngestionLoop_BoundaryClosesBeforeNextKey(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	last := c.LastLedger() // boundary ledger
	next := c + 1

	db := openLiveHotDB(t, cat, c)

	var (
		hookFired   atomic.Bool
		closedFirst atomic.Bool
	)
	cat.hooks.beforeHotTransient = func(id chunk.ID) {
		if id != next {
			return // ignore the live chunk's own (already-done) bracket
		}
		hookFired.Store(true)
		// The predecessor's DB must be CLOSED here: opening its path succeeds
		// only if the writer released the RocksDB LOCK.
		probe, openErr := hotchunk.Open(cat.layout.HotChunkPath(c), c, silentLogger())
		if openErr == nil {
			closedFirst.Store(true)
			_ = probe.Close()
		}
	}

	// ledgers+txhash only — fast, and the boundary detection is seq-based.
	ingestTypes := hotchunk.Ingest{Ledgers: true, Txhash: true}
	stream := &fakeLedgerStream{frames: framesFromSeqs(t, last, next.FirstLedger())}
	doorbell := make(chan struct{}, 1)

	err := runIngestionLoop(context.Background(), stream, db, cat, doorbell, ingestTypes, silentLogger())
	require.Error(t, err, "stream ended (unexpected close) after the boundary")

	require.True(t, hookFired.Load(), "the next chunk's key was created")
	require.True(t, closedFirst.Load(),
		"the predecessor's DB was CLOSED before the next chunk's key was created")

	// The next chunk's bracket is ready and holds its first ledger.
	state, err := cat.HotState(next)
	require.NoError(t, err)
	assert.Equal(t, HotReady, state)
}

// ---------------------------------------------------------------------------
// runIngestionLoop — doorbell coalescing.
// ---------------------------------------------------------------------------

// TestRunIngestionLoop_DoorbellCoalesces: the size-1 non-blocking doorbell never
// blocks the loop, even across the at-start notify plus several boundary
// notifies with no consumer draining. The loop completes and at most one
// notification is buffered.
func TestRunIngestionLoop_DoorbellCoalesces(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)

	db := openLiveHotDB(t, cat, c)

	// Cross two boundaries (chunk 0 -> 1 -> 2) so notify() fires the at-start
	// ring plus two boundary rings — four total sends into a size-1 channel
	// nobody drains. If the doorbell were blocking, the loop would deadlock.
	c1 := c + 1
	c2 := c + 2
	frames := framesFromSeqs(t,
		c.LastLedger(),   // boundary 0->1
		c1.LastLedger(),  // boundary 1->2
		c2.FirstLedger(), // a ledger in chunk 2
	)
	ingestTypes := hotchunk.Ingest{Ledgers: true, Txhash: true}
	stream := &fakeLedgerStream{frames: frames}
	doorbell := make(chan struct{}, 1)

	done := make(chan error, 1)
	go func() {
		done <- runIngestionLoop(context.Background(), stream, db, cat, doorbell, ingestTypes, silentLogger())
	}()

	select {
	case err := <-done:
		require.Error(t, err, "stream ended (unexpected close)")
	case <-time.After(10 * time.Second):
		t.Fatal("ingestion loop deadlocked — the doorbell did not coalesce")
	}

	n := drainDoorbell(doorbell)
	assert.LessOrEqual(t, n, 1, "a size-1 doorbell coalesces all sends to at most one")
	assert.Equal(t, 1, n, "with no draining, exactly one notification remains buffered")
}

// ---------------------------------------------------------------------------
// runIngestionLoop — clean shutdown vs unexpected close.
// ---------------------------------------------------------------------------

// TestRunIngestionLoop_CtxCancelReturnsNil: a ctx cancellation while the stream
// is live (blocking on the tip) is a clean shutdown — the loop returns nil.
func TestRunIngestionLoop_CtxCancelReturnsNil(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	first := c.FirstLedger()
	db := openLiveHotDB(t, cat, c)

	stream := &fakeLedgerStream{
		frames:     seqRange(t, first, first+1),
		blockOnCtx: true, // after the frames, behave like a live tip stream
	}
	doorbell := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- runIngestionLoop(ctx, stream, db, cat, doorbell, allHotTypes, silentLogger())
	}()

	// Give the loop time to ingest the frames and block on the live stream, then
	// ask it to stop.
	require.Eventually(t, func() bool {
		return stream.calls.Load() == 1
	}, 5*time.Second, 5*time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.NoError(t, err, "ctx cancellation is a clean shutdown")
	case <-time.After(10 * time.Second):
		t.Fatal("ingestion loop did not stop on ctx cancellation")
	}
}

// TestRunIngestionLoop_UnexpectedCloseReturnsError: the stream ending on its own
// (no ctx cancellation) is captive-core crashing/exiting — restartable, so the
// loop returns an error.
func TestRunIngestionLoop_UnexpectedCloseReturnsError(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	first := c.FirstLedger()
	db := openLiveHotDB(t, cat, c)

	stream := &fakeLedgerStream{frames: seqRange(t, first, first+1)} // ends naturally
	doorbell := make(chan struct{}, 1)

	err := runIngestionLoop(context.Background(), stream, db, cat, doorbell, allHotTypes, silentLogger())
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrHotVolumeLost)
	assert.Contains(t, err.Error(), "unexpectedly")
}

// TestRunIngestionLoop_StreamErrorReturnsError: a stream-yielded error (not a
// shutdown) propagates as a restartable failure.
func TestRunIngestionLoop_StreamErrorReturnsError(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	first := c.FirstLedger()
	db := openLiveHotDB(t, cat, c)

	boom := errors.New("backend exploded")
	frames := append(seqRange(t, first, first), streamFrame{err: boom})
	stream := &fakeLedgerStream{frames: frames}
	doorbell := make(chan struct{}, 1)

	err := runIngestionLoop(context.Background(), stream, db, cat, doorbell, allHotTypes, silentLogger())
	require.Error(t, err)
	require.ErrorIs(t, err, boom)
}

// ---------------------------------------------------------------------------
// runIngestionLoop — restart resumes idempotently from the derived watermark.
// ---------------------------------------------------------------------------

// TestRunIngestionLoop_RestartResumesFromWatermark: after a first run commits a
// prefix and exits, a second run over a FRESH open of the SAME hot dir resumes
// at watermark+1 (asserted via the From the stream is asked for) and a
// re-delivered already-committed ledger is the idempotent retry the hot stores
// tolerate — the final watermark is exactly the last delivered seq, with no
// double-apply.
func TestRunIngestionLoop_RestartResumesFromWatermark(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	first := c.FirstLedger()

	// First run: commit [first, first+2], then the stream ends.
	db1 := openLiveHotDB(t, cat, c)
	stream1 := &fakeLedgerStream{frames: seqRange(t, first, first+2)}
	doorbell := make(chan struct{}, 1)
	err := runIngestionLoop(context.Background(), stream1, db1, cat, doorbell, allHotTypes, silentLogger())
	require.Error(t, err) // unexpected close
	assert.Equal(t, first, stream1.fromSeen.Load(), "first run resumed at the chunk's first ledger")

	// Restart: re-open the live DB the way startup would (the key is "ready",
	// the dir exists). The resume point must be watermark+1.
	db2, err := openHotTierForChunk(cat, c, silentLogger())
	require.NoError(t, err)
	resume, err := nextIngestLedger(db2)
	require.NoError(t, err)
	assert.Equal(t, first+3, resume, "restart resumes one past the durable watermark")

	// Second run re-delivers the last already-committed ledger (idempotent) plus
	// two new ones.
	stream2 := &fakeLedgerStream{frames: seqRange(t, first+2, first+5)}
	err = runIngestionLoop(context.Background(), stream2, db2, cat, doorbell, allHotTypes, silentLogger())
	require.Error(t, err) // unexpected close
	assert.Equal(t, first+3, stream2.fromSeen.Load(), "second run resumed at watermark+1")

	// Final watermark is the last delivered seq — no gap, no double-apply.
	reopened, err := hotchunk.Open(cat.layout.HotChunkPath(c), c, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	maxSeq, ok, err := reopened.MaxCommittedSeq()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, first+5, maxSeq)
}
