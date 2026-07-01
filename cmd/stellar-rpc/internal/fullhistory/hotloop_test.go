package fullhistory

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/lifecycle"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// ledgerEntry builds a ledgers-CF entry carrying a real zero-tx LCM for seq —
// the bytes the cold pipeline can later re-read if the chunk freezes from the
// hot DB.
func ledgerEntry(t *testing.T, seq uint32) ledger.Entry {
	t.Helper()
	return ledger.Entry{Seq: seq, Bytes: zeroTxLCMBytes(t, seq)}
}

// ---------------------------------------------------------------------------
// fakeLedgerGetter — an injectable LedgerGetter the ingestion loop polls by
// sequence (the design's indexed core.GetLedger(ctx, seq)). For seqs it has a
// programmed frame it returns those bytes; once the poll runs past the last
// programmed seq it either blocks until ctx is cancelled (a live tip stream that
// only ends on shutdown) or returns endErr (a crashed backend). It records the
// FIRST seq it was asked for (the restart resume point) and the GetLedger call
// count.
// ---------------------------------------------------------------------------

type fakeLedgerGetter struct {
	frames     map[uint32][]byte // seq -> raw LCM bytes
	maxSeq     uint32            // highest programmed seq
	blockOnCtx bool              // past the last frame, block until ctx.Done
	endErr     error             // past the last frame, return this (when not blocking)
	yieldErrAt uint32            // if non-zero, return errAt at this seq instead of bytes
	errAt      error

	calls     atomic.Int32
	firstSeen atomic.Uint32
	sawFirst  atomic.Bool
}

var _ LedgerGetter = (*fakeLedgerGetter)(nil)

func (g *fakeLedgerGetter) GetLedger(ctx context.Context, seq uint32) (xdr.LedgerCloseMetaView, error) {
	g.calls.Add(1)
	if g.sawFirst.CompareAndSwap(false, true) {
		g.firstSeen.Store(seq)
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if g.yieldErrAt != 0 && seq == g.yieldErrAt {
		return nil, g.errAt
	}
	if raw, ok := g.frames[seq]; ok {
		return xdr.LedgerCloseMetaView(raw), nil
	}
	// Past the programmed frames.
	if g.blockOnCtx {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if g.endErr != nil {
		return nil, g.endErr
	}
	return nil, errors.New("fakeLedgerGetter: no frame for seq")
}

// getterForSeqs builds a fakeLedgerGetter with zero-tx LCM frames for [from,to].
func getterForSeqs(t *testing.T, from, to uint32) *fakeLedgerGetter {
	t.Helper()
	g := &fakeLedgerGetter{frames: map[uint32][]byte{}, maxSeq: to}
	for seq := from; seq <= to; seq++ {
		g.frames[seq] = zeroTxLCMBytes(t, seq)
	}
	return g
}

// openLiveHotDB opens (and brackets ready) the live hot DB for a chunk via the
// production opener, returning the handle and the catalog it lives under.
func openLiveHotDB(t *testing.T, cat *catalog.Catalog, c chunk.ID) *hotchunk.DB {
	t.Helper()
	db, err := openHotDBForChunk(cat, c, silentLogger())
	require.NoError(t, err)
	return db
}

// seedWatermark writes a single ledgers-CF entry at seq into the chunk's hot DB
// so the indexed poll resumes at seq+1 — letting a boundary test drive the loop
// over only the last ledger or two of a chunk instead of all 10,000. The
// returned DB is the (re-opened, ready) live handle the loop then owns. The
// zero-event fixtures keep the sparse ledgers-CF watermark valid for boundary
// tests without preloading all preceding event offsets.
func seedWatermark(t *testing.T, cat *catalog.Catalog, c chunk.ID, seq uint32) *hotchunk.DB {
	t.Helper()
	db := openLiveHotDB(t, cat, c)
	require.NoError(t, db.Ledgers().AddLedgers(ledgerEntry(t, seq)))
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

// TestOpenHotTier_ReadyButDirMissingIsCase4 is the case-4 fatal: a "ready" key
// whose dir is gone is hot-volume loss, never auto-healed.
func TestOpenHotTier_ReadyButDirMissingIsCase4(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(5)
	require.NoError(t, cat.PutHotTransient(c))
	require.NoError(t, cat.FlipHotReady(c)) // key says ready, but no dir created

	_, err := openHotDBForChunk(cat, c, silentLogger())
	require.Error(t, err)
	require.ErrorIs(t, err, backfill.ErrHotVolumeLost)
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
	// strict contiguity from FirstLedger), then the poll runs dry and errs.
	getter := getterForSeqs(t, first, first+2)
	getter.endErr = errors.New("backend crashed")
	ch := make(chan chunk.ID, lifecycle.LifecycleQueueDepth)

	err := runIngestionLoop(context.Background(), getter, db, cat, ch, silentLogger(), nil, nil)
	require.Error(t, err, "poll ran past the prefix and the getter errored")
	require.NotErrorIs(t, err, backfill.ErrHotVolumeLost)

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
	assert.Equal(t, uint32(0), reopened.Events().NextEventID(), "zero-tx ledgers carry no events")
}

// ---------------------------------------------------------------------------
// runIngestionLoop — boundary notifications carry the completed chunk id.
// ---------------------------------------------------------------------------

// TestRunIngestionLoop_BoundaryNotifiesCompletedChunk: crossing the chunk 0 -> 1
// boundary sends chunk 0 into the buffered lifecycle channel. The watermark is
// seeded just below the boundary so the poll crosses it in one step. The buffer
// is far above the at-most-one a healthy daemon holds, so it never blocks the
// loop.
func TestRunIngestionLoop_BoundaryNotifiesCompletedChunk(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	c1 := c + 1
	db := seedWatermark(t, cat, c, c.LastLedger()-1)

	getter := &fakeLedgerGetter{frames: map[uint32][]byte{
		c.LastLedger():   zeroTxLCMBytes(t, c.LastLedger()),   // boundary 0->1
		c1.FirstLedger(): zeroTxLCMBytes(t, c1.FirstLedger()), // a ledger in chunk 1
	}, endErr: errors.New("end")}
	ch := make(chan chunk.ID, lifecycle.LifecycleQueueDepth)

	done := make(chan error, 1)
	go func() {
		done <- runIngestionLoop(context.Background(), getter, db, cat, ch, silentLogger(), nil, nil)
	}()

	select {
	case err := <-done:
		require.Error(t, err, "poll ran dry")
	case <-time.After(10 * time.Second):
		t.Fatal("ingestion loop deadlocked")
	}

	sent := drainLifecycle(ch)
	assert.Equal(t, []chunk.ID{c}, sent, "the completed chunk id was sent at the boundary")
}

// ---------------------------------------------------------------------------
// runIngestionLoop — clean shutdown vs crash (classified at the daemon top
// level: ctx-cancelled return is clean, any other error is restartable).
// ---------------------------------------------------------------------------

// TestRunIngestionLoop_CtxCancelReturnsCtxErr: a ctx cancellation while the poll
// is blocking on the tip makes GetLedger return ctx.Err(); the loop returns that
// (the daemon top level classifies a ctx-cancelled return as a clean shutdown).
func TestRunIngestionLoop_CtxCancelReturnsCtxErr(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	first := c.FirstLedger()
	db := openLiveHotDB(t, cat, c)

	getter := getterForSeqs(t, first, first+1)
	getter.blockOnCtx = true // after the frames, behave like a live tip stream
	ch := make(chan chunk.ID, lifecycle.LifecycleQueueDepth)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- runIngestionLoop(ctx, getter, db, cat, ch, silentLogger(), nil, nil)
	}()

	require.Eventually(t, func() bool {
		return getter.calls.Load() >= 3 // ingested 2 frames, blocked on the 3rd
	}, 5*time.Second, 5*time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled, "the loop surfaces the ctx-cancelled GetLedger error")
	case <-time.After(10 * time.Second):
		t.Fatal("ingestion loop did not stop on ctx cancellation")
	}
}

// TestRunIngestionLoop_GetLedgerErrorReturnsError: a GetLedger error (not a
// shutdown) propagates as a restartable failure.
func TestRunIngestionLoop_GetLedgerErrorReturnsError(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	first := c.FirstLedger()
	db := openLiveHotDB(t, cat, c)

	boom := errors.New("backend exploded")
	getter := getterForSeqs(t, first, first)
	getter.yieldErrAt = first + 1
	getter.errAt = boom
	ch := make(chan chunk.ID, lifecycle.LifecycleQueueDepth)

	err := runIngestionLoop(context.Background(), getter, db, cat, ch, silentLogger(), nil, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, boom)
	require.NotErrorIs(t, err, backfill.ErrHotVolumeLost)
}

// ---------------------------------------------------------------------------
// runIngestionLoop — restart resumes idempotently from the derived watermark.
// ---------------------------------------------------------------------------

// TestRunIngestionLoop_RestartResumesFromWatermark: after a first run commits a
// prefix and exits, a second run over a FRESH open of the SAME hot dir resumes
// at watermark+1 (asserted via the FIRST seq the getter is asked for) and a
// re-delivered already-committed ledger is the idempotent retry the hot stores
// tolerate — the final watermark is exactly the last delivered seq.
func TestRunIngestionLoop_RestartResumesFromWatermark(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	first := c.FirstLedger()

	// First run: commit [first, first+2], then the getter errs.
	db1 := openLiveHotDB(t, cat, c)
	getter1 := getterForSeqs(t, first, first+2)
	getter1.endErr = errors.New("end")
	ch := make(chan chunk.ID, lifecycle.LifecycleQueueDepth)
	err := runIngestionLoop(context.Background(), getter1, db1, cat, ch, silentLogger(), nil, nil)
	require.Error(t, err)
	assert.Equal(t, first, getter1.firstSeen.Load(), "first run resumed at the chunk's first ledger")

	// Restart: re-open the live DB the way startup would. The resume point must
	// be watermark+1.
	db2, err := openHotDBForChunk(cat, c, silentLogger())
	require.NoError(t, err)
	resume, err := nextIngestLedger(db2)
	require.NoError(t, err)
	assert.Equal(t, first+3, resume, "restart resumes one past the durable watermark")

	// Second run re-delivers the last already-committed ledger (idempotent) plus
	// two new ones.
	getter2 := getterForSeqs(t, first+2, first+5)
	getter2.endErr = errors.New("end")
	err = runIngestionLoop(context.Background(), getter2, db2, cat, ch, silentLogger(), nil, nil)
	require.Error(t, err)
	assert.Equal(t, first+3, getter2.firstSeen.Load(), "second run resumed at watermark+1")

	reopened, err := hotchunk.Open(cat.Layout().HotChunkPath(c), c, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	maxSeq, ok, err := reopened.MaxCommittedSeq()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, first+5, maxSeq)
}
