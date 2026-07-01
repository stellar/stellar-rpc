package lifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ---------------------------------------------------------------------------
// Loop: selects on BOTH ctx.Done and the notification channel; drains
// to the most-recent queued chunk id.
// ---------------------------------------------------------------------------

// TestLifecycleLoop_RunsTickPerNotifyThenStopsOnCtx: a notification (a completed
// chunk id) runs a tick; a ctx cancellation returns the loop. The loop never
// blocks forever and never fatals on shutdown.
func TestLifecycleLoop_RunsTickPerNotifyThenStopsOnCtx(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 1)
	cfg, rec := lifecycleTestConfig(t, cat, 0)

	// Make the tick observable WITHOUT a slow full ingest: chunk 0 is already
	// fully frozen and folded into its (terminal, cpi=1) window, with a leftover
	// "ready" hot DB on disk. The plan stage is a no-op; the discard scan retires
	// chunk 0's hot DB. A live chunk 1 keeps chunk 0 below the partition.
	freezeKinds(t, cat, 0, geometry.KindLedgers, geometry.KindEvents, geometry.KindTxHash)
	freezeCoverage(t, cat, cat.TxHashIndexLayout().TxHashIndexID(0), 0, 0) // terminal coverage of chunk 0
	makeReadyHotDirNoData(t, cat, 0)
	live := openLiveHotDB(t, cat, 1)
	t.Cleanup(func() { _ = live.Close() })

	ch := make(chan chunk.ID, LifecycleQueueDepth)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		Loop(ctx, cfg, cat, ch)
		close(done)
	}()

	ch <- chunk.ID(0) // ingestion hands over the just-completed chunk 0
	require.Eventually(t, func() bool {
		has, err := hotKeyExists(cat, 0)
		return err == nil && !has
	}, 10*time.Second, 20*time.Millisecond, "the notification ran a tick that discarded chunk 0")
	require.False(t, rec.fired())

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not return on ctx cancellation")
	}
}

// TestLifecycleLoop_DrainsToMostRecent: several chunk ids queued behind one
// notification are coalesced into ONE tick over the most-recent. With chunks 0
// and 1 both frozen+covered and a live chunk 2, sending 0 then 1 runs a single
// tick up to chunk 1 that discards both.
func TestLifecycleLoop_DrainsToMostRecent(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 1)
	cfg, rec := lifecycleTestConfig(t, cat, 0)

	for c := chunk.ID(0); c <= 1; c++ {
		freezeKinds(t, cat, c, geometry.KindLedgers, geometry.KindEvents, geometry.KindTxHash)
		freezeCoverage(t, cat, cat.TxHashIndexLayout().TxHashIndexID(c), c, c)
		makeReadyHotDirNoData(t, cat, c)
	}
	live := openLiveHotDB(t, cat, 2)
	t.Cleanup(func() { _ = live.Close() })

	ch := make(chan chunk.ID, LifecycleQueueDepth)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		Loop(ctx, cfg, cat, ch)
		close(done)
	}()

	ch <- chunk.ID(0)
	ch <- chunk.ID(1) // drained-to: one tick over [floor, 1] discards both
	require.Eventually(t, func() bool {
		h0, e0 := hotKeyExists(cat, 0)
		h1, e1 := hotKeyExists(cat, 1)
		return e0 == nil && e1 == nil && !h0 && !h1
	}, 10*time.Second, 20*time.Millisecond, "one drained tick discarded both completed chunks")
	require.False(t, rec.fired())

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not return on ctx cancellation")
	}
}

// TestLifecycleLoop_ReturnsImmediatelyOnAlreadyCancelledCtx: an already-canceled
// ctx makes the loop return without running any tick (never blocks on the
// channel forever).
func TestLifecycleLoop_ReturnsImmediatelyOnAlreadyCancelledCtx(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 1)
	cfg, _ := lifecycleTestConfig(t, cat, 0)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ch := make(chan chunk.ID) // unbuffered, never sent to
	done := make(chan struct{})
	go func() {
		Loop(ctx, cfg, cat, ch)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop blocked instead of observing the canceled ctx")
	}
}
