package lifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
)

// ---------------------------------------------------------------------------
// Loop: selects on BOTH ctx.Done and the boundary signal's wake; reads the
// most-recent published chunk id from the latest-cell.
// ---------------------------------------------------------------------------

// TestLifecycleLoop_RunsTickPerNotifyThenStopsOnCtx: a boundary signal (a completed
// chunk id) runs a tick; a ctx cancellation returns the loop. The loop never
// blocks forever and never fatals on shutdown.
func TestLifecycleLoop_RunsTickPerNotifyThenStopsOnCtx(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 1)
	cfg := lifecycleTestConfig(t, cat, 0)

	// Make the tick observable WITHOUT a slow full ingest: chunk 0 is already
	// fully frozen and folded into its (terminal, cpi=1) window, with a leftover
	// "ready" hot DB on disk. The plan stage is a no-op; the discard scan retires
	// chunk 0's hot DB. A live chunk 1 keeps chunk 0 below the partition.
	freezeKinds(t, cat, 0, geometry.KindLedgers, geometry.KindEvents, geometry.KindTxHash)
	freezeCoverage(t, cat, cat.TxHashIndexLayout().TxHashIndexID(0), 0, 0) // terminal coverage of chunk 0
	makeReadyHotDirNoData(t, cat, 0)
	live := openLiveHotDB(t, cat, 1)
	t.Cleanup(func() { _ = live.Close() })

	sig := NewBoundarySignal()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Loop(ctx, cfg, cat, sig) }()

	sig.Publish(chunk.ID(0)) // ingestion hands over the just-completed chunk 0
	require.Eventually(t, func() bool {
		has, err := hotKeyExists(cat, 0)
		return err == nil && !has
	}, 10*time.Second, 20*time.Millisecond, "the signal ran a tick that discarded chunk 0")

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err, "a ctx-canceled Loop is a clean return")
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not return on ctx cancellation")
	}
}

// TestLifecycleLoop_DrainsToMostRecent: the latest-cell coalesces rapid
// boundaries — publishing 0 then 1 lands a tick over the most-recent (chunk 1)
// that subsumes chunk 0. With chunks 0 and 1 both frozen+covered and a live chunk
// 2, both are discarded (whether that takes one coalesced tick or two).
func TestLifecycleLoop_DrainsToMostRecent(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 1)
	cfg := lifecycleTestConfig(t, cat, 0)

	for c := chunk.ID(0); c <= 1; c++ {
		freezeKinds(t, cat, c, geometry.KindLedgers, geometry.KindEvents, geometry.KindTxHash)
		freezeCoverage(t, cat, cat.TxHashIndexLayout().TxHashIndexID(c), c, c)
		makeReadyHotDirNoData(t, cat, c)
	}
	live := openLiveHotDB(t, cat, 2)
	t.Cleanup(func() { _ = live.Close() })

	sig := NewBoundarySignal()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Loop(ctx, cfg, cat, sig) }()

	sig.Publish(chunk.ID(0))
	sig.Publish(chunk.ID(1)) // latest-cell coalesces: a tick over [floor, 1] discards both
	require.Eventually(t, func() bool {
		h0, e0 := hotKeyExists(cat, 0)
		h1, e1 := hotKeyExists(cat, 1)
		return e0 == nil && e1 == nil && !h0 && !h1
	}, 10*time.Second, 20*time.Millisecond, "one drained tick discarded both completed chunks")

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err, "a ctx-canceled Loop is a clean return")
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not return on ctx cancellation")
	}
}

// TestLifecycleLoop_ReturnsImmediatelyOnAlreadyCancelledCtx: an already-canceled
// ctx makes the loop return without running any tick (never blocks on the
// channel forever).
func TestLifecycleLoop_ReturnsImmediatelyOnAlreadyCancelledCtx(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 1)
	cfg := lifecycleTestConfig(t, cat, 0)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	sig := NewBoundarySignal() // never published to
	done := make(chan error, 1)
	go func() { done <- Loop(ctx, cfg, cat, sig) }()
	select {
	case err := <-done:
		require.NoError(t, err, "an already-canceled ctx is a clean return")
	case <-time.After(5 * time.Second):
		t.Fatal("the loop blocked instead of observing the canceled ctx")
	}
}
