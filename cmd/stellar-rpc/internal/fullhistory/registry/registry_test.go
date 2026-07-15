package registry

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/eventstore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/ledger"
)

func TestAdmit_InitialState(t *testing.T) {
	reg := newTestRegistry(t, Options{})
	snap := reg.Admit()
	require.Zero(t, snap.Latest)
	require.NotNil(t, snap.View)
	require.Equal(t, chunk.ID(0), snap.View.Floor())
	_, err := reg.LedgerReaderFor(snap.View, 0)
	require.ErrorIs(t, err, ErrUnavailable)
}

// TestAdmit_LatestThenViewUnderConcurrentPublish pins the admission order:
// because the writer publishes a chunk's serving home BEFORE advancing latest
// into it, every admitted snapshot must be able to resolve the chunk holding
// its own latest. Loading the View before latest would (under this hammer)
// eventually surface a latest the snapshot's View cannot serve.
func TestAdmit_LatestThenViewUnderConcurrentPublish(t *testing.T) {
	reg := newTestRegistry(t, Options{})

	const chunks = 300
	done := make(chan struct{})
	go func() {
		defer close(done)
		for c := range chunk.ID(chunks) {
			reg.PublishFrozen(c, geometry.KindLedgers)
			reg.AdvanceLatest(c.LastLedger())
		}
	}()

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				snap := reg.Admit()
				if snap.Latest != 0 {
					c := chunk.IDFromLedger(snap.Latest)
					if _, err := snap.View.resolve(c, geometry.KindLedgers); err != nil {
						t.Errorf("admitted latest %d but chunk %s has no serving home: %v", snap.Latest, c, err)
						return
					}
				}
				select {
				case <-done:
					return
				default:
				}
			}
		}()
	}
	wg.Wait()

	final := reg.Admit()
	require.Equal(t, chunk.ID(chunks-1).LastLedger(), final.Latest)
}

func TestPublish_CloneIsolation(t *testing.T) {
	reg := newTestRegistry(t, Options{})
	reg.PublishFrozen(5, geometry.KindLedgers)

	before := reg.Admit()
	reg.PublishFrozen(9, geometry.KindLedgers, geometry.KindEvents)
	after := reg.Admit()

	require.NotSame(t, before.View, after.View)

	_, err := before.View.resolve(9, geometry.KindLedgers)
	require.ErrorIs(t, err, ErrUnavailable, "the pre-publish snapshot must not see the later publish")
	_, err = after.View.resolve(9, geometry.KindLedgers)
	require.NoError(t, err)
	_, err = after.View.resolve(9, geometry.KindEvents)
	require.NoError(t, err)
	_, err = after.View.resolve(5, geometry.KindLedgers)
	require.NoError(t, err)
}

func TestResolve_ColdWinsOverHot(t *testing.T) {
	reg := newTestRegistry(t, Options{})
	const c = chunk.ID(2)
	db := openHotDB(t, reg.layout, c)
	reg.PublishHot(c, db)

	hotOnly := reg.Admit().View
	h, err := reg.LedgerReaderFor(hotOnly, c)
	require.NoError(t, err)
	require.IsType(t, &ledger.HotStore{}, h, "no cold artifact: the hot handle serves")
	er, err := reg.EventReaderFor(hotOnly, c)
	require.NoError(t, err)
	require.IsType(t, &eventstore.HotStore{}, er)

	reg.PublishFrozen(c, geometry.KindLedgers)
	ledgersCold := reg.Admit().View
	h, err = reg.LedgerReaderFor(ledgersCold, c)
	require.NoError(t, err)
	require.IsType(t, &ledger.ColdReader{}, h, "both tiers exist: cold wins deterministically")
	er, err = reg.EventReaderFor(ledgersCold, c)
	require.NoError(t, err)
	require.IsType(t, &eventstore.HotStore{}, er, "events not frozen yet: still the hot facade")

	reg.PublishFrozen(c, geometry.KindEvents)
	bothCold := reg.Admit().View
	er, err = reg.EventReaderFor(bothCold, c)
	require.NoError(t, err)
	require.IsType(t, &eventstore.ColdReader{}, er)

	// The older snapshot keeps resolving hot — Views are immutable.
	h, err = reg.LedgerReaderFor(hotOnly, c)
	require.NoError(t, err)
	require.IsType(t, &ledger.HotStore{}, h)

	_, err = reg.LedgerReaderFor(bothCold, c+1)
	require.ErrorIs(t, err, ErrUnavailable)
}

func TestPublishFrozen_TxHashKindIsNeverChunkServed(t *testing.T) {
	reg := newTestRegistry(t, Options{})
	reg.PublishFrozen(4, geometry.KindTxHash)
	view := reg.Admit().View
	for _, k := range geometry.AllKinds() {
		_, err := view.resolve(4, k)
		require.ErrorIs(t, err, ErrUnavailable, "kind %s", k)
	}
}

func TestPublishHot_NilHandleIgnored(t *testing.T) {
	reg := newTestRegistry(t, Options{})
	reg.PublishHot(3, nil)
	_, err := reg.LedgerReaderFor(reg.Admit().View, 3)
	require.ErrorIs(t, err, ErrUnavailable)
}

func TestSwapTxIndex_ReplacesAndRetiresPredecessor(t *testing.T) {
	reg := newTestRegistry(t, Options{Grace: 30 * time.Millisecond})

	covV1 := frozenCoverage(0, 0, 0)
	buildIdxFile(t, reg.layout, covV1)
	require.NoError(t, reg.SwapTxIndex(covV1))
	v1 := reg.Admit().View.Indexes()
	require.Len(t, v1, 1)
	require.Equal(t, chunk.ID(0), v1[0].Hi)
	oldReader := v1[0].Idx

	covV2 := frozenCoverage(0, 0, 1)
	buildIdxFile(t, reg.layout, covV2)
	require.NoError(t, reg.SwapTxIndex(covV2))
	v2 := reg.Admit().View.Indexes()
	require.Len(t, v2, 1, "the same window swaps in place")
	require.Equal(t, chunk.ID(1), v2[0].Hi)
	require.NotSame(t, oldReader, v2[0].Idx)

	require.Eventually(t, func() bool {
		_, err := oldReader.Get([32]byte{})
		return errors.Is(err, stores.ErrStoreClosed)
	}, 10*time.Second, 5*time.Millisecond, "the superseded reader closes after the grace period")

	// A second window appends and sorts.
	covW1 := frozenCoverage(1, 1000, 1001)
	buildIdxFile(t, reg.layout, covW1)
	require.NoError(t, reg.SwapTxIndex(covW1))
	both := reg.Admit().View.Indexes()
	require.Len(t, both, 2)
	require.Equal(t, geometry.TxHashIndexID(0), both[0].Window)
	require.Equal(t, geometry.TxHashIndexID(1), both[1].Window)
}

func TestSwapTxIndex_RefusesTransientCoverage(t *testing.T) {
	reg := newTestRegistry(t, Options{})
	cov := frozenCoverage(0, 0, 0)
	cov.State = geometry.StateFreezing
	require.Error(t, reg.SwapTxIndex(cov))
	require.Empty(t, reg.Admit().View.Indexes(), "a refused swap leaves the view unchanged")
}

func TestUnpublishHot_ClosesHandleThenDestroysAfterGrace(t *testing.T) {
	const grace = 300 * time.Millisecond
	reg := newTestRegistry(t, Options{Grace: grace})
	const c = chunk.ID(1)
	db := openHotDB(t, reg.layout, c)
	reg.PublishHot(c, db)
	before := reg.Admit().View

	start := time.Now()
	var destroyedAt atomic.Int64
	var handleClosedFirst atomic.Bool
	reg.UnpublishHot(c, func() error {
		_, _, err := db.Ledgers().LastSeq()
		handleClosedFirst.Store(errors.Is(err, stores.ErrStoreClosed))
		destroyedAt.Store(int64(time.Since(start)))
		return nil
	})

	_, err := reg.LedgerReaderFor(reg.Admit().View, c)
	require.ErrorIs(t, err, ErrUnavailable, "unpublish removes the chunk from new admissions immediately")
	require.Zero(t, destroyedAt.Load(), "physical destruction must not run inline")
	_, _, err = db.Ledgers().LastSeq()
	require.NoError(t, err, "the handle stays open through the grace period for older views")
	_, err = reg.LedgerReaderFor(before, c)
	require.NoError(t, err, "a view admitted before the unpublish still resolves the chunk")

	require.Eventually(t, func() bool { return destroyedAt.Load() != 0 }, 10*time.Second, 5*time.Millisecond)
	require.GreaterOrEqual(t, time.Duration(destroyedAt.Load()), grace)
	require.True(t, handleClosedFirst.Load(), "the reaper closes the handle before running destroy")
}

func TestAdvanceFloor_DropsBelowFloorAndRetires(t *testing.T) {
	reg := newTestRegistry(t, Options{Grace: 30 * time.Millisecond})

	db0 := openHotDB(t, reg.layout, 0)
	db3 := openHotDB(t, reg.layout, 3)
	reg.PublishHot(0, db0)
	reg.PublishHot(3, db3)
	reg.PublishFrozen(0, geometry.KindLedgers, geometry.KindEvents)
	reg.PublishFrozen(1, geometry.KindLedgers)
	reg.PublishFrozen(3, geometry.KindLedgers)
	covW0 := frozenCoverage(0, 0, 2) // Hi below the new floor: dropped
	covW1 := frozenCoverage(1, 1000, 1001)
	buildIdxFile(t, reg.layout, covW0)
	buildIdxFile(t, reg.layout, covW1)
	require.NoError(t, reg.SwapTxIndex(covW0))
	require.NoError(t, reg.SwapTxIndex(covW1))

	old := reg.Admit()
	_, err := reg.LedgerReaderFor(old.View, 1) // populate the cold-reader cache below the future floor
	require.NoError(t, err)
	require.Equal(t, 1, reg.ledgerCache.len())
	w0Reader := old.View.Indexes()[0].Idx

	reg.AdvanceFloor(3)

	snap := reg.Admit()
	require.Equal(t, chunk.ID(3), snap.View.Floor())
	require.Equal(t, chunk.ID(3).FirstLedger(), snap.View.FloorLedger())
	_, ok := snap.View.HotDB(0)
	require.False(t, ok, "below-floor hot handle dropped")
	_, ok = snap.View.HotDB(3)
	require.True(t, ok)
	require.Equal(t, []chunk.ID{3}, snap.View.HotChunks())
	for _, c := range []chunk.ID{0, 1} {
		_, err := snap.View.resolve(c, geometry.KindLedgers)
		require.ErrorIs(t, err, ErrUnavailable, "below-floor cold flags dropped (chunk %s)", c)
	}
	_, err = snap.View.resolve(3, geometry.KindLedgers)
	require.NoError(t, err)
	idxs := snap.View.Indexes()
	require.Len(t, idxs, 1, "a coverage wholly below the floor is dropped")
	require.Equal(t, geometry.TxHashIndexID(1), idxs[0].Window)

	require.Zero(t, reg.ledgerCache.len(), "cached cold readers for dropped chunks are retired")

	// The pre-advance snapshot is untouched.
	require.Equal(t, chunk.ID(0), old.View.Floor())
	_, err = old.View.resolve(0, geometry.KindLedgers)
	require.NoError(t, err)

	// Retired resources close only after the grace period; kept ones stay open.
	require.Eventually(t, func() bool {
		_, _, err := db0.Ledgers().LastSeq()
		return errors.Is(err, stores.ErrStoreClosed)
	}, 10*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool {
		_, err := w0Reader.Get([32]byte{})
		return errors.Is(err, stores.ErrStoreClosed)
	}, 10*time.Second, 5*time.Millisecond)
	_, _, err = db3.Ledgers().LastSeq()
	require.NoError(t, err)

	// The floor never regresses.
	reg.AdvanceFloor(1)
	require.Equal(t, chunk.ID(3), reg.Admit().View.Floor())
}

func TestClose_ClosesEverythingWithoutGrace(t *testing.T) {
	reg := newTestRegistry(t, Options{Grace: time.Hour})

	const c = chunk.ID(0)
	db := openHotDB(t, reg.layout, c)
	reg.PublishHot(c, db)
	reg.PublishFrozen(1, geometry.KindLedgers)
	cov := frozenCoverage(0, 0, 1)
	buildIdxFile(t, reg.layout, cov)
	require.NoError(t, reg.SwapTxIndex(cov))
	idx := reg.Admit().View.Indexes()[0].Idx
	_, err := reg.LedgerReaderFor(reg.Admit().View, 1)
	require.NoError(t, err)
	require.Equal(t, 1, reg.ledgerCache.len())

	reg.Close()

	_, _, err = db.Ledgers().LastSeq()
	require.ErrorIs(t, err, stores.ErrStoreClosed, "Close bypasses the hour-long grace")
	_, err = idx.Get([32]byte{})
	require.ErrorIs(t, err, stores.ErrStoreClosed)
	require.Zero(t, reg.ledgerCache.len())

	post := reg.Admit()
	_, err = reg.LedgerReaderFor(post.View, c)
	require.ErrorIs(t, err, ErrUnavailable, "nothing is servable after Close")

	reg.Close() // idempotent
}
