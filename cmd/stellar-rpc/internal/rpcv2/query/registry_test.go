package query

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

func newTestLogger(buf *bytes.Buffer) *supportlog.Entry {
	log := supportlog.New()
	log.SetLevel(logrus.DebugLevel)
	log.SetOutput(buf)
	return log
}

func silentLogger() *supportlog.Entry {
	var buf bytes.Buffer
	return newTestLogger(&buf)
}

func openTestCatalog(t *testing.T, logger *supportlog.Entry) *catalog.Catalog {
	t.Helper()
	idxLayout, err := geometry.NewTxHashIndexLayout(geometry.ChunksPerTxhashIndex)
	require.NoError(t, err)
	cat, err := catalog.Open(
		filepath.Join(t.TempDir(), "rocksdb"), geometry.NewLayout(t.TempDir()), idxLayout, logger,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })
	return cat
}

func newTestRegistry(t *testing.T, size uint32, earliest chunk.ID) (*Registry, *catalog.Catalog) {
	t.Helper()
	cat := openTestCatalog(t, silentLogger())
	return NewRegistry(cat, geometry.NewRetention(size, earliest)), cat
}

// makeReadyHotChunk creates a real hot DB dir for chunk c and marks its key ready,
// leaving no open handle — the on-disk state OpenRegistry reopens.
func makeReadyHotChunk(t *testing.T, cat *catalog.Catalog, c chunk.ID) {
	t.Helper()
	db, err := hotchunk.Open(cat.Layout().HotChunkPath(c), c, silentLogger())
	require.NoError(t, err)
	require.NoError(t, db.Close())
	require.NoError(t, cat.FlipHotReady(c))
}

// TestOpenRegistry pins that the constructor returns a serving-ready registry:
// every completed ready chunk's handle reopened and published, the caller's live
// handle published under its own chunk (not a second open), and the latest
// ledger seeded — no half-initialized state is observable.
func TestOpenRegistry(t *testing.T) {
	cat := openTestCatalog(t, silentLogger())
	for _, c := range []chunk.ID{5, 6, 7} {
		makeReadyHotChunk(t, cat, c)
	}
	live, err := hotchunk.OpenExisting(cat.Layout().HotChunkPath(7), 7, silentLogger())
	require.NoError(t, err)

	r, err := OpenRegistry(cat, geometry.NewRetention(0, 0), live, 70_500)
	require.NoError(t, err)
	defer r.Close()

	_, ok5 := r.Handle(5)
	_, ok6 := r.Handle(6)
	got7, ok7 := r.Handle(7)
	assert.True(t, ok5, "completed ready chunk published")
	assert.True(t, ok6, "completed ready chunk published")
	require.True(t, ok7, "live chunk published")
	assert.Same(t, live, got7, "the live handle is the caller's, not a second open")
	assert.Equal(t, uint32(70_500), r.LatestLedger(), "latest ledger seeded")
}

func TestSetLatestLedger(t *testing.T) {
	r, _ := newTestRegistry(t, 0, 0)
	assert.Equal(t, uint32(0), r.LatestLedger())
	r.SetLatestLedger(42)
	assert.Equal(t, uint32(42), r.LatestLedger())
}

// TestNewReadView_FloorDerivation pins that the view floor is Retention.FloorAt
// anchored on the highest ready hot chunk minus one.
func TestNewReadView_FloorDerivation(t *testing.T) {
	tests := []struct {
		name     string
		size     uint32
		earliest chunk.ID
		ready    []chunk.ID
		want     chunk.ID
	}{
		{"full history ignores the frontier", 0, 3, []chunk.ID{5, 6}, 3},
		{"sliding window from the frontier", 3, 0, []chunk.ID{5, 6, 7}, 4},       // frontier 6, 6-3+1
		{"sliding clamped to earliest", 10, 2, []chunk.ID{5, 6, 7}, 2},           // 6-10+1 < 2
		{"young store: chunk 0 ready, nothing complete", 3, 2, []chunk.ID{0}, 2}, // anchor -1
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, cat := newTestRegistry(t, tc.size, tc.earliest)
			for _, c := range tc.ready {
				require.NoError(t, cat.FlipHotReady(c))
			}
			a, err := r.NewReadView()
			require.NoError(t, err)
			defer a.Release()
			assert.Equal(t, tc.want, a.FloorChunk())
		})
	}
}

// TestNewReadView_FloorPinnedToSnapshot pins that the floor is fixed at the acquisition
// instant: a chunk opened after acquisition raises a later view's floor but
// never the earlier one's.
func TestNewReadView_FloorPinnedToSnapshot(t *testing.T) {
	r, cat := newTestRegistry(t, 2, 0) // sliding window of 2 chunks
	require.NoError(t, cat.FlipHotReady(5))
	require.NoError(t, cat.FlipHotReady(6)) // live chunk 6, frontier 5

	a1, err := r.NewReadView()
	require.NoError(t, err)
	defer a1.Release()
	assert.Equal(t, chunk.ID(4), a1.FloorChunk()) // 5-2+1

	require.NoError(t, cat.FlipHotReady(7)) // live chunk advances to 7, frontier 6

	a2, err := r.NewReadView()
	require.NoError(t, err)
	defer a2.Release()
	assert.Equal(t, chunk.ID(5), a2.FloorChunk(), "a fresh view sees the advanced frontier")
	assert.Equal(t, chunk.ID(4), a1.FloorChunk(), "the earlier view's floor is unchanged")
}

// TestNewReadView_CapturesStateAtAcquisitionInstant pins that all three loads are frozen
// at acquisition: latest ledger and handle set reflect the instant NewReadView ran, not later
// mutations.
func TestNewReadView_CapturesStateAtAcquisitionInstant(t *testing.T) {
	r, cat := newTestRegistry(t, 0, 0)
	require.NoError(t, cat.FlipHotReady(5))
	require.NoError(t, cat.FlipHotReady(6))
	r.SetLatestLedger(65_000)
	r.PublishHandle(5, &hotchunk.DB{})

	a, err := r.NewReadView()
	require.NoError(t, err)
	defer a.Release()

	// Mutate every piece of serving state after acquisition.
	r.SetLatestLedger(70_000)
	require.NoError(t, cat.FlipHotReady(7))
	r.PublishHandle(7, &hotchunk.DB{})
	r.DiscardHandle(5)

	assert.Equal(t, uint32(65_000), a.LatestLedger(), "latest ledger frozen at acquisition")

	_, has5 := a.handles.byChunk[5]
	_, has7 := a.handles.byChunk[7]
	assert.True(t, has5, "handle present at acquisition is retained")
	assert.False(t, has7, "handle published after acquisition is not visible")
}

// TestHotHandles_CopyOnWrite pins that publish/discard replace the map wholesale
// and never mutate a map already loaded by a query.
func TestHotHandles_CopyOnWrite(t *testing.T) {
	r, _ := newTestRegistry(t, 0, 0)
	r.PublishHandle(5, &hotchunk.DB{})

	loaded := r.handles.Load()

	r.PublishHandle(6, &hotchunk.DB{})
	_, has6 := loaded.byChunk[6]
	assert.False(t, has6, "publish must not mutate a previously loaded map")

	r.DiscardHandle(5)
	_, has5 := loaded.byChunk[5]
	assert.True(t, has5, "discard must not mutate a previously loaded map")

	// The live map reflects both mutations.
	live := r.handles.Load()
	_, live5 := live.byChunk[5]
	_, live6 := live.byChunk[6]
	assert.False(t, live5)
	assert.True(t, live6)
}

// publishReadyHandle makes a ready on-disk chunk and publishes an open handle to it.
func publishReadyHandle(t *testing.T, r *Registry, cat *catalog.Catalog, c chunk.ID) {
	t.Helper()
	makeReadyHotChunk(t, cat, c)
	db, err := hotchunk.OpenExisting(cat.Layout().HotChunkPath(c), c, silentLogger())
	require.NoError(t, err)
	r.PublishHandle(c, db)
}

// TestDiscardThenTryCloseHandle pins the retire path: DiscardHandle unpublishes the
// handle into the closing set, TryCloseHandle closes the idle handle, and a repeat
// TryCloseHandle is a no-op (nothing left pending). This is the close-retry seam
// the deferred-deletion fix relies on — the handle survives in closing across the
// discard and the (later) close.
func TestDiscardThenTryCloseHandle(t *testing.T) {
	cat := openTestCatalog(t, silentLogger())
	r := NewRegistry(cat, geometry.NewRetention(0, 0))
	publishReadyHandle(t, r, cat, 5)

	r.DiscardHandle(5)
	_, ok := r.Handle(5)
	assert.False(t, ok, "discarded handle is unpublished")

	assert.True(t, r.TryCloseHandle(5), "idle discarded handle closes")
	assert.True(t, r.TryCloseHandle(5), "second call: nothing pending")

	// DiscardHandle for a chunk not published is a harmless no-op (the retry case).
	r.DiscardHandle(5)
	assert.True(t, r.TryCloseHandle(5), "no-op discard leaves nothing to close")
}

// TestTryCloseHandle_BusyRetainsThenRetryDrains pins the retry behavior the fix
// promises: while a reader is in flight, TryCloseHandle reports false and keeps the
// handle in the closing set; once the reader drains, a later call closes it and
// removes it. A parked ledger scan holds the store's lock to force the busy path.
func TestTryCloseHandle_BusyRetainsThenRetryDrains(t *testing.T) {
	cat := openTestCatalog(t, silentLogger())
	r := NewRegistry(cat, geometry.NewRetention(0, 0))
	const c chunk.ID = 5

	db, err := hotchunk.Open(cat.Layout().HotChunkPath(c), c, silentLogger())
	require.NoError(t, err)
	rpcv2test.IngestLedger(t, db, c.FirstLedger(), rpcv2test.ZeroTxLCMBytes(t, c.FirstLedger()))
	r.PublishHandle(c, db)

	// Park a reader inside the ledger stream so the store's read-lock stays held,
	// which makes CloseIfIdle (under TryCloseHandle) report busy.
	parked, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		first := true
		for _, ierr := range db.Source().RawLedgers(
			context.Background(), ledgerbackend.BoundedRange(c.FirstLedger(), c.FirstLedger()),
		) {
			if ierr != nil {
				return
			}
			if first {
				close(parked)
				<-release
				first = false
			}
		}
	}()
	<-parked

	r.DiscardHandle(c)
	require.False(t, r.TryCloseHandle(c), "reader in flight → close deferred")
	_, retained := r.closing[c]
	assert.True(t, retained, "the handle is retained in closing for a later retry")

	close(release)
	<-done

	require.True(t, r.TryCloseHandle(c), "after the reader drains, the retained handle closes")
	_, stillThere := r.closing[c]
	assert.False(t, stillThere, "closing is drained once the handle closes")
}

// TestClose_ClosesAndClearsHandles pins that shutdown closes every hot handle —
// both published and awaiting-close — and empties both sets. A second Close is a
// no-op (idempotent handle Close, e.g. the live chunk the ingestion loop closes).
func TestClose_ClosesAndClearsHandles(t *testing.T) {
	cat := openTestCatalog(t, silentLogger())
	r := NewRegistry(cat, geometry.NewRetention(0, 0))
	publishReadyHandle(t, r, cat, 5)
	publishReadyHandle(t, r, cat, 6)

	// Discard 6 so it sits in the closing set (unpublished, not yet closed).
	r.DiscardHandle(6)

	r.Close()

	_, ok5 := r.Handle(5)
	assert.False(t, ok5, "published handle cleared on close")
	// The discarded-but-unclosed handle is drained too: a later TryCloseHandle finds
	// nothing pending.
	assert.True(t, r.TryCloseHandle(6), "closing set drained on close")

	r.Close() // idempotent
}

// TestNewReadView_ReleaseFreesSnapshot pins that Release returns the snapshot, so a
// clean acquire/release cycle leaves no leak at catalog close.
func TestNewReadView_ReleaseFreesSnapshot(t *testing.T) {
	var buf bytes.Buffer
	cat := openTestCatalog(t, newTestLogger(&buf))
	r := NewRegistry(cat, geometry.NewRetention(0, 0))
	require.NoError(t, cat.FlipHotReady(0)) // a working daemon always has a ready live chunk

	a, err := r.NewReadView()
	require.NoError(t, err)
	a.Release()

	require.NoError(t, cat.Close())
	assert.NotContains(t, buf.String(), "unreleased snapshot")
}

// TestNewReadView_LeakedSnapshotWarnsAtClose pins the other direction: a read view
// never released is reported as a leak when the catalog closes.
func TestNewReadView_LeakedSnapshotWarnsAtClose(t *testing.T) {
	var buf bytes.Buffer
	cat := openTestCatalog(t, newTestLogger(&buf))
	r := NewRegistry(cat, geometry.NewRetention(0, 0))
	require.NoError(t, cat.FlipHotReady(0))

	_, err := r.NewReadView()
	require.NoError(t, err) // deliberately not released

	require.NoError(t, cat.Close())
	assert.Contains(t, buf.String(), "unreleased snapshot")
}

// TestNewReadView_NoReadyChunkErrors pins that an EMPTY ready scan fails the
// acquisition: it cannot happen in a working daemon (the live chunk's key exists
// before serving starts and is never demoted), so it marks a broken catalog, and
// erroring beats deriving the widest possible floor from broken state.
func TestNewReadView_NoReadyChunkErrors(t *testing.T) {
	r, _ := newTestRegistry(t, 3, 2) // no ready keys at all
	_, err := r.NewReadView()
	require.ErrorIs(t, err, catalog.ErrNoReadyHotChunk)
}

// TestNewReadView_LoadOrderPinned pins the three-load order the design's skew
// argument depends on: the latest ledger and the handle set are loaded BEFORE
// the catalog snapshot. The hook mutates both from inside the snapshot call —
// with the correct order the view holds the pre-hook values; with the loads
// swapped it would observe the mutations and this test fails.
func TestNewReadView_LoadOrderPinned(t *testing.T) {
	r, cat := newTestRegistry(t, 0, 0)
	require.NoError(t, cat.FlipHotReady(5))
	r.SetLatestLedger(100)

	inner := r.newSnapshot
	r.newSnapshot = func() (*catalog.Snapshot, error) {
		r.SetLatestLedger(200)             // lands after the latest-ledger load
		r.PublishHandle(6, &hotchunk.DB{}) // lands after the handle-set load
		return inner()
	}

	a, err := r.NewReadView()
	require.NoError(t, err)
	defer a.Release()

	assert.Equal(t, uint32(100), a.LatestLedger(), "latest ledger loaded before the snapshot")
	_, has6 := a.handles.byChunk[6]
	assert.False(t, has6, "handle set loaded before the snapshot")
}

// TestNewReadView_LatestBeforeHandles pins the half of the load order the
// snapshot seam cannot see: the latest ledger is loaded BEFORE the handle set.
// Swapped, a boundary between the loads leaves the view's latestLedger pointing
// into a chunk its handle set predates — resolveTier would return ErrUnavailable
// for an in-range tip ledger. The hook advances the latest ledger from inside the
// handle-set load; with the correct order the view holds the pre-hook value.
func TestNewReadView_LatestBeforeHandles(t *testing.T) {
	r, cat := newTestRegistry(t, 0, 0)
	require.NoError(t, cat.FlipHotReady(5))
	r.SetLatestLedger(100)

	inner := r.loadHandles
	r.loadHandles = func() *handleSet {
		r.SetLatestLedger(200) // lands after the latest-ledger load
		return inner()
	}

	a, err := r.NewReadView()
	require.NoError(t, err)
	defer a.Release()

	assert.Equal(t, uint32(100), a.LatestLedger(), "latest ledger loaded before the handle set")
}

// TestOpenRegistry_ErrorClosesOpenedHandles pins the constructor's error path: a
// ready chunk that will not open fails the call with the bootstrap wrap, every
// handle opened before the failure is closed (a fresh read-write open would
// otherwise be blocked by the leaked LOCK), and the caller's live handle — never
// published before the failure — stays the caller's and stays usable.
func TestOpenRegistry_ErrorClosesOpenedHandles(t *testing.T) {
	cat := openTestCatalog(t, silentLogger())
	makeReadyHotChunk(t, cat, 5)            // opens fine
	require.NoError(t, cat.FlipHotReady(6)) // ready key with NO dir: the open fails
	live, err := hotchunk.Open(cat.Layout().HotChunkPath(9), 9, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = live.Close() })

	_, err = OpenRegistry(cat, geometry.NewRetention(0, 0), live, 100)
	require.Error(t, err)
	require.ErrorContains(t, err, "bootstrap: open hot chunk")

	db5, err := hotchunk.OpenExisting(cat.Layout().HotChunkPath(5), 5, silentLogger())
	require.NoError(t, err, "chunk 5's LOCK is free: the error path closed the handle it opened")
	_ = db5.Close()

	_, _, err = live.MaxCommittedSeq()
	require.NoError(t, err, "the live handle was not closed by the failed constructor")
}
