package serving

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
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
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
// leaving no open handle — the on-disk state PublishReadyHandles reopens.
func makeReadyHotChunk(t *testing.T, cat *catalog.Catalog, c chunk.ID) {
	t.Helper()
	db, err := hotchunk.Open(cat.Layout().HotChunkPath(c), c, silentLogger())
	require.NoError(t, err)
	require.NoError(t, db.Close())
	require.NoError(t, cat.FlipHotReady(c))
}

// TestPublishReadyHandles pins that startup publishes a handle for every ready hot
// chunk except the live one (which the ingestion loop publishes).
func TestPublishReadyHandles(t *testing.T) {
	cat := openTestCatalog(t, silentLogger())
	r := NewRegistry(cat, geometry.NewRetention(0, 0))
	for _, c := range []chunk.ID{5, 6, 7} {
		makeReadyHotChunk(t, cat, c)
	}

	require.NoError(t, r.PublishReadyHandles(7, silentLogger()))

	_, ok5 := r.Handle(5)
	_, ok6 := r.Handle(6)
	_, ok7 := r.Handle(7)
	assert.True(t, ok5, "completed ready chunk published")
	assert.True(t, ok6, "completed ready chunk published")
	assert.False(t, ok7, "live chunk skipped; the ingestion loop publishes it")

	// The registry owns the bootstrapped handles; close them (no loop here).
	for _, c := range []chunk.ID{5, 6} {
		if db, ok := r.Handle(c); ok {
			_ = db.Close()
		}
	}
}

func TestSetLatestLedger(t *testing.T) {
	r, _ := newTestRegistry(t, 0, 0)
	assert.Equal(t, uint32(0), r.LatestLedger())
	r.SetLatestLedger(42)
	assert.Equal(t, uint32(42), r.LatestLedger())
}

// TestAcquireReadView_FloorDerivation pins that the view floor is Retention.FloorAt
// anchored on the highest ready hot chunk minus one.
func TestAcquireReadView_FloorDerivation(t *testing.T) {
	tests := []struct {
		name     string
		size     uint32
		earliest chunk.ID
		ready    []chunk.ID
		want     chunk.ID
	}{
		{"full history ignores the frontier", 0, 3, []chunk.ID{5, 6}, 3},
		{"sliding window from the frontier", 3, 0, []chunk.ID{5, 6, 7}, 4}, // frontier 6, 6-3+1
		{"sliding clamped to earliest", 10, 2, []chunk.ID{5, 6, 7}, 2},     // 6-10+1 < 2
		{"no ready hot chunk", 3, 2, nil, 2},                               // frontier -1
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, cat := newTestRegistry(t, tc.size, tc.earliest)
			for _, c := range tc.ready {
				require.NoError(t, cat.FlipHotReady(c))
			}
			a, err := r.AcquireReadView()
			require.NoError(t, err)
			defer a.Release()
			assert.Equal(t, tc.want, a.FloorChunk())
		})
	}
}

// TestAcquireReadView_FloorPinnedToSnapshot pins that the floor is fixed at the acquisition
// instant: a chunk opened after acquisition raises a later view.s floor but
// never the earlier one's.
func TestAcquireReadView_FloorPinnedToSnapshot(t *testing.T) {
	r, cat := newTestRegistry(t, 2, 0) // sliding window of 2 chunks
	require.NoError(t, cat.FlipHotReady(5))
	require.NoError(t, cat.FlipHotReady(6)) // live chunk 6, frontier 5

	a1, err := r.AcquireReadView()
	require.NoError(t, err)
	defer a1.Release()
	assert.Equal(t, chunk.ID(4), a1.FloorChunk()) // 5-2+1

	require.NoError(t, cat.FlipHotReady(7)) // live chunk advances to 7, frontier 6

	a2, err := r.AcquireReadView()
	require.NoError(t, err)
	defer a2.Release()
	assert.Equal(t, chunk.ID(5), a2.FloorChunk(), "a fresh view sees the advanced frontier")
	assert.Equal(t, chunk.ID(4), a1.FloorChunk(), "the earlier view.s floor is unchanged")
}

// TestAcquireReadView_CapturesStateAtAcquisitionInstant pins that all three loads are frozen
// at acquisition: latest ledger and handle set reflect the instant AcquireReadView ran, not later
// mutations.
func TestAcquireReadView_CapturesStateAtAcquisitionInstant(t *testing.T) {
	r, cat := newTestRegistry(t, 0, 0)
	require.NoError(t, cat.FlipHotReady(5))
	require.NoError(t, cat.FlipHotReady(6))
	r.SetLatestLedger(65_000)
	r.PublishHandle(5, &hotchunk.DB{})

	a, err := r.AcquireReadView()
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

// TestDiscardThenCloseDiscarded pins the retire path: DiscardHandle unpublishes the
// handle into the closing set, CloseDiscarded closes the idle handle, and a repeat
// CloseDiscarded is a no-op (nothing left pending). This is the close-retry seam
// the deferred-deletion fix relies on — the handle survives in closing across the
// discard and the (later) close.
func TestDiscardThenCloseDiscarded(t *testing.T) {
	cat := openTestCatalog(t, silentLogger())
	r := NewRegistry(cat, geometry.NewRetention(0, 0))
	publishReadyHandle(t, r, cat, 5)

	r.DiscardHandle(5)
	_, ok := r.Handle(5)
	assert.False(t, ok, "discarded handle is unpublished")

	assert.True(t, r.CloseDiscarded(5), "idle discarded handle closes")
	assert.True(t, r.CloseDiscarded(5), "second call: nothing pending")

	// DiscardHandle for a chunk not published is a harmless no-op (the retry case).
	r.DiscardHandle(5)
	assert.True(t, r.CloseDiscarded(5), "no-op discard leaves nothing to close")
}

// TestCloseDiscarded_BusyRetainsThenRetryDrains pins the retry behavior the fix
// promises: while a reader is in flight, CloseDiscarded reports false and keeps the
// handle in the closing set; once the reader drains, a later call closes it and
// removes it. A parked ledger scan holds the store's lock to force the busy path.
func TestCloseDiscarded_BusyRetainsThenRetryDrains(t *testing.T) {
	cat := openTestCatalog(t, silentLogger())
	r := NewRegistry(cat, geometry.NewRetention(0, 0))
	const c chunk.ID = 5

	db, err := hotchunk.Open(cat.Layout().HotChunkPath(c), c, silentLogger())
	require.NoError(t, err)
	_, err = db.IngestLedger(c.FirstLedger(), rpcv2test.ZeroTxLCMBytes(t, c.FirstLedger()))
	require.NoError(t, err)
	r.PublishHandle(c, db)

	// Park a reader inside the ledger stream so the store's read-lock stays held,
	// which makes CloseIfIdle (under CloseDiscarded) report busy.
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
	require.False(t, r.CloseDiscarded(c), "reader in flight → close deferred")
	_, retained := r.closing[c]
	assert.True(t, retained, "the handle is retained in closing for a later retry")

	close(release)
	<-done

	require.True(t, r.CloseDiscarded(c), "after the reader drains, the retained handle closes")
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
	// The discarded-but-unclosed handle is drained too: a later CloseDiscarded finds
	// nothing pending.
	assert.True(t, r.CloseDiscarded(6), "closing set drained on close")

	r.Close() // idempotent
}

// TestAcquireReadView_ReleaseFreesSnapshot pins that Release returns the snapshot, so a
// clean acquire/release cycle leaves no leak at catalog close.
func TestAcquireReadView_ReleaseFreesSnapshot(t *testing.T) {
	var buf bytes.Buffer
	cat := openTestCatalog(t, newTestLogger(&buf))
	r := NewRegistry(cat, geometry.NewRetention(0, 0))

	a, err := r.AcquireReadView()
	require.NoError(t, err)
	a.Release()

	require.NoError(t, cat.Close())
	assert.NotContains(t, buf.String(), "unreleased snapshot")
}

// TestAcquireReadView_LeakedSnapshotWarnsAtClose pins the other direction: a read view
// never released is reported as a leak when the catalog closes.
func TestAcquireReadView_LeakedSnapshotWarnsAtClose(t *testing.T) {
	var buf bytes.Buffer
	cat := openTestCatalog(t, newTestLogger(&buf))
	r := NewRegistry(cat, geometry.NewRetention(0, 0))

	_, err := r.AcquireReadView()
	require.NoError(t, err) // deliberately not released

	require.NoError(t, cat.Close())
	assert.Contains(t, buf.String(), "unreleased snapshot")
}
