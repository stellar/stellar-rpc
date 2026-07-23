package serving

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
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

func newTestRouter(t *testing.T, size uint32, earliest chunk.ID) (*Router, *catalog.Catalog) {
	t.Helper()
	cat := openTestCatalog(t, silentLogger())
	return NewRouter(cat, geometry.NewRetention(size, earliest)), cat
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
	r := NewRouter(cat, geometry.NewRetention(0, 0))
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

	// The router owns the bootstrapped handles; close them (no loop here).
	for _, c := range []chunk.ID{5, 6} {
		if db, ok := r.Handle(c); ok {
			_ = db.Close()
		}
	}
}

func TestSetWatermark(t *testing.T) {
	r, _ := newTestRouter(t, 0, 0)
	assert.Equal(t, uint32(0), r.Watermark())
	r.SetWatermark(42)
	assert.Equal(t, uint32(42), r.Watermark())
}

// TestAdmit_FloorDerivation pins that the admitted floor is Retention.FloorAt
// anchored on the highest ready hot chunk minus one.
func TestAdmit_FloorDerivation(t *testing.T) {
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
			r, cat := newTestRouter(t, tc.size, tc.earliest)
			for _, c := range tc.ready {
				require.NoError(t, cat.FlipHotReady(c))
			}
			a, err := r.Admit()
			require.NoError(t, err)
			defer a.Release()
			assert.Equal(t, tc.want, a.Floor())
		})
	}
}

// TestAdmit_FloorPinnedToSnapshot pins that the floor is fixed at the admission
// instant: a chunk opened after admission raises a later admission's floor but
// never the earlier one's.
func TestAdmit_FloorPinnedToSnapshot(t *testing.T) {
	r, cat := newTestRouter(t, 2, 0) // sliding window of 2 chunks
	require.NoError(t, cat.FlipHotReady(5))
	require.NoError(t, cat.FlipHotReady(6)) // live chunk 6, frontier 5

	a1, err := r.Admit()
	require.NoError(t, err)
	defer a1.Release()
	assert.Equal(t, chunk.ID(4), a1.Floor()) // 5-2+1

	require.NoError(t, cat.FlipHotReady(7)) // live chunk advances to 7, frontier 6

	a2, err := r.Admit()
	require.NoError(t, err)
	defer a2.Release()
	assert.Equal(t, chunk.ID(5), a2.Floor(), "a fresh admission sees the advanced frontier")
	assert.Equal(t, chunk.ID(4), a1.Floor(), "the earlier admission's floor is unchanged")
}

// TestAdmit_CapturesStateAtAdmissionInstant pins that all three loads are frozen
// at admission: watermark and handle set reflect the instant Admit ran, not later
// mutations.
func TestAdmit_CapturesStateAtAdmissionInstant(t *testing.T) {
	r, cat := newTestRouter(t, 0, 0)
	require.NoError(t, cat.FlipHotReady(5))
	require.NoError(t, cat.FlipHotReady(6))
	r.SetWatermark(65_000)
	r.PublishHandle(5, &hotchunk.DB{})

	a, err := r.Admit()
	require.NoError(t, err)
	defer a.Release()

	// Mutate every piece of serving state after admission.
	r.SetWatermark(70_000)
	require.NoError(t, cat.FlipHotReady(7))
	r.PublishHandle(7, &hotchunk.DB{})
	r.DiscardHandle(5)

	assert.Equal(t, uint32(65_000), a.Latest(), "watermark frozen at admission")

	_, has5 := a.handles.byChunk[5]
	_, has7 := a.handles.byChunk[7]
	assert.True(t, has5, "handle present at admission is retained")
	assert.False(t, has7, "handle published after admission is not visible")
}

// TestHotHandles_CopyOnWrite pins that publish/discard replace the map wholesale
// and never mutate a map already loaded by a query.
func TestHotHandles_CopyOnWrite(t *testing.T) {
	r, _ := newTestRouter(t, 0, 0)
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

// TestClose_ClosesAndClearsHandles pins that shutdown closes every published hot
// handle and empties the set. The handles are real on-disk DBs so Close flushes;
// a second Close (the idempotent live-chunk case) is a no-op.
func TestClose_ClosesAndClearsHandles(t *testing.T) {
	cat := openTestCatalog(t, silentLogger())
	r := NewRouter(cat, geometry.NewRetention(0, 0))
	for _, c := range []chunk.ID{5, 6} {
		makeReadyHotChunk(t, cat, c)
		db, err := hotchunk.OpenExisting(cat.Layout().HotChunkPath(c), c, silentLogger())
		require.NoError(t, err)
		r.PublishHandle(c, db)
	}

	r.Close()

	_, ok5 := r.Handle(5)
	_, ok6 := r.Handle(6)
	assert.False(t, ok5, "handles cleared on close")
	assert.False(t, ok6, "handles cleared on close")

	r.Close() // idempotent: closing an already-closed, emptied router is safe
}

// TestAdmit_ReleaseFreesSnapshot pins that Release returns the snapshot, so a
// clean admit/release cycle leaves no leak at catalog close.
func TestAdmit_ReleaseFreesSnapshot(t *testing.T) {
	var buf bytes.Buffer
	cat := openTestCatalog(t, newTestLogger(&buf))
	r := NewRouter(cat, geometry.NewRetention(0, 0))

	a, err := r.Admit()
	require.NoError(t, err)
	a.Release()

	require.NoError(t, cat.Close())
	assert.NotContains(t, buf.String(), "unreleased snapshot")
}

// TestAdmit_LeakedSnapshotWarnsAtClose pins the other direction: an admission
// never released is reported as a leak when the catalog closes.
func TestAdmit_LeakedSnapshotWarnsAtClose(t *testing.T) {
	var buf bytes.Buffer
	cat := openTestCatalog(t, newTestLogger(&buf))
	r := NewRouter(cat, geometry.NewRetention(0, 0))

	_, err := r.Admit()
	require.NoError(t, err) // deliberately not released

	require.NoError(t, cat.Close())
	assert.Contains(t, buf.String(), "unreleased snapshot")
}
