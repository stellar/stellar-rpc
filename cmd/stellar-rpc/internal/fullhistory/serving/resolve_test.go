package serving

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/fhtest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
)

// admitFor sets up a router over a fresh catalog, runs setup against the catalog
// and router, then admits — returning the admission for resolveTier assertions.
func admitFor(t *testing.T, setup func(cat *catalog.Catalog, r *Router)) (*Admission, *catalog.Catalog) {
	t.Helper()
	cat := openTestCatalog(t, silentLogger())
	r := NewRouter(cat, geometry.NewRetention(0, 0))
	setup(cat, r)
	a, err := r.Admit()
	require.NoError(t, err)
	t.Cleanup(a.Release)
	return a, cat
}

// TestResolveTier_Matrix pins the single routing-decision site for both per-chunk
// kinds: frozen wins (even alongside hot), a ready hot chunk with a published
// handle serves hot, and everything else (freezing/pruning/transient, ready
// without a handle, or absent) is unroutable.
func TestResolveTier_Matrix(t *testing.T) {
	const c chunk.ID = 42
	for _, kind := range []geometry.Kind{geometry.KindLedgers, geometry.KindEvents} {
		t.Run(string(kind), func(t *testing.T) {
			t.Run("frozen artifact wins", func(t *testing.T) {
				a, _ := admitFor(t, func(cat *catalog.Catalog, _ *Router) {
					require.NoError(t, cat.FlipChunkFrozen(c, kind))
				})
				got, db, err := a.resolveTier(c, kind)
				require.NoError(t, err)
				assert.Equal(t, tierCold, got)
				assert.Nil(t, db)
			})

			t.Run("ready hot with handle serves hot", func(t *testing.T) {
				a, _ := admitFor(t, func(cat *catalog.Catalog, r *Router) {
					require.NoError(t, cat.FlipHotReady(c))
					r.PublishHandle(c, &hotchunk.DB{})
				})
				got, db, err := a.resolveTier(c, kind)
				require.NoError(t, err)
				assert.Equal(t, tierHot, got)
				assert.NotNil(t, db)
			})

			t.Run("frozen wins over hot (freeze-to-discard overlap)", func(t *testing.T) {
				a, _ := admitFor(t, func(cat *catalog.Catalog, r *Router) {
					require.NoError(t, cat.FlipChunkFrozen(c, kind))
					require.NoError(t, cat.FlipHotReady(c))
					r.PublishHandle(c, &hotchunk.DB{})
				})
				got, _, err := a.resolveTier(c, kind)
				require.NoError(t, err)
				assert.Equal(t, tierCold, got, "cold wins while both tiers exist")
			})

			t.Run("freezing artifact is not served (R1)", func(t *testing.T) {
				a, _ := admitFor(t, func(cat *catalog.Catalog, _ *Router) {
					require.NoError(t, cat.MarkChunkFreezing(c, kind))
				})
				got, _, err := a.resolveTier(c, kind)
				require.NoError(t, err)
				assert.Equal(t, tierNone, got)
			})

			t.Run("ready hot without a published handle is unreachable", func(t *testing.T) {
				a, _ := admitFor(t, func(cat *catalog.Catalog, _ *Router) {
					require.NoError(t, cat.FlipHotReady(c)) // ready key, but no handle published
				})
				got, _, err := a.resolveTier(c, kind)
				require.NoError(t, err)
				assert.Equal(t, tierNone, got)
			})

			t.Run("absent chunk is unroutable", func(t *testing.T) {
				a, _ := admitFor(t, func(*catalog.Catalog, *Router) {})
				got, _, err := a.resolveTier(c, kind)
				require.NoError(t, err)
				assert.Equal(t, tierNone, got)
			})
		})
	}
}

// TestLedgerReader_Hot resolves a ready hot chunk with a real committed ledger and
// reads it back through the hot facade; the closer is a no-op (router-owned).
func TestLedgerReader_Hot(t *testing.T) {
	const c chunk.ID = 5
	cat := openTestCatalog(t, silentLogger())
	r := NewRouter(cat, geometry.NewRetention(0, 0))
	db, err := hotchunk.Open(cat.Layout().HotChunkPath(c), c, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.IngestLedger(c.FirstLedger(), fhtest.ZeroTxLCMBytes(t, c.FirstLedger()))
	require.NoError(t, err)
	require.NoError(t, cat.FlipHotReady(c))
	r.PublishHandle(c, db)

	a, err := r.Admit()
	require.NoError(t, err)
	defer a.Release()

	lr, closeFn, err := a.LedgerReader(c)
	require.NoError(t, err)
	defer func() { require.NoError(t, closeFn()) }()
	raw, err := lr.GetLedgerRaw(c.FirstLedger())
	require.NoError(t, err)
	assert.NotEmpty(t, raw, "the hot facade returns the committed ledger")
}

// TestEventReader_Hot resolves the same ready hot chunk as the common
// eventstore.Reader; a zero-tx ledger carries no events.
func TestEventReader_Hot(t *testing.T) {
	const c chunk.ID = 5
	cat := openTestCatalog(t, silentLogger())
	r := NewRouter(cat, geometry.NewRetention(0, 0))
	db, err := hotchunk.Open(cat.Layout().HotChunkPath(c), c, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.IngestLedger(c.FirstLedger(), fhtest.ZeroTxLCMBytes(t, c.FirstLedger()))
	require.NoError(t, err)
	require.NoError(t, cat.FlipHotReady(c))
	r.PublishHandle(c, db)

	a, err := r.Admit()
	require.NoError(t, err)
	defer a.Release()

	er, closeFn, err := a.EventReader(c)
	require.NoError(t, err)
	defer func() { require.NoError(t, closeFn()) }()
	n, err := er.EventCount()
	require.NoError(t, err)
	assert.Equal(t, uint32(0), n)
}

// TestLedgerReader_ColdRoutesToColdOpen pins that a frozen chunk routes to the
// cold tier and the wrapper opens the chunk's cold pack — not the hot facade, not
// unavailable. With no pack on disk the eager cold open fails at the expected pack
// path, which is exactly what proves the branch and target. Read-back over a real
// pack is covered by the ledger store tests and the daemon e2e.
func TestLedgerReader_ColdRoutesToColdOpen(t *testing.T) {
	const c chunk.ID = 9
	a, cat := admitFor(t, func(cat *catalog.Catalog, _ *Router) {
		require.NoError(t, cat.FlipChunkFrozen(c, geometry.KindLedgers))
	})
	lr, closeFn, err := a.LedgerReader(c)
	require.NoError(t, err, "frozen routes to the cold tier; the cold reader opens lazily")
	require.NotNil(t, lr)
	t.Cleanup(func() { _ = closeFn() })

	// The lazy cold reader validates on first use; with no pack on disk the read
	// fails at the chunk's pack path — proving the cold branch (not hot /
	// unavailable) and the right target. Read-back over a real pack is covered by
	// the ledger store tests and the daemon e2e.
	_, err = lr.GetLedgerRaw(c.FirstLedger())
	require.Error(t, err)
	require.ErrorContains(t, err, cat.Layout().LedgerPackPath(c), "cold reads target the chunk's pack path")
}

// TestReaders_Unavailable pins that an unroutable chunk surfaces ErrUnavailable
// on both read paths.
func TestReaders_Unavailable(t *testing.T) {
	const c chunk.ID = 3
	a, _ := admitFor(t, func(*catalog.Catalog, *Router) {})

	_, _, err := a.LedgerReader(c)
	require.ErrorIs(t, err, ErrUnavailable)
	_, _, err = a.EventReader(c)
	require.ErrorIs(t, err, ErrUnavailable)
}
