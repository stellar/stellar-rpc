package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

// viewFor sets up a registry over a fresh catalog, runs setup against the catalog
// and registry, then acquires a read view — returned for resolveTier assertions.
func viewFor(t *testing.T, setup func(cat *catalog.Catalog, r *Registry)) (*ReadView, *catalog.Catalog) {
	t.Helper()
	cat := openTestCatalog(t, silentLogger())
	r := NewRegistry(cat, geometry.NewRetention(0, 0))
	// A working daemon always has a ready live chunk; seed one well above the
	// chunks the tests route, so acquisition succeeds without affecting them.
	require.NoError(t, cat.FlipHotReady(999))
	setup(cat, r)
	a, err := r.NewReadView()
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
				a, _ := viewFor(t, func(cat *catalog.Catalog, _ *Registry) {
					require.NoError(t, cat.FlipChunkFrozen(c, kind))
				})
				got, db, err := a.resolveTier(c, kind)
				require.NoError(t, err)
				assert.Equal(t, tierCold, got)
				assert.Nil(t, db)
			})

			t.Run("ready hot with handle serves hot", func(t *testing.T) {
				a, _ := viewFor(t, func(cat *catalog.Catalog, r *Registry) {
					require.NoError(t, cat.FlipHotReady(c))
					r.PublishHandle(c, &hotchunk.DB{})
				})
				got, db, err := a.resolveTier(c, kind)
				require.NoError(t, err)
				assert.Equal(t, tierHot, got)
				assert.NotNil(t, db)
			})

			t.Run("frozen wins over hot (freeze-to-discard overlap)", func(t *testing.T) {
				a, _ := viewFor(t, func(cat *catalog.Catalog, r *Registry) {
					require.NoError(t, cat.FlipChunkFrozen(c, kind))
					require.NoError(t, cat.FlipHotReady(c))
					r.PublishHandle(c, &hotchunk.DB{})
				})
				got, _, err := a.resolveTier(c, kind)
				require.NoError(t, err)
				assert.Equal(t, tierCold, got, "cold wins while both tiers exist")
			})

			t.Run("freezing artifact is not served (R1)", func(t *testing.T) {
				a, _ := viewFor(t, func(cat *catalog.Catalog, _ *Registry) {
					require.NoError(t, cat.MarkChunkFreezing(c, kind))
				})
				got, _, err := a.resolveTier(c, kind)
				require.NoError(t, err)
				assert.Equal(t, tierNone, got)
			})

			t.Run("ready hot without a published handle is unreachable", func(t *testing.T) {
				a, _ := viewFor(t, func(cat *catalog.Catalog, _ *Registry) {
					require.NoError(t, cat.FlipHotReady(c)) // ready key, but no handle published
				})
				got, _, err := a.resolveTier(c, kind)
				require.NoError(t, err)
				assert.Equal(t, tierNone, got)
			})

			t.Run("absent chunk is unroutable", func(t *testing.T) {
				a, _ := viewFor(t, func(*catalog.Catalog, *Registry) {})
				got, _, err := a.resolveTier(c, kind)
				require.NoError(t, err)
				assert.Equal(t, tierNone, got)
			})
		})
	}
}

// TestLedgerReader_Hot resolves a ready hot chunk with a real committed ledger and
// reads it back through the hot facade; the closer is a no-op (registry-owned).
func TestLedgerReader_Hot(t *testing.T) {
	const c chunk.ID = 5
	cat := openTestCatalog(t, silentLogger())
	r := NewRegistry(cat, geometry.NewRetention(0, 0))
	db, err := hotchunk.Open(cat.Layout().HotChunkPath(c), c, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.IngestLedger(c.FirstLedger(), rpcv2test.ZeroTxLCMBytes(t, c.FirstLedger()))
	require.NoError(t, err)
	require.NoError(t, cat.FlipHotReady(c))
	r.PublishHandle(c, db)

	a, err := r.NewReadView()
	require.NoError(t, err)
	defer a.Release()

	lr, err := a.Ledgers(c)
	require.NoError(t, err)
	raw, err := lr.GetLedgerRaw(c.FirstLedger())
	require.NoError(t, err)
	assert.NotEmpty(t, raw, "the hot facade returns the committed ledger")
}

// TestEventReader_Hot resolves the same ready hot chunk as the common
// event.Reader; a zero-tx ledger carries no events.
func TestEventReader_Hot(t *testing.T) {
	const c chunk.ID = 5
	cat := openTestCatalog(t, silentLogger())
	r := NewRegistry(cat, geometry.NewRetention(0, 0))
	db, err := hotchunk.Open(cat.Layout().HotChunkPath(c), c, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.IngestLedger(c.FirstLedger(), rpcv2test.ZeroTxLCMBytes(t, c.FirstLedger()))
	require.NoError(t, err)
	require.NoError(t, cat.FlipHotReady(c))
	r.PublishHandle(c, db)

	a, err := r.NewReadView()
	require.NoError(t, err)
	defer a.Release()

	er, err := a.Events(c)
	require.NoError(t, err)
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
	a, cat := viewFor(t, func(cat *catalog.Catalog, _ *Registry) {
		require.NoError(t, cat.FlipChunkFrozen(c, geometry.KindLedgers))
	})
	lr, err := a.Ledgers(c)
	require.NoError(t, err, "frozen routes to the cold tier; the cold reader opens lazily")
	require.NotNil(t, lr)

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
	a, _ := viewFor(t, func(*catalog.Catalog, *Registry) {})

	_, err := a.Ledgers(c)
	require.ErrorIs(t, err, ErrUnavailable)
	_, err = a.Events(c)
	require.ErrorIs(t, err, ErrUnavailable)
}

// TestRelease_ClosesViewOwnedReaders pins the ownership contract: a cold reader
// opened through the view is registered for closing and Release drains the list;
// a hot facade never registers (it is registry-owned).
func TestRelease_ClosesViewOwnedReaders(t *testing.T) {
	const cold, hot chunk.ID = 9, 10
	a, _ := viewFor(t, func(cat *catalog.Catalog, r *Registry) {
		require.NoError(t, cat.FlipChunkFrozen(cold, geometry.KindLedgers))
		require.NoError(t, cat.FlipHotReady(hot))
		r.PublishHandle(hot, &hotchunk.DB{})
	})

	_, err := a.Ledgers(cold) // lazy cold open: no pack needed until first use
	require.NoError(t, err)
	require.Len(t, a.closers, 1, "the cold reader is view-owned")

	_, err = a.Ledgers(hot)
	require.NoError(t, err)
	require.Len(t, a.closers, 1, "the hot facade is registry-owned, not view-owned")

	// A sentinel closer proves Release invokes what was registered; the double
	// Release from viewFor's cleanup is a no-op (closers drained, snapshot inert).
	closed := false
	a.closers = append(a.closers, func() error { closed = true; return nil })
	a.Release()
	assert.True(t, closed, "Release runs the view-owned closers")
	assert.Nil(t, a.closers, "the closer list is drained")
}

// TestEventReader_ColdRoutesToColdOpen pins that a frozen events chunk routes to
// the cold tier: the reader targets the chunk's bucket dir (proven by the lazy
// first-use failure naming it) and is view-owned (its closer is registered for
// Release). A wrong dir or a dropped closer fails here.
func TestEventReader_ColdRoutesToColdOpen(t *testing.T) {
	const c chunk.ID = 9
	a, cat := viewFor(t, func(cat *catalog.Catalog, _ *Registry) {
		require.NoError(t, cat.FlipChunkFrozen(c, geometry.KindEvents))
	})

	before := len(a.closers)
	er, err := a.Events(c)
	require.NoError(t, err, "frozen routes to the cold tier; the cold reader opens lazily")
	require.NotNil(t, er)
	require.Len(t, a.closers, before+1, "the cold events reader is view-owned")

	// The lazy reader validates on first use; with no bucket on disk the read
	// fails inside the chunk's bucket dir — proving the cold branch and target.
	_, err = er.EventCount()
	require.Error(t, err)
	require.ErrorContains(t, err, cat.Layout().EventsBucketDir(c), "cold reads target the chunk's bucket dir")
}
