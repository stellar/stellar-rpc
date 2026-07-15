package registry

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/fhtest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/eventstore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/ledger"
)

// TestBuildFromCatalog_InitialView drives the startup build over a real
// catalog + real stores:
//
//	chunks 0-2 frozen (ledgers+events), chunk 3 live hot ("ready", real DB),
//	window 0's tx index frozen over [0,2], retention 2 chunks,
//	latest mid-chunk-3 ⇒ floor = chunk 1 (chunk 0 is prune debris).
//
// Plus R1 noise that must stay invisible: a "freezing" chunk, a "freezing"
// coverage, and a below-floor "ready" hot key with no directory behind it.
func TestBuildFromCatalog_InitialView(t *testing.T) {
	cat := newTestCatalog(t)
	layout := cat.Layout()
	const c0, c1, c2, c3 = chunk.ID(0), chunk.ID(1), chunk.ID(2), chunk.ID(3)

	for _, c := range []chunk.ID{c0, c1, c2} {
		freezeKinds(t, cat, c, geometry.KindLedgers, geometry.KindEvents)
	}
	writeLedgerPack(t, layout, c1, 3)
	createReadyHotChunk(t, cat, c3, 3)
	cov := freezeCoverage(t, cat, 0, c0, c2)
	buildIdxFile(t, layout, cov)

	// R1 noise: transient states and a below-floor ready key without a dir.
	require.NoError(t, cat.MarkChunkFreezing(4, geometry.KindLedgers))
	_, err := cat.MarkTxHashIndexFreezing(1, 1000, 1000)
	require.NoError(t, err)
	require.NoError(t, cat.PutHotTransient(5))
	require.NoError(t, cat.PutHotTransient(c0))
	require.NoError(t, cat.FlipHotReady(c0))

	ret := geometry.NewRetention(2, 0)
	latest := c3.FirstLedger() + 2

	reg, err := BuildFromCatalog(cat, ret, latest, Options{Logger: silentLogger()})
	require.NoError(t, err)

	snap := reg.Admit()
	require.Equal(t, latest, snap.Latest, "latest seeds from the caller-derived last committed ledger")
	require.Equal(t, c1, snap.View.Floor(), "floor = FloorAt(last complete chunk at latest)")

	require.Equal(t, []chunk.ID{c3}, snap.View.HotChunks(), "only the ready, in-retention hot chunk enters")
	hotDB, ok := snap.View.HotDB(c3)
	require.True(t, ok)

	// Below-floor and transient chunks are invisible.
	for _, c := range []chunk.ID{c0, 4, 5} {
		_, rerr := snap.View.resolve(c, geometry.KindLedgers)
		require.ErrorIs(t, rerr, ErrUnavailable, "chunk %s", c)
	}

	// Cold read-through: chunk 1 resolves to the cold pack and serves bytes.
	h, err := reg.LedgerReaderFor(snap.View, c1)
	require.NoError(t, err)
	require.IsType(t, &ledger.ColdReader{}, h)
	raw, err := h.GetLedgerRaw(c1.FirstLedger())
	require.NoError(t, err)
	require.Equal(t, fhtest.ZeroTxLCMBytes(t, c1.FirstLedger()), raw)

	// Hot read-through: chunk 3 was reopened read-write and serves what the
	// fixture committed before closing.
	h, err = reg.LedgerReaderFor(snap.View, c3)
	require.NoError(t, err)
	require.IsType(t, &ledger.HotStore{}, h)
	raw, err = h.GetLedgerRaw(c3.FirstLedger())
	require.NoError(t, err)
	require.Equal(t, fhtest.ZeroTxLCMBytes(t, c3.FirstLedger()), raw)

	// The reopened handle's events facade is warmed (read-write open).
	er, err := reg.EventReaderFor(snap.View, c3)
	require.NoError(t, err)
	require.IsType(t, &eventstore.HotStore{}, er)
	n, err := er.EventCount()
	require.NoError(t, err)
	require.Zero(t, n)

	// Cold events resolve through the cache (lazy: no segment files needed
	// until a read).
	er, err = reg.EventReaderFor(snap.View, c2)
	require.NoError(t, err)
	require.IsType(t, &eventstore.ColdReader{}, er)
	require.Equal(t, c2, er.ChunkID())

	idxs := snap.View.Indexes()
	require.Len(t, idxs, 1, "one open reader per frozen in-retention coverage; the freezing one is invisible")
	require.Equal(t, geometry.TxHashIndexID(0), idxs[0].Window)
	require.Equal(t, c0, idxs[0].Lo)
	require.Equal(t, c2, idxs[0].Hi)
	require.Equal(t, c0.FirstLedger(), idxs[0].Idx.MinLedger())
	require.Equal(t, c2.LastLedger(), idxs[0].Idx.MaxLedger())

	reg.Close()
	_, _, err = hotDB.Ledgers().LastSeq()
	require.ErrorIs(t, err, stores.ErrStoreClosed, "Close releases the build-opened hot handle")
	_, err = idxs[0].Idx.Get([32]byte{})
	require.ErrorIs(t, err, stores.ErrStoreClosed)

	// PreOpened: hand the live chunk's handle in; the build adopts it instead
	// of opening a second write handle, and the registry becomes its owner.
	db3 := openHotDB(t, layout, c3)
	db9 := openHotDB(t, layout, 9) // no catalog key: must be rejected, ownership stays here
	reg2, err := BuildFromCatalog(cat, ret, latest, Options{
		Logger:    silentLogger(),
		PreOpened: map[chunk.ID]*hotchunk.DB{c3: db3, 9: db9},
	})
	require.NoError(t, err)
	adopted, ok := reg2.Admit().View.HotDB(c3)
	require.True(t, ok)
	require.Same(t, db3, adopted)
	_, ok = reg2.Admit().View.HotDB(9)
	require.False(t, ok)
	_, _, err = db9.Ledgers().LastSeq()
	require.NoError(t, err, "a rejected pre-opened handle is left open for its owner")
	require.NoError(t, db9.Close())

	reg2.Close()
	_, _, err = db3.Ledgers().LastSeq()
	require.ErrorIs(t, err, stores.ErrStoreClosed, "an adopted pre-opened handle is registry-owned")
}

func TestBuildFromCatalog_PristineCatalog(t *testing.T) {
	cat := newTestCatalog(t)
	reg, err := BuildFromCatalog(cat, geometry.NewRetention(0, 0), 0, Options{Logger: silentLogger()})
	require.NoError(t, err)
	defer reg.Close()

	snap := reg.Admit()
	require.Zero(t, snap.Latest)
	require.Equal(t, chunk.ID(0), snap.View.Floor())
	require.Empty(t, snap.View.HotChunks())
	require.Empty(t, snap.View.Indexes())
}

func TestBuildFromCatalog_ReadyChunkThatWontOpenFailsTheBuild(t *testing.T) {
	cat := newTestCatalog(t)
	// The catalog promises a ready hot chunk, but no directory exists — the
	// never-auto-heal rule: fail the build, let the supervisor restart.
	require.NoError(t, cat.PutHotTransient(0))
	require.NoError(t, cat.FlipHotReady(0))

	_, err := BuildFromCatalog(cat, geometry.NewRetention(0, 0), 0, Options{Logger: silentLogger()})
	require.Error(t, err)
	require.ErrorContains(t, err, "open hot chunk")
}
