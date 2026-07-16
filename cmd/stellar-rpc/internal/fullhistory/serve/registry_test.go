package serve

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/fhtest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/txhash"
)

func silentLogger() *supportlog.Entry {
	log := supportlog.New()
	log.SetLevel(logrus.ErrorLevel)
	return log
}

// newTestCatalog opens a temp catalog with a one-chunk-per-index window layout
// (keeps index arithmetic trivial: every chunk is its own window).
func newTestCatalog(t *testing.T) (*catalog.Catalog, geometry.Layout) {
	t.Helper()
	layout := geometry.NewLayout(t.TempDir())
	windows, err := geometry.NewTxHashIndexLayout(1)
	require.NoError(t, err)
	cat, err := catalog.Open(layout.CatalogPath(), layout, windows, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })
	return cat, layout
}

// openHotChunk runs the real create bracket (BeginHotCreate → Open → ingest →
// FinishHotCreate) so the chunk has a "ready" hot key AND a live write DB, then
// returns the write handle. Seqs are ingested as zero-tx ledgers.
func openHotChunk(t *testing.T, cat *catalog.Catalog, layout geometry.Layout, c chunk.ID, seqs ...uint32) *hotchunk.DB {
	t.Helper()
	require.NoError(t, cat.BeginHotCreate(c))
	db, err := hotchunk.Open(layout.HotChunkPath(c), c, silentLogger())
	require.NoError(t, err)
	for _, s := range seqs {
		_, err := db.IngestLedger(s, xdr.LedgerCloseMetaView(fhtest.ZeroTxLCMBytes(t, s)))
		require.NoError(t, err)
	}
	require.NoError(t, cat.FinishHotCreate(c))
	return db
}

// freezeLedgers marks chunk c's ledger artifact frozen in the catalog.
func freezeLedgers(t *testing.T, cat *catalog.Catalog, c chunk.ID) {
	t.Helper()
	require.NoError(t, cat.MarkChunkFreezing(c, geometry.KindLedgers))
	require.NoError(t, cat.FlipChunkFrozen(c, geometry.KindLedgers))
}

// writeColdLedgerPack writes a real cold ledger pack at c's layout path holding
// one ledger at seq with the given raw bytes (the cold store persists the bytes
// verbatim, so a distinctive payload lets a test prove the COLD reader served).
func writeColdLedgerPack(t *testing.T, layout geometry.Layout, c chunk.ID, seq uint32, raw []byte) {
	t.Helper()
	path := layout.LedgerPackPath(c)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	w, err := ledger.NewColdWriter(path, seq, ledger.ColdWriterOptions{})
	require.NoError(t, err)
	require.NoError(t, w.AppendLedger(seq, raw))
	require.NoError(t, w.Commit())
	require.NoError(t, w.Close())
}

// freezeTxIndex builds a real frozen tx-hash index for window w covering chunks
// [lo, hi] over ledgers [minLedger, maxLedger] and holding entries: writes a
// .bin, runs BuildColdIndex to the layout's .idx path, then marks + commits it
// frozen in the catalog (the backfill build path, minus the daemon).
func freezeTxIndex(
	t *testing.T, cat *catalog.Catalog, layout geometry.Layout,
	w geometry.TxHashIndexID, lo, hi chunk.ID, minLedger, maxLedger uint32, entries []txhash.ColdEntry,
) {
	t.Helper()
	binPath := filepath.Join(t.TempDir(), "index.bin")
	require.NoError(t, txhash.WriteColdBin(binPath, entries))

	cov, err := cat.MarkTxHashIndexFreezing(w, lo, hi)
	require.NoError(t, err)
	idxPath := layout.TxHashIndexFilePath(cov)
	require.NoError(t, os.MkdirAll(filepath.Dir(idxPath), 0o755))
	require.NoError(t, txhash.BuildColdIndex(context.Background(), []string{binPath}, idxPath, minLedger, maxLedger))
	require.NoError(t, cat.CommitTxHashIndex(cov))
}

// (a) Admit returns the seeded latest and an initial View with a frozen chunk in
// Cold and a (non-resume) ready chunk in Hot.
func TestAdmit_InitialView(t *testing.T) {
	cat, layout := newTestCatalog(t)

	// Two ready hot chunks: 0 (completed/fenced) + 1 (resume). BuildInitial opens
	// a read view for 0 and leaves 1 for HotOpened.
	db0 := openHotChunk(t, cat, layout, chunk.ID(0), 2)
	require.NoError(t, db0.Close()) // fenced completed chunk: write handle closed
	db1 := openHotChunk(t, cat, layout, chunk.ID(1))
	t.Cleanup(func() { _ = db1.Close() })

	// A frozen cold chunk with no hot key.
	freezeLedgers(t, cat, chunk.ID(3))

	r := NewRegistry(cat, fhtest.RetentionFor(t, cat, 0), silentLogger())
	const lastCommitted = 12345
	require.NoError(t, r.BuildInitial(lastCommitted))

	latest, v := r.Admit()
	assert.EqualValues(t, lastCommitted, latest)
	require.NotNil(t, v)

	// chunk 0 was opened as a read view; chunk 1 (resume) is NOT in Hot yet.
	assert.Contains(t, v.Hot, chunk.ID(0), "completed ready chunk opened as read view")
	assert.NotContains(t, v.Hot, chunk.ID(1), "resume chunk is published via HotOpened, not BuildInitial")
	// chunk 3 is frozen cold ledgers.
	assert.True(t, v.Cold[chunk.ID(3)].Ledgers, "frozen chunk is in Cold")
}

// (b) Admission order: LedgerCommitted advances latest but must NOT churn the
// View pointer — a query admitting after LedgerCommitted still sees the same
// immutable View until a real publish.
func TestAdmit_LatestAdvancesWithoutViewChurn(t *testing.T) {
	cat, _ := newTestCatalog(t)
	r := NewRegistry(cat, fhtest.RetentionFor(t, cat, 0), silentLogger())
	require.NoError(t, r.BuildInitial(100))

	latest0, v0 := r.Admit()
	assert.EqualValues(t, 100, latest0)

	r.LedgerCommitted(101)
	latest1, v1 := r.Admit()
	assert.EqualValues(t, 101, latest1, "latest advanced")
	assert.Same(t, v0, v1, "View pointer unchanged by LedgerCommitted")

	// A real publish DOES swap the pointer.
	r.HotOpened(chunk.ID(0), nil)
	_, v2 := r.Admit()
	assert.NotSame(t, v1, v2, "HotOpened publishes a new View")
}

// (c) Resolution: cold wins when both a cold flag and a hot handle exist;
// unknown chunk is ErrUnavailable; hot-only serves from the hot handle.
func TestResolveLedgers_ColdWinsAndUnavailable(t *testing.T) {
	cat, layout := newTestCatalog(t)
	r := NewRegistry(cat, fhtest.RetentionFor(t, cat, 0), silentLogger())
	require.NoError(t, r.BuildInitial(0))

	// hot-only chunk 1 with a real ledger.
	seq1 := chunk.ID(1).FirstLedger()
	db1 := openHotChunk(t, cat, layout, chunk.ID(1), seq1)
	t.Cleanup(func() { _ = db1.Close() })
	r.HotOpened(chunk.ID(1), db1)

	// chunk 0: both a hot handle AND frozen cold ledgers. Hot and cold both hold
	// seq 2, but the cold pack stores a distinctive payload — so a read returning
	// those bytes proves the COLD reader was chosen over the hot handle.
	db0 := openHotChunk(t, cat, layout, chunk.ID(0), 2)
	t.Cleanup(func() { _ = db0.Close() })
	r.HotOpened(chunk.ID(0), db0)
	coldBytes := []byte("cold-copy-of-seq-2")
	writeColdLedgerPack(t, layout, chunk.ID(0), 2, coldBytes)
	freezeLedgers(t, cat, chunk.ID(0))
	r.TickCompleted() // picks up Cold[0].Ledgers

	_, v := r.Admit()

	// hot-only resolves and serves.
	hotChunk, err := v.ResolveLedgers(chunk.ID(1), layout)
	require.NoError(t, err)
	got, err := hotChunk.Get(seq1)
	require.NoError(t, err, "hot handle serves the ledger")
	assert.NotEmpty(t, got)
	require.NoError(t, hotChunk.Close())

	// chunk 0 has both flags → cold wins. seq 2 lives only in the cold pack, so a
	// successful read returning the cold bytes proves the cold reader was used.
	bothChunk, err := v.ResolveLedgers(chunk.ID(0), layout)
	require.NoError(t, err)
	gotCold, err := bothChunk.Get(2)
	require.NoError(t, err, "cold reader serves seq 2 (chosen over the hot handle)")
	assert.Equal(t, coldBytes, gotCold, "served the cold copy")
	require.NoError(t, bothChunk.Close())

	// unknown chunk → ErrUnavailable.
	_, err = v.ResolveLedgers(chunk.ID(99), layout)
	assert.ErrorIs(t, err, ErrUnavailable)
}

// (d) TickCompleted moves a frozen chunk hot→cold (keeping its hot handle) and
// closes a discarded chunk's handle — a read on the stale View then returns
// stores.ErrStoreClosed rather than crashing.
func TestTickCompleted_FreezeAndDiscard(t *testing.T) {
	cat, layout := newTestCatalog(t)
	r := NewRegistry(cat, fhtest.RetentionFor(t, cat, 0), silentLogger())
	require.NoError(t, r.BuildInitial(0))

	// chunk A (0): stays hot, will be frozen (hot→cold overlap).
	dbA := openHotChunk(t, cat, layout, chunk.ID(0), 2)
	t.Cleanup(func() { _ = dbA.Close() })
	r.HotOpened(chunk.ID(0), dbA)

	// chunk B (1): a read-view handle we then discard.
	seqB := chunk.ID(1).FirstLedger()
	dbBWrite := openHotChunk(t, cat, layout, chunk.ID(1), seqB)
	require.NoError(t, dbBWrite.Close()) // fence the write handle before reopening read-only
	r.ChunkClosed(chunk.ID(1))           // registry opens + publishes a read-view handle

	_, staleView := r.Admit()
	bReader, err := staleView.ResolveLedgers(chunk.ID(1), layout)
	require.NoError(t, err)
	_, err = bReader.Get(seqB)
	require.NoError(t, err, "chunk B read view serves before discard")

	// Freeze A; discard B.
	freezeLedgers(t, cat, chunk.ID(0))
	require.NoError(t, cat.DiscardHotChunk(chunk.ID(1)))

	r.TickCompleted()

	_, v := r.Admit()
	// A moved hot→cold: still hot AND now cold.
	assert.Contains(t, v.Hot, chunk.ID(0), "frozen chunk keeps its hot handle during overlap")
	assert.True(t, v.Cold[chunk.ID(0)].Ledgers, "frozen chunk is now cold")
	// B was discarded: gone from the new View.
	assert.NotContains(t, v.Hot, chunk.ID(1), "discarded chunk removed from View")

	// The stale View's handle was closed by the tick: a read now returns
	// ErrStoreClosed (memory-safe), not a crash.
	_, err = bReader.Get(seqB)
	assert.ErrorIs(t, err, stores.ErrStoreClosed, "discarded handle closed; stale read is store-closed")
}

// (regression, I-2) TickCompleted must never delete-and-close a chunk whose live
// write handle is in the current View. openHotDBForChunk creates the hot:chunk
// key BEFORE HotOpened publishes the handle, and HotOpened serializes on the same
// mu the tick's hot-key scan now takes — so a chunk in Hot is always in the scan
// set, and the live handle survives the tick (and stays open: it still reads and
// still ingests). Before the fix the scan ran outside the publish lock, so a
// chunk that went live between a stale scan and the publish was closed while live.
func TestTickCompleted_DoesNotCloseLiveChunk(t *testing.T) {
	cat, layout := newTestCatalog(t)
	r := NewRegistry(cat, fhtest.RetentionFor(t, cat, 0), silentLogger())
	require.NoError(t, r.BuildInitial(0))

	// A completed chunk 0 kept hot as a read-view stand-in, plus the LIVE write
	// handle for chunk 1 published via HotOpened. openHotChunk runs the real create
	// bracket, so both chunks have a "ready" hot:chunk key exactly as the ingestion
	// loop leaves them at a boundary.
	db0 := openHotChunk(t, cat, layout, chunk.ID(0), 2)
	t.Cleanup(func() { _ = db0.Close() })
	r.HotOpened(chunk.ID(0), db0)

	seq1 := chunk.ID(1).FirstLedger()
	liveDB := openHotChunk(t, cat, layout, chunk.ID(1), seq1)
	t.Cleanup(func() { _ = liveDB.Close() })
	r.HotOpened(chunk.ID(1), liveDB)

	r.TickCompleted()

	_, v := r.Admit()
	require.Contains(t, v.Hot, chunk.ID(1), "live chunk survives the tick")
	assert.Same(t, liveDB, v.Hot[chunk.ID(1)], "live write handle is un-swapped")

	// The handle was NOT closed by the tick: it still serves reads and still
	// accepts writes (a closed store would ErrStoreClosed both).
	got, err := v.Hot[chunk.ID(1)].Ledgers().GetLedgerRaw(seq1)
	require.NoError(t, err, "live handle still serves reads after the tick")
	require.NotEmpty(t, got)
	_, err = liveDB.IngestLedger(seq1+1, xdr.LedgerCloseMetaView(fhtest.ZeroTxLCMBytes(t, seq1+1)))
	require.NoError(t, err, "live write handle still ingests after the tick")
}

// TxIdx: BuildInitial opens a frozen window's .idx into a usable reader, and a
// steady tick reuses the same *ColdReader when the coverage is unchanged.
func TestTxIdx_BuildProbeAndReuse(t *testing.T) {
	cat, layout := newTestCatalog(t)

	// A known hash resolving to seq 5, in window 0 / chunk 0 (cpi=1).
	var hash [32]byte
	for i := range hash {
		hash[i] = byte(i + 1)
	}
	var entry txhash.ColdEntry
	copy(entry.Key[:], hash[:])
	entry.Seq = 5
	freezeTxIndex(t, cat, layout, geometry.TxHashIndexID(0), chunk.ID(0), chunk.ID(0),
		chunk.ID(0).FirstLedger(), chunk.ID(0).LastLedger(), []txhash.ColdEntry{entry})

	r := NewRegistry(cat, fhtest.RetentionFor(t, cat, 0), silentLogger())
	require.NoError(t, r.BuildInitial(chunk.ID(0).LastLedger()))

	_, v := r.Admit()
	require.Len(t, v.TxIdx, 1)
	assert.Equal(t, chunk.ID(0), v.TxIdx[0].Lo)
	assert.Equal(t, chunk.ID(0), v.TxIdx[0].Hi)
	require.NotNil(t, v.TxIdx[0].Reader)
	got, err := v.TxIdx[0].Reader.Get(hash)
	require.NoError(t, err, "the frozen .idx reader resolves the known hash")
	assert.EqualValues(t, 5, got)

	// A steady tick (coverage unchanged) reuses the same reader — no reopen/leak.
	before := v.TxIdx[0].Reader
	r.TickCompleted()
	_, v2 := r.Admit()
	require.Len(t, v2.TxIdx, 1)
	assert.Same(t, before, v2.TxIdx[0].Reader, "unchanged coverage reuses the reader")
}

// ResolveEvents: hot handle serves when no cold flag; unknown chunk is
// ErrUnavailable.
func TestResolveEvents_HotAndUnavailable(t *testing.T) {
	cat, layout := newTestCatalog(t)
	r := NewRegistry(cat, fhtest.RetentionFor(t, cat, 0), silentLogger())
	require.NoError(t, r.BuildInitial(0))

	db0 := openHotChunk(t, cat, layout, chunk.ID(0), 2)
	t.Cleanup(func() { _ = db0.Close() })
	r.HotOpened(chunk.ID(0), db0)

	_, v := r.Admit()
	ev, err := v.ResolveEvents(chunk.ID(0), layout)
	require.NoError(t, err)
	require.NotNil(t, ev.Reader())
	_, err = ev.Reader().EventCount()
	require.NoError(t, err, "hot events reader is usable")
	require.NoError(t, ev.Close())

	_, err = v.ResolveEvents(chunk.ID(99), layout)
	assert.ErrorIs(t, err, ErrUnavailable)
}

// (e) Every hook is safe on a nil *Registry (the no-serve path needs no guards).
func TestNilRegistry_HooksAreNoOps(t *testing.T) {
	var r *Registry
	assert.NotPanics(t, func() {
		r.LedgerCommitted(1)
		r.HotOpened(chunk.ID(0), nil)
		r.ChunkClosed(chunk.ID(0))
		r.TickCompleted()
		assert.NoError(t, r.BuildInitial(0))
	})
}
