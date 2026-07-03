package lifecycle

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// seedLedgersCF reopens a CLOSED chunk hot DB raw and commits sparse ledgers-CF
// entries in one batch via the production AddLedgerToBatch. These fixtures need
// arbitrary frontier heights without the events CF's contiguity requirement, so
// they write the one CF the watermark refinement reads (MaxCommittedSeq only
// looks at the ledgers CF's last key; the payload bytes are never decoded).
func seedLedgersCF(t *testing.T, cat *catalog.Catalog, c chunk.ID, entries ...ledger.Entry) {
	t.Helper()
	store, err := rocksdb.New(rocksdb.Config{
		Path:           cat.Layout().HotChunkPath(c),
		ColumnFamilies: hotchunk.ColumnFamilies(),
		Logger:         silentLogger(),
	})
	require.NoError(t, err)
	h := ledger.NewWithStore(store)
	require.NoError(t, store.Batch(func(b *rocksdb.BatchWriter) error {
		for _, e := range entries {
			if berr := h.AddLedgerToBatch(b, e); berr != nil {
				return berr
			}
		}
		return nil
	}))
	require.NoError(t, store.Close())
}

// seedReadyLiveDB brackets a "ready" hot DB for chunk c (via the production
// opener) and commits a single ledgers-CF entry at seq `top` so MaxCommittedSeq
// reads back `top`. top==0 leaves the DB empty (present=false). It closes the DB
// as hygiene — a read-only reopen takes no RocksDB LOCK, so this isn't required
// for the refinement to open, but it keeps the fixtures single-handle.
func seedReadyLiveDB(t *testing.T, cat *catalog.Catalog, c chunk.ID, top uint32) {
	t.Helper()
	db := openLiveHotDB(t, cat, c) // ready key + real dir + empty DB
	require.NoError(t, db.Close())
	if top > 0 {
		seedLedgersCF(t, cat, c, ledger.Entry{Seq: top, Bytes: []byte("ledger")})
	}
}

// TestDeriveWatermark_RealHotDB_RefinementIsNotStale exercises the watermark
// refinement against a REAL per-chunk hotchunk DB opened read-only by its Layout
// path (the same open production does). It proves the single-DB MaxCommittedSeq
// refinement reads the actual committed ledger frontier (the ledgers CF's last
// key) and is not a stale/constant value: the bound rises to exactly the highest
// seq committed to the live chunk's real DB.
func TestDeriveWatermark_RealHotDB_RefinementIsNotStale(t *testing.T) {
	cat, _ := testCatalog(t)

	live := chunk.ID(5)
	// Production bracket: creates the hot dir, opens the SINGLE shared multi-CF
	// DB, flips the hot key "ready". This is exactly what ingestion does.
	db := openLiveHotDB(t, cat, live)
	// Close the live writer before seeding — hygiene (the refinement's read-only
	// reopen takes no RocksDB LOCK), keeping the fixture single-handle.
	require.NoError(t, db.Close())

	// Commit two real ledgers into the ledgers CF (the CF MaxCommittedSeq reads).
	first := live.FirstLedger()
	committedTop := first + 200
	seedLedgersCF(t, cat, live,
		ledger.Entry{Seq: first, Bytes: []byte("ledger-A")},
		ledger.Entry{Seq: committedTop, Bytes: []byte("ledger-B")},
	)

	// Sanity: positional baseline (live chunk 5 ⇒ everything below 5) is chunk 4's
	// last ledger, strictly below the committed top — so the assertion below can
	// only pass if the refinement actually read the real DB.
	baseline := geometry.CompleteThrough(int64(live) - 1)
	require.Equal(t, chunk.ID(4).LastLedger(), baseline)
	require.Greater(t, committedTop, baseline, "fixture must put the real frontier above the baseline")

	got, err := deriveWatermark(cat, silentLogger())
	require.NoError(t, err)
	require.Equal(t, committedTop, got,
		"watermark must equal the REAL ledgers-CF last key, not the positional baseline")
}

// TestDeriveWatermark_RealHotDB_OpensHighestReady proves the refinement opens the
// HIGHEST ready chunk (the live chunk), not just any ready chunk. Two ready chunks
// have independent real hot DBs with DIFFERENT committed frontiers; the watermark
// must reflect the higher chunk's DB. Only opening the real per-chunk DB by its
// Layout path distinguishes the two — a "open ready[0] instead of ready[len-1]"
// regression would land on the wrong frontier.
func TestDeriveWatermark_RealHotDB_OpensHighestReady(t *testing.T) {
	cat, _ := testCatalog(t)

	lower, higher := chunk.ID(4), chunk.ID(7)

	// Lower ready chunk: a real DB committed near the TOP of chunk 4. If the
	// refinement wrongly opened the lower chunk, the bound would land here.
	lowDB := openLiveHotDB(t, cat, lower)
	require.NoError(t, lowDB.Close())
	lowTop := lower.FirstLedger() + 9000
	seedLedgersCF(t, cat, lower, ledger.Entry{Seq: lowTop, Bytes: []byte("low")})

	// Higher ready chunk (the live chunk): committed mid-chunk 7.
	highDB := openLiveHotDB(t, cat, higher)
	require.NoError(t, highDB.Close())
	highMid := higher.FirstLedger() + 1234
	seedLedgersCF(t, cat, higher, ledger.Entry{Seq: highMid, Bytes: []byte("high")})

	// The two frontiers must be unambiguous: chunk 7 mid-seq is far above chunk 4's
	// top, so reading the wrong chunk yields a strictly different (lower) answer.
	require.Greater(t, highMid, lowTop)

	got, err := deriveWatermark(cat, silentLogger())
	require.NoError(t, err)
	require.Equal(t, highMid, got,
		"refinement must open the HIGHEST ready chunk (7), reading its committed mid-seq")
}

// TestDeriveWatermark_RealHotDB_EmptyLiveFallsBack is the count-only-ready case
// against a real DB: a "ready" live chunk whose real hot DB has NO committed
// ledger (MaxCommittedSeq ok=false) must fall back to deriveCompleteThrough, not
// fabricate a frontier. Read through a real read-only open by Layout path.
func TestDeriveWatermark_RealHotDB_EmptyLiveFallsBack(t *testing.T) {
	cat, _ := testCatalog(t)
	makeChunkDurable(t, cat, 0) // cold term => chunk 0 last ledger

	live := chunk.ID(3)
	db := openLiveHotDB(t, cat, live) // ready key + real dir, but NOTHING committed
	require.NoError(t, db.Close())

	// A read-only open of the empty ledgers CF: ok=false, no refinement.
	got, err := deriveWatermark(cat, silentLogger())
	require.NoError(t, err)
	require.Equal(t, chunk.ID(2).LastLedger(), got,
		"empty live DB ⇒ positional baseline (max ready 3 - 1 = chunk 2), no fabricated frontier")
}
