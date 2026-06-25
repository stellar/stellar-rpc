package lifecycle

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// TestDeriveWatermark_RealHotDB_RefinementIsNotStale exercises the watermark
// refinement against a REAL per-chunk hotchunk DB read through the production
// rocksHotProbe — the path the fakeHotProbe table tests stub out. It proves the
// single-DB MaxCommittedSeq refinement reads the actual committed ledger frontier
// (the ledgers CF's last key) and is not a stale/constant value: the bound rises
// to exactly the highest seq committed to the live chunk's real DB.
func TestDeriveWatermark_RealHotDB_RefinementIsNotStale(t *testing.T) {
	cat, _ := testCatalog(t)

	live := chunk.ID(5)
	// Production bracket: creates the hot dir, opens the SINGLE shared multi-CF
	// DB, flips the hot key "ready". This is exactly what ingestion does.
	db := openLiveHotDB(t, cat, live)

	// Commit two real ledgers into the ledgers CF (the CF MaxCommittedSeq reads).
	first := live.FirstLedger()
	committedTop := first + 200
	require.NoError(t, db.Ledgers().AddLedgers(
		ledger.Entry{Seq: first, Bytes: []byte("ledger-A")},
		ledger.Entry{Seq: committedTop, Bytes: []byte("ledger-B")},
	))
	// Close the live writer before the probe re-opens read-only (RocksDB LOCK).
	require.NoError(t, db.Close())

	// Sanity: positional baseline (live chunk 5 ⇒ everything below 5) is chunk 4's
	// last ledger, strictly below the committed top — so the assertion below can
	// only pass if the refinement actually read the real DB.
	baseline := mustDeriveCompleteThrough(t, cat)
	require.Equal(t, chunk.ID(4).LastLedger(), baseline)
	require.Greater(t, committedTop, baseline, "fixture must put the real frontier above the baseline")

	probe := NewRocksHotProbe(cat.Layout().HotChunkPath, silentLogger())
	got, err := deriveWatermark(cat, probe)
	require.NoError(t, err)
	require.Equal(t, committedTop, got,
		"watermark must equal the REAL ledgers-CF last key, not the positional baseline")
}

// TestDeriveWatermark_RealHotDB_OpensHighestReady proves the refinement opens the
// HIGHEST ready chunk (the live chunk), not just any ready chunk. Two ready chunks
// have independent real hot DBs with DIFFERENT committed frontiers; the watermark
// must reflect the higher chunk's DB. The fakeHotProbe table tests CANNOT cover
// this: fakeHotProbe.OpenHotChunk ignores its chunk-id argument and returns one
// canned DB, so a "open ready[0] instead of ready[len-1]" regression is invisible
// to them — only a real per-chunk probe distinguishes the two.
func TestDeriveWatermark_RealHotDB_OpensHighestReady(t *testing.T) {
	cat, _ := testCatalog(t)

	lower, higher := chunk.ID(4), chunk.ID(7)

	// Lower ready chunk: a real DB committed near the TOP of chunk 4. If the
	// refinement wrongly opened the lower chunk, the bound would land here.
	lowDB := openLiveHotDB(t, cat, lower)
	lowTop := lower.FirstLedger() + 9000
	require.NoError(t, lowDB.Ledgers().AddLedgers(ledger.Entry{Seq: lowTop, Bytes: []byte("low")}))
	require.NoError(t, lowDB.Close())

	// Higher ready chunk (the live chunk): committed mid-chunk 7.
	highDB := openLiveHotDB(t, cat, higher)
	highMid := higher.FirstLedger() + 1234
	require.NoError(t, highDB.Ledgers().AddLedgers(ledger.Entry{Seq: highMid, Bytes: []byte("high")}))
	require.NoError(t, highDB.Close())

	// The two frontiers must be unambiguous: chunk 7 mid-seq is far above chunk 4's
	// top, so reading the wrong chunk yields a strictly different (lower) answer.
	require.Greater(t, highMid, lowTop)

	probe := NewRocksHotProbe(cat.Layout().HotChunkPath, silentLogger())
	got, err := deriveWatermark(cat, probe)
	require.NoError(t, err)
	require.Equal(t, highMid, got,
		"refinement must open the HIGHEST ready chunk (7), reading its committed mid-seq")
}

// TestDeriveWatermark_RealHotDB_EmptyLiveFallsBack is the count-only-ready case
// against a real DB: a "ready" live chunk whose real hot DB has NO committed
// ledger (MaxCommittedSeq ok=false) must fall back to deriveCompleteThrough, not
// fabricate a frontier. Read through the production probe.
func TestDeriveWatermark_RealHotDB_EmptyLiveFallsBack(t *testing.T) {
	cat, _ := testCatalog(t)
	makeChunkDurable(t, cat, 0) // cold term => chunk 0 last ledger

	live := chunk.ID(3)
	db := openLiveHotDB(t, cat, live) // ready key + real dir, but NOTHING committed
	require.NoError(t, db.Close())

	// Real probe reads the empty ledgers CF: ok=false, no refinement.
	probe := NewRocksHotProbe(cat.Layout().HotChunkPath, silentLogger())
	got, err := deriveWatermark(cat, probe)
	require.NoError(t, err)
	require.Equal(t, chunk.ID(2).LastLedger(), got,
		"empty live DB ⇒ positional baseline (max ready 3 - 1 = chunk 2), no fabricated frontier")
}
