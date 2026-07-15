package bench

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
)

// TestCSVSinkExactOutput replays a fixed signal sequence from two goroutines
// and asserts the exact CSV bytes: aggregation is order-independent (sorted
// percentiles, summed totals), so the concurrent interleaving must not change
// the report. Covers both sink interfaces (ingest.MetricSink and the
// observability.Metrics signals RunBackfill emits), the zero-duration
// exclusion rule, row suppression, file suppression (no per-type txhash or
// events stage signals → no txhash.csv / events.csv), and the fixed row
// orders.
func TestCSVSinkExactOutput(t *testing.T) {
	sink := newCSVSink()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		sink.ColdExtract(10, 1, nil)
		sink.ColdExtract(20, 2, nil)
		sink.ColdExtract(0, 5, nil) // zero duration: excluded, items dropped
		sink.IngestStage("ledgers", "write", 5, 1)
		sink.IngestStage("ledgers", "finalize", 7, 0)
		sink.ColdIngest("ledgers", 100, 10000, nil)
		sink.HotPhase(hotchunk.PhaseExtract, 10, 0, nil)
		sink.Freeze(300)
	}()
	go func() {
		defer wg.Done()
		sink.ColdExtract(30, 3, nil)
		sink.ColdExtract(40, 4, nil)
		sink.IngestStage("ledgers", "write", 5, 1)
		sink.ColdIngest("events", 50, 20, nil)
		sink.ColdChunkTotal(200)
		sink.Rebuild(40)
		sink.HotPhase(hotchunk.PhaseCommit, 50, 0, nil)
		sink.HotPhase(hotchunk.PhaseCommit, 60, 3, nil)
	}()
	wg.Wait()

	outDir := t.TempDir()
	written, err := sink.writeCSVs(outDir)
	require.NoError(t, err)
	require.Len(t, written, 3)

	want := map[string]string{
		"ledgers.csv": csvHeader + "\n" +
			"write,2,2,10,5,5,5,5\n" +
			"finalize,1,0,7,7,7,7,7\n",
		"hot.csv": csvHeader + "\n" +
			"extract,1,0,10,10,10,10,10\n" +
			"commit,2,3,110,60,60,60,60\n",
		"driver.csv": csvHeader + "\n" +
			"backfill_wall,1,0,300,300,300,300,300\n" +
			"index_rebuild,1,0,40,40,40,40,40\n" +
			"chunk_total,1,0,200,200,200,200,200\n" +
			"ledgers_total,1,10000,100,100,100,100,100\n" +
			"events_total,1,20,50,50,50,50,50\n" +
			"cold_extract,4,10,100,30,40,40,40\n",
	}
	for name, content := range want {
		got, rerr := os.ReadFile(filepath.Join(outDir, name))
		require.NoError(t, rerr, name)
		assert.Equal(t, content, string(got), name)
	}
	assert.NoFileExists(t, filepath.Join(outDir, "txhash.csv"))
	assert.NoFileExists(t, filepath.Join(outDir, "events.csv"))
}

// TestCSVSinkHotIngestTotal drives the ingest_total reconstruction directly:
// two complete per-ledger HotPhase bursts (extract→ledgers→txhash→events→
// commit→apply) plus one FAILED burst (extract, then a phase carrying an error,
// no apply). Only PhaseApply — the terminal, success-only phase — emits an
// ingest_total sample, so the failed burst contributes nothing; each complete
// burst contributes one sample (items=1) whose duration is the sum of that
// burst's phases.
func TestCSVSinkHotIngestTotal(t *testing.T) {
	sink := newCSVSink()

	// burst plays one complete per-ledger phase burst, each phase 1ns longer
	// than the last, so its six phases sum to base*6+21ns.
	burst := func(base time.Duration) {
		sink.HotPhase(hotchunk.PhaseExtract, base+1, 0, nil)
		sink.HotPhase(hotchunk.PhaseLedgers, base+2, 1, nil)
		sink.HotPhase(hotchunk.PhaseTxhash, base+3, 4, nil)
		sink.HotPhase(hotchunk.PhaseEvents, base+4, 2, nil)
		sink.HotPhase(hotchunk.PhaseCommit, base+5, 0, nil)
		sink.HotPhase(hotchunk.PhaseApply, base+6, 0, nil)
	}
	burst(100) // 6*100 + 21 = 621
	burst(200) // 6*200 + 21 = 1221

	// A failed ledger: extract ran, then PhaseCommit failed — no apply, so no
	// ingest_total sample.
	sink.HotPhase(hotchunk.PhaseExtract, 10, 0, nil)
	sink.HotPhase(hotchunk.PhaseCommit, 20, 0, errors.New("commit failed"))

	outDir := t.TempDir()
	_, err := sink.writeCSVs(outDir)
	require.NoError(t, err)

	driver := readCSV(t, filepath.Join(outDir, "driver.csv"))
	require.Contains(t, driver, "ingest_total")
	assert.EqualValues(t, 2, driver["ingest_total"]["n"])
	assert.EqualValues(t, 2, driver["ingest_total"]["n_items"])
	assert.EqualValues(t, 621+1221, driver["ingest_total"]["total_ns"])
}

// TestCSVSinkEmpty asserts a sink with no signals writes no files.
func TestCSVSinkEmpty(t *testing.T) {
	outDir := t.TempDir()
	written, err := newCSVSink().writeCSVs(outDir)
	require.NoError(t, err)
	assert.Empty(t, written)
	entries, err := os.ReadDir(outDir)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

// TestCSVSinkLastCommitted pins the gauge semantics the hot driver's
// completion check relies on: latest value wins, and dropped observability
// signals stay dropped.
func TestCSVSinkLastCommitted(t *testing.T) {
	sink := newCSVSink()
	assert.Zero(t, sink.lastCommittedSeq())
	sink.LastCommitted(41)
	sink.LastCommitted(42)
	assert.EqualValues(t, 42, sink.lastCommittedSeq())

	// Dropped signals must not fabricate CSV rows.
	sink.RetentionFloor(7)
	sink.ChunkBoundary()
	sink.LiveHotChunks(3)
	sink.BackfillPass(10)
	sink.Discard(1, 10)
	sink.Prune(2, 10)
	written, err := sink.writeCSVs(t.TempDir())
	require.NoError(t, err)
	assert.Empty(t, written)
}
