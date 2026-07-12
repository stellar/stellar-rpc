package bench

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
)

// TestCSVSinkExactOutput replays a fixed signal sequence from two goroutines
// and asserts the exact CSV bytes: aggregation is order-independent (sorted
// percentiles, summed totals), so the concurrent interleaving must not change
// the report. Covers the zero-duration exclusion rule, row suppression, file
// suppression (no txhash signals → no txhash.csv), and the fixed row orders.
func TestCSVSinkExactOutput(t *testing.T) {
	sink := newCSVSink()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		sink.IngestStage("events", "extract", 10, 1)
		sink.IngestStage("events", "extract", 20, 2)
		sink.IngestStage("events", "extract", 0, 5) // zero duration: excluded, items dropped
		sink.IngestStage("ledgers", "write", 5, 1)
		sink.IngestStage("ledgers", "finalize", 7, 0)
		sink.ColdIngest("ledgers", 100, 10000, nil)
		sink.HotPhase(hotchunk.PhaseExtract, 10, 0, nil)
		sink.observeDriver(driverChunkWall, 300, 0)
	}()
	go func() {
		defer wg.Done()
		sink.IngestStage("events", "extract", 30, 3)
		sink.IngestStage("events", "extract", 40, 4)
		sink.IngestStage("ledgers", "write", 5, 1)
		sink.ColdIngest("events", 50, 20, nil)
		sink.ColdChunkTotal(200)
		sink.HotPhase(hotchunk.PhaseCommit, 50, 0, nil)
		sink.HotPhase(hotchunk.PhaseCommit, 60, 3, nil)
	}()
	wg.Wait()

	outDir := t.TempDir()
	written, err := sink.writeCSVs(outDir)
	require.NoError(t, err)
	require.Len(t, written, 4)

	want := map[string]string{
		"ledgers.csv": csvHeader + "\n" +
			"write,2,2,10,5,5,5,5\n" +
			"finalize,1,0,7,7,7,7,7\n",
		"events.csv": csvHeader + "\n" +
			"extract,4,10,100,30,40,40,40\n",
		"hot.csv": csvHeader + "\n" +
			"extract,1,0,10,10,10,10,10\n" +
			"commit,2,3,110,60,60,60,60\n",
		"driver.csv": csvHeader + "\n" +
			"chunk_wall,1,0,300,300,300,300,300\n" +
			"chunk_total,1,0,200,200,200,200,200\n" +
			"ledgers_total,1,10000,100,100,100,100,100\n" +
			"events_total,1,20,50,50,50,50,50\n",
	}
	for name, content := range want {
		got, rerr := os.ReadFile(filepath.Join(outDir, name))
		require.NoError(t, rerr, name)
		assert.Equal(t, content, string(got), name)
	}
	assert.NoFileExists(t, filepath.Join(outDir, "txhash.csv"))
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

func TestParseTypes(t *testing.T) {
	cfg, err := parseTypes("ledgers,events")
	require.NoError(t, err)
	assert.True(t, cfg.Ledgers)
	assert.False(t, cfg.Txhash)
	assert.True(t, cfg.Events)

	_, err = parseTypes("ledgers,bogus")
	require.ErrorContains(t, err, "bogus")
}
