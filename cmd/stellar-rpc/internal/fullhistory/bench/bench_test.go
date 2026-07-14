package bench

import (
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/fhtest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/ledger"
)

// eventEvery: every eventEvery-th ledger of a fixture chunk carries one
// transaction with one contract event; the rest are zero-tx ledgers.
const eventEvery = 100

func testLogger() *supportlog.Entry {
	l := supportlog.New()
	l.SetLevel(logrus.ErrorLevel)
	return l
}

// writeSourcePack materializes a source ledger pack for chunkID under
// root/ledgers (the tree --pack-dir points at), containing numLedgers ledgers
// from the chunk's first sequence: every eventEvery-th one carries a
// transaction with one contract event, the rest are zero-tx. It returns the
// ledgers tree root and the number of tx/event-bearing ledgers written.
func writeSourcePack(t *testing.T, root string, chunkID chunk.ID, numLedgers uint32) (string, int) {
	t.Helper()
	layout := geometry.NewLayout(root)
	packPath := layout.LedgerPackPath(chunkID)
	require.NoError(t, os.MkdirAll(filepath.Dir(packPath), 0o755))

	w, err := ledger.NewColdWriter(packPath, chunkID.FirstLedger(), ledger.ColdWriterOptions{})
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	txLedgers := 0
	first := chunkID.FirstLedger()
	for seq := first; seq < first+numLedgers; seq++ {
		var raw []byte
		if (seq-first)%eventEvery == 0 {
			raw = fhtest.EventLCMBytes(t, seq)
			txLedgers++
		} else {
			raw = fhtest.ZeroTxLCMBytes(t, seq)
		}
		require.NoError(t, w.AppendLedger(seq, raw))
	}
	require.NoError(t, w.Commit())
	return layout.LedgersRoot(), txLedgers
}

// readCSV parses one report file into rows keyed by stage name; each row maps
// the header column name to its integer value.
func readCSV(t *testing.T, path string) map[string]map[string]int64 {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	records, err := csv.NewReader(f).ReadAll()
	require.NoError(t, err)
	require.NotEmpty(t, records)
	header := records[0]
	rows := make(map[string]map[string]int64, len(records)-1)
	for _, rec := range records[1:] {
		row := make(map[string]int64, len(header)-1)
		for i := 1; i < len(header); i++ {
			v, perr := strconv.ParseInt(rec[i], 10, 64)
			require.NoError(t, perr)
			row[header[i]] = v
		}
		rows[rec[0]] = row
	}
	return rows
}

// TestRunColdFromPack is the end-to-end cold path: fabricate a full chunk's
// source pack, run the production WriteColdChunk through runCold with all
// three data types, and check the CSV report and the cold artifacts.
func TestRunColdFromPack(t *testing.T) {
	chunkID := chunk.ID(0)
	packDir, txLedgers := writeSourcePack(t, t.TempDir(), chunkID, chunk.LedgersPerChunk)
	outRoot := t.TempDir()
	csvDir := filepath.Join(t.TempDir(), "csv")

	err := runCold(context.Background(), testLogger(), coldOptions{
		Source:       sourceConfig{Kind: sourcePack, PackDir: packDir},
		Types:        ingest.Config{Ledgers: true, Txhash: true, Events: true},
		StartChunk:   chunkID,
		NumChunks:    1,
		ChunkWorkers: 1,
		ArtifactRoot: outRoot,
		OutDir:       csvDir,
	})
	require.NoError(t, err)

	// Ledger writes are µs-scale (zstd), so every ledger lands in the write
	// row; finalize runs once per chunk.
	ledgers := readCSV(t, filepath.Join(csvDir, "ledgers.csv"))
	require.Contains(t, ledgers, "write")
	assert.EqualValues(t, chunk.LedgersPerChunk, ledgers["write"]["n"])
	assert.EqualValues(t, chunk.LedgersPerChunk, ledgers["write"]["n_items"])
	require.Contains(t, ledgers, "finalize")
	assert.EqualValues(t, 1, ledgers["finalize"]["n"])

	// Sub-tick (zero-duration) samples are excluded from n / n_items, so
	// per-ledger txhash/events rows only bound loosely; the finalize rows are
	// per-chunk and deterministic (txhash finalize items = total hashes).
	txhash := readCSV(t, filepath.Join(csvDir, "txhash.csv"))
	require.Contains(t, txhash, "extract")
	require.Contains(t, txhash, "finalize")
	assert.EqualValues(t, 1, txhash["finalize"]["n"])
	assert.EqualValues(t, txLedgers, txhash["finalize"]["n_items"])

	events := readCSV(t, filepath.Join(csvDir, "events.csv"))
	require.Contains(t, events, "extract")
	require.Contains(t, events, "write")
	require.Contains(t, events, "finalize")
	assert.EqualValues(t, 1, events["finalize"]["n"])

	// The driver rows are per-chunk: exactly one sample each, with the
	// per-type ColdIngest item totals independent of timer granularity.
	driver := readCSV(t, filepath.Join(csvDir, "driver.csv"))
	for _, name := range []string{"chunk_wall", "chunk_total", "ledgers_total", "txhash_total", "events_total"} {
		require.Contains(t, driver, name)
		assert.EqualValues(t, 1, driver[name]["n"], name)
	}
	assert.EqualValues(t, chunk.LedgersPerChunk, driver["ledgers_total"]["n_items"])
	assert.EqualValues(t, txLedgers, driver["txhash_total"]["n_items"])
	assert.EqualValues(t, txLedgers, driver["events_total"]["n_items"])

	// The cold artifacts landed at the Layout-resolved paths.
	layout := geometry.NewLayout(outRoot)
	assert.FileExists(t, layout.LedgerPackPath(chunkID))
	assert.FileExists(t, layout.TxHashBinPath(chunkID))
	for _, p := range layout.EventsPaths(chunkID) {
		assert.FileExists(t, p)
	}
}

// TestRunColdMultiChunkWorkers exercises the chunk-worker fan-out: two chunks
// ingested with two concurrent workers against one shared sink.
func TestRunColdMultiChunkWorkers(t *testing.T) {
	srcRoot := t.TempDir()
	packDir, _ := writeSourcePack(t, srcRoot, chunk.ID(0), chunk.LedgersPerChunk)
	_, _ = writeSourcePack(t, srcRoot, chunk.ID(1), chunk.LedgersPerChunk)
	csvDir := filepath.Join(t.TempDir(), "csv")

	err := runCold(context.Background(), testLogger(), coldOptions{
		Source:       sourceConfig{Kind: sourcePack, PackDir: packDir},
		Types:        ingest.Config{Ledgers: true},
		StartChunk:   chunk.ID(0),
		NumChunks:    2,
		ChunkWorkers: 2,
		ArtifactRoot: t.TempDir(),
		OutDir:       csvDir,
	})
	require.NoError(t, err)

	driver := readCSV(t, filepath.Join(csvDir, "driver.csv"))
	assert.EqualValues(t, 2, driver["chunk_wall"]["n"])
	assert.EqualValues(t, 2, driver["chunk_total"]["n"])
	assert.EqualValues(t, 2, driver["ledgers_total"]["n"])
	assert.Equal(t, 2*int64(chunk.LedgersPerChunk), driver["ledgers_total"]["n_items"])
}

// TestRunColdRefusesInPlaceRepack asserts the source/destination collision
// guard.
func TestRunColdRefusesInPlaceRepack(t *testing.T) {
	root := t.TempDir()
	err := runCold(context.Background(), testLogger(), coldOptions{
		Source:       sourceConfig{Kind: sourcePack, PackDir: geometry.NewLayout(root).LedgersRoot()},
		Types:        ingest.Config{Ledgers: true},
		StartChunk:   chunk.ID(0),
		NumChunks:    1,
		ChunkWorkers: 1,
		ArtifactRoot: root,
		OutDir:       t.TempDir(),
	})
	require.ErrorContains(t, err, "must differ from --pack-dir")
}

// TestRunHotFromPack is the end-to-end hot path: a capped run over a fixture
// pack through the production HotService into a fresh hot RocksDB, checking
// the per-phase report and the fixed-starting-state guard.
func TestRunHotFromPack(t *testing.T) {
	const numLedgers = 200
	chunkID := chunk.ID(0)
	packDir, _ := writeSourcePack(t, t.TempDir(), chunkID, numLedgers)
	hotRoot := t.TempDir()
	csvDir := filepath.Join(t.TempDir(), "csv")

	opts := hotOptions{
		Source:     sourceConfig{Kind: sourcePack, PackDir: packDir},
		Chunk:      chunkID,
		NumLedgers: numLedgers,
		HotRoot:    hotRoot,
		OutDir:     csvDir,
	}
	require.NoError(t, runHot(context.Background(), testLogger(), opts))

	// The commit phase (WAL append + fsync) is far above timer granularity,
	// so every ledger contributes a sample; extract likewise.
	hot := readCSV(t, filepath.Join(csvDir, "hot.csv"))
	require.Contains(t, hot, "extract")
	require.Contains(t, hot, "commit")
	assert.EqualValues(t, numLedgers, hot["commit"]["n"])
	require.Contains(t, hot, "apply")

	driver := readCSV(t, filepath.Join(csvDir, "driver.csv"))
	require.Contains(t, driver, "chunk_wall")
	assert.EqualValues(t, 1, driver["chunk_wall"]["n"])
	assert.EqualValues(t, numLedgers, driver["chunk_wall"]["n_items"])
	// Every ledger's end-to-end ingest includes the fsync'd commit, so all
	// samples are non-zero and n = n_items = the ledger count.
	require.Contains(t, driver, "ingest_total")
	assert.EqualValues(t, numLedgers, driver["ingest_total"]["n"])
	assert.EqualValues(t, numLedgers, driver["ingest_total"]["n_items"])
	require.Contains(t, driver, "read_blocked")

	// A second run against the same hot root must refuse: hot timings are
	// only comparable from an empty starting state.
	err := runHot(context.Background(), testLogger(), opts)
	require.ErrorContains(t, err, "already exists")
}
