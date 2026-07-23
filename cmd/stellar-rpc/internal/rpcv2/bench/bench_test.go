package bench

import (
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
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
			raw = rpcv2test.EventLCMBytes(t, seq)
			txLedgers++
		} else {
			raw = rpcv2test.ZeroTxLCMBytes(t, seq)
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

// txhashIndexPath resolves where a backfill over [lo, hi] freezes its txhash
// index .idx (both chunks inside one window, as every test range here is).
func txhashIndexPath(t *testing.T, layout geometry.Layout, lo, hi chunk.ID) string {
	t.Helper()
	txLayout, err := geometry.NewTxHashIndexLayout(geometry.ChunksPerTxhashIndex)
	require.NoError(t, err)
	w := txLayout.TxHashIndexID(lo)
	require.Equal(t, w, txLayout.TxHashIndexID(hi), "test range must stay inside one index window")
	return layout.TxHashIndexFilePath(geometry.TxHashIndexCoverage{Index: w, Lo: lo, Hi: hi})
}

// TestRunColdFromPack is the end-to-end cold path: fabricate a full chunk's
// source pack, run the production backfill (RunBackfill: chunk freeze + txhash
// index build) through runCold, and check the CSV report and the cold
// artifacts.
func TestRunColdFromPack(t *testing.T) {
	chunkID := chunk.ID(0)
	packDir, txLedgers := writeSourcePack(t, t.TempDir(), chunkID, chunk.LedgersPerChunk)
	outRoot := t.TempDir()
	csvDir := filepath.Join(t.TempDir(), "csv")

	err := runCold(context.Background(), testLogger(), coldOptions{
		Source:     sourceConfig{Kind: sourcePack, PackDir: packDir},
		StartChunk: chunkID,
		NumChunks:  1,
		Workers:    1,
		ColdRoot:   outRoot,
		OutDir:     csvDir,
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
	// per-ledger events rows only bound loosely; the finalize rows are
	// per-chunk and deterministic (txhash finalize items = total hashes).
	txhash := readCSV(t, filepath.Join(csvDir, "txhash.csv"))
	require.Contains(t, txhash, "finalize")
	assert.EqualValues(t, 1, txhash["finalize"]["n"])
	assert.EqualValues(t, txLedgers, txhash["finalize"]["n_items"])

	events := readCSV(t, filepath.Join(csvDir, "events.csv"))
	require.Contains(t, events, "write")
	require.Contains(t, events, "finalize")
	assert.EqualValues(t, 1, events["finalize"]["n"])

	// The driver rows: the engine's per-chunk aggregates (one sample each,
	// with the per-type ColdIngest item totals independent of timer
	// granularity) plus the scheduler's whole-run backfill wall and the one
	// index build the range plans.
	driver := readCSV(t, filepath.Join(csvDir, "driver.csv"))
	for _, name := range []string{
		"backfill_wall", "index_rebuild", "chunk_total", "ledgers_total", "txhash_total", "events_total",
	} {
		require.Contains(t, driver, name)
		assert.EqualValues(t, 1, driver[name]["n"], name)
	}
	assert.EqualValues(t, chunk.LedgersPerChunk, driver["ledgers_total"]["n_items"])
	assert.EqualValues(t, txLedgers, driver["txhash_total"]["n_items"])
	assert.EqualValues(t, txLedgers, driver["events_total"]["n_items"])

	// The shared per-ledger ExtractLedgerEvents walk is ledger-scoped (no data
	// type), so it reports as its own driver row; per-ledger samples bound
	// loosely (sub-tick walks are excluded).
	require.Contains(t, driver, "cold_extract")

	// The rss_test.go tests use fake readers; this is the only assertion that
	// exercises the real readPeakRSS. It only works on Linux — which is what
	// CI runs — so on other platforms the read fails and the row is skipped.
	if runtime.GOOS == "linux" {
		require.Contains(t, driver, "peak_rss_bytes")
		assert.EqualValues(t, 1, driver["peak_rss_bytes"]["n"])
		assert.Positive(t, driver["peak_rss_bytes"]["total_ns"])
	}

	// The cold artifacts landed at the Layout-resolved paths — including the
	// cross-chunk txhash index the backfill builds beyond WriteColdChunk. The
	// window is partial (chunk 0 of a ChunksPerTxhashIndex-chunk window), so
	// the .bin inputs are NOT swept.
	layout := geometry.NewLayout(outRoot)
	assert.FileExists(t, layout.LedgerPackPath(chunkID))
	assert.FileExists(t, layout.TxHashBinPath(chunkID))
	for _, p := range layout.EventsPaths(chunkID) {
		assert.FileExists(t, p)
	}
	assert.FileExists(t, txhashIndexPath(t, layout, chunkID, chunkID))
}

// TestRunColdMultiChunk exercises the scheduler fan-out: two chunks backfilled
// with a two-slot worker pool against one shared sink, and one index build
// covering both.
func TestRunColdMultiChunk(t *testing.T) {
	srcRoot := t.TempDir()
	packDir, _ := writeSourcePack(t, srcRoot, chunk.ID(0), chunk.LedgersPerChunk)
	_, _ = writeSourcePack(t, srcRoot, chunk.ID(1), chunk.LedgersPerChunk)
	outRoot := t.TempDir()
	csvDir := filepath.Join(t.TempDir(), "csv")

	err := runCold(context.Background(), testLogger(), coldOptions{
		Source:     sourceConfig{Kind: sourcePack, PackDir: packDir},
		StartChunk: chunk.ID(0),
		NumChunks:  2,
		Workers:    2,
		ColdRoot:   outRoot,
		OutDir:     csvDir,
	})
	require.NoError(t, err)

	driver := readCSV(t, filepath.Join(csvDir, "driver.csv"))
	assert.EqualValues(t, 2, driver["chunk_total"]["n"])
	assert.EqualValues(t, 2, driver["ledgers_total"]["n"])
	assert.Equal(t, 2*int64(chunk.LedgersPerChunk), driver["ledgers_total"]["n_items"])
	assert.EqualValues(t, 1, driver["backfill_wall"]["n"])
	assert.EqualValues(t, 1, driver["index_rebuild"]["n"])

	layout := geometry.NewLayout(outRoot)
	for c := chunk.ID(0); c <= chunk.ID(1); c++ {
		assert.FileExists(t, layout.LedgerPackPath(c))
		assert.FileExists(t, layout.TxHashBinPath(c))
	}
	assert.FileExists(t, txhashIndexPath(t, layout, chunk.ID(0), chunk.ID(1)))
}

// TestRunColdRefusesInPlaceRepack asserts the source/destination collision
// guard.
func TestRunColdRefusesInPlaceRepack(t *testing.T) {
	root := t.TempDir()
	err := runCold(context.Background(), testLogger(), coldOptions{
		Source:     sourceConfig{Kind: sourcePack, PackDir: geometry.NewLayout(root).LedgersRoot()},
		StartChunk: chunk.ID(0),
		NumChunks:  1,
		Workers:    1,
		ColdRoot:   root,
		OutDir:     t.TempDir(),
	})
	require.ErrorContains(t, err, "must differ from --pack-dir")
}

// TestBenchRejectsInvalidSourceEarly asserts a bad --source invocation fails in
// validate(), before either driver creates its output or scratch directories.
func TestBenchRejectsInvalidSourceEarly(t *testing.T) {
	base := t.TempDir()
	coldRoot := filepath.Join(base, "cold")
	hotRoot := filepath.Join(base, "hot")
	outDir := filepath.Join(base, "csv")

	err := runCold(context.Background(), testLogger(), coldOptions{
		Source:     sourceConfig{Kind: sourcePack}, // --pack-dir missing
		StartChunk: chunk.ID(0),
		NumChunks:  1,
		Workers:    1,
		ColdRoot:   coldRoot,
		OutDir:     outDir,
	})
	require.ErrorContains(t, err, "--pack-dir is required")

	err = runHot(context.Background(), testLogger(), hotOptions{
		Source:     sourceConfig{Kind: "bogus"},
		StartChunk: chunk.ID(0),
		NumChunks:  1,
		HotRoot:    hotRoot,
		OutDir:     outDir,
	})
	require.ErrorContains(t, err, "expected pack|bsb")

	require.ErrorContains(t, sourceConfig{Kind: sourceBSB}.validate(), "--bucket-path is required")

	for _, dir := range []string{coldRoot, hotRoot, outDir} {
		require.NoDirExists(t, dir, "invalid invocation must not create %s", dir)
	}
}

// TestPackBackendMultiChunkRange exercises the pack source's chunk routing: one
// bounded RawLedgers call spanning two packs streams both, in order — what the
// hot driver relies on when a run crosses a chunk boundary.
func TestPackBackendMultiChunkRange(t *testing.T) {
	srcRoot := t.TempDir()
	packDir, _ := writeSourcePack(t, srcRoot, chunk.ID(0), chunk.LedgersPerChunk)
	_, _ = writeSourcePack(t, srcRoot, chunk.ID(1), chunk.LedgersPerChunk)

	first := chunk.ID(0).LastLedger() - 2
	last := chunk.ID(1).FirstLedger() + 2
	next := first
	for raw, err := range (packBackend{root: packDir}).RawLedgers(
		context.Background(), ledgerbackend.BoundedRange(first, last),
	) {
		require.NoError(t, err)
		require.NotEmpty(t, raw)
		next++
	}
	require.Equal(t, last+1, next, "stream must cover the whole cross-chunk range")

	// A chunk with no pack fails fast with a clear error.
	for _, err := range (packBackend{root: packDir}).RawLedgers(
		context.Background(), ledgerbackend.BoundedRange(chunk.ID(2).FirstLedger(), chunk.ID(2).FirstLedger()),
	) {
		require.ErrorContains(t, err, "stat source pack")
	}
}

// TestRunHotFromPack is the end-to-end hot path: a capped run over a fixture
// pack through the production ingestion loop into a fresh hot RocksDB,
// checking the per-phase report and the fixed-starting-state semantics.
func TestRunHotFromPack(t *testing.T) {
	const numLedgers = 200
	chunkID := chunk.ID(0)
	packDir, _ := writeSourcePack(t, t.TempDir(), chunkID, numLedgers)
	hotRoot := t.TempDir()
	csvDir := filepath.Join(t.TempDir(), "csv")

	opts := hotOptions{
		Source:     sourceConfig{Kind: sourcePack, PackDir: packDir},
		StartChunk: chunkID,
		NumChunks:  1,
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

	// The loop pulls the stream itself, so run_wall is the only row the driver
	// times directly; ingest_total is reconstructed inside the sink from each
	// ledger's HotPhase burst — one sample per ledger (items=1), giving the
	// per-ledger end-to-end latency the per-phase rows can't be summed into.
	// Every sample includes the fsync'd commit, so none is sub-tick: n counts
	// all numLedgers. (read_blocked was a bench-loop artifact and stays gone.)
	driver := readCSV(t, filepath.Join(csvDir, "driver.csv"))
	require.Contains(t, driver, "run_wall")
	assert.EqualValues(t, 1, driver["run_wall"]["n"])
	assert.EqualValues(t, numLedgers, driver["run_wall"]["n_items"])
	require.Contains(t, driver, "ingest_total")
	assert.EqualValues(t, numLedgers, driver["ingest_total"]["n"])
	assert.EqualValues(t, numLedgers, driver["ingest_total"]["n_items"])
	assert.NotContains(t, driver, "read_blocked")
	// An unpaced run must not add the pace_lag row — the CSV rows are a de
	// facto contract, and pace_lag belongs to --close-interval runs only.
	assert.NotContains(t, driver, "pace_lag")
	if runtime.GOOS == "linux" {
		require.Contains(t, driver, "peak_rss_bytes")
		assert.EqualValues(t, 1, driver["peak_rss_bytes"]["n"])
		assert.Positive(t, driver["peak_rss_bytes"]["total_ns"])
	}

	// A second run against the same hot root succeeds from a fixed (empty)
	// starting state: the production create bracket wipes the leftover DB.
	require.NoError(t, runHot(context.Background(), testLogger(), opts))
}

// TestRunHotIncompleteStream asserts an undersized source is a hard error, not
// a silently short report. A pack holding 50 ledgers covers seqs [2, 51]; asking
// for 60 makes runHot request [2, 61]. The pack stream requires the whole
// requested range to fall within its coverage, so it refuses the overshoot up
// front (stores.ErrOutOfRange, no ledgers streamed at all) and the loop surfaces
// that wrapped as "ingestion stream: ...". So it is that stream-error path that
// fires here, NOT runHot's own post-loop completion check (last-committed !=
// requested last): the loop returns the error before ingesting anything, so the
// completion check is never reached. Pin the exact refusal so an unrelated
// failure (pack-open error, config mistake) can't masquerade as this assertion.
func TestRunHotIncompleteStream(t *testing.T) {
	const packed = 50
	chunkID := chunk.ID(0)
	packDir, _ := writeSourcePack(t, t.TempDir(), chunkID, packed)

	err := runHot(context.Background(), testLogger(), hotOptions{
		Source:     sourceConfig{Kind: sourcePack, PackDir: packDir},
		StartChunk: chunkID,
		NumChunks:  1,
		NumLedgers: packed + 10, // asks for more than the pack holds
		HotRoot:    t.TempDir(),
		OutDir:     filepath.Join(t.TempDir(), "csv"),
	})
	// Seqs are fixture-determined: 50 ledgers from chunk 0 → coverage [2, 51];
	// packed+10 requested → [2, 61]. Both bounds are deterministic, so pinning
	// them is exact rather than volatile.
	require.ErrorContains(t, err, "ingestion stream: stores: out of range: "+
		"requested [2, 61] outside store coverage [2, 51]")
}

// TestRunHotPaced is the end-to-end paced hot path: a capped run with a small
// --close-interval must hold each ledger to its due time, so the whole run
// cannot finish before the last ledger comes due — at least (n−1) intervals of
// real wall time — and the driver report must carry the pace rows the paced
// mode adds. The interval is chosen well above this machine's per-ledger
// ingest cost (a few ms of fsync'd commit), so unpaced ingestion alone cannot
// satisfy the bound — only real sleeping can. It is a lower bound, so the
// timing assertion never flakes.
func TestRunHotPaced(t *testing.T) {
	const numLedgers = 20
	const interval = 25 * time.Millisecond
	chunkID := chunk.ID(0)
	packDir, _ := writeSourcePack(t, t.TempDir(), chunkID, numLedgers)
	csvDir := filepath.Join(t.TempDir(), "csv")

	start := time.Now()
	require.NoError(t, runHot(context.Background(), testLogger(), hotOptions{
		Source:        sourceConfig{Kind: sourcePack, PackDir: packDir},
		StartChunk:    chunkID,
		NumChunks:     1,
		NumLedgers:    numLedgers,
		HotRoot:       t.TempDir(),
		CloseInterval: interval,
		OutDir:        csvDir,
	}))
	// The last ledger (position numLedgers-1) yields no sooner than its due time,
	// anchor + (numLedgers-1)*interval, and the anchor is set after this start.
	assert.GreaterOrEqual(t, time.Since(start), time.Duration(numLedgers-1)*interval)

	// The paced run adds the pace_lag row: every ledger's ingest takes real
	// time, so no lag sample is zero and the row is not suppressed.
	driver := readCSV(t, filepath.Join(csvDir, "driver.csv"))
	require.Contains(t, driver, "pace_lag")
	assert.EqualValues(t, numLedgers, driver["pace_lag"]["n_items"])
}
