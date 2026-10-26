package bench

import (
	"bytes"
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
)

// --trace writes one row per ledger, whose total is its phases' sum.
func TestRunHotTrace(t *testing.T) {
	const numLedgers = 20
	chunkID := chunk.ID(0)
	packDir, _ := writeSourcePack(t, t.TempDir(), chunkID, numLedgers)
	csvDir := filepath.Join(t.TempDir(), "csv")
	require.NoError(t, os.MkdirAll(csvDir, 0o755))
	require.NoError(t, runHot(context.Background(), testLogger(), hotOptions{
		Source: sourceConfig{Kind: sourcePack, PackDir: packDir}, StartChunk: chunkID, NumChunks: 1,
		NumLedgers: numLedgers, HotRoot: t.TempDir(), OutDir: csvDir, Trace: true,
	}))

	f, err := os.Open(filepath.Join(csvDir, traceFile))
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	records, err := csv.NewReader(f).ReadAll()
	require.NoError(t, err)
	require.Len(t, records, numLedgers+1)
	for i, rec := range records[1:] {
		require.Equal(t, strconv.Itoa(int(chunkID.FirstLedger())+i), rec[0])
		var phases int64
		for _, v := range rec[2:8] {
			n, err := strconv.ParseInt(v, 10, 64)
			require.NoError(t, err)
			phases += n
		}
		require.Equal(t, strconv.FormatInt(phases, 10), rec[8])
	}
}

// A chunk bench ingest hot populated freezes from its hot DB into any number
// of cold datasets, each with the same artifacts.
func TestRunFreezeFromHot(t *testing.T) {
	if testing.Short() {
		t.Skip("full-chunk populate")
	}
	chunkID := chunk.ID(0)
	packDir, _ := writeSourcePack(t, t.TempDir(), chunkID, chunk.LedgersPerChunk)
	hotRoot := t.TempDir()
	require.NoError(t, runHot(context.Background(), testLogger(), hotOptions{
		Source: sourceConfig{Kind: sourcePack, PackDir: packDir}, StartChunk: chunkID, NumChunks: 1,
		HotRoot: hotRoot, OutDir: t.TempDir(),
	}))

	freeze := func() geometry.Layout {
		coldRoot, csvDir := t.TempDir(), t.TempDir()
		require.NoError(t, runFreeze(context.Background(), testLogger(), freezeOptions{
			HotRoot: hotRoot, Chunk: chunkID, ColdRoot: coldRoot, OutDir: csvDir,
		}))
		driver := readCSV(t, filepath.Join(csvDir, "driver.csv"))
		assert.EqualValues(t, 1, driver[driverChunkTotal]["n"])
		assert.NotContains(t, driver, driverColdExtract, "the freeze extracts nothing again")
		return geometry.NewLayout(coldRoot)
	}
	a, b := freeze(), freeze()
	for _, kind := range []geometry.Kind{geometry.KindLedgers, geometry.KindTxHash, geometry.KindEvents} {
		pa, pb := a.ArtifactPaths(chunkID, kind), b.ArtifactPaths(chunkID, kind)
		for i := range pa {
			wa, err := os.ReadFile(pa[i])
			require.NoError(t, err)
			wb, err := os.ReadFile(pb[i])
			require.NoError(t, err)
			require.True(t, bytes.Equal(wa, wb), "%s differs across freezes", filepath.Base(pa[i]))
		}
	}
	require.Error(t, runFreeze(context.Background(), testLogger(), freezeOptions{
		HotRoot: t.TempDir(), Chunk: chunkID, ColdRoot: t.TempDir(), OutDir: t.TempDir(),
	}), "a hot dir without the chunk")
}

// writeWindowBins writes a one-key .bin for each of chunks into dir.
func writeWindowBins(t *testing.T, dir string, chunks ...chunk.ID) {
	t.Helper()
	secret := [stores.SecretLen]byte{3}
	for _, c := range chunks {
		var key [txhash.ColdKeySize]byte
		key[0] = byte(c + 1)
		bin := txhash.ColdBin{Secret: secret, Base: chunk.ID(0).FirstLedger(), Chunk: c}
		entries := []txhash.ColdEntry{{Key: key, Seq: c.FirstLedger()}}
		rpcv2test.WriteColdBin(t, filepath.Join(dir, txhash.ColdBinName(c)), bin, entries)
	}
}

func TestRunTxindex(t *testing.T) {
	dir := t.TempDir()
	writeWindowBins(t, dir, 0, 1, 3)
	index := filepath.Join(t.TempDir(), "x.idx")
	require.NoError(t, runTxindex(context.Background(), testLogger(), txindexOptions{
		BinDir: dir, NumBins: 2, IndexOut: index, OutDir: t.TempDir(),
	}))
	require.FileExists(t, index)
	require.ErrorContains(t, runTxindex(context.Background(), testLogger(), txindexOptions{
		BinDir: dir, IndexOut: index, OutDir: t.TempDir(),
	}), "chunk gap")
}

// A hot run with --txindex-bin-dir rebuilds the window index back to back
// beside the ingest loop, and a failed build fails the run.
func TestRunHotTxindex(t *testing.T) {
	const numLedgers = 20
	chunkID := chunk.ID(0)
	packDir, _ := writeSourcePack(t, t.TempDir(), chunkID, numLedgers)
	binDir := t.TempDir()
	writeWindowBins(t, binDir, 0, 1, 3)
	csvDir := t.TempDir()
	opts := hotOptions{
		Source: sourceConfig{Kind: sourcePack, PackDir: packDir}, StartChunk: chunkID, NumChunks: 1,
		NumLedgers: numLedgers, HotRoot: t.TempDir(), CloseInterval: 10 * time.Millisecond, OutDir: csvDir,
		Txindex: hotTxindexOptions{
			BinDir: binDir, NumBins: 2, Workers: 2, Yield: true, IndexOut: filepath.Join(t.TempDir(), "x.idx"),
		},
	}
	require.NoError(t, runHot(context.Background(), testLogger(), opts))

	_, builds := readCSVTable(t, filepath.Join(csvDir, txindexFile))
	completed := 0
	for i, build := range builds {
		require.Equal(t, strconv.Itoa(i+1), build["build"])
		require.Equal(t, "2", build["keys"], "the keys of the first two .bin files")
		if build["status"] == buildOK {
			completed++
			require.NotEmpty(t, build["keys_per_s"])
			continue
		}
		require.Equal(t, buildCanceled, build["status"])
		require.Len(t, builds, i+1, "only the build in flight at the end is canceled")
		require.Empty(t, build["keys_per_s"])
	}
	require.Positive(t, completed)
	driver := readCSV(t, filepath.Join(csvDir, "driver.csv"))
	require.EqualValues(t, completed, driver[driverIndexRebuild]["n"])

	opts.HotRoot, opts.OutDir = t.TempDir(), t.TempDir()
	opts.Txindex.IndexOut = filepath.Join(t.TempDir(), "missing", "x.idx")
	require.ErrorContains(t, runHot(context.Background(), testLogger(), opts), "txindex build 1:")
	require.FileExists(t, filepath.Join(opts.OutDir, txindexFile))
}
