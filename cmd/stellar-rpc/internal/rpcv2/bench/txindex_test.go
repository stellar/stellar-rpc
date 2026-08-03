package bench

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
)

// writeFixtureBin writes chunk c's synthetic sorted .bin with n entries.
func writeFixtureBin(t *testing.T, dir string, c chunk.ID, n int) {
	t.Helper()
	entries := make([]txhash.ColdEntry, n)
	state := uint64(0xBEEF) + uint64(c)*0x9E3779B97F4A7C15
	for i := range entries {
		for w := range 2 {
			state ^= state << 13
			state ^= state >> 7
			state ^= state << 17
			for b := range 8 {
				entries[i].Key[w*8+b] = byte(state >> (56 - 8*b))
			}
		}
		entries[i].Seq = c.FirstLedger() + uint32(state%uint64(chunk.LedgersPerChunk))
	}
	slices.SortFunc(entries, func(a, b txhash.ColdEntry) int { return bytes.Compare(a.Key[:], b.Key[:]) })
	require.NoError(t, txhash.WriteColdBin(filepath.Join(dir, fmt.Sprintf("%08d.bin", uint32(c))), entries))
}

// TestRunTxindex: a three-chunk window builds through the production
// BuildColdIndex call and reports the index_rebuild row.
func TestRunTxindex(t *testing.T) {
	binDir := t.TempDir()
	for c := chunk.ID(0); c <= 2; c++ {
		writeFixtureBin(t, binDir, c, 1000)
	}
	csvDir := filepath.Join(t.TempDir(), "csv")
	idxPath := filepath.Join(t.TempDir(), "window.idx")
	require.NoError(t, runTxindex(context.Background(), testLogger(), txindexOptions{
		BinDir:   binDir,
		IndexOut: idxPath,
		OutDir:   csvDir,
	}))
	st, err := os.Stat(idxPath)
	require.NoError(t, err)
	require.Positive(t, st.Size())
	driver := readCSV(t, filepath.Join(csvDir, "driver.csv"))
	require.Contains(t, driver, "index_rebuild")
}

// TestRunTxindex_NumBinsCap: the cap narrows the window from the front.
func TestRunTxindex_NumBinsCap(t *testing.T) {
	binDir := t.TempDir()
	for c := chunk.ID(0); c <= 4; c++ {
		writeFixtureBin(t, binDir, c, 500)
	}
	require.NoError(t, runTxindex(context.Background(), testLogger(), txindexOptions{
		BinDir:   binDir,
		NumBins:  2,
		IndexOut: filepath.Join(t.TempDir(), "w.idx"),
		OutDir:   filepath.Join(t.TempDir(), "csv"),
	}))
}

// TestRunTxindex_RejectsGap: a missing chunk in the window is a hard error,
// not a silently narrower coverage.
func TestRunTxindex_RejectsGap(t *testing.T) {
	binDir := t.TempDir()
	writeFixtureBin(t, binDir, 0, 100)
	writeFixtureBin(t, binDir, 2, 100) // gap at 1
	err := runTxindex(context.Background(), testLogger(), txindexOptions{
		BinDir:   binDir,
		IndexOut: filepath.Join(t.TempDir(), "w.idx"),
		OutDir:   filepath.Join(t.TempDir(), "csv"),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "chunk gap")
}
