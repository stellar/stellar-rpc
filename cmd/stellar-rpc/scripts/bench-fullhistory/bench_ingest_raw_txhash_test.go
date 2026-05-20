package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/zstd"
)

// TestExtractTxHashes_ViewVsFullByteEqual runs both extractors over a
// real cold-pack chunk and asserts they yield identical hashes in
// identical order for every ledger. Catches a regression where
// extractTxHashesView and extractTxHashesFull silently diverge — the
// kind of bug only surface-tested by an end-to-end sha256 comparison
// otherwise.
//
// Fixture-dependent: skips when /tmp/cold-bench/00005/00005870.pack
// (the chunk we ingest in the dev workflow) isn't present. CI runs
// without the fixture will skip; dev runs with the worktree set up
// will exercise it.
func TestExtractTxHashes_ViewVsFullByteEqual(t *testing.T) {
	const (
		fixtureDir   = "/tmp/cold-bench"
		fixtureChunk = uint32(5870)
		sampleN      = 50 // first N ledgers of the chunk — enough to cover variation
	)
	packPathStr := filepath.Join(fixtureDir, "00005", "00005870.pack")
	if _, err := os.Stat(packPathStr); err != nil {
		t.Skipf("fixture pack %s not present: %v", packPathStr, err)
	}

	dec := zstd.NewDecompressor()
	r, err := ledger.NewColdStoreReader(packPathStr, dec)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	first, err := r.FirstSeq()
	require.NoError(t, err)

	collect := func(into *[][32]byte) func(uint32, []byte) {
		return func(_ uint32, h []byte) {
			var x [32]byte
			copy(x[:], h[:32])
			*into = append(*into, x)
		}
	}

	checked := 0
	for entry, iterErr := range r.IterateLedgers(first, first+sampleN-1) {
		require.NoError(t, iterErr)

		var fullHashes, viewHashes [][32]byte
		require.NoError(t, extractTxHashesFull(entry.Bytes, entry.Seq, collect(&fullHashes)))
		require.NoError(t, extractTxHashesView(entry.Bytes, entry.Seq, collect(&viewHashes)))

		require.Equalf(t, fullHashes, viewHashes,
			"view-vs-full divergence at seq %d (%d tx)", entry.Seq, len(fullHashes))
		checked++
	}
	require.Positive(t, checked, "expected to iterate at least one ledger from the fixture")
	t.Logf("ledgers checked: %d, hashes confirmed identical", checked)
}
