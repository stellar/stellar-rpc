package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	goxdr "github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// TestExtractTxHashes_ViewVsParsedByteEqual runs both extractors over a
// real cold-pack chunk and asserts they yield identical Entry slices
// for every ledger. Catches a regression where the view path and the
// parsed path silently diverge — the kind of bug only surface-tested
// by an end-to-end hash comparison otherwise.
//
// Fixture-dependent: skips when the pack at fixturePath isn't present.
// CI runs without the fixture will skip; dev runs with the worktree
// set up will exercise it.
func TestExtractTxHashes_ViewVsParsedByteEqual(t *testing.T) {
	const (
		fixturePath = "/mnt/nvme/ledgers/cold/00005/00005860.pack"
		sampleN     = 50 // first N ledgers of the chunk
	)
	if _, err := os.Stat(fixturePath); err != nil {
		t.Skipf("fixture pack %s not present: %v", fixturePath, err)
	}

	r, err := ledger.OpenColdReader(fixturePath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	first, err := r.FirstSeq()
	require.NoError(t, err)

	checked := 0
	for entry, iterErr := range r.IterateLedgers(first, first+sampleN-1) {
		require.NoError(t, iterErr)

		viewEntries, err := extractTxHashesView(entry.Bytes, entry.Seq)
		require.NoError(t, err)

		var lcm goxdr.LedgerCloseMeta
		require.NoError(t, lcm.UnmarshalBinary(entry.Bytes))
		parsedEntries, err := extractTxHashesParsed(lcm, entry.Seq)
		require.NoError(t, err)

		require.Equalf(t, viewEntries, parsedEntries,
			"view-vs-parsed divergence at seq %d (%d tx)", entry.Seq, len(viewEntries))
		checked++
	}
	require.Positive(t, checked, "expected to iterate at least one ledger from the fixture")
	t.Logf("ledgers checked: %d, hashes confirmed identical across view/parsed paths", checked)
}

