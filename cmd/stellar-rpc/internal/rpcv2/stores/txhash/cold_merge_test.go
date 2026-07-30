package txhash

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMergeTree_FanInSortedAndComplete drives buildMergeTree with
// numLeaves > mergeFanIn directly, so the multi-level fan-in tree is
// exercised regardless of NumCPU. (The production cap, maxMergeLeaves,
// can fall below mergeFanIn on small-core hosts, so the BuildColdIndex
// tests alone don't guarantee fan-in coverage.) It verifies the merged
// stream is globally key-sorted and contains every entry exactly once.
func TestMergeTree_FanInSortedAndComplete(t *testing.T) {
	dir := t.TempDir()
	entries := makeFixtureEntries(600)
	inputs := writeFixtureBins(t, dir, entries)
	require.Greater(t, len(inputs), mergeFanIn, "fixture must span > mergeFanIn files to build a tree")

	m := newMerger(context.Background())
	defer m.stop()
	// numLeaves == file count forces one leaf per file; with > mergeFanIn
	// leaves, buildMergeTree adds at least one intermediate fan-in level.
	finalCh, finalPool := m.buildMergeTree(inputs, len(inputs), mergeFileBufBytes)

	var (
		got            int
		prevK0, prevK1 uint64
		havePrev       bool
	)
	for batch := range finalCh {
		data := batch.data[:batch.count*coldBinEntrySize]
		for off := 0; off < len(data); off += coldBinEntrySize {
			k0 := binary.BigEndian.Uint64(data[off:])
			k1 := binary.BigEndian.Uint64(data[off+8:])
			if havePrev {
				// Keys are unique (16-byte prefix), so the merged order
				// must be strictly increasing by (k0, k1).
				require.Truef(t, k0 > prevK0 || (k0 == prevK0 && k1 > prevK1),
					"merged stream not strictly key-sorted at entry %d", got)
			}
			prevK0, prevK1, havePrev = k0, k1, true
			got++
		}
		require.True(t, m.send(finalPool, batch))
	}
	require.NoError(t, m.firstErr())
	require.Equal(t, len(entries), got, "every entry must appear exactly once in the merged stream")
}
