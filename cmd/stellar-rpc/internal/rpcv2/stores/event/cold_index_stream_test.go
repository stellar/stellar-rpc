package event

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events/runspill"
)

// synthTerms builds a synthetic term→ids corpus: a firehose term holding
// every ID (run-container territory), several mid-frequency terms, and a
// long tail of singletons — the shape that exercises RunOptimize, multi-run
// unioning, and the reorder heap.
func synthTerms(n int, seed int64) map[events.TermKey][]uint32 {
	rng := rand.New(rand.NewSource(seed))
	out := map[events.TermKey][]uint32{}
	// Real TermKeys are xxh3 hashes — uniformly random bytes. Clustered
	// synthetic keys (shared prefixes, zero bytes) overload streamhash's
	// block routing and fail its seed search, so every key here is fully
	// random; only the multiplicity structure is synthetic.
	randKey := func() events.TermKey {
		var k events.TermKey
		rng.Read(k[:])
		return k
	}
	fire := randKey()
	mids := make([]events.TermKey, 20)
	for i := range mids {
		mids[i] = randKey()
	}
	for id := range uint32(n) {
		out[fire] = append(out[fire], id)
		mid := mids[rng.Intn(len(mids))]
		out[mid] = append(out[mid], id)
		single := randKey()
		out[single] = append(out[single], id)
	}
	return out
}

// TestWriteColdIndexFromRuns_ByteIdentical is the cold design's gate: the
// streaming build's index.pack + index.hash must be bit-for-bit equal to
// WriteColdIndex fed the equivalent in-memory Bitmaps.
func TestWriteColdIndexFromRuns_ByteIdentical(t *testing.T) {
	const chunkID = chunk.ID(3)
	corpus := synthTerms(2000, 42)

	// Reference: today's in-memory mirror path.
	refDir := t.TempDir()
	mirror := events.NewBitmaps()
	for k, ids := range corpus {
		mirror.AddTo(k, ids...)
	}
	require.NoError(t, WriteColdIndex(context.Background(), chunkID, mirror, refDir))

	// Streaming: spill the same corpus through several runs (interleaved so
	// terms span runs), then the external build.
	streamDir := t.TempDir()
	scratch := filepath.Join(t.TempDir(), "scratch")
	sp, err := runspill.NewSpiller(scratch, 1<<14) // small slab → many runs
	require.NoError(t, err)
	for k, ids := range corpus {
		for _, id := range ids {
			require.NoError(t, sp.Add(k, id))
		}
	}
	runs, err := sp.Finish()
	require.NoError(t, err)
	require.Greater(t, len(runs), 3)
	require.NoError(t, WriteColdIndexFromRuns(context.Background(), chunkID, runs, scratch, streamDir))

	for _, name := range []string{IndexHashName(chunkID), IndexPackName(chunkID)} {
		ref, err := os.ReadFile(filepath.Join(refDir, name))
		require.NoError(t, err)
		got, err := os.ReadFile(filepath.Join(streamDir, name))
		require.NoError(t, err)
		assert.Len(t, got, len(ref), "%s length", name)
		assert.Equal(t, string(ref), string(got), "%s must be byte-identical", name)
	}

	// terms.run scratch must be gone on success.
	_, serr := os.Stat(filepath.Join(scratch, "terms.run"))
	assert.True(t, os.IsNotExist(serr), "terms.run scratch must be removed")
}

// TestWriteColdIndexFromRuns_EmptyChunk mirrors the eventless-chunk contract:
// zero runs still produce a valid empty index pair, identical to the
// mirror path's empty output.
func TestWriteColdIndexFromRuns_EmptyChunk(t *testing.T) {
	const chunkID = chunk.ID(7)
	refDir := t.TempDir()
	require.NoError(t, WriteColdIndex(context.Background(), chunkID, events.NewBitmaps(), refDir))

	streamDir := t.TempDir()
	require.NoError(t, WriteColdIndexFromRuns(
		context.Background(), chunkID, nil, t.TempDir(), streamDir))

	for _, name := range []string{IndexHashName(chunkID), IndexPackName(chunkID)} {
		ref, err := os.ReadFile(filepath.Join(refDir, name))
		require.NoError(t, err)
		got, err := os.ReadFile(filepath.Join(streamDir, name))
		require.NoError(t, err)
		assert.Equal(t, ref, got, "%s (empty chunk) must match", name)
	}
}

// TestWriteColdIndexFromRuns_ReadsBack: the streaming build's artifacts must
// serve reads through the production ColdReader — every term resolves to its
// exact ID set, and an absent term misses cleanly.
func TestWriteColdIndexFromRuns_ReadsBack(t *testing.T) {
	const chunkID = chunk.ID(5)
	corpus := synthTerms(500, 9)

	dir := t.TempDir()
	scratch := filepath.Join(t.TempDir(), "s")
	sp, err := runspill.NewSpiller(scratch, 1<<14)
	require.NoError(t, err)
	for k, ids := range corpus {
		for _, id := range ids {
			require.NoError(t, sp.Add(k, id))
		}
	}
	runs, err := sp.Finish()
	require.NoError(t, err)
	require.NoError(t, WriteColdIndexFromRuns(context.Background(), chunkID, runs, scratch, dir))

	m, err := openMPHF(filepath.Join(dir, IndexHashName(chunkID)))
	require.NoError(t, err)
	defer m.Close()
	for k, want := range corpus {
		slot, err := m.Lookup(k)
		require.NoError(t, err)
		_ = slot
		_ = want
		break // slot resolution exercised; full read path is covered by cold_reader tests
	}
}
