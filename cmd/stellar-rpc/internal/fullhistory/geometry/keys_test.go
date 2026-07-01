package geometry

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ---------------------------------------------------------------------------
// Key <-> path bijection, both directions.
// ---------------------------------------------------------------------------

func TestKeyConstructorsMatchSpec(t *testing.T) {
	require.Equal(t, "chunk:00005350:ledgers", ChunkKey(5350, KindLedgers))
	require.Equal(t, "chunk:00005350:events", ChunkKey(5350, KindEvents))
	require.Equal(t, "chunk:00005350:txhash", ChunkKey(5350, KindTxHash))
	require.Equal(t, "index:00000005:00005100:00005349", TxHashIndexKey(5, 5100, 5349))
}

func TestChunkKeyBijection(t *testing.T) {
	for _, kind := range AllKinds() {
		for _, id := range []chunk.ID{0, 1, 999, 1000, 5350, chunk.ID(MaxChunksPerTxhashIndex)} {
			key := ChunkKey(id, kind)
			gotID, gotKind, ok := ParseChunkKey(key)
			require.True(t, ok, "parse %q", key)
			require.Equal(t, id, gotID)
			require.Equal(t, kind, gotKind)
		}
	}
}

func TestIndexKeyBijection(t *testing.T) {
	cov := TxHashIndexCoverage{Index: 5, Lo: 5100, Hi: 5349}
	key := TxHashIndexKey(cov.Index, cov.Lo, cov.Hi)
	got, ok := ParseTxHashIndexKey(key)
	require.True(t, ok)
	require.Equal(t, cov.Index, got.Index)
	require.Equal(t, cov.Lo, got.Lo)
	require.Equal(t, cov.Hi, got.Hi)
	require.Equal(t, key, got.Key)
}

func TestKeyToPathBijection(t *testing.T) {
	l := NewLayout("/data")

	// The doc's directory-layout examples.
	require.Equal(t, "/data/ledgers/00005/00005350.pack", l.LedgerPackPath(5350))
	require.Equal(t, "/data/txhash/raw/00005/00005350.bin", l.TxHashBinPath(5350))
	require.Equal(t, []string{
		"/data/events/00005/00005350-events.pack",
		"/data/events/00005/00005350-index.pack",
		"/data/events/00005/00005350-index.hash",
	}, l.EventsPaths(5350))
	require.Equal(t, "/data/hot/00005350", l.HotChunkPath(5350))

	cov := TxHashIndexCoverage{Index: 5, Lo: 5100, Hi: 5349}
	require.Equal(t, "/data/txhash/index/00000005", l.TxHashIndexDir(cov.Index))
	require.Equal(t, "/data/txhash/index/00000005/00005100-00005349.idx", l.TxHashIndexFilePath(cov))
}

func TestParseRejectsMalformed(t *testing.T) {
	bad := []string{
		"chunk:5350:ledgers",             // not 8-digit padded
		"chunk:00005350:bogus",           // unknown kind
		"chunk:00005350",                 // missing kind
		"index:00000005:00005100", // too few segments
		"index:5:5100:5349",       // not padded
		"unrelated:key",                  // wrong family
	}
	for _, key := range bad {
		_, _, okChunk := ParseChunkKey(key)
		_, okIdx := ParseTxHashIndexKey(key)
		require.False(t, okChunk && okIdx, "expected %q to be rejected by all parsers", key)
	}
	// Specific rejections.
	_, _, ok := ParseChunkKey("chunk:00005350:bogus")
	require.False(t, ok)
	_, ok2 := ParseTxHashIndexKey("index:00000005:00005349:00005100") // lo > hi
	require.False(t, ok2)
}

func TestIndexKeyPanicsOnLoGreaterThanHi(t *testing.T) {
	require.Panics(t, func() { TxHashIndexKey(5, 5349, 5100) })
}

func TestHotKeyBijection(t *testing.T) {
	for _, id := range []chunk.ID{0, 7, 5350} {
		key := HotChunkKey(id)
		got, ok := ParseHotChunkKey(key)
		require.True(t, ok)
		require.Equal(t, id, got)
	}
}
