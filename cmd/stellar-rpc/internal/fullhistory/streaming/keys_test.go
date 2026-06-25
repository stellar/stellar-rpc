package streaming

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ---------------------------------------------------------------------------
// Key <-> path bijection, both directions.
// ---------------------------------------------------------------------------

func TestKeyConstructorsMatchSpec(t *testing.T) {
	require.Equal(t, "chunk:00005350:ledgers", chunkKey(5350, KindLedgers))
	require.Equal(t, "chunk:00005350:events", chunkKey(5350, KindEvents))
	require.Equal(t, "chunk:00005350:txhash", chunkKey(5350, KindTxHash))
	require.Equal(t, "txhash_index:00000005:00005100:00005349", txhashIndexKey(5, 5100, 5349))
}

func TestChunkKeyBijection(t *testing.T) {
	for _, kind := range AllKinds() {
		for _, id := range []chunk.ID{0, 1, 999, 1000, 5350, chunk.ID(MaxChunksPerTxhashIndex)} {
			key := chunkKey(id, kind)
			gotID, gotKind, ok := parseChunkKey(key)
			require.True(t, ok, "parse %q", key)
			require.Equal(t, id, gotID)
			require.Equal(t, kind, gotKind)
		}
	}
}

func TestIndexKeyBijection(t *testing.T) {
	cov := TxHashIndexCoverage{Index: 5, Lo: 5100, Hi: 5349}
	key := txhashIndexKey(cov.Index, cov.Lo, cov.Hi)
	got, ok := parseTxHashIndexKey(key)
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
		"txhash_index:00000005:00005100", // too few segments
		"txhash_index:5:5100:5349",       // not padded
		"unrelated:key",                  // wrong family
	}
	for _, key := range bad {
		_, _, okChunk := parseChunkKey(key)
		_, okIdx := parseTxHashIndexKey(key)
		require.False(t, okChunk && okIdx, "expected %q to be rejected by all parsers", key)
	}
	// Specific rejections.
	_, _, ok := parseChunkKey("chunk:00005350:bogus")
	require.False(t, ok)
	_, ok2 := parseTxHashIndexKey("txhash_index:00000005:00005349:00005100") // lo > hi
	require.False(t, ok2)
}

func TestIndexKeyPanicsOnLoGreaterThanHi(t *testing.T) {
	require.Panics(t, func() { txhashIndexKey(5, 5349, 5100) })
}

// ---------------------------------------------------------------------------
// Round-trip every key family through the real metastore.
// ---------------------------------------------------------------------------

func TestRoundTripChunkKeys(t *testing.T) {
	cat, _ := testCatalog(t)

	for _, kind := range AllKinds() {
		state, err := cat.State(42, kind)
		require.NoError(t, err)
		require.Equal(t, State(""), state, "absent key reads as empty State")
	}

	require.NoError(t, cat.MarkChunkFreezing(42, AllKinds()...))
	for _, kind := range AllKinds() {
		state, err := cat.State(42, kind)
		require.NoError(t, err)
		require.Equal(t, StateFreezing, state)
	}

	require.NoError(t, cat.FlipChunkFrozen(42, AllKinds()...))
	for _, kind := range AllKinds() {
		state, err := cat.State(42, kind)
		require.NoError(t, err)
		require.Equal(t, StateFrozen, state)
	}
}

func TestRoundTripIndexKey(t *testing.T) {
	cat, _ := testCatalog(t)

	cov, err := cat.MarkTxHashIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	require.Equal(t, StateFreezing, cov.State)

	keys, err := cat.TxHashIndexKeys(5)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, StateFreezing, keys[0].State)
	require.Equal(t, chunk.ID(5100), keys[0].Lo)
	require.Equal(t, chunk.ID(5349), keys[0].Hi)
}
