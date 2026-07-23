package lifecycle

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// TestStartupSweep pins that a crashed run's leftover demotions are destroyed
// before serving — a transient hot chunk below the live one and a pruning cold
// artifact — while a transient key AT the live chunk (a mid-create leftover) is
// left for openHotDBForChunk to recreate.
func TestStartupSweep(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 1)
	const live chunk.ID = 10

	// Mid-discard leftover: a transient hot chunk below the live one.
	makeReadyHotDirNoData(t, cat, 3)
	require.NoError(t, cat.PutHotTransient(3))

	// Mid-create leftover at the live chunk: must survive the sweep.
	require.NoError(t, cat.PutHotTransient(live))

	// A pruned cold artifact left demoted.
	freezeKinds(t, cat, 2, geometry.KindLedgers)
	writeArtifact(t, cat.Layout().LedgerPackPath(2))
	require.NoError(t, cat.DemoteChunkArtifacts(
		[]catalog.ArtifactRef{{Chunk: 2, Kind: geometry.KindLedgers, State: geometry.StateFrozen}}))

	require.NoError(t, StartupSweep(cat, live))

	has3, err := hotKeyExists(cat, 3)
	require.NoError(t, err)
	assert.False(t, has3, "transient hot chunk below live is swept")

	hasLive, err := hotKeyExists(cat, live)
	require.NoError(t, err)
	assert.True(t, hasLive, "transient key at the live chunk is a mid-create leftover, not swept")

	st, err := cat.State(2, geometry.KindLedgers)
	require.NoError(t, err)
	assert.Equal(t, geometry.State(""), st, "pruning cold key is swept")
	assert.NoFileExists(t, cat.Layout().LedgerPackPath(2), "pruning cold file is swept")
}
