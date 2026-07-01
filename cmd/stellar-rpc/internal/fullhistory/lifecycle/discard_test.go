package lifecycle

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// TestDiscardHotTier_RemovesDirAndKey retires the bracket: the key is deleted
// and the dir is gone. A second discard is a no-op.
func TestDiscardHotTier_RemovesDirAndKey(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(4)
	db := openLiveHotDB(t, cat, c)
	require.NoError(t, db.Close())

	require.NoError(t, discardHotDBForChunk(cat, c))

	has, err := hotKeyExists(cat, c)
	require.NoError(t, err)
	assert.False(t, has, "the hot key is deleted")
	_, statErr := os.Stat(cat.Layout().HotChunkPath(c))
	assert.True(t, os.IsNotExist(statErr), "the dir is removed")

	require.NoError(t, discardHotDBForChunk(cat, c), "second discard is a no-op")
}
