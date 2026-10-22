package lifecycle

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// TestDiscardHotTier_RemovesDirAndKey retires the bracket via the demote+destroy
// split a lifecycle run performs: the key is deleted and the dir is gone. A
// second destroy is a no-op.
func TestDiscardHotTier_RemovesDirAndKey(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(4)
	db := openLiveHotDB(t, cat, c)
	require.NoError(t, db.Close())

	require.NoError(t, cat.PutHotTransient(c))
	require.NoError(t, cat.DestroyHotChunk(c))

	has, err := hotKeyExists(cat, c)
	require.NoError(t, err)
	assert.False(t, has, "the hot key is deleted")
	_, statErr := os.Stat(cat.Layout().HotChunkPath(c))
	assert.True(t, os.IsNotExist(statErr), "the dir is removed")

	require.NoError(t, cat.DestroyHotChunk(c), "second destroy is a no-op")
}
