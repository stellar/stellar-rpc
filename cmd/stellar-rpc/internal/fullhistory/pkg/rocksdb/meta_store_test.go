package rocksdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetaStore_PutGet(t *testing.T) {
	store, err := NewMetaStore(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	// Empty initially.
	val, err := store.Get("key1")
	require.NoError(t, err)
	require.Empty(t, val)

	// Put and Get.
	require.NoError(t, store.Put("key1", "value1"))
	val, err = store.Get("key1")
	require.NoError(t, err)
	require.Equal(t, "value1", val)

	// Different key is still empty.
	val, err = store.Get("key2")
	require.NoError(t, err)
	require.Empty(t, val)
}

func TestMetaStore_Overwrite(t *testing.T) {
	store, err := NewMetaStore(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.Put("k", "v1"))
	require.NoError(t, store.Put("k", "v2"))

	val, err := store.Get("k")
	require.NoError(t, err)
	require.Equal(t, "v2", val)
}

func TestMetaStore_Reopen(t *testing.T) {
	dir := t.TempDir()

	// Write, close.
	store1, err := NewMetaStore(dir)
	require.NoError(t, err)
	require.NoError(t, store1.Put("persist", "yes"))
	store1.Close()

	// Reopen, read.
	store2, err := NewMetaStore(dir)
	require.NoError(t, err)
	defer store2.Close()

	val, err := store2.Get("persist")
	require.NoError(t, err)
	require.Equal(t, "yes", val)
}
