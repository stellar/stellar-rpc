package catalog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSecret_MintedOnceAndStable pins Secret()'s persistence contract: Open
// mints a non-zero secret on first open, and reopening the same catalog returns
// the identical bytes. A silent remint would make previously keyed txhash .bin
// files incompatible with later index builds, so this is a load-bearing
// invariant, not a nicety.
func TestSecret_MintedOnceAndStable(t *testing.T) {
	path := t.TempDir()

	cat, err := openKVAt(t, path)
	require.NoError(t, err)
	first := cat.Secret()
	require.NotEqual(t, [32]byte{}, first, "Open must mint a non-zero secret")
	require.NoError(t, cat.Close())

	reopened, err := openKVAt(t, path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	require.Equal(t, first, reopened.Secret(), "reopening the same catalog returns the identical secret")
}

// TestSecret_RejectsCorruptedPersisted pins that a wrong-length persisted
// secret fails Open loudly instead of silently reminting: a truncated write or
// downgraded encoding must not swap in a fresh secret, which would orphan every
// txhash .bin already keyed under the old one. The census catches it (before
// the mint), and its message must not print the secret's value.
func TestSecret_RejectsCorruptedPersisted(t *testing.T) {
	path := t.TempDir()

	cat, err := openKVAt(t, path)
	require.NoError(t, err)
	require.NoError(t, cat.put(catalogSecretStoreKey, "short"))
	require.NoError(t, cat.Close())

	_, err = openKVAt(t, path)
	require.Error(t, err, "a corrupted persisted secret must fail Open, not remint")
	require.ErrorIs(t, err, ErrForeignCatalog)
	require.ErrorContains(t, err, "value is 5 bytes, want 32")
	require.NotContains(t, err.Error(), "short", "the secret's value must never appear in the error")
}
