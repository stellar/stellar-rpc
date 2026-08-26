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
// txhash .bin already keyed under the old one.
func TestSecret_RejectsCorruptedPersisted(t *testing.T) {
	path := t.TempDir()

	cat, err := openKVAt(t, path)
	require.NoError(t, err)
	require.NoError(t, cat.put(catalogSecretStoreKey, "short"))
	require.NoError(t, cat.Close())

	_, err = openKVAt(t, path)
	require.Error(t, err, "a corrupted persisted secret must fail Open, not remint")
	require.ErrorContains(t, err, "persisted cold-index secret is 5 bytes")
}

// TestSecret_RejectsPersistedAllZero pins that an all-zero persisted secret is
// rejected at Open rather than adopted. It is the right LENGTH, so the
// corruption check above waves it through, but blinding under it is no
// blinding at all: every derived per-index secret becomes a constant an
// attacker can reproduce, so influenced keys can be steered into one block.
// A zeroed page from a torn write, or a deployment that never minted one, must
// fail loudly instead of silently serving an unblinded deployment.
func TestSecret_RejectsPersistedAllZero(t *testing.T) {
	path := t.TempDir()

	cat, err := openKVAt(t, path)
	require.NoError(t, err)
	var zero [32]byte
	require.NoError(t, cat.put(catalogSecretStoreKey, string(zero[:])))
	require.NoError(t, cat.Close())

	_, err = openKVAt(t, path)
	require.Error(t, err, "an all-zero persisted secret must fail Open, not be adopted")
	require.ErrorContains(t, err, "all zero")
}
