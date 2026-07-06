package lifecycle

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// writeArtifact writes a placeholder artifact file at path (creating parents),
// so a test can assert presence/absence around the catalog protocol.
func writeArtifact(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte("artifact"), 0o644))
}

// hotKeyExists reports whether chunk c has a hot:chunk key (any value). The
// catalog's key existence read is unexported; this is the streaming-package test
// shim over the public HotState ("" ⇒ absent).
func hotKeyExists(cat *catalog.Catalog, c chunk.ID) (bool, error) {
	s, err := cat.HotState(c)
	return s != "", err
}

func TestRoundTripHotKeys(t *testing.T) {
	cat, _ := testCatalog(t)

	state, err := cat.HotState(7)
	require.NoError(t, err)
	require.Equal(t, geometry.HotState(""), state)

	require.NoError(t, cat.PutHotTransient(7))
	state, err = cat.HotState(7)
	require.NoError(t, err)
	require.Equal(t, geometry.HotTransient, state)

	require.NoError(t, cat.FlipHotReady(7))
	state, err = cat.HotState(7)
	require.NoError(t, err)
	require.Equal(t, geometry.HotReady, state)

	require.NoError(t, cat.DeleteHotKey(7))
	state, err = cat.HotState(7)
	require.NoError(t, err)
	require.Equal(t, geometry.HotState(""), state)
	// Idempotent on a missing key.
	require.NoError(t, cat.DeleteHotKey(7))
}
