package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrepareRoots_CreatesMissingRoots(t *testing.T) {
	base := t.TempDir()
	a := filepath.Join(base, "deep", "ledgers")
	b := filepath.Join(base, "events")

	require.NoError(t, PrepareRoots(a, b, "")) // empty roots are skipped

	for _, dir := range []string{a, b} {
		info, err := os.Stat(dir)
		require.NoError(t, err)
		assert.True(t, info.IsDir())
	}
}

func TestPrepareRoots_LeavesExistingRootsUntouched(t *testing.T) {
	root := t.TempDir()
	marker := filepath.Join(root, "existing-file")
	require.NoError(t, os.WriteFile(marker, []byte("x"), 0o644))

	require.NoError(t, PrepareRoots(root, root)) // duplicate spelling de-duplicated

	_, err := os.Stat(marker)
	require.NoError(t, err, "existing contents survive PrepareRoots")
}
