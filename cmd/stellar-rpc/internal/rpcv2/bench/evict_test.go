package bench

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A missing artifact is skipped, not an error.
func TestEvictColdArtifactsSkipsMissingFile(t *testing.T) {
	if !evictSupported {
		t.Skip("page-cache eviction is Linux only")
	}
	dir := t.TempDir()
	present := filepath.Join(dir, "present.pack")
	require.NoError(t, os.WriteFile(present, []byte("ledgers"), 0o600))
	missing := filepath.Join(dir, "missing.pack")

	ds := &queryDataset{EvictPaths: []string{missing, present}}
	evicted, err := ds.evictColdArtifacts()
	require.NoError(t, err)
	assert.Equal(t, 1, evicted, "the present file is advised, the missing one skipped")
}
