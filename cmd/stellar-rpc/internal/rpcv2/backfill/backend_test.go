package backfill

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"
)

const (
	testnetPassphrase = "Test SDF Network ; September 2015"
	pubnetPassphrase  = "Public Global Stellar Network ; September 2015"
)

// filesystemLake creates a local lake directory whose manifest (.config.json)
// names the given network passphrase, and returns a datastore config pointing
// at it. An empty passphrase writes no manifest (a manifest-less lake).
func filesystemLake(t *testing.T, manifestPassphrase string) datastore.DataStoreConfig {
	t.Helper()
	dir := t.TempDir()
	if manifestPassphrase != "" {
		manifest := fmt.Sprintf(
			`{"networkPassphrase":%q,"version":"1.0","ledgersPerBatch":1,"batchesPerPartition":64000}`,
			manifestPassphrase)
		require.NoError(t, os.WriteFile(filepath.Join(dir, ".config.json"), []byte(manifest), 0o644))
	}
	return datastore.DataStoreConfig{
		Type:   "Filesystem",
		Params: map[string]string{"destination_path": dir},
	}
}

func TestNewBSBBackendFromConfig_RejectsWrongNetworkLake(t *testing.T) {
	dsCfg := filesystemLake(t, testnetPassphrase)
	dsCfg.NetworkPassphrase = pubnetPassphrase

	_, _, err := NewBSBBackendFromConfig(context.Background(), dsCfg,
		ledgerbackend.BufferedStorageBackendConfig{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "verify datastore manifest")
	assert.Contains(t, err.Error(), "networkPassphrase")
}

func TestNewBSBBackendFromConfig_AcceptsMatchingNetworkLake(t *testing.T) {
	dsCfg := filesystemLake(t, pubnetPassphrase)
	dsCfg.NetworkPassphrase = pubnetPassphrase

	backend, release, err := NewBSBBackendFromConfig(context.Background(), dsCfg,
		ledgerbackend.BufferedStorageBackendConfig{})
	require.NoError(t, err)
	require.NotNil(t, backend)
	release()
}

func TestNewBSBBackendFromConfig_EmptyPassphraseSkipsManifestCheck(t *testing.T) {
	// Injected test cores carry no passphrase; the check must not fire even
	// against a lake whose manifest names some network.
	dsCfg := filesystemLake(t, testnetPassphrase)

	backend, release, err := NewBSBBackendFromConfig(context.Background(), dsCfg,
		ledgerbackend.BufferedStorageBackendConfig{})
	require.NoError(t, err)
	require.NotNil(t, backend)
	release()
}

func TestNewBSBBackendFromConfig_RejectsManifestWithoutPassphrase(t *testing.T) {
	// A manifest that exists but names no network fails the check (fail fast:
	// empty vs configured non-empty is a mismatch, not a skip).
	dsCfg := filesystemLake(t, "")
	manifest := `{"networkPassphrase":"","version":"1.0","ledgersPerBatch":1,"batchesPerPartition":64000}`
	require.NoError(t, os.WriteFile(
		filepath.Join(dsCfg.Params["destination_path"], ".config.json"), []byte(manifest), 0o644))
	dsCfg.NetworkPassphrase = pubnetPassphrase

	_, _, err := NewBSBBackendFromConfig(context.Background(), dsCfg,
		ledgerbackend.BufferedStorageBackendConfig{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "networkPassphrase")
}

func TestNewBSBBackendFromConfig_ManifestlessLakeNeedsSchema(t *testing.T) {
	dsCfg := filesystemLake(t, "")
	dsCfg.NetworkPassphrase = pubnetPassphrase

	t.Run("schema in config: check skipped, lake accepted", func(t *testing.T) {
		cfg := dsCfg
		cfg.Schema = datastore.DataStoreSchema{LedgersPerFile: 1, FilesPerPartition: 64000}
		backend, release, err := NewBSBBackendFromConfig(context.Background(), cfg,
			ledgerbackend.BufferedStorageBackendConfig{})
		require.NoError(t, err)
		require.NotNil(t, backend)
		release()
	})

	t.Run("no schema anywhere: rejected", func(t *testing.T) {
		_, _, err := NewBSBBackendFromConfig(context.Background(), dsCfg,
			ledgerbackend.BufferedStorageBackendConfig{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "manifest")
	})
}
