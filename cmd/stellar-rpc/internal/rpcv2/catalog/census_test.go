package catalog

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
)

// reopenAfter opens a catalog at path, applies mutate, closes it, and returns
// the error from reopening — the census's verdict on the mutated store.
func reopenAfter(t *testing.T, mutate func(c *Catalog)) error {
	t.Helper()
	path := t.TempDir()
	cat, err := openKVAt(t, path)
	require.NoError(t, err)
	mutate(cat)
	require.NoError(t, cat.Close())
	reopened, err := openKVAt(t, path)
	if reopened != nil {
		t.Cleanup(func() { _ = reopened.Close() })
	}
	return err
}

// TestCensus_AcceptsOwnVocabulary pins that every key and value this binary
// writes — all three state families in every state, the pin, and the minted
// secret — reopens cleanly. This is the whole release-1 write surface.
func TestCensus_AcceptsOwnVocabulary(t *testing.T) {
	err := reopenAfter(t, func(c *Catalog) {
		for i, s := range geometry.AllStates() {
			id := chunk.ID(i)
			for _, kind := range geometry.AllKinds() {
				require.NoError(t, c.put(geometry.ChunkKey(id, kind), string(s)))
			}
			// Coverage endpoints must lie in the key's own index window.
			first := chunk.ID(i) * chunk.ID(geometry.ChunksPerTxhashIndex)
			idxKey := geometry.TxHashIndexKey(geometry.TxHashIndexID(i), first, first+5)
			require.NoError(t, c.put(idxKey, string(s)))
		}
		for i, s := range geometry.AllHotStates() {
			require.NoError(t, c.put(geometry.HotChunkKey(chunk.ID(7+i)), string(s)))
		}
		require.NoError(t, c.PinEarliestLedger(2))
	})
	require.NoError(t, err)
}

// TestCensus_AcceptsFirstStartResidues pins the crash-shaped first-start
// catalogs: secret-only (crash after Open, before the pin) and secret+pin.
func TestCensus_AcceptsFirstStartResidues(t *testing.T) {
	require.NoError(t, reopenAfter(t, func(*Catalog) {}), "secret-only catalog")
	require.NoError(t, reopenAfter(t, func(c *Catalog) {
		require.NoError(t, c.PinEarliestLedger(10002))
	}), "secret+pin catalog")
}

// TestCensus_RefusesForeignEntries walks the refusal matrix: every entry shape
// a newer binary (or corruption) could leave must fail reopen with
// ErrForeignCatalog and an actionable message.
func TestCensus_RefusesForeignEntries(t *testing.T) {
	cases := []struct {
		name, key, value string
	}{
		{"novel prefix", "format:events", "2"},
		{"novel meta key", "meta/other", "WOULD-BE-SECRET-BYTES"},
		{"unknown kind under chunk prefix", "chunk:00000001:bogus", "frozen"},
		{"version suffix on chunk state", geometry.ChunkKey(1, geometry.KindEvents), "frozen@2"},
		{"version suffix on hot state", geometry.HotChunkKey(3), "ready@2"},
		{"version suffix on index state", geometry.TxHashIndexKey(0, 0, 9), "frozen@2"},
		{"unknown hot state", geometry.HotChunkKey(4), "warm"},
		{"unpadded chunk id", "chunk:123:ledgers", "frozen"},
		{"index lo above hi", "index:00000000:00000005:00000002", "frozen"},
		{"cross-window index coverage", "index:00000001:00000000:00003000", "frozen"},
		{"non-canonical pin", geometry.ConfigEarliestLedger, "007"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := reopenAfter(t, func(c *Catalog) {
				require.NoError(t, c.put(tc.key, tc.value))
			})
			require.ErrorIs(t, err, ErrForeignCatalog)
			require.ErrorContains(t, err, "deploy that version or newer")
			require.ErrorContains(t, err, tc.key)
			require.NotContains(t, err.Error(), "WOULD-BE-SECRET-BYTES",
				"an unknown key's value must never be printed; it may be a newer binary's key material")
		})
	}
}

// TestCensus_CapsDetailButCountsAll pins the refusal shape on a store with
// more offenders than the detail cap: every offender is counted, only the
// first censusMaxDetailed are spelled out.
func TestCensus_CapsDetailButCountsAll(t *testing.T) {
	const n = censusMaxDetailed + 2
	err := reopenAfter(t, func(c *Catalog) {
		for i := range n {
			require.NoError(t, c.put(fmt.Sprintf("future:%02d", i), "x"))
		}
	})
	require.ErrorIs(t, err, ErrForeignCatalog)
	require.ErrorContains(t, err, fmt.Sprintf("%d offending entries", n))
	require.ErrorContains(t, err, fmt.Sprintf("future:%02d", censusMaxDetailed-1))
	require.NotContains(t, err.Error(), fmt.Sprintf("future:%02d", censusMaxDetailed))
}

// TestCensus_RefusalIsWriteFree pins that a refused Open leaves the store
// untouched: in particular it must NOT mint a secret into a foreign tree (the
// census runs before ensureSecret). Simulates a tree whose secret a newer
// binary relocated: a foreign key present, the secret key absent.
func TestCensus_RefusalIsWriteFree(t *testing.T) {
	path := t.TempDir()
	cat, err := openKVAt(t, path)
	require.NoError(t, err)
	require.NoError(t, cat.put("future:key", "x"))
	require.NoError(t, cat.del(catalogSecretStoreKey))
	require.NoError(t, cat.Close())

	_, err = openKVAt(t, path)
	require.ErrorIs(t, err, ErrForeignCatalog)

	// Inspect the raw store: the secret key must still be absent.
	store, err := rocksdb.New(rocksdb.Config{Path: path, Logger: silentLogger()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	_, found, err := store.Get("", []byte(catalogSecretStoreKey))
	require.NoError(t, err)
	require.False(t, found, "the refused Open must not have minted a secret")
}
