package backfill

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// seedReadyHotChunk brackets a "ready" hot DB for c (transient -> create -> ready)
// and commits ONE ledgers-CF entry at seq `top` so MaxCommittedSeq reads back
// `top`. It writes just the ledgers CF (the only CF the completeness gate reads)
// and closes the store — hygiene, not a lock requirement: a read-only open takes
// no RocksDB LOCK and would succeed against a writer-held DB too. The daemon opens
// this exact on-disk DB by its Layout path.
func seedReadyHotChunk(t *testing.T, cat *catalog.Catalog, c chunk.ID, top uint32) {
	t.Helper()
	require.NoError(t, cat.PutHotTransient(c))
	store, err := rocksdb.New(rocksdb.Config{
		Path:           cat.Layout().HotChunkPath(c),
		ColumnFamilies: hotchunk.ColumnFamilies(),
		Logger:         silentLogger(),
	})
	require.NoError(t, err)
	h := ledger.NewWithStore(store, c)
	require.NoError(t, store.Batch(func(b *rocksdb.BatchWriter) error {
		return h.AddLedgerToBatch(b, ledger.Entry{Seq: top, Bytes: []byte("ledger")})
	}))
	require.NoError(t, store.Close())
	require.NoError(t, cat.FlipHotReady(c))
}

// TestBackfillSource_HotComplete: a "ready" hot DB whose committed frontier
// reaches the chunk's last ledger IS the source — backfillSource returns it with
// NO backend configured, so success alone proves the hot branch was taken.
func TestBackfillSource_HotComplete(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat) // no Backend

	c := chunk.ID(0)
	seedReadyHotChunk(t, cat, c, c.LastLedger()) // complete: maxSeq == last ledger

	src, closeSrc, err := backfillSource(context.Background(), c, catalog.AllArtifacts(), cfg)
	require.NoError(t, err, "complete hot tier is used; no bulk backend needed")
	require.NotNil(t, src)
	require.NoError(t, closeSrc())
}

// TestBackfillSource_HotIncompleteFallsThrough: a "ready" but incomplete hot DB is
// staleness — backfillSource falls past it. With no pack and no backend, that
// fall-through surfaces as the "no bulk backend" error (not a hot-tier error).
func TestBackfillSource_HotIncompleteFallsThrough(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat) // no Backend, no frozen pack

	c := chunk.ID(0)
	seedReadyHotChunk(t, cat, c, c.FirstLedger()) // incomplete: maxSeq < last ledger

	_, _, err := backfillSource(context.Background(), c, catalog.AllArtifacts(), cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no bulk backend",
		"an incomplete hot tier falls through; it is not itself an error")
}

// TestBackfillSource_HotReadyButDirMissing: a "ready" key whose hot DB won't open
// (dir gone) is an ordinary restartable error — the read-only open never
// auto-heals it into a fresh empty DB.
func TestBackfillSource_HotReadyButDirMissing(t *testing.T) {
	cat, _ := testCatalog(t)
	cfg := testProcessConfig(t, cat)

	c := chunk.ID(0)
	require.NoError(t, cat.PutHotTransient(c))
	require.NoError(t, cat.FlipHotReady(c)) // ready key, NO dir on disk

	_, _, err := backfillSource(context.Background(), c, catalog.AllArtifacts(), cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "won't open")
}
