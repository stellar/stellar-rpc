package ingest

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

// TestFreezeEventsCold_ByteIdenticalToWalk is the composition half of the
// events identity gate: one full chunk ingested hot through the production
// service, then built cold both ways, the walk draining the raw fixtures
// through WriteColdChunk and the freeze reading the hot DB, must give
// byte-identical events.pack, index.pack and index.hash. Seven events per
// ledger put the chunk past one slab, so the freeze consumes a sealed slab
// and re-derives a tail. The populate is a full LedgersPerChunk pass with a
// synced WriteBatch per ledger, so this is one of the package's slower tests.
func TestFreezeEventsCold_ByteIdenticalToWalk(t *testing.T) {
	if testing.Short() {
		t.Skip("full-chunk populate")
	}
	chunkID := chunk.ID(0)
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
	const eventsPerLedger = 7

	fixtures := make([][]byte, 0, chunk.LedgersPerChunk)
	for seq := first; seq <= last; seq++ {
		evs := make([]xdr.ContractEvent, 0, eventsPerLedger)
		for i := range eventsPerLedger {
			var cid xdr.ContractId
			cid[0], cid[1] = byte(i), byte(seq)
			evs = append(evs, rpcv2test.SymbolContractEvent(cid, "transfer", "to"))
		}
		fixtures = append(fixtures, rpcv2test.EventsLCMBytesAt(t, seq, int64(seq)*5, evs...))
	}

	dbPath := t.TempDir()
	db, err := hotchunk.Open(dbPath, chunkID, testHotSecrets{}, hotTestLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	svc := NewHotService(db, nil, nil)
	for i, raw := range fixtures {
		_, ierr := svc.Ingest(first+uint32(i), xdr.LedgerCloseMetaView(raw))
		require.NoError(t, ierr)
	}

	cfg := Config{Events: true, EventsSecret: testEventsSecretBytes()}
	walkDir := t.TempDir()
	rawIter := func(yield func([]byte, error) bool) {
		for _, b := range fixtures {
			if !yield(b, nil) {
				return
			}
		}
	}
	require.NoError(t, WriteColdChunk(
		context.Background(), testLogger(), chunkID, rawIter, coldDirsAt(walkDir, chunkID), nil, cfg))

	freezeDirs := sameDirs(filepath.Join(t.TempDir(), dataTypeEvents, chunkID.BucketID()))
	n, err := db.FreezeEventsCold(context.Background(), freezeDirs, testEventsSecret(), event.ColdWriterOptions{
		Concurrency:  coldEncoderConcurrency,
		BytesPerSync: coldBytesPerSync,
	})
	require.NoError(t, err)
	require.Equal(t, eventsPerLedger*chunk.LedgersPerChunk, n)

	walkEvents := coldDirsAt(walkDir, chunkID).Events
	names := []string{event.EventsPackName(chunkID), event.IndexPackName(chunkID), event.IndexHashName(chunkID)}
	for _, name := range names {
		want, err := os.ReadFile(filepath.Join(walkEvents.Data, name))
		require.NoError(t, err)
		got, err := os.ReadFile(filepath.Join(freezeDirs.Data, name))
		require.NoError(t, err)
		require.Len(t, got, len(want), "%s: size", name)
		require.Equal(t, want, got, "%s: bytes differ", name)
	}

	// Tripwire: the hot index must hold a sealed slab, or the gate covers
	// only the re-derived tail.
	require.NoError(t, db.Close())
	raw, err := rocksdb.New(rocksdb.Config{
		Path: dbPath, ColumnFamilies: hotchunk.ColumnFamilies(), Logger: hotTestLogger(), ReadOnly: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	_, sealed, err := raw.LastKey(event.IndexCF)
	require.NoError(t, err)
	require.True(t, sealed, "the fixture must seal a slab for the freeze to consume")
}
