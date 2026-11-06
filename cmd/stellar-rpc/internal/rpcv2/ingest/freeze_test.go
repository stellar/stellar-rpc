package ingest

import (
	"bytes"
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
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
)

// One full chunk ingested hot through the production service, then built
// cold both ways, the walk draining the raw fixtures through WriteColdChunk
// and the freeze reading the hot DB, gives byte-identical artifacts. Seven
// events per ledger put the chunk past one events slab and a transaction per
// ledger past many txhash seals, so both indexes freeze sealed state and an
// unsealed tail. The populate is a full chunk with a synced write per ledger,
// so this is one of the package's slower tests.
func TestFreezeColdChunk_ByteIdenticalToWalk(t *testing.T) {
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

	cfg := testColdConfig()
	walk, frozen := coldDirsAt(t.TempDir(), chunkID), coldDirsAt(t.TempDir(), chunkID)
	plantStaleRun(t, frozen.TxhashRuns) // a crashed walk build's, which the freeze removes
	rawIter := func(yield func([]byte, error) bool) {
		for _, b := range fixtures {
			if !yield(b, nil) {
				return
			}
		}
	}
	ctx := context.Background()
	require.NoError(t, WriteColdChunk(ctx, testLogger(), chunkID, rawIter, walk, nil, cfg))
	require.NoError(t, FreezeColdChunk(ctx, testLogger(), chunkID, db, frozen, nil, cfg))
	require.NoDirExists(t, frozen.TxhashRuns)

	eventFiles := []string{event.EventsPackName(chunkID), event.IndexPackName(chunkID), event.IndexHashName(chunkID)}
	pairs := make([][2]string, 0, 2+len(eventFiles))
	pairs = append(pairs, [2]string{walk.LedgerPack, frozen.LedgerPack}, [2]string{walk.TxhashBin, frozen.TxhashBin})
	for _, name := range eventFiles {
		pairs = append(pairs, [2]string{filepath.Join(walk.Events.Data, name), filepath.Join(frozen.Events.Data, name)})
	}
	for _, p := range pairs {
		requireSameBytes(t, p[0], p[1])
	}

	// Both indexes must hold sealed state, or the gate covers only their tails.
	require.NoError(t, db.Close())
	raw, err := rocksdb.New(rocksdb.Config{
		Path: dbPath, ColumnFamilies: hotchunk.ColumnFamilies(), Logger: hotTestLogger(), ReadOnly: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	for _, cf := range []string{event.IndexCF, txhash.CFNames()[1]} {
		_, sealed, err := raw.LastKey(cf)
		require.NoError(t, err)
		require.True(t, sealed, "%s holds nothing sealed", cf)
	}
}

// requireSameBytes fails unless the walk's file and the freeze's are
// byte-identical.
func requireSameBytes(t *testing.T, walkPath, freezePath string) {
	t.Helper()
	want, err := os.ReadFile(walkPath)
	require.NoError(t, err)
	got, err := os.ReadFile(freezePath)
	require.NoError(t, err)
	require.True(t, bytes.Equal(want, got), "%s: the freeze's bytes differ from the walk's", filepath.Base(walkPath))
}
