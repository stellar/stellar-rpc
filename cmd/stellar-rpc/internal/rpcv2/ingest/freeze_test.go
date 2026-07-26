package ingest

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
)

// TestFreezeColdChunk_ByteIdenticalToWalk is the composition half of the
// cross-path identity gate: one full chunk ingested hot through the
// production service, then materialized cold BOTH ways — WriteColdChunk
// draining the raw fixtures (the derivation path) and FreezeColdChunk
// scanning the hot DB's CFs (the zero-decompression path) — must produce
// byte-identical artifacts for all three kinds. This also retroactively
// gates the already-landed events freeze-by-merge against the walk build.
//
// The populate is a full LedgersPerChunk pass with a synced WriteBatch per
// ledger, so this is one of the package's slower tests.
func TestFreezeColdChunk_ByteIdenticalToWalk(t *testing.T) {
	chunkID := chunk.ID(0)
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
	const eventEvery = 100 // every eventEvery-th ledger carries one tx + one contract event

	fixtures := make([][]byte, 0, chunk.LedgersPerChunk)
	for seq := first; seq <= last; seq++ {
		if (seq-first)%eventEvery == 0 {
			fixtures = append(fixtures, rpcv2test.EventLCMBytes(t, seq))
		} else {
			fixtures = append(fixtures, rpcv2test.ZeroTxLCMBytes(t, seq))
		}
	}

	// Hot DB populated through the production ingest service (compression,
	// extraction, and CF layout exactly as a live chunk's).
	ctx := context.Background()
	db, err := hotchunk.Open(t.TempDir(), chunkID, hotTestLogger(), hotchunk.DefaultTuning())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	svc := NewHotService(db, nil)
	for i, raw := range fixtures {
		require.NoError(t, svc.Ingest(ctx, first+uint32(i), xdr.LedgerCloseMetaView(raw)))
	}

	// The walk half must encode ledger frames with the SAME workers value the
	// hot DB above was opened with (DefaultTuning) — the setting is
	// format-affecting and this test is the freeze-vs-walk identity arbiter.
	cfgAll := Config{
		Ledgers: true, Txhash: true, Events: true,
		ZstdEncodeWorkers: ledger.DefaultZstdEncodeWorkers,
	}
	dirsFor := func(root string) ColdDirs {
		return ColdDirs{
			LedgerPack: filepath.Join(root, "ledgers", "chunk.pack"),
			TxhashBin:  filepath.Join(root, "txhash", "chunk.bin"),
			EventsDir:  filepath.Join(root, "events"),
		}
	}

	// Walk path: drain the same fixtures WriteColdChunk-style.
	walkDirs := dirsFor(t.TempDir())
	rawIter := func(yield func([]byte, error) bool) {
		for _, b := range fixtures {
			if !yield(b, nil) {
				return
			}
		}
	}
	require.NoError(t, WriteColdChunk(ctx, hotTestLogger(), chunkID, rawIter, walkDirs, nil, cfgAll))

	// Freeze path: no ledger stream exists to hand over.
	freezeDirs := dirsFor(t.TempDir())
	require.NoError(t, FreezeColdChunk(ctx, hotTestLogger(), chunkID, db, freezeDirs, nil, cfgAll))

	requireSameBytes(t, walkDirs.LedgerPack, freezeDirs.LedgerPack)
	requireSameBytes(t, walkDirs.TxhashBin, freezeDirs.TxhashBin)

	// Events: both bucket dirs must hold the same artifact set (scratch dirs
	// are removed on success) with identical bytes.
	walkNames := dirEntryNames(t, walkDirs.EventsDir)
	require.Equal(t, walkNames, dirEntryNames(t, freezeDirs.EventsDir))
	require.NotEmpty(t, walkNames)
	for _, name := range walkNames {
		requireSameBytes(t, filepath.Join(walkDirs.EventsDir, name), filepath.Join(freezeDirs.EventsDir, name))
	}
}

// TestEventsScratchDir_SharedAcrossMaterializers pins the scratch-lifecycle
// contract: both materializers (walk and freeze) resolve the SAME scratch
// dir for a chunk, so whichever path retries a crashed attempt wipes the
// other's remains on entry — scratch has no catalog key, and the shared
// deterministic name is its only cross-path cleanup.
func TestEventsScratchDir_SharedAcrossMaterializers(t *testing.T) {
	dir := t.TempDir()
	id := chunk.ID(123)
	got := eventsScratchDir(dir, id)
	require.Equal(t, filepath.Join(dir, ".events-scratch-"+id.String()), got)
	require.Equal(t, got, eventsScratchDir(dir, id))
}

// TestFreezeColdChunk_NilDB: a nil hot DB is a caller bug, reported before
// any artifact path is touched.
func TestFreezeColdChunk_NilDB(t *testing.T) {
	err := FreezeColdChunk(context.Background(), hotTestLogger(), chunk.ID(0), nil,
		ColdDirs{LedgerPack: "x", TxhashBin: "y", EventsDir: "z"},
		nil, Config{Ledgers: true})
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil hot DB")
}

func requireSameBytes(t *testing.T, a, b string) {
	t.Helper()
	ab, err := os.ReadFile(a)
	require.NoError(t, err)
	bb, err := os.ReadFile(b)
	require.NoError(t, err)
	require.Len(t, bb, len(ab), "%s vs %s: size", a, b)
	require.Equal(t, string(ab), string(bb), "%s vs %s: bytes diverge", a, b)
}

func dirEntryNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}
