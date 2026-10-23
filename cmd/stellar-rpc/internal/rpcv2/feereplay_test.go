package rpcv2

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/feewindow"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
)

// TestFeeWindowReplay_SpansChunkBoundary is issue #888's core case: a restart
// lands mid-chunk within the first `window` ledgers of the live chunk (the
// COMMON case, not an edge case), so the replay range spans the 0→1 chunk
// boundary. The rebuilt windows must equal a from-scratch computation over the
// last `window` committed ledgers — no ledger counted twice, stale content
// from the previous run gone, and the boundary ledger present exactly once.
func TestFeeWindowReplay_SpansChunkBoundary(t *testing.T) {
	t.Parallel() // seeds a full chunk (one synced commit per ledger)
	cat, _ := testCatalog(t)
	c0, c1 := chunk.ID(0), chunk.ID(1)
	lastCommitted := c1.FirstLedger() + 2

	feeBySeq := map[uint32]int64{
		c0.LastLedger() - 2: 110, c0.LastLedger() - 1: 120, c0.LastLedger(): 130,
		c1.FirstLedger(): 210, c1.FirstLedger() + 1: 220, c1.FirstLedger() + 2: 230,
	}
	lcmFor := func(seq uint32) []byte {
		if fee, ok := feeBySeq[seq]; ok {
			return rpcv2test.FeeTxLCMBytes(t, seq, fee)
		}
		return rpcv2test.ZeroTxLCMBytes(t, seq)
	}

	// Chunk 0 committed in full; its handle is closed so the registry reopens
	// it as a ready chunk — the state a restart finds.
	db0 := openLiveHotDB(t, cat, c0)
	for seq := c0.FirstLedger(); seq <= c0.LastLedger(); seq++ {
		rpcv2test.IngestLedger(t, db0, seq, lcmFor(seq))
	}
	require.NoError(t, db0.Close())

	// The live chunk 1 holds three committed ledgers.
	db1 := openLiveHotDB(t, cat, c1)
	for seq := c1.FirstLedger(); seq <= lastCommitted; seq++ {
		rpcv2test.IngestLedger(t, db1, seq, lcmFor(seq))
	}

	registry, err := query.OpenRegistry(cat, geometry.NewRetention(0, 0), db1, lastCommitted)
	require.NoError(t, err)
	defer registry.Close()

	// Already-populated windows: the replay's contract is to recompute from
	// scratch on any input, not stack on top — pinned here on dirty windows.
	windows := feewindow.NewFeeWindows(6, 4)
	require.NoError(t, windows.AppendLedgerFees(c0.LastLedger(),
		sdkingest.LedgerFees{ClassicFeesPerOp: []uint64{9999}}))

	require.NoError(t, replayFeeWindows(registry, windows, lastCommitted))

	// From-scratch reference over the last max(classic, soroban) = 6 ledgers.
	want := feewindow.NewFeeWindows(6, 4)
	for seq := lastCommitted - 5; seq <= lastCommitted; seq++ {
		require.NoError(t, want.AppendLedgerFees(seq,
			sdkingest.LedgerFees{ClassicFeesPerOp: []uint64{uint64(feeBySeq[seq])}}))
	}
	assert.Equal(t, want.ClassicFeeDistribution(), windows.ClassicFeeDistribution(),
		"replayed windows must equal a from-scratch computation")
	assert.Equal(t, want.SorobanInclusionFeeDistribution(), windows.SorobanInclusionFeeDistribution())

	classic := windows.ClassicFeeDistribution()
	assert.Equal(t, uint32(6), classic.LedgerCount)
	assert.Equal(t, uint32(6), classic.FeeCount, "no ledger counted twice, the boundary ledger included exactly once")
	assert.Equal(t, uint64(110), classic.Min, "the pre-boundary ledgers were replayed")
	assert.Equal(t, uint64(230), classic.Max)
}

// TestFeeWindowReplay_FreshStartIsNoOp: with nothing committed, OldestLedger()
// exceeds lastCommitted by one — the replay must return cleanly with empty
// windows, not error.
func TestFeeWindowReplay_FreshStartIsNoOp(t *testing.T) {
	cat, _ := testCatalog(t)
	c0 := chunk.ID(0)
	db0 := openLiveHotDB(t, cat, c0) // empty live chunk
	lastCommitted := c0.FirstLedger() - 1

	registry, err := query.OpenRegistry(cat, geometry.NewRetention(0, 0), db0, lastCommitted)
	require.NoError(t, err)
	defer registry.Close()

	windows := feewindow.NewFeeWindows(10, 10)
	require.NoError(t, replayFeeWindows(registry, windows, lastCommitted))
	assert.Zero(t, windows.ClassicFeeDistribution().LedgerCount)
	assert.Zero(t, windows.SorobanInclusionFeeDistribution().LedgerCount)
}

// TestFeeWindowReplay_ShortHistoryClampsToOldest: a window larger than the
// committed history clamps to what exists instead of erroring below the floor.
func TestFeeWindowReplay_ShortHistoryClampsToOldest(t *testing.T) {
	cat, _ := testCatalog(t)
	c0 := chunk.ID(0)
	first := c0.FirstLedger()
	lastCommitted := first + 3

	db0 := openLiveHotDB(t, cat, c0)
	for seq := first; seq <= lastCommitted; seq++ {
		rpcv2test.IngestLedger(t, db0, seq, rpcv2test.FeeTxLCMBytes(t, seq, int64(seq)))
	}

	registry, err := query.OpenRegistry(cat, geometry.NewRetention(0, 0), db0, lastCommitted)
	require.NoError(t, err)
	defer registry.Close()

	windows := feewindow.NewFeeWindows(10, 10)
	require.NoError(t, replayFeeWindows(registry, windows, lastCommitted))

	classic := windows.ClassicFeeDistribution()
	assert.Equal(t, uint32(4), classic.LedgerCount, "everything that exists is replayed")
	assert.Equal(t, uint64(first), classic.Min)
	assert.Equal(t, uint64(lastCommitted), classic.Max)
}
