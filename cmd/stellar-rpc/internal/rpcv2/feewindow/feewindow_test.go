package feewindow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

func TestAppendLedgerFees(t *testing.T) {
	windows := NewFeeWindows(10, 10)

	require.NoError(t, windows.AppendLedgerFees(100, ingest.LedgerFees{
		ClassicFeesPerOp:     []uint64{100, 200},
		SorobanInclusionFees: []uint64{50},
	}))

	classic := windows.ClassicFeeDistribution()
	assert.Equal(t, uint32(2), classic.FeeCount)
	assert.Equal(t, uint32(1), classic.LedgerCount)
	assert.Equal(t, uint64(100), classic.Min)
	assert.Equal(t, uint64(200), classic.Max)

	soroban := windows.SorobanInclusionFeeDistribution()
	assert.Equal(t, uint32(1), soroban.FeeCount)
	assert.Equal(t, uint32(1), soroban.LedgerCount)
	assert.Equal(t, uint64(50), soroban.Min)
	assert.Equal(t, uint64(50), soroban.Max)

	// A fee-less ledger still lands as a bucket: LedgerCount advances, fees don't.
	require.NoError(t, windows.AppendLedgerFees(101, ingest.LedgerFees{}))
	assert.Equal(t, uint32(2), windows.ClassicFeeDistribution().LedgerCount)
	assert.Equal(t, uint32(2), windows.ClassicFeeDistribution().FeeCount)
}

func TestMaxRetentionWindow(t *testing.T) {
	assert.Equal(t, uint32(6), NewFeeWindows(6, 4).MaxRetentionWindow())
	assert.Equal(t, uint32(6), NewFeeWindows(4, 6).MaxRetentionWindow())
}

func TestAppendLedgerFeesNonContiguousSeqErrors(t *testing.T) {
	windows := NewFeeWindows(10, 10)
	require.NoError(t, windows.AppendLedgerFees(100, ingest.LedgerFees{}))
	require.Error(t, windows.AppendLedgerFees(102, ingest.LedgerFees{}))
}

func TestAppendLedgerFeesTrimsToEachRetention(t *testing.T) {
	windows := NewFeeWindows(2, 3)
	for seq := uint32(100); seq < 105; seq++ {
		require.NoError(t, windows.AppendLedgerFees(seq, ingest.LedgerFees{
			ClassicFeesPerOp:     []uint64{uint64(seq)},
			SorobanInclusionFees: []uint64{uint64(seq) * 2},
		}))
	}

	classic := windows.ClassicFeeDistribution()
	assert.Equal(t, uint32(2), classic.LedgerCount, "classic window keeps its own retention")
	assert.Equal(t, uint64(103), classic.Min, "older ledgers' fees evicted")

	soroban := windows.SorobanInclusionFeeDistribution()
	assert.Equal(t, uint32(3), soroban.LedgerCount, "soroban window keeps its own retention")
	assert.Equal(t, uint64(102*2), soroban.Min)
}

func TestReset(t *testing.T) {
	windows := NewFeeWindows(10, 10)
	require.NoError(t, windows.AppendLedgerFees(100, ingest.LedgerFees{
		ClassicFeesPerOp: []uint64{100},
	}))
	windows.Reset()
	assert.Equal(t, store.FeeDistribution{}, windows.ClassicFeeDistribution())
	// A reset window accepts any starting sequence again.
	require.NoError(t, windows.AppendLedgerFees(50, ingest.LedgerFees{}))
}
