package feewindow

import (
	"math/rand"
	"slices"
	"testing"

	"github.com/montanaflynn/stats"
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

// The distribution-math tests below are copied with computeFeeDistribution
// from internal/rpcv1/feewindow, differential oracle included, so the two
// copies are pinned to the same behavior independently.

func TestBasicComputeFeeDistribution(t *testing.T) {
	testCases := []struct {
		name   string
		input  []uint64
		output store.FeeDistribution
	}{
		{"nil", nil, store.FeeDistribution{}},
		{"empty", []uint64{}, store.FeeDistribution{}},
		{
			"one",
			[]uint64{100},
			store.FeeDistribution{
				Max: 100, Min: 100, Mode: 100,
				P10: 100, P20: 100, P30: 100, P40: 100, P50: 100,
				P60: 100, P70: 100, P80: 100, P90: 100, P95: 100, P99: 100,
				FeeCount: 1,
			},
		},
		{
			"even number of elements: four 100s and six 1000s",
			[]uint64{100, 100, 100, 1000, 100, 1000, 1000, 1000, 1000, 1000},
			store.FeeDistribution{
				Max: 1000, Min: 100, Mode: 1000,
				P10: 100, P20: 100, P30: 100, P40: 100, P50: 1000,
				P60: 1000, P70: 1000, P80: 1000, P90: 1000, P95: 1000, P99: 1000,
				FeeCount: 10,
			},
		},
		{
			"odd number of elements: five 100s and six 1000s",
			[]uint64{100, 100, 100, 1000, 100, 1000, 1000, 1000, 1000, 1000, 100},
			store.FeeDistribution{
				Max: 1000, Min: 100, Mode: 1000,
				P10: 100, P20: 100, P30: 100, P40: 100, P50: 1000,
				P60: 1000, P70: 1000, P80: 1000, P90: 1000, P95: 1000, P99: 1000,
				FeeCount: 11,
			},
		},
		{
			"multiple modes favors the smallest value",
			[]uint64{100, 1000},
			store.FeeDistribution{
				Max: 1000, Min: 100, Mode: 100,
				P10: 100, P20: 100, P30: 100, P40: 100, P50: 100,
				P60: 1000, P70: 1000, P80: 1000, P90: 1000, P95: 1000, P99: 1000,
				FeeCount: 2,
			},
		},
		{
			"random distribution with a repetition",
			[]uint64{515, 245, 245, 530, 221, 262, 927},
			store.FeeDistribution{
				Max: 927, Min: 221, Mode: 245,
				P10: 221, P20: 245, P30: 245, P40: 245, P50: 262,
				P60: 515, P70: 515, P80: 530, P90: 927, P95: 927, P99: 927,
				FeeCount: 7,
			},
		},
		{
			"random distribution with a repetition of its largest value",
			[]uint64{515, 245, 530, 221, 262, 927, 927},
			store.FeeDistribution{
				Max: 927, Min: 221, Mode: 927,
				P10: 221, P20: 245, P30: 262, P40: 262, P50: 515,
				P60: 530, P70: 530, P80: 927, P90: 927, P95: 927, P99: 927,
				FeeCount: 7,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := computeFeeDistribution(tc.input, 0)
			assert.Equal(t, tc.output, result)
		})
	}
}

func TestComputeFeeDistributionAgainstAlternative(t *testing.T) {
	for range 100_000 {
		fees := generateFees(nil)
		feesCopy := make([]uint64, len(fees))
		copy(feesCopy, fees)
		actual := computeFeeDistribution(feesCopy, 0)
		expected, err := alternativeComputeFeeDistribution(feesCopy, 0)
		require.NoError(t, err)
		assert.Equalf(t, expected, actual, "input fees: %v", fees)
	}
}

func generateFees(l *int) []uint64 {
	var length int
	if l != nil {
		length = *l
	} else {
		// Generate sequences with a length between 0 and 1000
		length = rand.Intn(100)
	}
	result := make([]uint64, length)
	lastFee := uint64(0)
	for i := range length {
		if lastFee != 0 && rand.Intn(100) <= 25 {
			// To test the Mode correctly, generate a repetition with a chance of 25%
			result[i] = lastFee
		} else {
			// generate fees between 100 and 1000
			lastFee = uint64(rand.Intn(900) + 100)
			result[i] = lastFee
		}
	}
	return result
}

func alternativeComputeFeeDistribution(fees []uint64, ledgerCount uint32) (store.FeeDistribution, error) {
	if len(fees) == 0 {
		return store.FeeDistribution{}, nil
	}

	input := stats.LoadRawData(fees)

	maxValue, minValue, mode, err := computeBasicStats(input, fees)
	if err != nil {
		return store.FeeDistribution{}, err
	}

	percentiles, err := computePercentiles(input)
	if err != nil {
		return store.FeeDistribution{}, err
	}

	return store.FeeDistribution{
		Max:         uint64(maxValue),
		Min:         uint64(minValue),
		Mode:        mode,
		P10:         uint64(percentiles[0]),
		P20:         uint64(percentiles[1]),
		P30:         uint64(percentiles[2]),
		P40:         uint64(percentiles[3]),
		P50:         uint64(percentiles[4]),
		P60:         uint64(percentiles[5]),
		P70:         uint64(percentiles[6]),
		P80:         uint64(percentiles[7]),
		P90:         uint64(percentiles[8]),
		P95:         uint64(percentiles[9]),
		P99:         uint64(percentiles[10]),
		FeeCount:    uint32(len(fees)),
		LedgerCount: ledgerCount,
	}, nil
}

func computeBasicStats(input stats.Float64Data, fees []uint64) (float64, float64, uint64, error) {
	maxValue, err := input.Max()
	if err != nil {
		return 0, 0, 0, err
	}

	minValue, err := input.Min()
	if err != nil {
		return 0, 0, 0, err
	}

	modeSeq, err := input.Mode()
	if err != nil {
		return 0, 0, 0, err
	}

	var mode uint64
	if len(modeSeq) == 0 {
		slices.Sort(fees)
		mode = fees[0]
	} else {
		mode = uint64(modeSeq[0])
	}

	return maxValue, minValue, mode, nil
}

func computePercentiles(input stats.Float64Data) ([]float64, error) {
	percentiles := []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 99}
	results := make([]float64, len(percentiles))

	for i, p := range percentiles {
		result, err := input.PercentileNearestRank(p)
		if err != nil {
			return nil, err
		}
		results[i] = result
	}

	return results, nil
}
