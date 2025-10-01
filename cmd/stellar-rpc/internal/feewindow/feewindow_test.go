package feewindow

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/montanaflynn/stats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasicComputeFeeDistribution(t *testing.T) {
	testCases := []struct {
		name   string
		input  []uint64
		output FeeDistribution
	}{
		{"nil", nil, FeeDistribution{}},
		{"empty", []uint64{}, FeeDistribution{}},
		{
			"one",
			[]uint64{100},
			FeeDistribution{
				Max: 100, Min: 100, Mode: 100,
				P10: 100, P20: 100, P30: 100, P40: 100, P50: 100,
				P60: 100, P70: 100, P80: 100, P90: 100, P95: 100, P99: 100,
				FeeCount: 1,
			},
		},
		{
			"even number of elements: four 100s and six 1000s",
			[]uint64{100, 100, 100, 1000, 100, 1000, 1000, 1000, 1000, 1000},
			FeeDistribution{
				Max: 1000, Min: 100, Mode: 1000,
				P10: 100, P20: 100, P30: 100, P40: 100, P50: 1000,
				P60: 1000, P70: 1000, P80: 1000, P90: 1000, P95: 1000, P99: 1000,
				FeeCount: 10,
			},
		},
		{
			"odd number of elements: five 100s and six 1000s",
			[]uint64{100, 100, 100, 1000, 100, 1000, 1000, 1000, 1000, 1000, 100},
			FeeDistribution{
				Max: 1000, Min: 100, Mode: 1000,
				P10: 100, P20: 100, P30: 100, P40: 100, P50: 1000,
				P60: 1000, P70: 1000, P80: 1000, P90: 1000, P95: 1000, P99: 1000,
				FeeCount: 11,
			},
		},
		{
			"multiple modes favors the smallest value",
			[]uint64{100, 1000},
			FeeDistribution{
				Max: 1000, Min: 100, Mode: 100,
				P10: 100, P20: 100, P30: 100, P40: 100, P50: 100,
				P60: 1000, P70: 1000, P80: 1000, P90: 1000, P95: 1000, P99: 1000,
				FeeCount: 2,
			},
		},
		{
			"random distribution with a repetition",
			[]uint64{515, 245, 245, 530, 221, 262, 927},
			FeeDistribution{
				Max: 927, Min: 221, Mode: 245,
				P10: 221, P20: 245, P30: 245, P40: 245, P50: 262,
				P60: 515, P70: 515, P80: 530, P90: 927, P95: 927, P99: 927,
				FeeCount: 7,
			},
		},
		{
			"random distribution with a repetition of its largest value",
			[]uint64{515, 245, 530, 221, 262, 927, 927},
			FeeDistribution{
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
		feesCopy1 := make([]uint64, len(fees))
		feesCopy2 := make([]uint64, len(fees))
		for i := range len(fees) {
			feesCopy1[i] = fees[i]
			feesCopy2[i] = fees[i]
		}
		actual := computeFeeDistribution(feesCopy2, 0)
		expected, err := alternativeComputeFeeDistribution(feesCopy2, 0)
		require.NoError(t, err)
		assert.Equal(t, expected, actual, fmt.Sprintf("input fees: %v", fees))
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

func BenchmarkComputeFeeDistribution(b *testing.B) {
	length := 5000
	fees := generateFees(&length)
	b.Run("computeFeeDistribution", func(b *testing.B) {
		for b.Loop() {
			computeFeeDistribution(fees, 0)
		}
	})
	b.Run("alternativeComputeFeeDistribution", func(b *testing.B) {
		for b.Loop() {
			_, err := alternativeComputeFeeDistribution(fees, 0)
			require.NoError(b, err)
		}
	})
}

func alternativeComputeFeeDistribution(fees []uint64, ledgerCount uint32) (FeeDistribution, error) {
	if len(fees) == 0 {
		return FeeDistribution{}, nil
	}

	input := stats.LoadRawData(fees)

	max, min, mode, err := computeBasicStats(input, fees)
	if err != nil {
		return FeeDistribution{}, err
	}

	percentiles, err := computePercentiles(input)
	if err != nil {
		return FeeDistribution{}, err
	}

	return FeeDistribution{
		Max:         uint64(max),
		Min:         uint64(min),
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
	max, err := input.Max()
	if err != nil {
		return 0, 0, 0, err
	}

	min, err := input.Min()
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

	return max, min, mode, nil
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
