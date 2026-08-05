package store

import "slices"

type FeeDistribution struct {
	Max         uint64
	Min         uint64
	Mode        uint64
	P10         uint64
	P20         uint64
	P30         uint64
	P40         uint64
	P50         uint64
	P60         uint64
	P70         uint64
	P80         uint64
	P90         uint64
	P95         uint64
	P99         uint64
	FeeCount    uint32
	LedgerCount uint32
}

// FeeStats is the handler-facing contract for fee statistics. Each backend
// keeps its own fee windows and exposes their current distributions here.
type FeeStats interface {
	SorobanInclusionFeeDistribution() FeeDistribution
	ClassicFeeDistribution() FeeDistribution
}

// ComputeFeeDistribution summarizes one window's fee observations into the
// FeeDistribution getFeeStats serves: min/max/mode plus nearest-rank
// percentiles. It lives here, next to the type it returns, because BOTH
// services' getFeeStats numbers come out of this one function: v1's and v2's
// fee windows keep their own classification and retention machinery, but they
// must not drift on the math itself. Pure — sorts fees in place, reads nothing
// but its arguments. ledgerCount is the number of ledgers the fees were
// collected over (the caller's window length, which an empty window reports
// as zero).
//
//nolint:mnd // percentile numbers are not really magical
func ComputeFeeDistribution(fees []uint64, ledgerCount uint32) FeeDistribution {
	if len(fees) == 0 {
		return FeeDistribution{}
	}
	slices.Sort(fees)
	mode := fees[0]
	lastVal := fees[0]
	maxRepetitions := 0
	localRepetitions := 0
	for i := 1; i < len(fees); i++ {
		if fees[i] == lastVal {
			localRepetitions++
			continue
		}

		// new cluster of values

		if localRepetitions > maxRepetitions {
			maxRepetitions = localRepetitions
			mode = lastVal
		}
		lastVal = fees[i]
		localRepetitions = 0
	}

	if localRepetitions > maxRepetitions {
		// the last cluster of values was the longest
		mode = fees[len(fees)-1]
	}

	count := len(fees)
	countUint64 := uint64(count)
	// nearest-rank percentile
	percentile := func(p uint64) uint64 {
		// ceiling(p*count/100)
		kth := ((p * countUint64) + 100 - 1) / 100
		return fees[kth-1]
	}
	return FeeDistribution{
		Max:  fees[len(fees)-1],
		Min:  fees[0],
		Mode: mode,
		P10:  percentile(10),
		P20:  percentile(20),
		P30:  percentile(30),
		P40:  percentile(40),
		P50:  percentile(50),
		P60:  percentile(60),
		P70:  percentile(70),
		P80:  percentile(80),
		P90:  percentile(90),
		P95:  percentile(95),
		P99:  percentile(99),
		//nolint:gosec // len() is non-negative and bounded by available memory
		FeeCount:    uint32(count),
		LedgerCount: ledgerCount,
	}
}
