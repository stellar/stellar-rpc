package store

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
