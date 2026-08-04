// Package feewindow holds v2's in-memory getFeeStats state: two sliding
// windows of per-ledger fee observations (classic per-op fees and soroban
// inclusion fees), each keeping a nearest-rank percentile distribution over
// its retained ledgers. Live ingestion appends every committed ledger's
// ingest.FeesFromTxParts product, and the startup replay rebuilds the windows
// from raw committed history.
//
// v1 has its own internal/rpcv1/feewindow and the two are DELIBERATELY
// separate, not a shared package: v1 classifies fees from parsed LCMs and
// envelopes (network passphrase, SQLite migration adapter), v2 from the
// TxProcessing walk's LedgerFees — the only overlap is the distribution math,
// which is copied verbatim (with v1's differential test) rather than shared.
// The #883 parity harness, not a shared package, is what keeps the two
// services' getFeeStats comparable. The store.FeeStats interface both satisfy
// stays shared because the getFeeStats HANDLER (internal/methods) is shared.
//
//nolint:mnd // percentile numbers are not really magical
package feewindow

import (
	"fmt"
	"slices"
	"sync"

	"github.com/stellar/go-stellar-sdk/ingest"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// FeeWindows is the daemon-owned pair of fee windows behind getFeeStats. It
// satisfies store.FeeStats, the interface the shared handler consumes.
type FeeWindows struct {
	classic          *feeWindow
	sorobanInclusion *feeWindow
}

// NewFeeWindows sizes the two windows in ledgers ([service.fee_stats],
// validated 1..1000 at startup).
func NewFeeWindows(classicRetention, sorobanInclusionRetention uint32) *FeeWindows {
	return &FeeWindows{
		classic:          newFeeWindow(classicRetention),
		sorobanInclusion: newFeeWindow(sorobanInclusionRetention),
	}
}

// AppendLedgerFees folds one ledger's fee observations — the
// ingest.FeesFromTxParts product of the shared ExtractLedgerTxParts walk —
// into both windows, each trimming to its own retention. Every ledger appends
// a bucket, fee-less ones included (an empty bucket still advances
// LedgerCount), and each window requires contiguous ledger sequences, so
// appending the same ledger twice or skipping one errors.
func (fw *FeeWindows) AppendLedgerFees(seq uint32, fees ingest.LedgerFees) error {
	if err := fw.classic.append(seq, fees.ClassicFeesPerOp); err != nil {
		return err
	}
	return fw.sorobanInclusion.append(seq, fees.SorobanInclusionFees)
}

// Reset clears both windows; the startup replay calls it before recomputing
// the windows from committed history.
func (fw *FeeWindows) Reset() {
	fw.classic.reset()
	fw.sorobanInclusion.reset()
}

// MaxRetentionWindow returns the larger of the two windows' sizes — how many
// trailing ledgers a from-scratch rebuild must replay to refill both.
func (fw *FeeWindows) MaxRetentionWindow() uint32 {
	return max(fw.classic.retentionWindow, fw.sorobanInclusion.retentionWindow)
}

func (fw *FeeWindows) ClassicFeeDistribution() store.FeeDistribution {
	return fw.classic.distribution()
}

func (fw *FeeWindows) SorobanInclusionFeeDistribution() store.FeeDistribution {
	return fw.sorobanInclusion.distribution()
}

// Compile-time check that FeeWindows implements store.FeeStats, the interface
// the shared getFeeStats handler consumes. Breaks the build here, next to the
// type, if the two drift.
var _ store.FeeStats = &FeeWindows{}

// feeWindow is one sliding window: a ring of the last retentionWindow
// ledgers' fee buckets plus the distribution recomputed on every append, so
// reads are a lock and a copy. The ring is the only container v2 needs —
// distribution serving reads no per-ledger sequences or close times back —
// so it lives here instead of depending on v1's generic ledger-bucket window.
type feeWindow struct {
	lock            sync.RWMutex
	buckets         []feeBucket // ring; grows to retentionWindow, then wraps
	start           int         // index of the OLDEST bucket once the ring is full
	dist            store.FeeDistribution
	retentionWindow uint32
}

// feeBucket is one ledger's fee observations. seq is kept only to enforce
// contiguity — the double-count guard: re-appending a replayed or retried
// ledger errors instead of silently skewing the distribution.
type feeBucket struct {
	seq  uint32
	fees []uint64
}

func newFeeWindow(retentionWindow uint32) *feeWindow {
	return &feeWindow{
		buckets:         make([]feeBucket, 0, retentionWindow),
		retentionWindow: retentionWindow,
	}
}

func (w *feeWindow) append(seq uint32, fees []uint64) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	if n := len(w.buckets); n > 0 {
		newest := w.buckets[(w.start+n-1)%n].seq
		if seq != newest+1 {
			return fmt.Errorf("feewindow: ledgers not contiguous: expected seq %d, got %d", newest+1, seq)
		}
	}

	b := feeBucket{seq: seq, fees: fees}
	if len(w.buckets) < int(w.retentionWindow) {
		w.buckets = append(w.buckets, b)
	} else {
		// Full: overwrite the oldest bucket and advance the ring's start.
		w.buckets[w.start] = b
		w.start = (w.start + 1) % len(w.buckets)
	}

	var allFees []uint64
	for i := range w.buckets {
		allFees = append(allFees, w.buckets[i].fees...)
	}
	w.dist = computeFeeDistribution(allFees, uint32(len(w.buckets))) //nolint:gosec // len ≤ retentionWindow ≤ 1000
	return nil
}

func (w *feeWindow) reset() {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.buckets = w.buckets[:0]
	w.start = 0
	w.dist = store.FeeDistribution{}
}

func (w *feeWindow) distribution() store.FeeDistribution {
	w.lock.RLock()
	defer w.lock.RUnlock()
	return w.dist
}

// computeFeeDistribution is copied verbatim from internal/rpcv1/feewindow (see
// the package doc for why the duplication is deliberate); the differential
// test against the stats library came along with it.
func computeFeeDistribution(fees []uint64, ledgerCount uint32) store.FeeDistribution {
	if len(fees) == 0 {
		return store.FeeDistribution{}
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
	return store.FeeDistribution{
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
