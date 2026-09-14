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
// TxProcessing walk's LedgerFees. What the two DO share lives in
// internal/store, because it is genuinely common: the store.FeeStats
// interface (the shared getFeeStats handler's contract) and
// store.ComputeFeeDistribution (the pure percentile math both services'
// numbers come out of — the one place where silent drift would matter).
// End-to-end agreement between the services is #883's parity harness's job.
package feewindow

import (
	"fmt"
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
	w.dist = store.ComputeFeeDistribution(allFees, uint32(len(w.buckets))) //nolint:gosec // len ≤ retentionWindow ≤ 1000
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
