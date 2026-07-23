package events

import (
	"sync/atomic"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// ConcurrentLedgerOffsets tracks cumulative event counts per ledger
// for live ingest paths: a single writer (HotStore.applyLedger via
// the post-commit hook) and many concurrent readers (queries calling
// View / TotalEvents / etc.). Lock-free reads via a fixed-capacity
// backing array plus an atomic counter publishing the valid prefix.
//
// Memory: 40KB up-front per instance (LedgersPerChunk × uint32).
// Same as the slice the locked version pre-allocates, just shaped
// differently.
//
// Concurrency model:
//
//   - Append (writer): writes backing[n] then atomic.Store(len, n+1).
//     The Store happens-before any reader's Load that observes the
//     new value (Go memory model), so the prior write is visible.
//   - Read methods (View, TotalEvents, etc.): atomic.Load(len),
//     then read backing[:n]. Disjoint memory from the writer's
//     next write at backing[n].
//
// Single-writer contract: Append must be called from a single
// goroutine. Concurrent Appends would race on the len counter and
// lose updates.
type ConcurrentLedgerOffsets struct {
	backing     [chunk.LedgersPerChunk]uint32
	count       atomic.Uint32
	startLedger uint32 // immutable after construction
}

// NewConcurrentLedgerOffsets creates an empty ConcurrentLedgerOffsets
// starting at the given absolute ledger sequence number.
func NewConcurrentLedgerOffsets(startLedger uint32) *ConcurrentLedgerOffsets {
	return &ConcurrentLedgerOffsets{startLedger: startLedger}
}

// Append records the number of events in the next ledger in sequence
// (startLedger + LedgerCount). The structure is purely positional —
// ledger N lives at slot N-startLedger — so there is no ledger argument;
// the caller delivers ledgers in order. A caller fed by an untrusted
// source (warmup replaying on-disk rows) must validate the ledger
// sequence itself before calling; appending past the chunk's capacity is
// a caller bug and panics via the backing-array bounds check.
//
// Single-writer: must not run concurrently with itself.
func (m *ConcurrentLedgerOffsets) Append(eventCount uint32) {
	n := m.count.Load()
	var cumulative uint32
	if n > 0 {
		cumulative = m.backing[n-1]
	}
	// Write the new entry before publishing the new count: the
	// atomic.Store synchronizes-with a reader's atomic.Load, so the
	// write at backing[n] is visible to any reader that observes
	// count >= n+1.
	m.backing[n] = cumulative + eventCount
	m.count.Store(n + 1)
}

// LedgerCount returns the number of ledgers recorded.
func (m *ConcurrentLedgerOffsets) LedgerCount() int {
	return int(m.count.Load())
}

// TotalEvents returns the total number of events across all recorded ledgers.
func (m *ConcurrentLedgerOffsets) TotalEvents() uint32 {
	n := m.count.Load()
	if n == 0 {
		return 0
	}
	return m.backing[n-1]
}

// StartLedger returns the absolute ledger sequence number of the first ledger.
func (m *ConcurrentLedgerOffsets) StartLedger() uint32 {
	return m.startLedger
}

// EndLedger returns the exclusive end ledger (one past the last recorded ledger).
func (m *ConcurrentLedgerOffsets) EndLedger() uint32 {
	return m.startLedger + m.count.Load()
}

// View returns a *LedgerOffsets sharing the live backing array,
// capped to the count visible at call time. Used by HotStore on
// the query hot path: each Query allocates one View (~24 bytes:
// slice header + startLedger) instead of a 40KB deep copy of the
// full backing array.
//
// Safety: the slice's cap is set equal to its len, so any caller
// that calls Append on the returned LedgerOffsets allocates a
// fresh backing array (forking the view from the live data). The
// ConcurrentLedgerOffsets writer only writes at positions ≥ count
// at the time of any prior Store; readers only see positions
// [0, captured count), which were stably written and published
// via the count.Store before this View captured count.
//
// Callers MUST treat the returned LedgerOffsets as read-only.
// Calling Append on it would silently fork the view and the
// resulting state is not visible to either the live
// ConcurrentLedgerOffsets or other readers.
func (m *ConcurrentLedgerOffsets) View() *LedgerOffsets {
	n := m.count.Load()
	return &LedgerOffsets{
		offsets:     m.backing[:n:n],
		startLedger: m.startLedger,
	}
}
