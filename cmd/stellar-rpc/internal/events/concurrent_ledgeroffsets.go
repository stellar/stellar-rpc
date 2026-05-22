package events

import (
	"fmt"
	"sync/atomic"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ConcurrentLedgerOffsets tracks cumulative event counts per ledger
// for live ingest paths: a single writer (IngestLedgerEvents) and
// many concurrent readers (queries calling EventIDs / TotalEvents /
// etc.). Lock-free reads via a fixed-capacity backing array plus an
// atomic counter publishing the valid prefix.
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
//   - Read methods (EventIDs, TotalEvents, etc.): atomic.Load(len),
//     then read backing[:n]. Disjoint memory from the writer's
//     next write at backing[n].
//   - Snapshot copies backing[:n] into a fresh slice and returns a
//     uniquely-owned LedgerOffsets for serialization (ColdWriter.Finish,
//     query handoff to the build-then-read shape).
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

// Append records the number of events in one ledger. The ledger must
// be the next expected in sequence.
//
// Single-writer: must not run concurrently with itself.
func (m *ConcurrentLedgerOffsets) Append(ledger, eventCount uint32) error {
	n := m.count.Load()
	expected := m.startLedger + n
	if ledger != expected {
		return fmt.Errorf("expected ledger %d, got %d", expected, ledger)
	}
	if int(n) >= len(m.backing) {
		return fmt.Errorf("offsets backing array full (chunk limit %d reached)", len(m.backing))
	}

	var cumulative uint32
	if n > 0 {
		cumulative = m.backing[n-1]
	}
	// Write the new entry before publishing the new count. The
	// atomic.Store synchronizes-with subsequent atomic.Load on
	// readers — the write at backing[n] is visible to any reader
	// that observes count >= n+1.
	m.backing[n] = cumulative + eventCount
	m.count.Store(n + 1)
	return nil
}

// EventIDs returns the half-open event ID range [start, end) for the
// given ledger.
func (m *ConcurrentLedgerOffsets) EventIDs(ledger uint32) (uint32, uint32, error) {
	n := m.count.Load()
	if n == 0 {
		return 0, 0, fmt.Errorf("ledger %d not found: no ledgers recorded", ledger)
	}
	endLedger := m.startLedger + n
	if ledger < m.startLedger || ledger >= endLedger {
		return 0, 0, fmt.Errorf("ledger %d is outside bounds [%d, %d)",
			ledger, m.startLedger, endLedger)
	}
	rel := ledger - m.startLedger
	var start uint32
	if rel > 0 {
		start = m.backing[rel-1]
	}
	return start, m.backing[rel], nil
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

// Snapshot returns a uniquely-owned LedgerOffsets containing the
// current state. Used by HotStore to hand a stable snapshot to
// readers (Reader.Offsets) and by the freeze path to hand a
// build-then-read shape to ColdWriter.Finish.
func (m *ConcurrentLedgerOffsets) Snapshot() *LedgerOffsets {
	n := m.count.Load()
	offsets := make([]uint32, n)
	copy(offsets, m.backing[:n])
	return &LedgerOffsets{
		offsets:     offsets,
		startLedger: m.startLedger,
	}
}
