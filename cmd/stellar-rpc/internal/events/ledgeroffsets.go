package events

import (
	"fmt"
	"sync"
)

// LedgersPerChunk is the number of ledgers in each chunk.
const LedgersPerChunk = 10_000

// LedgerOffsets tracks cumulative event counts per ledger within a chunk.
// It enables conversion from a ledger sequence number to an event ID,
// which is used to translate ledger range queries into bitmap index operations.
//
// Safe for concurrent use by a single writer and multiple readers.
//
// offsets[i] holds the cumulative event count through relative ledger i.
// Ledger i's events span IDs [offsets[i-1], offsets[i]).
// For i==0, the range is [0, offsets[0]).
type LedgerOffsets struct {
	mu          sync.RWMutex
	offsets     []uint32
	startLedger uint32
}

// NewLedgerOffsets creates an empty LedgerOffsets starting at the given
// absolute ledger sequence number. Pre-allocates capacity for one chunk.
func NewLedgerOffsets(startLedger uint32) *LedgerOffsets {
	return &LedgerOffsets{
		offsets:     make([]uint32, 0, LedgersPerChunk),
		startLedger: startLedger,
	}
}

// Append records the number of events in one ledger. The ledger must be the
// next expected in sequence.
func (m *LedgerOffsets) Append(ledger, eventCount uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// len(m.offsets) is bounded by LedgersPerChunk, safe to convert.
	expected := m.startLedger + uint32(len(m.offsets)) //nolint:gosec
	if ledger != expected {
		return fmt.Errorf("expected ledger %d, got %d", expected, ledger)
	}

	var cumulative uint32
	if len(m.offsets) > 0 {
		cumulative = m.offsets[len(m.offsets)-1]
	}
	m.offsets = append(m.offsets, cumulative+eventCount)
	return nil
}

// EventIDs returns the half-open event ID range [start, end) for the given
// ledger. The ledger must be in [StartLedger, EndLedger).
//
// For a multi-ledger range within a chunk, call EventIDs on the first and
// last ledgers and combine the results.
func (m *LedgerOffsets) EventIDs(ledger uint32) (uint32, uint32, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.offsets) == 0 {
		return 0, 0, fmt.Errorf("ledger %d not found: no ledgers recorded", ledger)
	}

	// len(m.offsets) is bounded by LedgersPerChunk, safe to convert.
	endLedger := m.startLedger + uint32(len(m.offsets)) //nolint:gosec
	if ledger < m.startLedger || ledger >= endLedger {
		return 0, 0, fmt.Errorf("ledger %d is outside bounds [%d, %d)",
			ledger, m.startLedger, endLedger)
	}

	rel := ledger - m.startLedger
	var start uint32
	if rel > 0 {
		start = m.offsets[rel-1]
	}
	return start, m.offsets[rel], nil
}

// LedgerCount returns the number of ledgers recorded.
func (m *LedgerOffsets) LedgerCount() int {
	m.mu.RLock()
	n := len(m.offsets)
	m.mu.RUnlock()
	return n
}

// TotalEvents returns the total number of events across all recorded ledgers.
func (m *LedgerOffsets) TotalEvents() uint32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.offsets) == 0 {
		return 0
	}
	return m.offsets[len(m.offsets)-1]
}

// Offsets returns a copy of the cumulative offset slice.
func (m *LedgerOffsets) Offsets() []uint32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cp := make([]uint32, len(m.offsets))
	copy(cp, m.offsets)
	return cp
}

// StartLedger returns the absolute ledger sequence number of the first ledger.
func (m *LedgerOffsets) StartLedger() uint32 {
	return m.startLedger
}

// EndLedger returns the exclusive end ledger (one past the last recorded ledger).
func (m *LedgerOffsets) EndLedger() uint32 {
	m.mu.RLock()
	// len(m.offsets) is bounded by LedgersPerChunk, safe to convert.
	n := uint32(len(m.offsets)) //nolint:gosec
	m.mu.RUnlock()
	return m.startLedger + n
}
