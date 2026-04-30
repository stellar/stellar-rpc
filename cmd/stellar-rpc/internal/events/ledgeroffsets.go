package events

import (
	"errors"
	"fmt"
	"sync"
)

// LedgersPerChunk is the number of ledgers in each chunk.
const LedgersPerChunk = 10_000

// LedgerOffsets tracks cumulative event counts per ledger within a chunk.
// It maps absolute ledger sequence numbers to event ID ranges, enabling
// efficient conversion from a ledger range query to an event ID range
// for bitmap index operations.
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

// Append records the cumulative event count after processing one ledger.
// The ledger must be the next expected in sequence. Counts must be non-decreasing.
func (m *LedgerOffsets) Append(ledger, cumulativeCount uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// len(m.offsets) is bounded by LedgersPerChunk, safe to convert.
	expected := m.startLedger + uint32(len(m.offsets)) //nolint:gosec
	if ledger != expected {
		return fmt.Errorf("expected ledger %d, got %d", expected, ledger)
	}
	m.offsets = append(m.offsets, cumulativeCount)
	return nil
}

// EventIDRange converts an inclusive absolute ledger range [startLedger, endLedger]
// into a half-open event ID range [startEventID, endEventID).
// If the range contains no events, startEventID == endEventID.
func (m *LedgerOffsets) EventIDRange(startLedger, endLedger uint32) (uint32, uint32, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.offsets) == 0 {
		return 0, 0, errors.New("no ledgers recorded")
	}

	if startLedger > endLedger {
		return 0, 0, fmt.Errorf("start ledger %d is after end ledger %d", startLedger, endLedger)
	}

	// len(m.offsets) is bounded by LedgersPerChunk, safe to convert.
	lastLedger := m.startLedger + uint32(len(m.offsets)) - 1 //nolint:gosec
	if startLedger < m.startLedger || endLedger > lastLedger {
		return 0, 0, fmt.Errorf("ledger range [%d, %d] is outside bounds [%d, %d]",
			startLedger, endLedger, m.startLedger, lastLedger)
	}

	relStart := startLedger - m.startLedger
	relEnd := endLedger - m.startLedger

	var startEventID uint32
	if relStart > 0 {
		startEventID = m.offsets[relStart-1]
	}
	endEventID := m.offsets[relEnd]

	return startEventID, endEventID, nil
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
