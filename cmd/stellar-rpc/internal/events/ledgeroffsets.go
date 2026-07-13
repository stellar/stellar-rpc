package events

import (
	"fmt"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// LedgerOffsets tracks cumulative event counts per ledger within a
// chunk for build-then-read paths (cold backfill, DecodeLedgerOffsets
// on the cold-reader side). Single-threaded by contract: callers
// either build it in one goroutine and hand it off to many readers
// via a sync-providing handoff (sync.OnceValues,
// struct-field-after-init, etc.), or use it from one goroutine
// throughout.
//
// For live ingest paths (HotStore) use ConcurrentLedgerOffsets;
// queries borrow a read-only view of it via View.
//
// offsets[i] holds the cumulative event count through relative ledger i.
// Ledger i's events span IDs [offsets[i-1], offsets[i]).
// For i==0, the range is [0, offsets[0]).
type LedgerOffsets struct {
	offsets     []uint32
	startLedger uint32
}

// NewLedgerOffsets creates an empty LedgerOffsets starting at the given
// absolute ledger sequence number. Pre-allocates capacity for one chunk.
func NewLedgerOffsets(startLedger uint32) *LedgerOffsets {
	return &LedgerOffsets{
		offsets:     make([]uint32, 0, chunk.LedgersPerChunk),
		startLedger: startLedger,
	}
}

// Append records the number of events in one ledger. The ledger must
// be the next expected in sequence; it validates because its caller
// decodes untrusted on-disk bytes. (The hot sibling
// ConcurrentLedgerOffsets.Append is positional and unchecked — its
// callers own the sequence invariant.)
//
// Not safe for concurrent use. The build-then-read contract is the
// caller's responsibility.
func (m *LedgerOffsets) Append(ledger, eventCount uint32) error {
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

// EventIDs returns the half-open event ID range [start, end) for the
// given ledger. The ledger must be in [StartLedger, EndLedger).
//
// For a multi-ledger range within a chunk, call EventIDs on the first
// and last ledgers and combine the results.
func (m *LedgerOffsets) EventIDs(ledger uint32) (uint32, uint32, error) {
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
	return len(m.offsets)
}

// TotalEvents returns the total number of events across all recorded ledgers.
func (m *LedgerOffsets) TotalEvents() uint32 {
	if len(m.offsets) == 0 {
		return 0
	}
	return m.offsets[len(m.offsets)-1]
}

// Offsets returns the underlying cumulative offset slice. Callers MUST
// NOT mutate it. Sharing the underlying slice is safe here because
// LedgerOffsets is single-threaded; concurrent readers using a value
// from this method must coordinate any retention themselves.
func (m *LedgerOffsets) Offsets() []uint32 {
	return m.offsets
}

// StartLedger returns the absolute ledger sequence number of the first ledger.
func (m *LedgerOffsets) StartLedger() uint32 {
	return m.startLedger
}

// EndLedger returns the exclusive end ledger (one past the last recorded ledger).
func (m *LedgerOffsets) EndLedger() uint32 {
	// len(m.offsets) is bounded by LedgersPerChunk, safe to convert.
	return m.startLedger + uint32(len(m.offsets)) //nolint:gosec
}
