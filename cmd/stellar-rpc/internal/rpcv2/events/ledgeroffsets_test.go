package events

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

func TestLedgerOffsets_Basic(t *testing.T) {
	m := NewLedgerOffsets(100)
	require.NoError(t, m.Append(100, 1042)) // ledger 100: 1,042 events, IDs 0–1041
	require.NoError(t, m.Append(101, 987))  // ledger 101: 987 events, IDs 1042–2028
	require.NoError(t, m.Append(102, 2500)) // ledger 102: 2,500 events, IDs 2029–4528

	assert.Equal(t, 3, m.LedgerCount())
	assert.Equal(t, uint32(4529), m.TotalEvents())
	assert.Equal(t, uint32(100), m.StartLedger())
	assert.Equal(t, uint32(103), m.EndLedger())
}

func TestLedgerOffsets_AppendOutOfOrder(t *testing.T) {
	m := NewLedgerOffsets(100)
	require.NoError(t, m.Append(100, 10))

	// Skip ledger 101.
	err := m.Append(102, 20)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected ledger 101, got 102")

	// Wrong start.
	m2 := NewLedgerOffsets(100)
	err = m2.Append(99, 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected ledger 100, got 99")
}

func TestLedgerOffsets_EventIDs(t *testing.T) {
	m := NewLedgerOffsets(100)
	require.NoError(t, m.Append(100, 1042)) // ledger 100: IDs 0–1041
	require.NoError(t, m.Append(101, 987))  // ledger 101: IDs 1042–2028
	require.NoError(t, m.Append(102, 2500)) // ledger 102: IDs 2029–4528

	// First ledger.
	start, end, err := m.EventIDs(100)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), start)
	assert.Equal(t, uint32(1042), end)

	// Middle ledger.
	start, end, err = m.EventIDs(101)
	require.NoError(t, err)
	assert.Equal(t, uint32(1042), start)
	assert.Equal(t, uint32(2029), end)

	// Last ledger.
	start, end, err = m.EventIDs(102)
	require.NoError(t, err)
	assert.Equal(t, uint32(2029), start)
	assert.Equal(t, uint32(4529), end)
}

func TestLedgerOffsets_EventIDs_ZeroEventLedger(t *testing.T) {
	m := NewLedgerOffsets(100)
	require.NoError(t, m.Append(100, 500)) // ledger 100: IDs 0-499
	require.NoError(t, m.Append(101, 0))   // ledger 101: 0 events
	require.NoError(t, m.Append(102, 300)) // ledger 102: IDs 500-799

	// Ledger 101 has 0 events, so start == end.
	start, end, err := m.EventIDs(101)
	require.NoError(t, err)
	assert.Equal(t, start, end)
	assert.Equal(t, uint32(500), start)
}

func TestLedgerOffsets_EventIDs_OutOfBounds(t *testing.T) {
	m := NewLedgerOffsets(100)
	require.NoError(t, m.Append(100, 1042))
	require.NoError(t, m.Append(101, 987))

	// Before chunk.
	_, _, err := m.EventIDs(99)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "outside bounds")

	// At EndLedger (exclusive upper bound).
	_, _, err = m.EventIDs(102)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "outside bounds")

	// After chunk.
	_, _, err = m.EventIDs(200)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "outside bounds")
}

func TestLedgerOffsets_EventIDs_Empty(t *testing.T) {
	m := NewLedgerOffsets(100)
	_, _, err := m.EventIDs(100)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no ledgers")
}

func TestLedgerOffsets_EventIDs_MultiLedger(t *testing.T) {
	// Demonstrates getting the event ID range for a multi-ledger query
	// by composing two EventIDs calls.
	m := NewLedgerOffsets(100)
	require.NoError(t, m.Append(100, 1042))
	require.NoError(t, m.Append(101, 987))
	require.NoError(t, m.Append(102, 2500))

	first, _, err := m.EventIDs(101)
	require.NoError(t, err)
	_, last, err := m.EventIDs(102)
	require.NoError(t, err)
	assert.Equal(t, uint32(1042), first)
	assert.Equal(t, uint32(4529), last)
}

func TestLedgerOffsets_TotalEvents_Empty(t *testing.T) {
	m := NewLedgerOffsets(100)
	assert.Equal(t, uint32(0), m.TotalEvents())
}

func TestLedgerOffsets_Offsets(t *testing.T) {
	m := NewLedgerOffsets(0)
	require.NoError(t, m.Append(0, 10))
	require.NoError(t, m.Append(1, 15))
	require.NoError(t, m.Append(2, 5))

	offsets := m.Offsets()
	assert.Equal(t, []uint32{10, 25, 30}, offsets)
}

// TestConcurrentLedgerOffsets_Basic mirrors TestLedgerOffsets_Basic
// against the concurrent variant: the read methods atomic-load the length
// and appends atomic-store after writing the backing slot. Append is
// positional (no ledger argument; each call lands at startLedger + count),
// and a zero-event ledger (101) covers the empty-ledger case.
func TestConcurrentLedgerOffsets_Basic(t *testing.T) {
	m := NewConcurrentLedgerOffsets(100)
	m.Append(1042) // ledger 100: ids 0-1041
	m.Append(0)    // ledger 101: empty
	m.Append(987)  // ledger 102: ids 1042-2028
	m.Append(2500) // ledger 103: ids 2029-4528

	assert.Equal(t, 4, m.LedgerCount())
	assert.Equal(t, uint32(4529), m.TotalEvents())
	assert.Equal(t, uint32(100), m.StartLedger())
	assert.Equal(t, uint32(104), m.EndLedger())

	// Per-ledger ranges resolve through a View (the query path's shape).
	start, end, err := m.View().EventIDs(102)
	require.NoError(t, err)
	assert.Equal(t, uint32(1042), start)
	assert.Equal(t, uint32(2029), end)

	// The empty ledger (101) is a zero-width range.
	s, e, err := m.View().EventIDs(101)
	require.NoError(t, err)
	assert.Equal(t, uint32(1042), s)
	assert.Equal(t, uint32(1042), e, "empty ledger is zero-width")
}

// TestConcurrentLedgerOffsets_AppendPastCapacity pins the contract that
// appending past the fixed backing array panics. This is a caller-bug
// guard, not a runtime path: callers (ingest, warmup) validate the ledger
// is in-chunk before reaching it, so the panic is unreachable in practice.
func TestConcurrentLedgerOffsets_AppendPastCapacity(t *testing.T) {
	m := NewConcurrentLedgerOffsets(0)
	// Fill the backing array.
	for range chunk.LedgersPerChunk {
		m.Append(1)
	}
	assert.Equal(t, int(chunk.LedgersPerChunk), m.LedgerCount())
	// One past — bounds check panics rather than corrupting adjacent state.
	assert.Panics(t, func() { m.Append(1) })
}

// TestConcurrentLedgerOffsets_ConcurrentReadWrite exercises the
// lock-free single-writer + multi-reader pattern under -race.
func TestConcurrentLedgerOffsets_ConcurrentReadWrite(t *testing.T) {
	m := NewConcurrentLedgerOffsets(0)

	const numLedgers = 10_000
	const eventsPerLedger = 1000
	const numReaders = 4

	var wg sync.WaitGroup

	wg.Go(func() {
		for range uint32(numLedgers) {
			m.Append(eventsPerLedger)
		}
	})

	for range numReaders {
		wg.Go(func() {
			for i := range uint32(numLedgers) {
				_, _, _ = m.View().EventIDs(i)
				_ = m.LedgerCount()
				_ = m.TotalEvents()
			}
		})
	}

	wg.Wait()

	assert.Equal(t, numLedgers, m.LedgerCount())
	assert.Equal(t, uint32(numLedgers*eventsPerLedger), m.TotalEvents())
}

// TestConcurrentLedgerOffsets_ViewSharesBacking pins the
// allocation-saving View() semantics: the returned LedgerOffsets
// shares the live backing array (capped to the count visible at
// call time). A subsequent Append on the source ConcurrentLedgerOffsets
// does NOT change the view's LedgerCount/TotalEvents (the cap is
// frozen), but the bytes the view sees were stably written before
// the view was created.
func TestConcurrentLedgerOffsets_ViewSharesBacking(t *testing.T) {
	m := NewConcurrentLedgerOffsets(0)
	m.Append(10)
	m.Append(20)

	view := m.View()
	assert.Equal(t, 2, view.LedgerCount())
	assert.Equal(t, uint32(30), view.TotalEvents())
	assert.Equal(t, uint32(0), view.StartLedger())
	assert.Equal(t, uint32(2), view.EndLedger())

	// Append after View — the view stays at its captured count.
	m.Append(5)
	assert.Equal(t, 3, m.LedgerCount())
	assert.Equal(t, 2, view.LedgerCount(),
		"View's len is capped at the count captured when View was called")
	assert.Equal(t, uint32(30), view.TotalEvents(),
		"View must not observe Append calls that landed after View()")
}
