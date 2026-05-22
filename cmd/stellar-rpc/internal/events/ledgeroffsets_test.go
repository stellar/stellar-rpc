package events

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
// against the concurrent variant. The read methods atomic-load the
// length; appends atomic-store after writing the backing slot.
func TestConcurrentLedgerOffsets_Basic(t *testing.T) {
	m := NewConcurrentLedgerOffsets(100)
	require.NoError(t, m.Append(100, 1042))
	require.NoError(t, m.Append(101, 987))
	require.NoError(t, m.Append(102, 2500))

	assert.Equal(t, 3, m.LedgerCount())
	assert.Equal(t, uint32(4529), m.TotalEvents())
	assert.Equal(t, uint32(100), m.StartLedger())
	assert.Equal(t, uint32(103), m.EndLedger())

	start, end, err := m.EventIDs(101)
	require.NoError(t, err)
	assert.Equal(t, uint32(1042), start)
	assert.Equal(t, uint32(2029), end)
}

// TestConcurrentLedgerOffsets_Snapshot pins that Snapshot returns a
// uniquely-owned LedgerOffsets containing the visible state at call
// time. Subsequent appends to the source don't affect the snapshot.
func TestConcurrentLedgerOffsets_Snapshot(t *testing.T) {
	m := NewConcurrentLedgerOffsets(0)
	require.NoError(t, m.Append(0, 10))
	require.NoError(t, m.Append(1, 20))

	snap := m.Snapshot()
	assert.Equal(t, 2, snap.LedgerCount())
	assert.Equal(t, uint32(30), snap.TotalEvents())

	// Append after snapshot.
	require.NoError(t, m.Append(2, 5))

	// Source advanced; snapshot unchanged.
	assert.Equal(t, 3, m.LedgerCount())
	assert.Equal(t, 2, snap.LedgerCount())
	assert.Equal(t, uint32(30), snap.TotalEvents())
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
		for i := range uint32(numLedgers) {
			require.NoError(t, m.Append(i, eventsPerLedger))
		}
	})

	for range numReaders {
		wg.Go(func() {
			for i := range uint32(numLedgers) {
				_, _, _ = m.EventIDs(i)
				_ = m.LedgerCount()
				_ = m.TotalEvents()
			}
		})
	}

	wg.Wait()

	assert.Equal(t, numLedgers, m.LedgerCount())
	assert.Equal(t, uint32(numLedgers*eventsPerLedger), m.TotalEvents())
}
