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
	require.NoError(t, m.Append(101, 2029)) // ledger 101: 987 events, IDs 1042–2028
	require.NoError(t, m.Append(102, 4529)) // ledger 102: 2,500 events, IDs 2029–4528

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

func TestLedgerOffsets_EventIDRange_FullRange(t *testing.T) {
	m := NewLedgerOffsets(100)
	require.NoError(t, m.Append(100, 1042))
	require.NoError(t, m.Append(101, 2029))
	require.NoError(t, m.Append(102, 4529))

	start, end, err := m.EventIDRange(100, 102)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), start)
	assert.Equal(t, uint32(4529), end)
}

func TestLedgerOffsets_EventIDRange_MultiLedger(t *testing.T) {
	m := NewLedgerOffsets(100)
	require.NoError(t, m.Append(100, 1042)) // ledger 100: IDs 0–1041
	require.NoError(t, m.Append(101, 2029)) // ledger 101: IDs 1042–2028
	require.NoError(t, m.Append(102, 4529)) // ledger 102: IDs 2029–4528
	require.NoError(t, m.Append(103, 4529)) // ledger 103: 0 events
	require.NoError(t, m.Append(104, 5000)) // ledger 104: IDs 4529–4999

	// Middle two ledgers (101-102).
	start, end, err := m.EventIDRange(101, 102)
	require.NoError(t, err)
	assert.Equal(t, uint32(1042), start)
	assert.Equal(t, uint32(4529), end)

	// Range spanning a zero-event ledger (102-104).
	start, end, err = m.EventIDRange(102, 104)
	require.NoError(t, err)
	assert.Equal(t, uint32(2029), start)
	assert.Equal(t, uint32(5000), end)

	// First two ledgers (100-101).
	start, end, err = m.EventIDRange(100, 101)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), start)
	assert.Equal(t, uint32(2029), end)

	// Last two ledgers (103-104), 103 has 0 events.
	start, end, err = m.EventIDRange(103, 104)
	require.NoError(t, err)
	assert.Equal(t, uint32(4529), start)
	assert.Equal(t, uint32(5000), end)
}

func TestLedgerOffsets_EventIDRange_SingleLedger(t *testing.T) {
	m := NewLedgerOffsets(100)
	require.NoError(t, m.Append(100, 0))    // ledger 100: 0 events
	require.NoError(t, m.Append(101, 2029)) // ledger 101: IDs 0–2028
	require.NoError(t, m.Append(102, 2029)) // ledger 102: 0 events
	require.NoError(t, m.Append(103, 4529)) // ledger 103: IDs 2029–4528

	// First ledger (0 events).
	start, end, err := m.EventIDRange(100, 100)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), start)
	assert.Equal(t, uint32(0), end)

	// Ledger with events.
	start, end, err = m.EventIDRange(101, 101)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), start)
	assert.Equal(t, uint32(2029), end)

	// Zero-event ledger in the middle.
	start, end, err = m.EventIDRange(102, 102)
	require.NoError(t, err)
	assert.Equal(t, uint32(2029), start)
	assert.Equal(t, uint32(2029), end)

	// Last ledger.
	start, end, err = m.EventIDRange(103, 103)
	require.NoError(t, err)
	assert.Equal(t, uint32(2029), start)
	assert.Equal(t, uint32(4529), end)
}

func TestLedgerOffsets_EventIDRange_OutOfBounds(t *testing.T) {
	m := NewLedgerOffsets(100)
	require.NoError(t, m.Append(100, 1042))
	require.NoError(t, m.Append(101, 2029))
	require.NoError(t, m.Append(102, 4529))

	// Before chunk.
	_, _, err := m.EventIDRange(90, 101)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "outside bounds")

	// After chunk.
	_, _, err = m.EventIDRange(101, 200)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "outside bounds")

	// Start > end.
	_, _, err = m.EventIDRange(102, 100)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "after end")
}

func TestLedgerOffsets_EventIDRange_NoOverlap(t *testing.T) {
	m := NewLedgerOffsets(100)
	require.NoError(t, m.Append(100, 1042))
	require.NoError(t, m.Append(101, 2029))

	_, _, err := m.EventIDRange(50, 99)
	require.Error(t, err)

	_, _, err = m.EventIDRange(102, 200)
	require.Error(t, err)
}

func TestLedgerOffsets_EventIDRange_Empty(t *testing.T) {
	m := NewLedgerOffsets(100)
	_, _, err := m.EventIDRange(100, 200)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no ledgers")
}

func TestLedgerOffsets_EventIDRange_LedgerWithZeroEvents(t *testing.T) {
	m := NewLedgerOffsets(100)
	require.NoError(t, m.Append(100, 500)) // ledger 100: 500 events
	require.NoError(t, m.Append(101, 500)) // ledger 101: 0 events
	require.NoError(t, m.Append(102, 800)) // ledger 102: 300 events

	// Zero-event ledger alone — returns empty range, no error.
	start, end, err := m.EventIDRange(101, 101)
	require.NoError(t, err)
	assert.Equal(t, start, end)

	// Across the zero-event ledger.
	start, end, err = m.EventIDRange(100, 102)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), start)
	assert.Equal(t, uint32(800), end)
}

func TestLedgerOffsets_TotalEvents_Empty(t *testing.T) {
	m := NewLedgerOffsets(100)
	assert.Equal(t, uint32(0), m.TotalEvents())
}

func TestLedgerOffsets_ConcurrentReadWrite(t *testing.T) {
	m := NewLedgerOffsets(0)

	const numLedgers = 10_000
	const eventsPerLedger = 1000
	const numReaders = 4

	var wg sync.WaitGroup

	wg.Go(func() {
		for i := range uint32(numLedgers) {
			require.NoError(t, m.Append(i, (i+1)*eventsPerLedger))
		}
	})

	for range numReaders {
		wg.Go(func() {
			for i := range uint32(numLedgers) {
				_, _, _ = m.EventIDRange(0, i)
				_ = m.LedgerCount()
				_ = m.TotalEvents()
			}
		})
	}

	wg.Wait()

	assert.Equal(t, numLedgers, m.LedgerCount())
	assert.Equal(t, uint32(numLedgers*eventsPerLedger), m.TotalEvents())
}

func TestLedgerOffsets_Offsets(t *testing.T) {
	m := NewLedgerOffsets(0)
	require.NoError(t, m.Append(0, 10))
	require.NoError(t, m.Append(1, 25))
	require.NoError(t, m.Append(2, 30))

	offsets := m.Offsets()
	assert.Equal(t, []uint32{10, 25, 30}, offsets)
}
