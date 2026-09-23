package packfile

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRecordRangeAndReadAt pins the raw accessors a caller uses to read part
// of a record: the extent RecordRange reports is exactly the bytes the record
// occupies, and ReadAt serves any slice of it.
func TestRecordRangeAndReadAt(t *testing.T) {
	items := [][]byte{[]byte("first record"), []byte("second record"), []byte("third")}
	path := writeItemsPack(t, items, 1)

	r := Open(path, ReaderOptions{})
	t.Cleanup(func() { _ = r.Close() })

	for i, want := range items {
		offset, size, err := r.RecordRange(i)
		require.NoError(t, err)
		require.EqualValues(t, len(want), size, "record %d size", i)

		whole := make([]byte, size)
		require.NoError(t, r.ReadAt(whole, offset))
		assert.Equal(t, want, whole, "record %d bytes", i)

		// A partial read lands where the caller asked, which is the point of
		// the accessor: a reader after a record's front need not take it all.
		head := make([]byte, 5)
		require.NoError(t, r.ReadAt(head, offset))
		assert.Equal(t, want[:5], head, "record %d head", i)
	}

	_, _, err := r.RecordRange(len(items))
	require.ErrorIs(t, err, ErrPositionOutOfRange)
	_, _, err = r.RecordRange(-1)
	require.ErrorIs(t, err, ErrPositionOutOfRange)
}

// TestRecordRangeSpansEveryItemOfAMultiItemRecord pins the documented caveat:
// with several items per record the extent covers them all plus the record's
// index, so only a one-item-per-record caller may treat it as an item.
func TestRecordRangeSpansEveryItemOfAMultiItemRecord(t *testing.T) {
	items := [][]byte{[]byte("aa"), []byte("bb"), []byte("cc"), []byte("dd")}
	path := writeItemsPack(t, items, 2)

	r := Open(path, ReaderOptions{})
	t.Cleanup(func() { _ = r.Close() })

	firstOff, firstSize, err := r.RecordRange(0)
	require.NoError(t, err)
	secondOff, secondSize, err := r.RecordRange(1)
	require.NoError(t, err)
	assert.Equal(t, firstOff, secondOff, "both items live in one record")
	assert.Equal(t, firstSize, secondSize)
	assert.Greater(t, firstSize, int64(len(items[0])+len(items[1])), "the extent includes the record index")

	record := make([]byte, firstSize)
	require.NoError(t, r.ReadAt(record, firstOff))
	assert.True(t, bytes.HasPrefix(record, append(append([]byte{}, items[0]...), items[1]...)))
}

// writeItemsPack writes a passthrough pack holding items at the given
// items-per-record and returns its path.
func writeItemsPack(t *testing.T, items [][]byte, perRecord int) string {
	t.Helper()
	path := t.TempDir() + "/items.pack"
	w, err := Create(path, WriterOptions{ItemsPerRecord: perRecord, Format: 7, Overwrite: true})
	require.NoError(t, err)
	for _, item := range items {
		require.NoError(t, w.AppendItem(item))
	}
	require.NoError(t, w.Finish(nil))
	return path
}
