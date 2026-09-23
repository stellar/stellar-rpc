package txspan

import (
	"encoding/binary"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// prefixHash returns a 32-byte hash whose first four bytes are b — the only
// part of a hash the index stores.
func prefixHash(b [prefixLen]byte) [32]byte {
	var h [32]byte
	copy(h[:], b[:])
	h[31] = 0xAA
	return h
}

func entry(prefix [prefixLen]byte, applyIdx uint16) IndexEntry {
	return IndexEntry{HashPrefix: prefix, ApplyIdx: applyIdx}
}

// sampleRows returns five rows whose elements tile [1000, 1500).
func sampleRows() []Row {
	return []Row{
		{EnvStart: 100, EnvEnd: 140, ElemStart: 1000, ElemEnd: 1100},
		{EnvStart: 200, EnvEnd: 260, ElemStart: 1100, ElemEnd: 1180},
		{EnvStart: 300, EnvEnd: 340, ElemStart: 1180, ElemEnd: 1300},
		{EnvStart: 400, EnvEnd: 470, ElemStart: 1300, ElemEnd: 1400},
		{EnvStart: 500, EnvEnd: 560, ElemStart: 1400, ElemEnd: 1500},
	}
}

func sampleIndex() []IndexEntry {
	return []IndexEntry{
		entry([prefixLen]byte{9}, 4),
		entry([prefixLen]byte{1}, 0),
		entry([prefixLen]byte{5, 5}, 2),
		entry([prefixLen]byte{1}, 3),
		entry([prefixLen]byte{0, 0, 0, 1}, 1),
	}
}

func sampleLayout() Layout {
	return Layout{LCMVersion: 2, LedgerSeq: 4_000}
}

func TestEncodeParseRoundTrip(t *testing.T) {
	layout := sampleLayout()
	layout.Frames = []Frame{{Compressed: 17, Raw: 64}, {Compressed: 3, Raw: 8}}
	rows := sampleRows()
	// Encode sorts the index in place, so index holds the stored order afterwards.
	index := sampleIndex()
	encoded := Encode(nil, layout, rows, index)

	tbl, err := Parse(encoded)
	require.NoError(t, err)
	assert.Equal(t, uint8(2), tbl.LCMVersion())
	assert.Equal(t, 4, tbl.ExtBytes())
	assert.Equal(t, 5, tbl.TxCount())
	assert.Equal(t, 5, tbl.IndexCount())
	require.Equal(t, 2, tbl.FrameCount())
	for i, want := range layout.Frames {
		assert.Equal(t, want, tbl.Frame(i), "frame %d", i)
	}

	for i, want := range rows {
		assert.Equal(t, want, tbl.Row(i), "row %d", i)
	}
	for i := range index {
		assert.Equal(t, index[i].HashPrefix[:], tbl.prefixAt(i), "prefix %d", i)
	}
	assertIndexSorted(t, tbl)
}

// TestRowElementEndIsDerived pins the layout change: only the last element's
// end is stored, and every other row's end is its successor's start.
func TestRowElementEndIsDerived(t *testing.T) {
	rows := sampleRows()
	tbl, err := Parse(Encode(nil, sampleLayout(), rows, sampleIndex()))
	require.NoError(t, err)

	for i := 0; i+1 < tbl.TxCount(); i++ {
		assert.Equal(t, tbl.Row(i+1).ElemStart, tbl.Row(i).ElemEnd, "row %d", i)
	}
	assert.Equal(t, rows[len(rows)-1].ElemEnd, tbl.Row(tbl.TxCount()-1).ElemEnd)
}

func TestEncodeEmptyTable(t *testing.T) {
	tbl, err := Parse(Encode(nil, Layout{LCMVersion: 1}, nil, nil))
	require.NoError(t, err)
	assert.Equal(t, uint8(1), tbl.LCMVersion())
	assert.Zero(t, tbl.ExtBytes())
	assert.Zero(t, tbl.TxCount())
	assert.Zero(t, tbl.IndexCount())
	assert.Zero(t, tbl.FrameCount())
	assert.Len(t, tbl, headerSize+trailerSize)
	assert.Empty(t, slices.Collect(tbl.Find(prefixHash([prefixLen]byte{1}))))
}

func TestEncodeAppendsToDestination(t *testing.T) {
	prefix := []byte("keep me")
	encoded := Encode(slices.Clone(prefix), sampleLayout(), sampleRows(), sampleIndex())
	assert.Equal(t, prefix, encoded[:len(prefix)])
	tbl, err := Parse(encoded[len(prefix):])
	require.NoError(t, err)
	assert.Equal(t, 5, tbl.TxCount())
}

func TestFindReturnsTheWholeEqualPrefixRun(t *testing.T) {
	shared := [prefixLen]byte{7, 7, 7, 7}
	index := []IndexEntry{
		entry([prefixLen]byte{1}, 0),
		entry(shared, 1),
		entry(shared, 2),
		entry(shared, 3),
		entry([prefixLen]byte{9}, 4),
	}
	rows := sampleRows()
	tbl, err := Parse(Encode(nil, sampleLayout(), rows, index))
	require.NoError(t, err)

	got := slices.Collect(tbl.Find(prefixHash(shared)))
	require.Len(t, got, 3)
	for _, want := range []int{1, 2, 3} {
		assert.Contains(t, got, Match{ApplyIdx: want, Row: rows[want]})
	}

	assert.Len(t, slices.Collect(tbl.Find(prefixHash([prefixLen]byte{1}))), 1)
	assert.Empty(t, slices.Collect(tbl.Find(prefixHash([prefixLen]byte{4}))))
	assert.Empty(t, slices.Collect(tbl.Find(prefixHash([prefixLen]byte{255}))))
}

// TestFindResolvesTheApplyIndex pins that a match carries the row at its OWN
// apply index, not the index entry's position.
func TestFindResolvesTheApplyIndex(t *testing.T) {
	rows := sampleRows()
	tbl, err := Parse(Encode(nil, sampleLayout(), rows, sampleIndex()))
	require.NoError(t, err)

	got := slices.Collect(tbl.Find(prefixHash([prefixLen]byte{5, 5})))
	require.Len(t, got, 1)
	assert.Equal(t, 2, got[0].ApplyIdx)
	assert.Equal(t, rows[2], got[0].Row)
}

func TestFindStopsEarlyWhenTheCallerBreaks(t *testing.T) {
	shared := [prefixLen]byte{7}
	index := []IndexEntry{entry(shared, 0), entry(shared, 1)}
	tbl, err := Parse(Encode(nil, sampleLayout(), sampleRows(), index))
	require.NoError(t, err)

	seen := 0
	for range tbl.Find(prefixHash(shared)) {
		seen++
		break
	}
	assert.Equal(t, 1, seen)
}

func TestParseRejectsEverySingleByteFlip(t *testing.T) {
	layout := sampleLayout()
	layout.Frames = []Frame{{Compressed: 1, Raw: 2}}
	encoded := Encode(nil, layout, sampleRows(), sampleIndex())
	for i := range encoded {
		corrupt := slices.Clone(encoded)
		corrupt[i] ^= 0x01
		_, err := Parse(corrupt)
		require.ErrorIs(t, err, ErrChecksum, "byte %d", i)
		require.ErrorIs(t, err, ErrCorrupt, "byte %d", i)
	}
}

func TestParseRejectsTruncation(t *testing.T) {
	layout := sampleLayout()
	layout.Frames = []Frame{{Compressed: 1, Raw: 2}}
	encoded := Encode(nil, layout, sampleRows(), sampleIndex())
	for n := range encoded {
		_, err := Parse(encoded[:n])
		require.ErrorIs(t, err, ErrCorrupt, "truncated to %d bytes", n)
	}
	_, err := Parse(append(slices.Clone(encoded), 0))
	require.ErrorIs(t, err, ErrCorrupt, "trailing byte")
}

func TestParseRejectsForeignHeaders(t *testing.T) {
	base := Encode(nil, sampleLayout(), sampleRows(), sampleIndex())

	for name, mutate := range map[string]func([]byte){
		"magic":         func(b []byte) { b[0] = 'X' },
		"lcm version":   func(b []byte) { b[offLCMVersion] = 3 },
		"reserved":      func(b []byte) { b[offReserved] = 1 },
		"tx count":      func(b []byte) { b[offTxCount+3]++ },
		"index count":   func(b []byte) { b[offIndexCount+3]++ },
		"frames":        func(b []byte) { b[offFrameCount+3]++ },
		"array end":     func(b []byte) { binary.BigEndian.PutUint32(b[offTxProcessingEnd:], 1200) },
		"apply index":   func(b []byte) { b[indexEntryOffset(b, 0)+offApplyIdx+1] = 9 },
		"index order":   func(b []byte) { b[indexEntryOffset(b, 0)] = 0xFF },
		"element order": func(b []byte) { binary.BigEndian.PutUint32(b[rowOffset(b, 1)+offElemStart:], 500) },
	} {
		t.Run(name, func(t *testing.T) {
			corrupt := slices.Clone(base)
			mutate(corrupt)
			_, err := Parse(restamp(corrupt))
			require.ErrorIs(t, err, ErrCorrupt)
			require.NotErrorIs(t, err, ErrChecksum)
		})
	}
}

// TestParseReportsAnUnknownVersionApart pins the one header field whose
// refusal is not corruption: a table a LATER build wrote. Its layout past the
// magic is not this build's to read, so Parse says so with a sentinel of its
// own and readers walk the ledger instead of failing the request — while a
// table of THIS version with a broken checksum stays an error.
func TestParseReportsAnUnknownVersionApart(t *testing.T) {
	base := Encode(nil, sampleLayout(), sampleRows(), sampleIndex())

	newer := slices.Clone(base)
	newer[offVersion] = formatVersion + 1
	_, err := Parse(restamp(newer))
	require.ErrorIs(t, err, ErrUnknownVersion)
	require.NotErrorIs(t, err, ErrCorrupt, "a newer artifact is not a broken one")

	// The same field, read by the one caller that may not walk: a copier
	// cannot locate the stamp it would check the pairing against.
	_, err = StampedSeq(restamp(newer))
	require.ErrorIs(t, err, ErrUnknownVersion)

	corrupt := slices.Clone(base)
	corrupt[len(corrupt)-1] ^= 0x01
	_, err = Parse(corrupt)
	require.ErrorIs(t, err, ErrChecksum)
	require.NotErrorIs(t, err, ErrUnknownVersion)
}

// rowOffset and indexEntryOffset locate a row and an index entry in an encoded
// buffer whose header the caller may not have disturbed yet.
func rowOffset(b []byte, i int) int {
	return headerSize + int(binary.BigEndian.Uint32(b[offFrameCount:]))*dirEntrySize + i*rowWidth
}

func indexEntryOffset(b []byte, i int) int {
	return rowOffset(b, int(binary.BigEndian.Uint32(b[offTxCount:]))) + i*indexWidth
}

// restamp rewrites the trailer over b, so a header or body mutation reaches the
// field checks instead of stopping at the checksum.
func restamp(b []byte) []byte {
	body := b[:len(b)-trailerSize]
	return binary.BigEndian.AppendUint32(slices.Clone(body), crc32c(body))
}

func assertIndexSorted(t *testing.T, tbl Table) {
	t.Helper()
	for i := 1; i < tbl.IndexCount(); i++ {
		require.LessOrEqual(t, string(tbl.prefixAt(i-1)), string(tbl.prefixAt(i)),
			"index entries are not sorted at %d", i)
	}
}
