package packfile

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// decodeOffsets returns where each record starts, then where the records end.
func decodeOffsets(section []byte, records, items, perRecord int, dataEnd int64) ([]int64, error) {
	x, err := parseIndex(section, records, items, perRecord, dataEnd)
	if err != nil {
		return nil, err
	}
	offsets := []int64{0}
	var tab groupTable
	for g := range x.groupCount {
		if err := tab.load(x, g); err != nil {
			return nil, err
		}
		offsets = append(offsets, tab.off[1:tab.n+1]...)
	}
	return offsets, nil
}

func reseal(section []byte) {
	payload := section[:len(section)-4]
	binary.LittleEndian.PutUint32(section[len(payload):], crc32c(payload))
}

func variedOffsets(records int) []int64 {
	offsets := make([]int64, records+1)
	for i := 1; i < len(offsets); i++ {
		offsets[i] = offsets[i-1] + int64(i%13)*100
	}
	return offsets
}

func TestIndexRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		offsets []int64
	}{
		{"single record", []int64{0, 1000}},
		{"few records", []int64{0, 1000, 2500, 5000}},
		{"uniform sizes", func() []int64 {
			offsets := make([]int64, 101)
			for i := range offsets {
				offsets[i] = int64(i) * 4096
			}

			return offsets
		}()},
		{"variable sizes", []int64{0, 100, 5000, 5100, 50000, 50001}},
		{"zero-size records", []int64{0, 0, 0, 100, 100}},
		{"exactly one group", variedOffsets(groupSize)},
		{"partial last group", variedOffsets(groupSize + 1)},
		{"several groups", variedOffsets(3*groupSize + 5)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := encodeIndex(tt.offsets, 4)
			require.NoError(t, err)

			records := len(tt.offsets) - 1
			decoded, err := decodeOffsets(encoded, records, 4*records, 4, tt.offsets[records])
			require.NoError(t, err)
			require.Equal(t, tt.offsets, decoded)
		})
	}
}

func TestIndexCRCCorruption(t *testing.T) {
	encoded, err := encodeIndex([]int64{0, 1000, 2000, 3000}, 1)
	require.NoError(t, err)

	// Corrupt a byte in the payload (before the CRC).
	encoded[0] ^= 0xFF

	_, err = parseIndex(encoded, 3, 3, 1, 3000)
	require.ErrorIs(t, err, ErrChecksum)
}

func TestIndexCorruptCRCBytes(t *testing.T) {
	encoded, err := encodeIndex([]int64{0, 1000, 2000, 3000}, 1)
	require.NoError(t, err)

	// Corrupt the CRC itself.
	binary.LittleEndian.PutUint32(encoded[len(encoded)-4:], 0xDEADBEEF)

	_, err = parseIndex(encoded, 3, 3, 1, 3000)
	require.ErrorIs(t, err, ErrChecksum)
}

func TestIndexTooSmall(t *testing.T) {
	_, err := parseIndex([]byte{1, 2, 3}, 1, 1, 1, 100)
	require.ErrorIs(t, err, ErrCorrupt)
}

// A trailer claiming more groups than the directory holds fails before any
// allocation sized by its claim.
func TestIndexImplausibleRecordCount(t *testing.T) {
	encoded, err := encodeIndex([]int64{0, 10}, 1)
	require.NoError(t, err)

	_, err = parseIndex(encoded, 1<<20, 1<<20, 1, 10)
	require.ErrorIs(t, err, ErrCorrupt)
	require.NotErrorIs(t, err, ErrChecksum)
}

// A wrong end of the records passes Open and fails the group's decode.
func TestIndexBaseMismatch(t *testing.T) {
	encoded, err := encodeIndex([]int64{0, 1000, 2000, 3000}, 1)
	require.NoError(t, err)

	x, err := parseIndex(encoded, 3, 3, 1, 9999)
	require.NoError(t, err)
	var tab groupTable
	require.ErrorIs(t, tab.load(x, 0), ErrCorrupt)
}

func TestIndexDirectoryChecks(t *testing.T) {
	const perRecord = 2
	offsets := variedOffsets(3 * groupSize)
	records := len(offsets) - 1
	items := records*perRecord - 1
	dataEnd := offsets[records]
	section, err := encodeIndex(offsets, perRecord)
	require.NoError(t, err)
	x, err := parseIndex(section, records, items, perRecord, dataEnd)
	require.NoError(t, err)

	field := func(g, off int) int { return len(x.groups) + g*dirEntryLen + off }
	put64 := func(at int, v int64) func([]byte) {
		return func(s []byte) { binary.LittleEndian.PutUint64(s[at:], uint64(v)) }
	}
	put32 := func(at, v int) func([]byte) {
		return func(s []byte) { binary.LittleEndian.PutUint32(s[at:], uint32(v)) }
	}
	for _, tc := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"flag set", func(s []byte) { s[field(1, dirFlags)] = 0x01 }},
		{"wrong first item", put32(field(1, dirFirstItem), groupSize*perRecord+1)},
		{"group 0 past byte 0", put64(field(0, dirFirstByte), 1)},
		{"group before its predecessor", put64(field(2, dirFirstByte), offsets[groupSize]-1)},
		{"group past the records", put64(field(2, dirFirstByte), dataEnd+1)},
		{"group shorter than a column", put32(field(1, dirEnd), x.groupEnd(0)+minColumnLen-1)},
		{"groups short of the region", put32(field(2, dirEnd), x.groupEnd(2)-1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := bytes.Clone(section)
			tc.mutate(s)
			reseal(s)
			_, err := parseIndex(s, records, items, perRecord, dataEnd)
			require.ErrorIs(t, err, ErrCorrupt)
			require.NotErrorIs(t, err, ErrChecksum)
		})
	}

	t.Run("item count", func(t *testing.T) {
		// The last record holds 1 to perRecord items.
		for _, n := range []int{records * perRecord, records*perRecord - perRecord + 1} {
			_, err := parseIndex(section, records, n, perRecord, dataEnd)
			require.NoError(t, err, "%d items", n)
		}
		for _, n := range []int{records*perRecord + 1, records*perRecord - perRecord} {
			_, err := parseIndex(section, records, n, perRecord, dataEnd)
			require.ErrorIs(t, err, ErrCorrupt, "%d items", n)
		}
	})

	t.Run("empty", func(t *testing.T) {
		empty, err := encodeIndex([]int64{0}, perRecord)
		require.NoError(t, err)
		_, err = parseIndex(empty, 0, 1, perRecord, 0)
		require.ErrorIs(t, err, ErrCorrupt)
		_, err = parseIndex(empty, 0, 0, perRecord, 1)
		require.ErrorIs(t, err, ErrCorrupt)
	})
}

// Bit flips under a valid CRC either decode or fail with ErrCorrupt.
func TestIndexMutationsNeverPanic(t *testing.T) {
	const perRecord = 3
	offsets := variedOffsets(groupSize + 3)
	records := len(offsets) - 1
	dataEnd := offsets[records]
	encoded, err := encodeIndex(offsets, perRecord)
	require.NoError(t, err)
	for i := range len(encoded) - 4 {
		for _, bit := range []byte{0x01, 0x80} {
			mutated := bytes.Clone(encoded)
			mutated[i] ^= bit
			reseal(mutated)
			if _, err := decodeOffsets(mutated, records, perRecord*records, perRecord, dataEnd); err != nil {
				require.ErrorIs(t, err, ErrCorrupt, "byte %d bit %#x", i, bit)
			}
		}
	}
}

// A pack whose group 1 cannot decode opens, and only reads of that group fail.
func TestIndexOpenDecodesNothing(t *testing.T) {
	items := makeItems(2*groupSize+1, 8)
	path := writeTestPackfile(t, items, WriterOptions{}) // one item per record
	corrupt := corruptAt(t, path, false, func(data []byte) {
		tr, err := unmarshalTrailer(data)
		require.NoError(t, err)
		indexBase := len(data) - trailerSize - int(tr.IndexSize)
		section := data[indexBase : len(data)-trailerSize]
		x, err := parseIndex(section, int(tr.RecordCount), int(tr.TotalItems), 1, int64(indexBase))
		require.NoError(t, err)
		section[x.groupEnd(1)-5] = 0 // group 1's FOR width
		reseal(section)
	})

	r := Open(corrupt, ReaderOptions{})
	defer r.Close()
	assert.Equal(t, items[0], readItemCopy(t, r, 0))
	assert.Equal(t, items[2*groupSize], readItemCopy(t, r, 2*groupSize))

	require.ErrorIs(t, r.ReadItem(groupSize, func([]byte) error { return nil }), ErrCorrupt)
	require.ErrorIs(t, readAllItems(t, corrupt, nil), ErrCorrupt)
	err := r.ReadItems(context.Background(), []int{0, groupSize}, func(int, []byte) error { return nil })
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestIndexEncodeEmptyOffsets(t *testing.T) {
	_, err := encodeIndex([]int64{}, 1)
	require.Error(t, err)
}

func TestIndexEncodeNonZeroStart(t *testing.T) {
	_, err := encodeIndex([]int64{100, 200}, 1)
	require.Error(t, err)
}

func TestIndexNonMonotonicOffsets(t *testing.T) {
	_, err := encodeIndex([]int64{0, 1000, 500}, 1)
	require.Error(t, err)
}

func TestIndexZeroRecords(t *testing.T) {
	encoded, err := encodeIndex([]int64{0}, 128)
	require.NoError(t, err)
	require.Len(t, encoded, 4)

	decoded, err := decodeOffsets(encoded, 0, 0, 128, 0)
	require.NoError(t, err)
	require.Equal(t, []int64{0}, decoded)
}

func TestIndexDeltaExceedsUint32(t *testing.T) {
	_, err := encodeIndex([]int64{0, math.MaxUint32 + 1}, 1)
	require.Error(t, err)
}

func TestIndexLargeDelta(t *testing.T) {
	// Delta near MaxUint32 exercises width=32 in the FOR encoder.
	offsets := []int64{0, math.MaxUint32}

	encoded, err := encodeIndex(offsets, 1)
	require.NoError(t, err)

	decoded, err := decodeOffsets(encoded, 1, 1, 1, math.MaxUint32)
	require.NoError(t, err)
	require.Equal(t, offsets, decoded)
}
