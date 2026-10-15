package packfile

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"slices"
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

func fullCounts(records, perRecord int) []uint32 {
	return slices.Repeat([]uint32{uint32(perRecord)}, records)
}

func itemCount(counts []uint32) int {
	n := 0
	for _, c := range counts {
		n += int(c)
	}
	return n
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
			records := len(tt.offsets) - 1
			encoded, err := encodeIndex(tt.offsets, fullCounts(records, 4), 4)
			require.NoError(t, err)

			decoded, err := decodeOffsets(encoded, records, 4*records, 4, tt.offsets[records])
			require.NoError(t, err)
			require.Equal(t, tt.offsets, decoded)
		})
	}
}

func TestIndexCRCCorruption(t *testing.T) {
	encoded, err := encodeIndex([]int64{0, 1000, 2000, 3000}, fullCounts(3, 1), 1)
	require.NoError(t, err)

	// Corrupt a byte in the payload (before the CRC).
	encoded[0] ^= 0xFF

	_, err = parseIndex(encoded, 3, 3, 1, 3000)
	require.ErrorIs(t, err, ErrChecksum)
}

func TestIndexCorruptCRCBytes(t *testing.T) {
	encoded, err := encodeIndex([]int64{0, 1000, 2000, 3000}, fullCounts(3, 1), 1)
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
	encoded, err := encodeIndex([]int64{0, 10}, fullCounts(1, 1), 1)
	require.NoError(t, err)

	_, err = parseIndex(encoded, 1<<20, 1<<20, 1, 10)
	require.ErrorIs(t, err, ErrCorrupt)
	require.NotErrorIs(t, err, ErrChecksum)
}

// A wrong end of the records passes Open and fails the group's decode.
func TestIndexBaseMismatch(t *testing.T) {
	encoded, err := encodeIndex([]int64{0, 1000, 2000, 3000}, fullCounts(3, 1), 1)
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
	section, err := encodeIndex(offsets, fullCounts(records, perRecord), perRecord)
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
		{"unknown flag", func(s []byte) { s[field(1, dirFlags)] = 0x02 }},
		{"group 0 past item 0", put32(field(0, dirFirstItem), 1)},
		{"wrong first item", put32(field(1, dirFirstItem), groupSize*perRecord+1)},
		{"group 0 past byte 0", put64(field(0, dirFirstByte), 1)},
		{"group before its predecessor", put64(field(2, dirFirstByte), offsets[groupSize]-1)},
		{"group past the records", put64(field(2, dirFirstByte), dataEnd+1)},
		{"group shorter than a column", put32(field(1, dirEnd), x.groupEnd(0)+minColumnLen-1)},
		{"groups short of the region", put32(field(2, dirEnd), x.groupEnd(2)-1)},
		{"group short of its records", put32(field(2, dirFirstItem), 2*groupSize*perRecord-1)},
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
		empty, err := encodeIndex([]int64{0}, nil, perRecord)
		require.NoError(t, err)
		_, err = parseIndex(empty, 0, 1, perRecord, 0)
		require.ErrorIs(t, err, ErrCorrupt)
		_, err = parseIndex(empty, 0, 0, perRecord, 1)
		require.ErrorIs(t, err, ErrCorrupt)
	})
}

// Counts that break the item limit or the directory fail at open or at decode.
func TestIndexCountChecks(t *testing.T) {
	records := 3 * groupSize
	offsets := variedOffsets(records)
	dataEnd := offsets[records]
	counts := func(perRecord int, set map[int]uint32) []uint32 {
		c := fullCounts(records, perRecord)
		for rec, n := range set {
			c[rec] = n
		}
		return c
	}
	uneven := make([]uint32, records)
	for i := range uneven {
		uneven[i] = uint32(1 + i%3)
	}
	dir := func(s []byte, g, off int) int { return len(s) - 4 - (3-g)*dirEntryLen + off }
	setFlags := func(g int, flags byte) func([]byte) {
		return func(s []byte) { s[dir(s, g, 16)] = flags }
	}
	shiftFirstItem := func(g, delta int) func([]byte) {
		return func(s []byte) {
			at := dir(s, g, 12)
			binary.LittleEndian.PutUint32(s[at:], uint32(int(binary.LittleEndian.Uint32(s[at:]))+delta))
		}
	}
	shortLast := counts(2, map[int]uint32{2*groupSize + 5: 1}) // only group 2 has counts

	for _, tc := range []struct {
		name      string
		perRecord int
		counts    []uint32
		mutate    func([]byte) // applied under a recomputed CRC
		extra     int          // items the trailer claims beyond the counts
		badGroup  int          // -1: Open fails; otherwise only loading this group fails
	}{
		{"counts with one item per record", 1, counts(1, nil), setFlags(1, groupHasCounts), 0, -1},
		{"no counts without an item limit", 0, uneven, setFlags(1, 0), 0, -1},
		{"counted group short of an item per record", 2, shortLast, nil, -groupSize, -1},
		{"counted group over the item limit", 2, shortLast, nil, 2, -1},
		{"uncounted group after a counted one", 2, counts(2, map[int]uint32{5: 1}), shiftFirstItem(2, 1), 0, -1},
		{"zero count", 2, counts(2, map[int]uint32{groupSize + 3: 0}), nil, 0, 1},
		{"count over the item limit", 2, counts(2, map[int]uint32{groupSize + 3: 3, groupSize + 4: 1}), nil, 0, 1},
		{"counts short of the next group", 2, shortLast, nil, 1, 2},
		{"flag without a count column", 2, counts(2, nil), setFlags(0, groupHasCounts), 0, 0},
		{"uncounted last group after a counted one", 2, counts(2, map[int]uint32{groupSize + 3: 1}), setFlags(2, 0), 0, -1},
		{"count column without the flag", 2, shortLast, setFlags(2, 0), 0, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			section, err := encodeIndex(offsets, tc.counts, tc.perRecord)
			require.NoError(t, err)
			if tc.mutate != nil {
				tc.mutate(section)
				reseal(section)
			}
			x, err := parseIndex(section, records, itemCount(tc.counts)+tc.extra, tc.perRecord, dataEnd)
			if tc.badGroup < 0 {
				require.ErrorIs(t, err, ErrCorrupt)
				require.NotErrorIs(t, err, ErrChecksum)
				return
			}
			require.NoError(t, err)
			var tab groupTable
			for g := range x.groupCount {
				if err := tab.load(x, g); g == tc.badGroup {
					require.ErrorIs(t, err, ErrCorrupt, "group %d", g)
				} else {
					require.NoError(t, err, "group %d", g)
				}
			}
		})
	}
}

// Bit flips under a valid CRC either decode or fail with ErrCorrupt.
func TestIndexMutationsNeverPanic(t *testing.T) {
	records := groupSize + 3
	offsets := variedOffsets(records)
	dataEnd := offsets[records]
	short := fullCounts(records, 3)
	short[5], short[groupSize+1] = 1, 2
	uneven := make([]uint32, records)
	for i := range uneven {
		uneven[i] = uint32(1 + i%4)
	}
	for _, tc := range []struct {
		name      string
		perRecord int
		counts    []uint32
	}{
		{"full records", 3, fullCounts(records, 3)},
		{"short records", 3, short},
		{"no item limit", 0, uneven},
	} {
		t.Run(tc.name, func(t *testing.T) {
			items := itemCount(tc.counts)
			encoded, err := encodeIndex(offsets, tc.counts, tc.perRecord)
			require.NoError(t, err)
			for i := range len(encoded) - 4 {
				for _, bit := range []byte{0x01, 0x80} {
					mutated := bytes.Clone(encoded)
					mutated[i] ^= bit
					reseal(mutated)
					if _, err := decodeOffsets(mutated, records, items, tc.perRecord, dataEnd); err != nil {
						require.ErrorIs(t, err, ErrCorrupt, "byte %d bit %#x", i, bit)
					}
				}
			}
		})
	}
}

// A pack whose group 1 cannot decode opens, and only reads of that group fail.
func TestIndexOpenDecodesNothing(t *testing.T) {
	items := makeItems(2*groupSize+1, 8)
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 1}, items)
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
	_, err := encodeIndex([]int64{}, nil, 1)
	require.Error(t, err)
}

func TestIndexEncodeNonZeroStart(t *testing.T) {
	_, err := encodeIndex([]int64{100, 200}, fullCounts(1, 1), 1)
	require.Error(t, err)
}

func TestIndexNonMonotonicOffsets(t *testing.T) {
	_, err := encodeIndex([]int64{0, 1000, 500}, fullCounts(2, 1), 1)
	require.Error(t, err)
}

func TestIndexZeroRecords(t *testing.T) {
	encoded, err := encodeIndex([]int64{0}, nil, 128)
	require.NoError(t, err)
	require.Len(t, encoded, 4)

	decoded, err := decodeOffsets(encoded, 0, 0, 128, 0)
	require.NoError(t, err)
	require.Equal(t, []int64{0}, decoded)
}

func TestIndexDeltaExceedsUint32(t *testing.T) {
	_, err := encodeIndex([]int64{0, math.MaxUint32 + 1}, fullCounts(1, 1), 1)
	require.Error(t, err)
}

func TestIndexLargeDelta(t *testing.T) {
	// Delta near MaxUint32 exercises width=32 in the FOR encoder.
	offsets := []int64{0, math.MaxUint32}

	encoded, err := encodeIndex(offsets, fullCounts(1, 1), 1)
	require.NoError(t, err)

	decoded, err := decodeOffsets(encoded, 1, 1, 1, math.MaxUint32)
	require.NoError(t, err)
	require.Equal(t, offsets, decoded)
}
