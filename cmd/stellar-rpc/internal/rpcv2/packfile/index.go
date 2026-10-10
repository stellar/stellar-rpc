package packfile

// The index section is [groups][directory][CRC32C]. For each group of
// groupSize records, groups holds one intpack FOR group of their byte sizes
// and the directory one entry. Open checks the CRC and the directory and
// decodes no group; a read decodes the groups it touches.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"sort"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/intpack"
)

const (
	groupSize = 128 // records per index group
	// minColumnLen is the smallest FOR group: one packed byte and the 5-byte footer.
	minColumnLen = 6
	// knownGroupFlags holds every directory flag; any other bit is corrupt.
	knownGroupFlags uint8 = 0
)

// Directory entry field offsets.
const (
	dirFirstByte = 0  // u64
	dirEnd       = 8  // u32
	dirFirstItem = 12 // u32
	dirFlags     = 16 // u8
	dirEntryLen  = 17
)

// Compile-time check: groupSize is stored as uint16 in the trailer, so it
// must not exceed MaxUint16 without a format version bump.
const _ uint16 = groupSize

// ErrCorrupt classifies errors caused by malformed or inconsistent file
// content (bad magic, wrong version, CRC mismatch, size-sum mismatch,
// unknown flag bits, …). Use errors.Is(err, ErrCorrupt) to distinguish
// corruption from I/O errors. I/O errors (file closed, EOF, permission
// denied, etc.) are returned wrapped with a packfile-prefixed message but
// are NOT classified as ErrCorrupt — unwrap to *os.PathError or check
// with errors.Is against fs sentinel errors when needed.
var (
	ErrCorrupt  = errors.New("packfile: corrupt file")
	ErrChecksum = fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli) //nolint:gochecknoglobals // immutable lookup table

func crc32c(b []byte) uint32 { return crc32.Checksum(b, crc32cTable) }

// index is the CRC-checked index section, read in place.
type index struct {
	groups     []byte
	dir        []byte
	groupCount int
	records    int
	items      int
	perRecord  int   // items in every record but the last
	dataEnd    int64 // where the records end
}

func (x *index) groupRecords(g int) int { return min(groupSize, x.records-g*groupSize) }

// groupFirstByte is the file offset of group g's first record, or dataEnd
// for g == groupCount.
func (x *index) groupFirstByte(g int) int64 {
	if g == x.groupCount {
		return x.dataEnd
	}
	return int64(binary.LittleEndian.Uint64(x.dir[g*dirEntryLen+dirFirstByte:])) //nolint:gosec // checked by parseIndex
}

// groupEnd is where group g's bytes end within the groups region, or 0 for
// g == -1.
func (x *index) groupEnd(g int) int {
	if g < 0 {
		return 0
	}
	return int(binary.LittleEndian.Uint32(x.dir[g*dirEntryLen+dirEnd:]))
}

// groupFirstItem is the position of group g's first item, or the item count
// for g == groupCount.
func (x *index) groupFirstItem(g int) int {
	if g == x.groupCount {
		return x.items
	}
	return int(binary.LittleEndian.Uint32(x.dir[g*dirEntryLen+dirFirstItem:]))
}

func (x *index) groupFlags(g int) uint8 { return x.dir[g*dirEntryLen+dirFlags] }

// parseIndex checks the section's CRC and directory against the trailer.
// The checks keep every group's records inside [0, dataEnd) whatever the
// other groups hold, so a read can trust a group without decoding the rest.
//
//nolint:cyclop // one check per layout rule; splitting hurts readability
func parseIndex(section []byte, records, items, perRecord int, dataEnd int64) (*index, error) {
	if len(section) < 4 {
		return nil, fmt.Errorf("%w: index too small (%d bytes)", ErrCorrupt, len(section))
	}
	payload := section[:len(section)-4]
	if binary.LittleEndian.Uint32(section[len(payload):]) != crc32c(payload) {
		return nil, ErrChecksum
	}
	groupCount := (records + groupSize - 1) / groupSize
	dirLen := groupCount * dirEntryLen
	if len(payload) < dirLen {
		return nil, fmt.Errorf("%w: index of %d bytes cannot hold %d groups", ErrCorrupt, len(section), groupCount)
	}
	x := &index{
		groups:     payload[:len(payload)-dirLen],
		dir:        payload[len(payload)-dirLen:],
		groupCount: groupCount,
		records:    records,
		items:      items,
		perRecord:  perRecord,
		dataEnd:    dataEnd,
	}
	prevEnd, prevByte := 0, int64(0)
	for g := range groupCount {
		end := x.groupEnd(g)
		if end-prevEnd < minColumnLen {
			return nil, fmt.Errorf("%w: index group %d spans [%d, %d)", ErrCorrupt, g, prevEnd, end)
		}
		first := x.groupFirstByte(g)
		if first < prevByte || first > dataEnd || (g == 0 && first != 0) {
			return nil, fmt.Errorf("%w: index group %d starts at byte %d", ErrCorrupt, g, first)
		}
		if f := x.groupFlags(g); f&^knownGroupFlags != 0 {
			return nil, fmt.Errorf("%w: index group %d has unknown flags %#x", ErrCorrupt, g, f)
		}
		if got, want := x.groupFirstItem(g), g*groupSize*perRecord; got != want {
			return nil, fmt.Errorf("%w: index group %d starts at item %d, want %d", ErrCorrupt, g, got, want)
		}
		prevEnd, prevByte = end, first
	}
	if prevEnd != len(x.groups) {
		return nil, fmt.Errorf("%w: index groups end at %d of %d bytes", ErrCorrupt, prevEnd, len(x.groups))
	}
	if groupCount == 0 {
		if dataEnd != 0 || items != 0 {
			return nil, fmt.Errorf("%w: empty index for %d data bytes and %d items", ErrCorrupt, dataEnd, items)
		}
		return x, nil
	}
	// Only the pack's last record may hold fewer than perRecord items.
	n := x.groupRecords(groupCount - 1)
	if span := items - x.groupFirstItem(groupCount-1); span <= (n-1)*perRecord || span > n*perRecord {
		return nil, fmt.Errorf("%w: last index group holds %d items in %d records of %d",
			ErrCorrupt, span, n, perRecord)
	}
	return x, nil
}

// groupTable is one decoded index group: where each of its records starts,
// in the file and in items. It keeps its group until a lookup needs another.
type groupTable struct {
	g     int
	n     int                  // records in group g; 0 when no group is loaded
	off   [groupSize + 1]int64 // off[n] is where the group's records end
	first [groupSize + 1]int   // first[n] is the next group's first item
	sizes [groupSize]uint32    // decode scratch
}

// load decodes group g unless it is the group already loaded. A group that
// fails to decode returns ErrCorrupt and leaves the table empty.
func (t *groupTable) load(x *index, g int) error {
	if g == t.g && t.n > 0 {
		return nil
	}
	t.n = 0
	n := x.groupRecords(g)
	column := x.groups[x.groupEnd(g-1):x.groupEnd(g)]
	sizes, consumed, err := intpack.DecodeGroup(column, n, t.sizes[:0])
	if err != nil {
		return fmt.Errorf("%w: index group %d: %w", ErrCorrupt, g, err)
	}
	if consumed != len(column) {
		return fmt.Errorf("%w: index group %d has %d unconsumed bytes", ErrCorrupt, g, len(column)-consumed)
	}
	off := x.groupFirstByte(g)
	t.off[0] = off
	for i, s := range sizes {
		off += int64(s)
		t.off[i+1] = off
	}
	if next := x.groupFirstByte(g + 1); off != next {
		return fmt.Errorf("%w: index group %d: records end at byte %d, next group starts at %d",
			ErrCorrupt, g, off, next)
	}
	first := x.groupFirstItem(g)
	for i := range n {
		t.first[i] = first + i*x.perRecord
	}
	t.first[n] = x.groupFirstItem(g + 1)
	t.g, t.n = g, n
	return nil
}

// recordSpan is one record's bytes in the file and the items it holds.
type recordSpan struct {
	start, end int64
	first, n   int // the position of its first item, and its item count
}

func (t *groupTable) record(x *index, rec int) (recordSpan, error) {
	if err := t.load(x, rec/groupSize); err != nil {
		return recordSpan{}, err
	}
	i := rec - t.g*groupSize
	return recordSpan{t.off[i], t.off[i+1], t.first[i], t.first[i+1] - t.first[i]}, nil
}

// locate returns the record holding item pos and its span. The item's index
// within the record is pos minus the span's first.
func (t *groupTable) locate(x *index, pos int) (int, recordSpan, error) {
	g := sort.Search(x.groupCount, func(g int) bool { return x.groupFirstItem(g) > pos }) - 1
	if err := t.load(x, g); err != nil {
		return 0, recordSpan{}, err
	}
	rec := g*groupSize + sort.Search(t.n, func(i int) bool { return t.first[i+1] > pos })
	s, err := t.record(x, rec)
	return rec, s, err
}

// encodeIndex encodes the index section, CRC32C included, for records that
// start at offsets (one entry per record plus a last one where the records
// end) and hold perRecord items each but the last.
func encodeIndex(offsets []int64, perRecord int) ([]byte, error) {
	if len(offsets) == 0 {
		return nil, errors.New("packfile: offsets must have at least one entry")
	}
	if offsets[0] != 0 {
		return nil, fmt.Errorf("packfile: first offset must be 0, got %d", offsets[0])
	}
	records := len(offsets) - 1
	if records > math.MaxUint32 {
		return nil, fmt.Errorf("packfile: record count %d exceeds uint32 max", records)
	}
	groupCount := (records + groupSize - 1) / groupSize
	var section []byte
	dir := make([]byte, groupCount*dirEntryLen) // flags stay 0
	sizes := make([]uint32, 0, groupSize)
	for g := range groupCount {
		base := g * groupSize
		sizes = sizes[:0]
		for j := base; j < min(base+groupSize, records); j++ {
			d := offsets[j+1] - offsets[j]
			if d < 0 {
				return nil, fmt.Errorf("packfile: offsets not monotonically increasing at index %d", j)
			}
			if d > math.MaxUint32 {
				return nil, fmt.Errorf("packfile: record size delta %d exceeds 4GB", d)
			}
			sizes = append(sizes, uint32(d))
		}
		section = append(section, intpack.EncodeGroup(sizes)...)
		e := dir[g*dirEntryLen:]
		binary.LittleEndian.PutUint64(e[dirFirstByte:], uint64(offsets[base]))  //nolint:gosec // offsets ascend from 0
		binary.LittleEndian.PutUint32(e[dirEnd:], uint32(len(section)))         //nolint:gosec // Finish checks the index size
		binary.LittleEndian.PutUint32(e[dirFirstItem:], uint32(base*perRecord)) //nolint:gosec // under the uint32 item count
	}
	section = append(section, dir...)
	return binary.LittleEndian.AppendUint32(section, crc32c(section)), nil
}
