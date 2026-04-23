package packfile

// Offset index codec for packfiles. The offset index maps record numbers to
// byte positions on disk. Rather than storing absolute offsets (which grow with
// file size), the index stores record byte sizes encoded as FOR groups of up
// to 128 values, with a trailing CRC32C over the entire index section.
//
// This builds on the low-level FOR codec in the intpack package.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/intpack"
)

const groupSize = 128 // values per FOR group in the offset index

var (
	ErrCorrupt  = errors.New("packfile: corrupt file")
	ErrChecksum = fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli) //nolint:gochecknoglobals // immutable lookup table

func crc32c(b []byte) uint32 { return crc32.Checksum(b, crc32cTable) }

// decodeIndex decodes a FOR-encoded offset index with CRC32C integrity.
// Returns recordCount+1 offsets: offsets[i] is the start byte of record i,
// and offsets[recordCount] equals indexBase.
func decodeIndex(buf []byte, recordCount int, indexSize int, indexBase int64) ([]int64, error) {
	if recordCount < 0 {
		return nil, fmt.Errorf("%w: negative recordCount %d", ErrCorrupt, recordCount)
	}

	if indexSize < 4 {
		return nil, fmt.Errorf("%w: index too small (%d bytes)", ErrCorrupt, indexSize)
	}

	if len(buf) < indexSize {
		return nil, fmt.Errorf("%w: buffer length %d < indexSize %d", ErrCorrupt, len(buf), indexSize)
	}

	buf = buf[:indexSize]

	// Sanity-check recordCount against indexSize to prevent OOM from crafted trailers.
	// Each FOR group requires at least 6 bytes (1 byte packed + 5-byte footer).
	maxGroups := (indexSize - 4) / 6 // subtract CRC, divide by min group size

	maxRecords := maxGroups * groupSize
	if recordCount > maxRecords {
		return nil, fmt.Errorf("%w: recordCount %d implausible for indexSize %d", ErrCorrupt, recordCount, indexSize)
	}

	// Verify CRC32C over raw index bytes (all groups, excluding trailing 4-byte CRC).
	payloadLen := indexSize - 4

	storedCRC := binary.LittleEndian.Uint32(buf[payloadLen:])
	if storedCRC != crc32c(buf[:payloadLen]) {
		return nil, ErrChecksum
	}

	// Groups are stored forward on disk but decoded backward: each group's metadata
	// (width, min) is at its tail, so intpack.DecodeGroup naturally reads from the end of
	// its window. Iterating backward lets us shrink the window after each group.
	groupCount := (recordCount + groupSize - 1) / groupSize
	deltas := make([]uint32, recordCount)
	pos := payloadLen

	for g := groupCount - 1; g >= 0; g-- {
		base := g * groupSize

		limit := groupSize
		if g == groupCount-1 && recordCount%groupSize != 0 {
			limit = recordCount % groupSize
		}

		_, consumed, err := intpack.DecodeGroup(buf[:pos], limit, deltas[base:base+limit])
		if err != nil {
			return nil, fmt.Errorf("%w: index group %d: %w", ErrCorrupt, g, err)
		}

		pos -= consumed
	}

	if pos != 0 {
		return nil, fmt.Errorf("%w: index has %d unconsumed bytes after decoding all groups", ErrCorrupt, pos)
	}

	// Forward prefix-sum to build absolute offsets from deltas.
	offsets := make([]int64, recordCount+1)

	offset := int64(0)
	for i, d := range deltas {
		offsets[i] = offset
		offset += int64(d)
	}

	// Structural sanity check: running sum must arrive at indexBase.
	if offset != indexBase {
		return nil, fmt.Errorf("%w: final offset %d != indexBase %d", ErrCorrupt, offset, indexBase)
	}

	offsets[recordCount] = indexBase

	return offsets, nil
}

// encodeIndex encodes record offsets into a FOR-128 index section with CRC32C.
// offsets must have recordCount+1 entries where offsets[i+1] >= offsets[i],
// and the last entry is the end-of-data offset.
// Returns the index bytes including the trailing CRC32C.
func encodeIndex(offsets []int64) ([]byte, error) {
	if len(offsets) == 0 {
		return nil, errors.New("packfile: offsets must have at least one entry")
	}

	if offsets[0] != 0 {
		return nil, fmt.Errorf("packfile: first offset must be 0, got %d", offsets[0])
	}

	recordCount := len(offsets) - 1
	if recordCount > math.MaxUint32 {
		return nil, fmt.Errorf("packfile: record count %d exceeds uint32 max", recordCount)
	}

	var buf bytes.Buffer

	deltas := make([]uint32, 0, groupSize)
	for g := 0; g*groupSize < recordCount; g++ {
		base := g * groupSize
		end := min(base+groupSize, recordCount)

		deltas = deltas[:0]

		for j := base; j < end; j++ {
			d := offsets[j+1] - offsets[j]
			if d < 0 {
				return nil, fmt.Errorf("packfile: offsets not monotonically increasing at index %d", j)
			}

			if d > math.MaxUint32 {
				return nil, fmt.Errorf("packfile: record size delta %d exceeds 4GB", d)
			}

			deltas = append(deltas, uint32(d))
		}

		buf.Write(intpack.EncodeGroup(deltas))
	}

	// Append CRC32C over raw index bytes.
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc32c(buf.Bytes()))
	buf.Write(crcBuf[:])

	return buf.Bytes(), nil
}
