package packfile

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/intpack"
)

// RecordDecoder is the read-side counterpart of RecordEncoder. It transforms
// one record's on-disk bytes back into the original record payload (the
// concatenation of items in the record). Typical implementations decompress
// (e.g. zstd) or strip a trailing CRC32C wrapper. Passthrough mode (no
// decoder) reads bytes verbatim — symmetric to the writer's nil
// NewRecordEncoder.
//
// Decode's returned slice may alias an internal buffer of the decoder and is
// valid until the next call on this RecordDecoder. A RecordDecoder is not
// safe for concurrent use — the reader creates one per worker (or one for
// serial reads) via ReaderOptions.NewRecordDecoder.
type RecordDecoder interface {
	Decode(in []byte) ([]byte, error)
	io.Closer
}

// decoder decodes packfile records: optional caller-supplied RecordDecoder
// over the record payload, followed by a per-record FOR item-size index
// when itemsPerRecord > 1.
//
// Configure totalItems and itemsPerRecord (via the owning Reader) before
// calling Decode.
type decoder struct {
	totalItems     int
	itemsPerRecord int

	rec          RecordDecoder // nil = passthrough
	scratch      []byte        // raw read buffer (record bytes from disk)
	decompressed []byte        // record payload after RecordDecoder; aliased into rec when non-nil
	sizes        []uint32
	offsets      []int // prefix sum: offsets[i] = byte offset of item i within the record
}

// Close releases the underlying RecordDecoder, if any.
func (rd *decoder) Close() error {
	if rd.rec == nil {
		return nil
	}
	err := rd.rec.Close()
	rd.rec = nil
	return err
}

// itemsInRecord returns the number of items in the given record index.
// Handles the last record which may be partial, and totalItems == 0.
// Panics if itemsPerRecord <= 0 or recordIdx is out of range.
func itemsInRecord(totalItems, itemsPerRecord, recordIdx int) int {
	if totalItems == 0 {
		return 0
	}
	if itemsPerRecord <= 0 {
		panic(fmt.Sprintf("packfile: itemsInRecord itemsPerRecord must be > 0, got %d", itemsPerRecord))
	}
	recordCount := (totalItems + itemsPerRecord - 1) / itemsPerRecord
	if recordIdx < 0 || recordIdx >= recordCount {
		panic(fmt.Sprintf("packfile: itemsInRecord recordIdx %d out of range [0, %d)", recordIdx, recordCount))
	}
	last := recordCount - 1
	if recordIdx < last {
		return itemsPerRecord
	}
	rem := totalItems % itemsPerRecord
	if rem == 0 {
		return itemsPerRecord
	}
	return rem
}

// Decode decodes the record at recordIdx.
//
// On disk a multi-item record is [payload][forIndex] where payload is the
// (possibly encoded) record bytes and forIndex is [packed][1B W][4B min][4B
// crc32c]. Decode strips and verifies the FOR index (if itemsPerRecord > 1),
// then runs the caller-supplied RecordDecoder over the payload (or copies it
// verbatim in passthrough mode). itemsPerRecord == 1 records have no forIndex
// and the entire record is the single item's bytes.
//
//nolint:nestif // strip-and-verify FOR index; flat sequence reads top-to-bottom
func (rd *decoder) Decode(data []byte, recordIdx int) error {
	n := itemsInRecord(rd.totalItems, rd.itemsPerRecord, recordIdx)

	var forIndexBytes []byte
	if rd.itemsPerRecord > 1 {
		const forFooterSize = 5 // 1B width + 4B min
		const crcSize = 4
		const metaSize = forFooterSize + crcSize
		if len(data) < metaSize {
			return fmt.Errorf("%w: record too short for FOR index: %d bytes", ErrCorrupt, len(data))
		}
		width := data[len(data)-metaSize]
		if width > 32 {
			return fmt.Errorf("%w: invalid FOR width %d", ErrCorrupt, width)
		}
		packSize := (int(width)*n + 7) / 8
		forStart := len(data) - metaSize - packSize
		if forStart < 0 {
			return fmt.Errorf("%w: FOR index size exceeds record size", ErrCorrupt)
		}
		forIndexBytes = data[forStart : len(data)-crcSize]
		storedCRC := binary.LittleEndian.Uint32(data[len(data)-crcSize:])
		if storedCRC != crc32c(forIndexBytes) {
			return fmt.Errorf("packfile: FOR index CRC32C: %w", ErrChecksum)
		}
		data = data[:forStart]
	}

	// Apply caller-supplied decoder, or copy verbatim in passthrough mode.
	if rd.rec != nil {
		decoded, err := rd.rec.Decode(data)
		if err != nil {
			return err
		}
		rd.decompressed = decoded
	} else {
		rd.decompressed = append(rd.decompressed[:0], data...)
	}

	if rd.itemsPerRecord > 1 {
		var err error
		rd.sizes, _, err = intpack.DecodeGroup(forIndexBytes, n, rd.sizes)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrCorrupt, err)
		}
		sum := 0
		for _, s := range rd.sizes {
			sum += int(s)
		}
		if sum != len(rd.decompressed) {
			return fmt.Errorf("%w: item size sum %d != payload len %d", ErrCorrupt, sum, len(rd.decompressed))
		}
	} else {
		if cap(rd.sizes) >= 1 {
			rd.sizes = rd.sizes[:1]
		} else {
			rd.sizes = make([]uint32, 1)
		}
		//nolint:gosec // record byte size already bounded by writer-side checks (uint32 max)
		rd.sizes[0] = uint32(len(rd.decompressed))
	}

	if cap(rd.offsets) <= n {
		rd.offsets = make([]int, n+1)
	} else {
		rd.offsets = rd.offsets[:n+1]
	}
	rd.offsets[0] = 0
	for i, s := range rd.sizes {
		rd.offsets[i+1] = rd.offsets[i] + int(s)
	}
	return nil
}

// Item returns the item at index i within the decoded record.
// The returned slice is valid only until the next Decode call.
// Panics if i is out of [0, n) where n is the number of items.
func (rd *decoder) Item(i int) []byte {
	if i < 0 || i >= len(rd.sizes) {
		panic(fmt.Sprintf("packfile: Item(%d) out of range [0, %d)", i, len(rd.sizes)))
	}
	return rd.decompressed[rd.offsets[i]:rd.offsets[i+1]]
}
