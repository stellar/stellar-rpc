package packfile

import (
	"encoding/binary"
	"fmt"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/intpack"
)

// record is the per-record processing workspace owned by a Reader. It bundles
// a caller-supplied RecordDecoder with scratch buffers and the decoded
// item-size state needed to slice individual items out of one record's bytes.
// Pooled by the Reader and vended via getRecord/putRecord.
//
// The reader back-pointer gives access to the file-level metadata
// (totalItems, itemsPerRecord) needed to compute partial-last-record sizes;
// avoids per-call copying of those fields into every pooled record.
type record struct {
	reader  *Reader       // for totalItems / itemsPerRecord
	decoder RecordDecoder // nil = passthrough
	scratch []byte        // raw read buffer (record bytes from disk)
	payload []byte        // record payload after decoder; aliased into decoder when non-nil
	sizes   []uint32
	offsets []int // prefix sum: offsets[i] = byte offset of item i within the record
}

// close releases the underlying RecordDecoder, if any.
func (r *record) close() error {
	if r.decoder == nil {
		return nil
	}
	err := r.decoder.Close()
	r.decoder = nil
	return err
}

// itemsInRecord returns the number of items in the record at recordIdx.
// Handles the last (potentially partial) record and totalItems == 0.
// Panics if itemsPerRecord <= 0 or recordIdx is out of range.
func (r *record) itemsInRecord(recordIdx int) int {
	total := r.reader.totalItems
	perRec := r.reader.itemsPerRecord
	if total == 0 {
		return 0
	}
	if perRec <= 0 {
		panic(fmt.Sprintf("packfile: record.itemsInRecord itemsPerRecord must be > 0, got %d", perRec))
	}
	recordCount := (total + perRec - 1) / perRec
	if recordIdx < 0 || recordIdx >= recordCount {
		panic(fmt.Sprintf("packfile: record.itemsInRecord recordIdx %d out of range [0, %d)", recordIdx, recordCount))
	}
	last := recordCount - 1
	if recordIdx < last {
		return perRec
	}
	rem := total % perRec
	if rem == 0 {
		return perRec
	}
	return rem
}

// decode populates this record's per-item state from one record's on-disk
// bytes. After decode succeeds, item(i) returns the i-th item's bytes.
//
// On disk a multi-item record is [payload][forIndex] where payload is the
// (possibly encoded) record bytes and forIndex is [packed][1B W][4B min][4B
// crc32c]. decode strips and verifies the FOR index (if itemsPerRecord > 1),
// then runs the caller-supplied RecordDecoder over the payload, or aliases
// the input verbatim in passthrough mode. itemsPerRecord == 1 records have
// no forIndex and the entire record is the single item's bytes.
//
// In passthrough mode r.payload aliases the caller's input slice; r.item's
// "valid until next decode" contract is preserved because every read path
// that calls decode owns the underlying buffer (r.scratch in ReadItem; the
// pooled coalesced-read buf in ReadRange / ReadItems) and does not reuse it
// before the next iteration finishes.
func (r *record) decode(data []byte, recordIdx int) error {
	n := r.itemsInRecord(recordIdx)
	itemsPerRecord := r.reader.itemsPerRecord

	if itemsPerRecord > 1 {
		const crcSize = 4
		if len(data) < crcSize {
			return fmt.Errorf("%w: record too short", ErrCorrupt)
		}
		storedCRC := binary.LittleEndian.Uint32(data[len(data)-crcSize:])
		forBuf := data[:len(data)-crcSize]

		// intpack.DecodeGroup catches forBuf-shorter-than-footer itself.
		var consumed int
		var err error
		r.sizes, consumed, err = intpack.DecodeGroup(forBuf, n, r.sizes)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrCorrupt, err)
		}
		if storedCRC != crc32c(forBuf[len(forBuf)-consumed:]) {
			return fmt.Errorf("%w: FOR index CRC32C", ErrChecksum)
		}
		data = forBuf[:len(forBuf)-consumed] // payload before the FOR group
	}

	// Apply caller-supplied decoder, or alias verbatim in passthrough mode.
	// Passthrough is safe to alias: data points into the caller's read buffer
	// (r.scratch in ReadItem; the pooled coalesced-read buf in ReadRange /
	// ReadItems), and r.item's documented validity ("until the next decode
	// call") matches the lifetime of those buffers.
	if r.decoder != nil {
		var err error
		r.payload, err = r.decoder.Decode(r.payload[:0], data)
		if err != nil {
			return err
		}
	} else {
		r.payload = data
	}

	if itemsPerRecord > 1 {
		sum := 0
		for _, s := range r.sizes {
			sum += int(s)
		}
		if sum != len(r.payload) {
			return fmt.Errorf("%w: item size sum %d != payload len %d", ErrCorrupt, sum, len(r.payload))
		}
	} else {
		if cap(r.sizes) >= 1 {
			r.sizes = r.sizes[:1]
		} else {
			r.sizes = make([]uint32, 1)
		}
		//nolint:gosec // record byte size already bounded by writer-side checks (uint32 max)
		r.sizes[0] = uint32(len(r.payload))
	}

	if cap(r.offsets) <= n {
		r.offsets = make([]int, n+1)
	} else {
		r.offsets = r.offsets[:n+1]
	}
	r.offsets[0] = 0
	for i, s := range r.sizes {
		r.offsets[i+1] = r.offsets[i] + int(s)
	}
	return nil
}

// item returns the item at index i within the decoded record.
// The returned slice is valid only until the next decode call.
// Panics if i is out of [0, n) where n is the number of items.
func (r *record) item(i int) []byte {
	if i < 0 || i >= len(r.sizes) {
		panic(fmt.Sprintf("packfile: item(%d) out of range [0, %d)", i, len(r.sizes)))
	}
	return r.payload[r.offsets[i]:r.offsets[i+1]]
}
