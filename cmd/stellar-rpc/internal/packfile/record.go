package packfile

import "fmt"

// record is the per-call processing workspace pulled from
// recordWorkspacePool. It carries scratch buffers and the decoded item-size
// state needed to slice individual items out of one record's bytes. The
// reader back-pointer gives access to file-level metadata (totalItems,
// itemsPerRecord, recordDecoder) without copying those fields per call.
//
// A *record holds no resources requiring explicit cleanup; putRecord
// resets the slices to length zero (preserving capacity for reuse) and
// returns the workspace to the pool.
type record struct {
	reader  *Reader // for totalItems / itemsPerRecord / recordDecoder
	scratch []byte  // raw read buffer (record bytes from disk)
	// payload is the record's owned output buffer for encoder mode (when
	// recordDecoder != nil). Its capacity is preserved across pool cycles
	// so the decoder's append-grow reuses warm memory. payload is unused
	// in passthrough mode.
	payload []byte
	// current is the active record bytes that item(i) slices into. In
	// passthrough mode it aliases the caller's read buffer (scratch in
	// ReadItem, readBufPool buf in ReadRange / ReadItems). In encoder
	// mode it points at payload after Decode. putRecord clears current
	// so the alias doesn't outlive the read call (which would let a
	// future borrower's encoder grow into a returned-to-pool buffer).
	current []byte
	sizes   []uint32
	offsets []int // prefix sum: offsets[i] = byte offset of item i within the record
}

// itemsInRecord returns the number of items in the record at recordIdx.
// Handles the last (potentially partial) record. Returns 0 when the
// packfile is empty (totalItems == 0), regardless of recordIdx. Panics
// if itemsPerRecord <= 0 or, when totalItems > 0, if recordIdx is out
// of range.
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
// then runs the Reader's RecordDecoder over the payload, or aliases
// the input verbatim in passthrough mode. itemsPerRecord == 1 records have
// no forIndex and the entire record is the single item's bytes.
//
// In passthrough mode r.current aliases the caller's input slice (r.payload
// stays owned and untouched); r.item's "valid until next decode" contract
// is preserved because every read path that calls decode owns the
// underlying buffer (r.scratch in ReadItem; the pooled coalesced-read buf
// in ReadRange / ReadItems) and does not reuse it before the next
// iteration finishes.
func (r *record) decode(data []byte, recordIdx int) error {
	n := r.itemsInRecord(recordIdx)
	itemsPerRecord := r.reader.itemsPerRecord

	if itemsPerRecord > 1 {
		// decodeForIndex is defined alongside its inverse encodeForIndex in
		// writer.go so the on-disk FOR-index wire format lives in one place.
		sizes, payload, err := decodeForIndex(data, n, r.sizes)
		if err != nil {
			return err
		}
		r.sizes = sizes
		data = payload
	}

	// Apply the Reader's RecordDecoder, or alias verbatim in passthrough mode.
	// Passthrough sets r.current to alias the caller's read buffer (r.scratch
	// in ReadItem; the pooled coalesced-read buf in ReadRange / ReadItems).
	// The aliasing is load-bearing on two invariants:
	//   1. r.item's documented validity ("until the next decode call")
	//      matches the lifetime of the caller's read buffer.
	//   2. A single record is decoded by exactly one goroutine; within
	//      ReadItems each worker owns its own buffer and decodes its
	//      records serially within the worker. If intra-record-decode
	//      parallelism is ever introduced, this aliasing breaks silently
	//      and a copy must replace the alias here.
	//
	// putRecord clears r.current so the alias doesn't outlive the read
	// call (which would let a future borrower's encoder grow into a
	// returned-to-pool buffer); r.payload (owned bytes for encoder mode)
	// keeps its capacity for cap-reuse on the next encoder decode.
	dec := r.reader.recordDecoder
	if dec != nil {
		var err error
		r.payload, err = dec.Decode(r.payload[:0], data)
		if err != nil {
			return err
		}
		r.current = r.payload
	} else {
		r.current = data
	}

	if itemsPerRecord == 1 {
		// Single-item record: sizes and offsets are trivial.
		if cap(r.sizes) < 1 {
			r.sizes = make([]uint32, 1)
		} else {
			r.sizes = r.sizes[:1]
		}
		//nolint:gosec // record byte size already bounded by writer-side checks (uint32 max)
		r.sizes[0] = uint32(len(r.current))
	}

	// Build the prefix-sum offsets. For multi-item records the running
	// total at offsets[n] also serves as the size-sum-vs-current-length
	// validator, so the two passes are fused.
	if cap(r.offsets) < n+1 {
		r.offsets = make([]int, n+1)
	} else {
		r.offsets = r.offsets[:n+1]
	}
	r.offsets[0] = 0
	for i, s := range r.sizes {
		r.offsets[i+1] = r.offsets[i] + int(s)
	}
	if itemsPerRecord > 1 && r.offsets[n] != len(r.current) {
		return fmt.Errorf("%w: item size sum %d != payload len %d",
			ErrCorrupt, r.offsets[n], len(r.current))
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
	return r.current[r.offsets[i]:r.offsets[i+1]]
}
