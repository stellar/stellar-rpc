package packfile

import (
	"slices"
	"testing"
)

// xorDecoder is the read-side counterpart of xorEncoder (defined in
// writer_test.go). XOR is its own inverse, so the same byte transform that
// "encoded" a record decodes it.
type xorDecoder struct{}

func (xorDecoder) Decode(dst, src []byte) ([]byte, error) { return xorTransform(dst, src), nil }

func newXorDecoder() RecordDecoder { return xorDecoder{} }

// buildPayload assembles items into a contiguous payload and returns item sizes.
func buildPayload(items [][]byte) ([]byte, []uint32) {
	sizes := make([]uint32, len(items))
	var payload []byte
	for i, e := range items {
		payload = append(payload, e...)
		sizes[i] = uint32(len(e))
	}
	return payload, sizes
}

// newTestRecord builds a record bound to a stub Reader configured for a
// single chunk of n items (totalItems == itemsPerRecord == n, so the record
// at index 0 contains all n items). The RecordDecoder lives on the stub
// Reader, since records read it via r.reader.recordDecoder.
func newTestRecord(n int, dec RecordDecoder) *record {
	return &record{
		reader: &Reader{totalItems: n, itemsPerRecord: n, recordDecoder: dec},
	}
}

func TestRecordWithDecoder(t *testing.T) {
	entries := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("!"),
	}
	payload, sizes := buildPayload(entries)
	forIndex := encodeForIndex(sizes)
	encoded, err := xorCompress(payload)
	if err != nil {
		t.Fatal(err)
	}
	data := slices.Concat(encoded, forIndex)

	rec := newTestRecord(len(entries), newXorDecoder())

	if err := rec.decode(data, 0); err != nil {
		t.Fatal(err)
	}
	for i, want := range entries {
		if got := string(rec.item(i)); got != string(want) {
			t.Errorf("Item(%d) = %q, want %q", i, got, want)
		}
	}
}

func TestRecordPassthrough(t *testing.T) {
	// nil RecordDecoder: the record bytes (minus the FOR index) are the
	// items concatenated verbatim.
	entries := [][]byte{[]byte("raw"), []byte("data")}
	payload, sizes := buildPayload(entries)
	forIndex := encodeForIndex(sizes)
	data := slices.Concat(payload, forIndex)

	rec := newTestRecord(len(entries), nil)

	if err := rec.decode(data, 0); err != nil {
		t.Fatal(err)
	}
	for i, want := range entries {
		if got := string(rec.item(i)); got != string(want) {
			t.Errorf("Item(%d) = %q, want %q", i, got, want)
		}
	}
}

func TestRecordNoForIndex(t *testing.T) {
	// itemsPerRecord=1: the entire payload is one item, no FOR index is appended.
	payload := []byte("single-item-payload")
	encoded, err := xorCompress(payload)
	if err != nil {
		t.Fatal(err)
	}

	rec := newTestRecord(1, newXorDecoder())

	if err := rec.decode(encoded, 0); err != nil {
		t.Fatal(err)
	}
	if got := string(rec.item(0)); got != string(payload) {
		t.Errorf("Item(0) = %q, want %q", got, payload)
	}
}

func TestItemBoundsCheck(t *testing.T) {
	entries := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	payload, sizes := buildPayload(entries)
	forIndex := encodeForIndex(sizes)
	data := slices.Concat(payload, forIndex)

	rec := newTestRecord(3, nil)
	if err := rec.decode(data, 0); err != nil {
		t.Fatal(err)
	}

	assertPanics(t, "Item(-1)", func() { rec.item(-1) })
	assertPanics(t, "Item(3)", func() { rec.item(3) })
}

func assertPanics(t *testing.T, name string, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("%s: expected panic", name)
		}
	}()
	f()
}

// --- Metadata tests ---

func TestItemsInRecord(t *testing.T) {
	tests := []struct {
		name           string
		total          int
		itemsPerRecord int
		recordIdx      int
		want           int
	}{
		{"full block", 300, 128, 0, 128},
		{"full block second", 300, 128, 1, 128},
		{"partial last block", 300, 128, 2, 44},
		{"exact multiple", 256, 128, 1, 128},
		{"single record", 50, 128, 0, 50},
		{"totalItems zero", 0, 128, 0, 0},
		{"small itemsPerRecord", 10, 3, 0, 3},
		{"small itemsPerRecord last", 10, 3, 3, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &record{reader: &Reader{totalItems: tt.total, itemsPerRecord: tt.itemsPerRecord}}
			got := rec.itemsInRecord(tt.recordIdx)
			if got != tt.want {
				t.Errorf("itemsInRecord(%d, %d, %d) = %d, want %d",
					tt.total, tt.itemsPerRecord, tt.recordIdx, got, tt.want)
			}
		})
	}
}

func TestRecordReuse(t *testing.T) {
	// Decode a 5-item record, then decode a 2-item record on the same
	// record. Verifies that stale state from the first decode (larger
	// sizes/offsets slices) doesn't leak into the second.
	entries1 := [][]byte{[]byte("a"), []byte("bb"), []byte("ccc"), []byte("dd"), []byte("e")}
	payload1, sizes1 := buildPayload(entries1)
	forIndex1 := encodeForIndex(sizes1)
	enc1, err := xorCompress(payload1)
	if err != nil {
		t.Fatal(err)
	}
	data1 := slices.Concat(enc1, forIndex1)

	rec := newTestRecord(5, newXorDecoder())

	if err := rec.decode(data1, 0); err != nil {
		t.Fatal(err)
	}
	for i, want := range entries1 {
		if got := string(rec.item(i)); got != string(want) {
			t.Errorf("first decode Item(%d) = %q, want %q", i, got, want)
		}
	}

	// Second decode: fewer items.
	entries2 := [][]byte{[]byte("xx"), []byte("yy")}
	payload2, sizes2 := buildPayload(entries2)
	forIndex2 := encodeForIndex(sizes2)
	enc2, err := xorCompress(payload2)
	if err != nil {
		t.Fatal(err)
	}
	data2 := slices.Concat(enc2, forIndex2)

	rec.reader.totalItems = 2
	rec.reader.itemsPerRecord = 2
	if err := rec.decode(data2, 0); err != nil {
		t.Fatal(err)
	}
	for i, want := range entries2 {
		if got := string(rec.item(i)); got != string(want) {
			t.Errorf("second decode Item(%d) = %q, want %q", i, got, want)
		}
	}

	// Bounds check: Item(2) should panic after the second decode.
	assertPanics(t, "Item(2) after shrink", func() { rec.item(2) })
}

func TestItemsInRecordPanics(t *testing.T) {
	mk := func(total, perRec int) *record {
		return &record{reader: &Reader{totalItems: total, itemsPerRecord: perRec}}
	}
	assertPanics(t, "itemsPerRecord=0", func() { mk(10, 0).itemsInRecord(0) })
	assertPanics(t, "itemsPerRecord=-1", func() { mk(10, -1).itemsInRecord(0) })
	assertPanics(t, "recordIdx=5", func() { mk(300, 128).itemsInRecord(5) })
	assertPanics(t, "recordIdx=-1", func() { mk(300, 128).itemsInRecord(-1) })
}

// TestPassthroughDecodePreservesPayload pins the alias-safety invariant:
// passthrough decode (recordDecoder == nil) must NOT touch rec.payload.
// payload is the owned buffer reserved for encoder mode's cap reuse; if a
// future change writes the aliased input into payload, the buffer escapes
// past the read call (via the workspace pool) and a subsequent encoder
// decode would grow into a returned-to-pool buffer — see the alias-bug
// commit and the comment above record.decode.
func TestPassthroughDecodePreservesPayload(t *testing.T) {
	rec := newTestRecord(3, nil) // nil decoder = passthrough
	// Pre-allocate payload with a known capacity (simulating a previous
	// encoder use). Passthrough must leave it untouched.
	rec.payload = make([]byte, 0, 100)
	originalCap := cap(rec.payload)

	entries := [][]byte{[]byte("hello"), []byte("world"), []byte("!")}
	payload, sizes := buildPayload(entries)
	forIndex := encodeForIndex(sizes)
	data := slices.Concat(payload, forIndex)

	if err := rec.decode(data, 0); err != nil {
		t.Fatal(err)
	}

	if cap(rec.payload) != originalCap {
		t.Errorf("passthrough decode mutated rec.payload (cap %d -> %d); "+
			"payload must stay owned & untouched in passthrough mode",
			originalCap, cap(rec.payload))
	}
	if rec.current == nil {
		t.Error("passthrough decode must set rec.current to alias the input")
	}
}

// TestPutRecordDropsCurrent pins the other half of the alias-safety
// invariant: putRecord must clear rec.current so any passthrough alias
// does not outlive the read call. If the next borrower of this workspace
// reads rec.current before its own decode runs, it would see stale bytes
// pointing into a buffer that has been returned to readBufPool.
func TestPutRecordDropsCurrent(t *testing.T) {
	rec := &record{}
	rec.current = make([]byte, 32) // simulates a passthrough alias
	rec.payload = make([]byte, 8, 64)
	rec.scratch = make([]byte, 4, 16)
	rec.sizes = make([]uint32, 2, 8)
	rec.offsets = make([]int, 3, 8)

	(&Reader{}).putRecord(rec)

	if rec.current != nil {
		t.Errorf("rec.current should be nil after putRecord; got len=%d cap=%d",
			len(rec.current), cap(rec.current))
	}
	// Owned slices keep capacity for steady-state reuse.
	if cap(rec.payload) != 64 {
		t.Errorf("rec.payload capacity not preserved: got %d, want 64", cap(rec.payload))
	}
	if cap(rec.scratch) != 16 {
		t.Errorf("rec.scratch capacity not preserved: got %d, want 16", cap(rec.scratch))
	}
	if cap(rec.sizes) != 8 {
		t.Errorf("rec.sizes capacity not preserved: got %d, want 8", cap(rec.sizes))
	}
	if cap(rec.offsets) != 8 {
		t.Errorf("rec.offsets capacity not preserved: got %d, want 8", cap(rec.offsets))
	}
	if rec.reader != nil {
		t.Error("rec.reader should be cleared in putRecord")
	}
}
