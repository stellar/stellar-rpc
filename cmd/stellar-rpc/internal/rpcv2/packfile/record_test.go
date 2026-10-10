package packfile

import (
	"errors"
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

// newTestRecord builds a record bound to a stub Reader with n items per record.
func newTestRecord(n int, dec RecordDecoder) *record {
	return &record{
		reader: &Reader{idx: &index{perRecord: n}, recordDecoder: dec},
	}
}

// buildRecordBytes assembles one record's on-disk bytes the way the writer
// does: payload, the FOR index over sizes, then the trailing CRC32C. wide
// selects the widened checksum, matching a Reader with recordChecksum set.
func buildRecordBytes(payload []byte, sizes []uint32, wide bool) []byte {
	// Clone so sealRecord's append cannot reach the caller's payload.
	return sealRecord(slices.Clone(payload), encodeForIndex(sizes), wide)
}

func TestRecordWithDecoder(t *testing.T) {
	entries := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("!"),
	}
	payload, sizes := buildPayload(entries)
	encoded, err := xorCompress(payload)
	if err != nil {
		t.Fatal(err)
	}
	data := buildRecordBytes(encoded, sizes, false)

	rec := newTestRecord(len(entries), newXorDecoder())

	if err := rec.decode(data, 0, len(entries)); err != nil {
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
	data := buildRecordBytes(payload, sizes, false)

	rec := newTestRecord(len(entries), nil)

	if err := rec.decode(data, 0, len(entries)); err != nil {
		t.Fatal(err)
	}
	for i, want := range entries {
		if got := string(rec.item(i)); got != string(want) {
			t.Errorf("Item(%d) = %q, want %q", i, got, want)
		}
	}
}

// TestRecordWidenedChecksum covers decode's read side of flagRecordChecksum
// directly: the same four trailing bytes, verified over the whole record.
func TestRecordWidenedChecksum(t *testing.T) {
	entries := [][]byte{[]byte("hello"), []byte("world"), []byte("!")}
	payload, sizes := buildPayload(entries)
	data := buildRecordBytes(payload, sizes, true)

	rec := newTestRecord(len(entries), nil)
	rec.reader.recordChecksum = true

	if err := rec.decode(data, 0, len(entries)); err != nil {
		t.Fatal(err)
	}
	if got := string(rec.item(0)); got != string(entries[0]) {
		t.Errorf("Item(0) = %q, want %q", got, entries[0])
	}

	// The payload is inside the covered range now, so a flipped bit there is
	// an error rather than a different item.
	data[0] ^= 0x01
	if err := rec.decode(data, 0, len(entries)); !errors.Is(err, ErrChecksum) {
		t.Errorf("decode of corrupted payload = %v, want ErrChecksum", err)
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

	if err := rec.decode(encoded, 0, 1); err != nil {
		t.Fatal(err)
	}
	if got := string(rec.item(0)); got != string(payload) {
		t.Errorf("Item(0) = %q, want %q", got, payload)
	}
}

func TestItemBoundsCheck(t *testing.T) {
	entries := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	payload, sizes := buildPayload(entries)
	data := buildRecordBytes(payload, sizes, false)

	rec := newTestRecord(3, nil)
	if err := rec.decode(data, 0, 3); err != nil {
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

func TestRecordReuse(t *testing.T) {
	// Decode a 5-item record, then decode a 2-item record on the same
	// record. Verifies that stale state from the first decode (larger
	// sizes/offsets slices) doesn't leak into the second.
	entries1 := [][]byte{[]byte("a"), []byte("bb"), []byte("ccc"), []byte("dd"), []byte("e")}
	payload1, sizes1 := buildPayload(entries1)
	enc1, err := xorCompress(payload1)
	if err != nil {
		t.Fatal(err)
	}
	data1 := buildRecordBytes(enc1, sizes1, false)

	rec := newTestRecord(5, newXorDecoder())

	if err := rec.decode(data1, 0, 5); err != nil {
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
	enc2, err := xorCompress(payload2)
	if err != nil {
		t.Fatal(err)
	}
	data2 := buildRecordBytes(enc2, sizes2, false)

	rec.reader.idx.perRecord = 2
	if err := rec.decode(data2, 0, 2); err != nil {
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

// TestPassthroughDecodePreservesPayload pins that passthrough decode leaves
// rec.payload alone: a payload sharing scratch's array would make a later
// encoder decode write its output over its own input.
func TestPassthroughDecodePreservesPayload(t *testing.T) {
	rec := newTestRecord(3, nil) // nil decoder = passthrough
	// Pre-allocate payload with a known capacity (simulating a previous
	// encoder use). Passthrough must leave it untouched.
	rec.payload = make([]byte, 0, 100)
	originalCap := cap(rec.payload)

	entries := [][]byte{[]byte("hello"), []byte("world"), []byte("!")}
	payload, sizes := buildPayload(entries)
	data := buildRecordBytes(payload, sizes, false)

	if err := rec.decode(data, 0, 3); err != nil {
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

func TestPutRecordDropsCurrent(t *testing.T) {
	rec := &record{}
	rec.current = make([]byte, 32) // simulates a passthrough alias
	rec.payload = make([]byte, 8, 64)
	rec.scratch = make([]byte, 4, 16)
	rec.sizes = make([]uint32, 2, 8)
	rec.offsets = make([]int, 3, 8)
	rec.tab.n = groupSize

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
	if rec.tab.n != 0 {
		t.Errorf("rec.tab should hold no group after putRecord; holds %d records", rec.tab.n)
	}
}
