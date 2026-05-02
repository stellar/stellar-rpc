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
func (xorDecoder) Close() error                           { return nil }

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
// at index 0 contains all n items).
func newTestRecord(n int, dec RecordDecoder) *record {
	return &record{
		reader:  &Reader{totalItems: n, itemsPerRecord: n},
		decoder: dec,
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
	defer rec.close()

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
	defer rec.close()

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
	defer rec.close()

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
	defer rec.close()
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
	defer rec.close()

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
