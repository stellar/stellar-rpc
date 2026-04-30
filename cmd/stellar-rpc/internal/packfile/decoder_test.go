package packfile

import (
	"encoding/binary"
	"testing"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/intpack"
)

// xorDecoder is the read-side counterpart of xorEncoder (defined in
// writer_test.go). XOR is its own inverse, so the same byte transform that
// "encoded" a record decodes it.
type xorDecoder struct{}

func (xorDecoder) Decode(in []byte) ([]byte, error) { return xorCompress(in) }
func (xorDecoder) Close() error                     { return nil }

func newXorDecoder() RecordDecoder { return xorDecoder{} }

// concat returns a fresh slice equal to a || b without aliasing either input.
func concat(a, b []byte) []byte {
	out := make([]byte, 0, len(a)+len(b))
	out = append(out, a...)
	out = append(out, b...)
	return out
}

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

// buildForIndex encodes sizes as a FOR index: [packed][1B W][4B min][4B CRC32C].
func buildForIndex(sizes []uint32) []byte {
	encoded := intpack.EncodeGroup(sizes)
	return binary.LittleEndian.AppendUint32(encoded, crc32c(encoded))
}

// configTestDecoder sets up a decoder to decode a single record containing n items.
func configTestDecoder(rd *decoder, n int) {
	rd.totalItems = n
	rd.itemsPerRecord = n
}

func TestDecoderWithRecordDecoder(t *testing.T) {
	entries := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("!"),
	}
	payload, sizes := buildPayload(entries)
	forIndex := buildForIndex(sizes)
	encoded, err := xorCompress(payload)
	if err != nil {
		t.Fatal(err)
	}
	data := concat(encoded, forIndex)

	rd := &decoder{rec: newXorDecoder()}
	defer rd.Close()
	configTestDecoder(rd, len(entries))

	if err := rd.decodeRecord(data, 0); err != nil {
		t.Fatal(err)
	}
	for i, want := range entries {
		if got := string(rd.Item(i)); got != string(want) {
			t.Errorf("Item(%d) = %q, want %q", i, got, want)
		}
	}
}

func TestDecoderPassthrough(t *testing.T) {
	// nil RecordDecoder: the record bytes (minus the FOR index) are the
	// items concatenated verbatim.
	entries := [][]byte{[]byte("raw"), []byte("data")}
	payload, sizes := buildPayload(entries)
	forIndex := buildForIndex(sizes)
	data := append(append([]byte{}, payload...), forIndex...)

	rd := &decoder{}
	defer rd.Close()
	configTestDecoder(rd, len(entries))

	if err := rd.decodeRecord(data, 0); err != nil {
		t.Fatal(err)
	}
	for i, want := range entries {
		if got := string(rd.Item(i)); got != string(want) {
			t.Errorf("Item(%d) = %q, want %q", i, got, want)
		}
	}
}

func TestDecoderNoForIndex(t *testing.T) {
	// itemsPerRecord=1: the entire payload is one item, no FOR index is appended.
	payload := []byte("single-item-payload")
	encoded, err := xorCompress(payload)
	if err != nil {
		t.Fatal(err)
	}

	rd := &decoder{rec: newXorDecoder()}
	defer rd.Close()
	configTestDecoder(rd, 1)

	if err := rd.decodeRecord(encoded, 0); err != nil {
		t.Fatal(err)
	}
	if got := string(rd.Item(0)); got != string(payload) {
		t.Errorf("Item(0) = %q, want %q", got, payload)
	}
}

func TestItemBoundsCheck(t *testing.T) {
	entries := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	payload, sizes := buildPayload(entries)
	forIndex := buildForIndex(sizes)
	data := append(append([]byte{}, payload...), forIndex...)

	rd := &decoder{}
	defer rd.Close()
	configTestDecoder(rd, 3)
	if err := rd.decodeRecord(data, 0); err != nil {
		t.Fatal(err)
	}

	assertPanics(t, "Item(-1)", func() { rd.Item(-1) })
	assertPanics(t, "Item(3)", func() { rd.Item(3) })
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
			got := itemsInRecord(tt.total, tt.itemsPerRecord, tt.recordIdx)
			if got != tt.want {
				t.Errorf("itemsInRecord(%d, %d, %d) = %d, want %d",
					tt.total, tt.itemsPerRecord, tt.recordIdx, got, tt.want)
			}
		})
	}
}

func TestDecoderReuse(t *testing.T) {
	// Decode a 5-item record, then decode a 2-item record on the same
	// decoder. Verifies that stale state from the first decode (larger
	// sizes/offsets slices) doesn't leak into the second.
	entries1 := [][]byte{[]byte("a"), []byte("bb"), []byte("ccc"), []byte("dd"), []byte("e")}
	payload1, sizes1 := buildPayload(entries1)
	forIndex1 := buildForIndex(sizes1)
	enc1, err := xorCompress(payload1)
	if err != nil {
		t.Fatal(err)
	}
	data1 := concat(enc1, forIndex1)

	rd := &decoder{rec: newXorDecoder()}
	defer rd.Close()
	configTestDecoder(rd, 5)

	if err := rd.decodeRecord(data1, 0); err != nil {
		t.Fatal(err)
	}
	for i, want := range entries1 {
		if got := string(rd.Item(i)); got != string(want) {
			t.Errorf("first decode Item(%d) = %q, want %q", i, got, want)
		}
	}

	// Second decode: fewer items.
	entries2 := [][]byte{[]byte("xx"), []byte("yy")}
	payload2, sizes2 := buildPayload(entries2)
	forIndex2 := buildForIndex(sizes2)
	enc2, err := xorCompress(payload2)
	if err != nil {
		t.Fatal(err)
	}
	data2 := concat(enc2, forIndex2)

	rd.totalItems = 2
	rd.itemsPerRecord = 2
	if err := rd.decodeRecord(data2, 0); err != nil {
		t.Fatal(err)
	}
	for i, want := range entries2 {
		if got := string(rd.Item(i)); got != string(want) {
			t.Errorf("second decode Item(%d) = %q, want %q", i, got, want)
		}
	}

	// Bounds check: Item(2) should panic after the second decode.
	assertPanics(t, "Item(2) after shrink", func() { rd.Item(2) })
}

func TestItemsInRecordPanics(t *testing.T) {
	assertPanics(t, "itemsPerRecord=0", func() { itemsInRecord(10, 0, 0) })
	assertPanics(t, "itemsPerRecord=-1", func() { itemsInRecord(10, -1, 0) })
	assertPanics(t, "recordIdx=5", func() { itemsInRecord(300, 128, 5) })
	assertPanics(t, "recordIdx=-1", func() { itemsInRecord(300, 128, -1) })
}
