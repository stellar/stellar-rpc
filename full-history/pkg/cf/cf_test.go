package cf

import "testing"

func TestIndex(t *testing.T) {
	tests := []struct {
		name      string
		firstByte byte
		want      int
	}{
		{"0x00 → CF 0", 0x00, 0},
		{"0x0F → CF 0", 0x0F, 0},
		{"0x10 → CF 1", 0x10, 1},
		{"0x1F → CF 1", 0x1F, 1},
		{"0x3F → CF 3", 0x3F, 3},
		{"0x80 → CF 8", 0x80, 8},
		{"0xA0 → CF 10", 0xA0, 10},
		{"0xAB → CF 10", 0xAB, 10},
		{"0xF0 → CF 15", 0xF0, 15},
		{"0xFF → CF 15", 0xFF, 15},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash := make([]byte, 32)
			hash[0] = tt.firstByte
			got := Index(hash)
			if got != tt.want {
				t.Errorf("Index(0x%02X...) = %d, want %d", tt.firstByte, got, tt.want)
			}
		})
	}
}

func TestName(t *testing.T) {
	tests := []struct {
		firstByte byte
		want      string
	}{
		{0x00, "0"},
		{0x1F, "1"},
		{0x9A, "9"},
		{0xAB, "a"},
		{0xFF, "f"},
	}
	for _, tt := range tests {
		hash := make([]byte, 32)
		hash[0] = tt.firstByte
		got := Name(hash)
		if got != tt.want {
			t.Errorf("Name(0x%02X...) = %q, want %q", tt.firstByte, got, tt.want)
		}
	}
}

func TestNamesCount(t *testing.T) {
	if len(Names) != Count {
		t.Errorf("Names length = %d, want %d", len(Names), Count)
	}
}

func TestIndexBoundaries(t *testing.T) {
	// Verify all 16 CFs are reachable
	seen := make(map[int]bool)
	for i := 0; i < 256; i++ {
		hash := []byte{byte(i), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		cfIdx := Index(hash)
		if cfIdx < 0 || cfIdx >= Count {
			t.Errorf("Index(0x%02X...) = %d, out of range [0, %d)", i, cfIdx, Count)
		}
		seen[cfIdx] = true
	}
	if len(seen) != Count {
		t.Errorf("only %d/%d CFs reachable from all 256 first-byte values", len(seen), Count)
	}
}
