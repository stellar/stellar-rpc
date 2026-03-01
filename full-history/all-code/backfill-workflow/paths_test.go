package backfill

import "testing"

func TestRawTxHashDir(t *testing.T) {
	tests := []struct {
		base    string
		rangeID uint32
		want    string
	}{
		{"/data/txhash", 0, "/data/txhash/0000/raw"},
		{"/data/txhash", 1, "/data/txhash/0001/raw"},
		{"/data/txhash", 99, "/data/txhash/0099/raw"},
	}
	for _, tt := range tests {
		got := RawTxHashDir(tt.base, tt.rangeID)
		if got != tt.want {
			t.Errorf("RawTxHashDir(%q, %d) = %q, want %q", tt.base, tt.rangeID, got, tt.want)
		}
	}
}

func TestRawTxHashPath(t *testing.T) {
	tests := []struct {
		base    string
		rangeID uint32
		chunkID uint32
		want    string
	}{
		{"/data/txhash", 0, 0, "/data/txhash/0000/raw/000000.bin"},
		{"/data/txhash", 0, 350, "/data/txhash/0000/raw/000350.bin"},
		{"/data/txhash", 0, 999, "/data/txhash/0000/raw/000999.bin"},
		{"/data/txhash", 1, 1000, "/data/txhash/0001/raw/001000.bin"},
		{"/data/txhash", 2, 2999, "/data/txhash/0002/raw/002999.bin"},
	}
	for _, tt := range tests {
		got := RawTxHashPath(tt.base, tt.rangeID, tt.chunkID)
		if got != tt.want {
			t.Errorf("RawTxHashPath(%q, %d, %d) = %q, want %q",
				tt.base, tt.rangeID, tt.chunkID, got, tt.want)
		}
	}
}

func TestRecSplitIndexDir(t *testing.T) {
	tests := []struct {
		base    string
		rangeID uint32
		want    string
	}{
		{"/data/txhash", 0, "/data/txhash/0000/index"},
		{"/data/txhash", 1, "/data/txhash/0001/index"},
	}
	for _, tt := range tests {
		got := RecSplitIndexDir(tt.base, tt.rangeID)
		if got != tt.want {
			t.Errorf("RecSplitIndexDir(%q, %d) = %q, want %q", tt.base, tt.rangeID, got, tt.want)
		}
	}
}

func TestRecSplitIndexPath(t *testing.T) {
	tests := []struct {
		base    string
		rangeID uint32
		nibble  string
		want    string
	}{
		{"/data/txhash", 0, "0", "/data/txhash/0000/index/cf-0.idx"},
		{"/data/txhash", 0, "a", "/data/txhash/0000/index/cf-a.idx"},
		{"/data/txhash", 0, "f", "/data/txhash/0000/index/cf-f.idx"},
		{"/data/txhash", 1, "5", "/data/txhash/0001/index/cf-5.idx"},
	}
	for _, tt := range tests {
		got := RecSplitIndexPath(tt.base, tt.rangeID, tt.nibble)
		if got != tt.want {
			t.Errorf("RecSplitIndexPath(%q, %d, %q) = %q, want %q",
				tt.base, tt.rangeID, tt.nibble, got, tt.want)
		}
	}
}
