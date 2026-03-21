package backfill

import "testing"

func TestIndexDir(t *testing.T) {
	tests := []struct {
		base    string
		indexID uint32
		want    string
	}{
		{"/data/immutable", 0, "/data/immutable/index-00000000"},
		{"/data/immutable", 1, "/data/immutable/index-00000001"},
		{"/data/immutable", 99, "/data/immutable/index-00000099"},
	}
	for _, tt := range tests {
		got := IndexDir(tt.base, tt.indexID)
		if got != tt.want {
			t.Errorf("IndexDir(%q, %d) = %q, want %q", tt.base, tt.indexID, got, tt.want)
		}
	}
}

func TestLedgerPackPath(t *testing.T) {
	tests := []struct {
		base    string
		indexID uint32
		chunkID uint32
		want    string
	}{
		{"/data/immutable", 0, 0, "/data/immutable/index-00000000/ledgers/00000000.pack"},
		{"/data/immutable", 0, 42, "/data/immutable/index-00000000/ledgers/00000042.pack"},
		{"/data/immutable", 1, 1000, "/data/immutable/index-00000001/ledgers/00001000.pack"},
	}
	for _, tt := range tests {
		got := LedgerPackPath(tt.base, tt.indexID, tt.chunkID)
		if got != tt.want {
			t.Errorf("LedgerPackPath(%q, %d, %d) = %q, want %q",
				tt.base, tt.indexID, tt.chunkID, got, tt.want)
		}
	}
}

func TestRawTxHashDir(t *testing.T) {
	tests := []struct {
		base    string
		indexID uint32
		want    string
	}{
		{"/data/immutable", 0, "/data/immutable/index-00000000/txhash/raw"},
		{"/data/immutable", 1, "/data/immutable/index-00000001/txhash/raw"},
		{"/data/immutable", 99, "/data/immutable/index-00000099/txhash/raw"},
	}
	for _, tt := range tests {
		got := RawTxHashDir(tt.base, tt.indexID)
		if got != tt.want {
			t.Errorf("RawTxHashDir(%q, %d) = %q, want %q", tt.base, tt.indexID, got, tt.want)
		}
	}
}

func TestRawTxHashPath(t *testing.T) {
	tests := []struct {
		base    string
		indexID uint32
		chunkID uint32
		want    string
	}{
		{"/data/immutable", 0, 0, "/data/immutable/index-00000000/txhash/raw/00000000.bin"},
		{"/data/immutable", 0, 42, "/data/immutable/index-00000000/txhash/raw/00000042.bin"},
		{"/data/immutable", 0, 999, "/data/immutable/index-00000000/txhash/raw/00000999.bin"},
		{"/data/immutable", 1, 1000, "/data/immutable/index-00000001/txhash/raw/00001000.bin"},
	}
	for _, tt := range tests {
		got := RawTxHashPath(tt.base, tt.indexID, tt.chunkID)
		if got != tt.want {
			t.Errorf("RawTxHashPath(%q, %d, %d) = %q, want %q",
				tt.base, tt.indexID, tt.chunkID, got, tt.want)
		}
	}
}

func TestRecSplitIndexDir(t *testing.T) {
	tests := []struct {
		base    string
		indexID uint32
		want    string
	}{
		{"/data/immutable", 0, "/data/immutable/index-00000000/txhash/index"},
		{"/data/immutable", 1, "/data/immutable/index-00000001/txhash/index"},
	}
	for _, tt := range tests {
		got := RecSplitIndexDir(tt.base, tt.indexID)
		if got != tt.want {
			t.Errorf("RecSplitIndexDir(%q, %d) = %q, want %q", tt.base, tt.indexID, got, tt.want)
		}
	}
}

func TestRecSplitIndexPath(t *testing.T) {
	tests := []struct {
		base    string
		indexID uint32
		nibble  string
		want    string
	}{
		{"/data/immutable", 0, "0", "/data/immutable/index-00000000/txhash/index/cf-0.idx"},
		{"/data/immutable", 0, "a", "/data/immutable/index-00000000/txhash/index/cf-a.idx"},
		{"/data/immutable", 0, "f", "/data/immutable/index-00000000/txhash/index/cf-f.idx"},
		{"/data/immutable", 1, "5", "/data/immutable/index-00000001/txhash/index/cf-5.idx"},
	}
	for _, tt := range tests {
		got := RecSplitIndexPath(tt.base, tt.indexID, tt.nibble)
		if got != tt.want {
			t.Errorf("RecSplitIndexPath(%q, %d, %q) = %q, want %q",
				tt.base, tt.indexID, tt.nibble, got, tt.want)
		}
	}
}

func TestEventsDataPath(t *testing.T) {
	got := EventsDataPath("/data/immutable", 0, 42)
	want := "/data/immutable/index-00000000/events/00000042-events.pack"
	if got != want {
		t.Errorf("EventsDataPath = %q, want %q", got, want)
	}
}

func TestEventsIndexPath(t *testing.T) {
	got := EventsIndexPath("/data/immutable", 0, 42)
	want := "/data/immutable/index-00000000/events/00000042-index.pack"
	if got != want {
		t.Errorf("EventsIndexPath = %q, want %q", got, want)
	}
}

func TestEventsHashPath(t *testing.T) {
	got := EventsHashPath("/data/immutable", 0, 42)
	want := "/data/immutable/index-00000000/events/00000042-index.hash"
	if got != want {
		t.Errorf("EventsHashPath = %q, want %q", got, want)
	}
}
