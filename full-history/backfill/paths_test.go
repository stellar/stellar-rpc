package backfill

import "testing"

func TestBucketID(t *testing.T) {
	tests := []struct {
		chunkID uint32
		want    uint32
	}{
		{0, 0},
		{999, 0},
		{1000, 1},
		{1999, 1},
		{5633, 5},
	}
	for _, tt := range tests {
		got := BucketID(tt.chunkID)
		if got != tt.want {
			t.Errorf("BucketID(%d) = %d, want %d", tt.chunkID, got, tt.want)
		}
	}
}

func TestLedgerPackPath(t *testing.T) {
	tests := []struct {
		base    string
		chunkID uint32
		want    string
	}{
		{"/mnt/nvme/ledgers", 0, "/mnt/nvme/ledgers/00000/00000000.pack"},
		{"/mnt/nvme/ledgers", 42, "/mnt/nvme/ledgers/00000/00000042.pack"},
		{"/mnt/nvme/ledgers", 999, "/mnt/nvme/ledgers/00000/00000999.pack"},
		{"/mnt/nvme/ledgers", 1000, "/mnt/nvme/ledgers/00001/00001000.pack"},
		{"/mnt/nvme/ledgers", 5633, "/mnt/nvme/ledgers/00005/00005633.pack"},
	}
	for _, tt := range tests {
		got := LedgerPackPath(tt.base, tt.chunkID)
		if got != tt.want {
			t.Errorf("LedgerPackPath(%q, %d) = %q, want %q", tt.base, tt.chunkID, got, tt.want)
		}
	}
}

func TestRawTxHashPath(t *testing.T) {
	tests := []struct {
		base    string
		chunkID uint32
		want    string
	}{
		{"/mnt/nvme/txhash/raw", 0, "/mnt/nvme/txhash/raw/00000/00000000.bin"},
		{"/mnt/nvme/txhash/raw", 42, "/mnt/nvme/txhash/raw/00000/00000042.bin"},
		{"/mnt/nvme/txhash/raw", 1000, "/mnt/nvme/txhash/raw/00001/00001000.bin"},
	}
	for _, tt := range tests {
		got := RawTxHashPath(tt.base, tt.chunkID)
		if got != tt.want {
			t.Errorf("RawTxHashPath(%q, %d) = %q, want %q", tt.base, tt.chunkID, got, tt.want)
		}
	}
}

func TestRecSplitIndexDir(t *testing.T) {
	tests := []struct {
		base    string
		indexID uint32
		want    string
	}{
		{"/mnt/nvme/txhash/index", 0, "/mnt/nvme/txhash/index/00000000"},
		{"/mnt/nvme/txhash/index", 1, "/mnt/nvme/txhash/index/00000001"},
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
		{"/mnt/nvme/txhash/index", 0, "0", "/mnt/nvme/txhash/index/00000000/cf-0.idx"},
		{"/mnt/nvme/txhash/index", 0, "a", "/mnt/nvme/txhash/index/00000000/cf-a.idx"},
		{"/mnt/nvme/txhash/index", 0, "f", "/mnt/nvme/txhash/index/00000000/cf-f.idx"},
		{"/mnt/nvme/txhash/index", 1, "5", "/mnt/nvme/txhash/index/00000001/cf-5.idx"},
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
	got := EventsDataPath("/mnt/nvme/events", 42)
	want := "/mnt/nvme/events/00000/00000042-events.pack"
	if got != want {
		t.Errorf("EventsDataPath = %q, want %q", got, want)
	}
}

func TestEventsIndexPath(t *testing.T) {
	got := EventsIndexPath("/mnt/nvme/events", 42)
	want := "/mnt/nvme/events/00000/00000042-index.pack"
	if got != want {
		t.Errorf("EventsIndexPath = %q, want %q", got, want)
	}
}

func TestEventsHashPath(t *testing.T) {
	got := EventsHashPath("/mnt/nvme/events", 42)
	want := "/mnt/nvme/events/00000/00000042-index.hash"
	if got != want {
		t.Errorf("EventsHashPath = %q, want %q", got, want)
	}
}

func TestStreamHashIndexPath(t *testing.T) {
	got := StreamHashIndexPath("/mnt/nvme/txhash/index", 0)
	want := "/mnt/nvme/txhash/index/00000000/txhash.idx"
	if got != want {
		t.Errorf("StreamHashIndexPath = %q, want %q", got, want)
	}
}

func TestRecSplitTmpDir(t *testing.T) {
	got := RecSplitTmpDir("/mnt/nvme/txhash/index", 0)
	want := "/mnt/nvme/txhash/index/00000000/tmp"
	if got != want {
		t.Errorf("RecSplitTmpDir = %q, want %q", got, want)
	}
}

func TestRecSplitCFTmpDir(t *testing.T) {
	got := RecSplitCFTmpDir("/mnt/nvme/txhash/index", 0, "a")
	want := "/mnt/nvme/txhash/index/00000000/tmp/cf-a"
	if got != want {
		t.Errorf("RecSplitCFTmpDir = %q, want %q", got, want)
	}
}

func TestRawTxHashDir(t *testing.T) {
	got := RawTxHashDir("/mnt/nvme/txhash/raw", 42)
	want := "/mnt/nvme/txhash/raw/00000"
	if got != want {
		t.Errorf("RawTxHashDir = %q, want %q", got, want)
	}
}
