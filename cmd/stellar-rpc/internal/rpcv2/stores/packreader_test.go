package stores

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
)

// writeCheckedPack writes a record-checksummed pack and returns its path.
func writeCheckedPack(t *testing.T, items int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "checked.pack")
	w, err := packfile.Create(path, packfile.WriterOptions{
		ItemsPerRecord: 8,
		RecordChecksum: packfile.ChecksumCRC32C,
	})
	if err != nil {
		t.Fatal(err)
	}
	item := make([]byte, 64)
	for i := range items {
		item[0] = byte(i)
		if aerr := w.AppendItem(item); aerr != nil {
			t.Fatal(aerr)
		}
	}
	if ferr := w.Finish(nil); ferr != nil {
		t.Fatal(ferr)
	}
	return path
}

// TestPackReader_ReadRangeTranslates covers the one method that translates per
// yielded element rather than once per call. Every other method reports through
// its return value, so a test that only drives those leaves this path unproven
// while still looking like coverage.
func TestPackReader_ReadRangeTranslates(t *testing.T) {
	const items = 64
	path := writeCheckedPack(t, items)

	// Flip a byte inside the first record's payload. The widened record CRC32C
	// covers it, so the read must fail rather than return different bytes.
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0x01
	if werr := os.WriteFile(path, b, 0o600); werr != nil {
		t.Fatal(werr)
	}

	p := OpenPack(path, packfile.ReaderOptions{})
	t.Cleanup(func() { _ = p.Close() })

	var got error
	for _, rerr := range p.ReadRange(0, items) {
		if rerr != nil {
			got = rerr
			break
		}
	}
	if got == nil {
		t.Fatal("corrupt record read back clean")
	}
	if !errors.Is(got, ErrCorrupt) {
		t.Fatalf("ReadRange error = %v, want it to wrap ErrCorrupt", got)
	}
	// The specific cause stays reachable underneath the sentinel.
	if !errors.Is(got, packfile.ErrChecksum) {
		t.Errorf("ReadRange error = %v, want the packfile cause preserved", got)
	}
}

// TestPackReader_CloseTranslates pins the close-only path: Close carries the
// deferred open error, so on a handle that is never read it is the only place
// corruption can surface.
func TestPackReader_CloseTranslates(t *testing.T) {
	path := writeCheckedPack(t, 16)

	// Corrupt the trailer's CRC-covered region so the open itself fails.
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-8] ^= 0xFF
	if werr := os.WriteFile(path, b, 0o600); werr != nil {
		t.Fatal(werr)
	}

	p := OpenPack(path, packfile.ReaderOptions{})
	if cerr := p.Close(); !errors.Is(cerr, ErrCorrupt) {
		t.Fatalf("Close error = %v, want it to wrap ErrCorrupt", cerr)
	}
}
