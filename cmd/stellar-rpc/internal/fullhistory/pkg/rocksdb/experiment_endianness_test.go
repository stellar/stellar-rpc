//go:build experiment

// Build-tagged experimental test, not part of the normal test suite.
// Excluded from `go test ./...`; runs only with
// `go test -tags experiment ./cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb/...`.
// Kept in-tree as the empirical record behind the big-endian choice
// in encoding.go (see the byteOrder comment there).
// Safe to delete in due course, once the rationale is settled and
// no longer being revisited.

package rocksdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/require"
)

// experimentSeqs has three groups:
//   - In numeric [100, 200]:        100, 101, 150, 199, 200
//   - LE-byte traps for [100, 200]: 356, 612, 65637
//     (their LE encodings start with 0x64 or 0x65, sandwiching
//     between LE(100) = 64 00 00 00 and LE(200) = c8 00 00 00)
//   - Plainly outside both ranges:  1, 50, 256, 1024,
//     65535, 65536, 1000000
var experimentSeqs = []uint32{
	1, 50,
	100, 101, 150, 199, 200,
	256, 356, 612, 1024,
	65535, 65536, 65637,
	1000000,
}

// TestExperiment_BE simulates GetLedgerRange(100, 200) against a
// store seeded with experimentSeqs encoded BE (the project's
// EncodeUint32 / DecodeUint32 helpers).
func TestExperiment_BE_GetLedgerRange_100_200(t *testing.T) {
	s, err := New(Config{Path: t.TempDir(), Logger: silentLogger()})
	require.NoError(t, err)
	require.NoError(t, s.Open())
	t.Cleanup(func() { _ = s.Close() })

	for _, seq := range experimentSeqs {
		require.NoError(t, s.Put("default", EncodeUint32(seq), fmt.Appendf(nil, "value-%d", seq)))
	}

	rangeScan(t, s, "BE", EncodeUint32(100), EncodeUint32(200), 100, 200, DecodeUint32)
}

// TestExperiment_LE_GetLedgerRange_100_200 — same store + same
// inputs + same iterator pattern, but keys are encoded LE directly
// (the helpers are BE-only).
func TestExperiment_LE_GetLedgerRange_100_200(t *testing.T) {
	s, err := New(Config{Path: t.TempDir(), Logger: silentLogger()})
	require.NoError(t, err)
	require.NoError(t, s.Open())
	t.Cleanup(func() { _ = s.Close() })

	encodeLE := func(n uint32) []byte {
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, n)
		return b
	}
	decodeLE := func(b []byte) uint32 { return binary.LittleEndian.Uint32(b) }

	for _, seq := range experimentSeqs {
		require.NoError(t, s.Put("default", encodeLE(seq), fmt.Appendf(nil, "value-%d", seq)))
	}

	rangeScan(t, s, "LE", encodeLE(100), encodeLE(200), 100, 200, decodeLE)
}

// rangeScan does what a real GetLedgerRange would: open a grocksdb
// iterator on the default CF, Seek to startBytes, walk forward,
// stop the first time the current key bytes are > endBytes.
//
// (The wrapper's Iterate(cf, prefix) only supports prefix scans,
// not range scans, so a real GetLedgerRange would either bypass the
// wrapper as we do here, or the wrapper would need to grow an
// IterateRange method — separate decision.)
//
// Prints every key visited with marker `**` if the decoded value
// falls outside [lo, hi] numerically.
func rangeScan(t *testing.T, s *Store, label string, startBytes, endBytes []byte, lo, hi uint32, decode func([]byte) uint32) {
	cfh, err := s.resolveCF("default")
	require.NoError(t, err)
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	it := s.db.NewIteratorCF(ro, cfh)
	defer it.Close()

	fmt.Printf("\n%s — getLedgerRange(%d, %d):\n", label, lo, hi)
	fmt.Printf("  Seek to:   bytes=% x  (encodes %d)\n", startBytes, lo)
	fmt.Printf("  Stop when: bytes > % x  (encodes %d)\n", endBytes, hi)
	fmt.Println("  Iterator output:")

	var returned, polluters []uint32
	for it.Seek(startBytes); it.Valid(); it.Next() {
		kSlice := it.Key()
		vSlice := it.Value()
		k := append([]byte{}, kSlice.Data()...)
		v := append([]byte{}, vSlice.Data()...)
		kSlice.Free()
		vSlice.Free()

		if bytes.Compare(k, endBytes) > 0 {
			break
		}

		d := decode(k)
		marker := "  "
		if d < lo || d > hi {
			marker = "**"
			polluters = append(polluters, d)
		}
		returned = append(returned, d)
		fmt.Printf("    %s key=% x  decoded=%-7d  value=%q\n", marker, k, d, v)
	}
	require.NoError(t, it.Err())

	fmt.Printf("  Returned %d values: %v\n", len(returned), returned)
	if len(polluters) > 0 {
		fmt.Printf("  Polluters (returned but outside [%d, %d]): %v\n", lo, hi, polluters)
	}
}

/*
=========================================================================
EXPERIMENT OUTPUT — captured from `go test -tags experiment -v`.
Same fixture (15 values) for both subtests.
Same Seek-Walk-Stop iterator pattern (the natural GetLedgerRange
implementation) for both.
Only the encoding of the keys differs.
=========================================================================

BE — getLedgerRange(100, 200):
  Seek to:   bytes=00 00 00 64  (encodes 100)
  Stop when: bytes > 00 00 00 c8  (encodes 200)
  Iterator output:
       key=00 00 00 64  decoded=100      value="value-100"
       key=00 00 00 65  decoded=101      value="value-101"
       key=00 00 00 96  decoded=150      value="value-150"
       key=00 00 00 c7  decoded=199      value="value-199"
       key=00 00 00 c8  decoded=200      value="value-200"
  Returned 5 values: [100 101 150 199 200]

LE — getLedgerRange(100, 200):
  Seek to:   bytes=64 00 00 00  (encodes 100)
  Stop when: bytes > c8 00 00 00  (encodes 200)
  Iterator output:
       key=64 00 00 00  decoded=100      value="value-100"
    ** key=64 01 00 00  decoded=356      value="value-356"
    ** key=64 02 00 00  decoded=612      value="value-612"
       key=65 00 00 00  decoded=101      value="value-101"
    ** key=65 00 01 00  decoded=65637    value="value-65637"
       key=96 00 00 00  decoded=150      value="value-150"
       key=c7 00 00 00  decoded=199      value="value-199"
       key=c8 00 00 00  decoded=200      value="value-200"
  Returned 8 values: [100 356 612 101 65637 150 199 200]
  Polluters (returned but outside [100, 200]): [356 612 65637]

-------------------------------------------------------------------------
What's breaking
-------------------------------------------------------------------------
Same store.
Same fixture.
Same iterator pattern (Seek + walk + stop-when-key-past-end).
The ONLY difference is how the uint32 keys were encoded into bytes
before being written.

BE returns exactly the 5 ledgers in the asked-for numeric range:
[100, 101, 150, 199, 200].

LE returns 8 values: the right 5, plus 3 polluters that aren't in
the numeric range at all (356, 612, 65637), and they come back
sandwiched between the legit values in iteration order — not even
appended at the end where a caller might detect "huh, that's weird,
let me filter".

The caller has no way to tell. It asked for ledgers 100..200 and got
back 8 records, including value-356 and value-65637, with no error
flagged anywhere.

-------------------------------------------------------------------------
Why this matters for the hot ledger store (#584)
-------------------------------------------------------------------------
GetLedgerRange(start, end) is the natural way for query handlers to
ask "give me ledgers 100 through 200" — a single RocksDB iterator
pass, exactly the pattern shown above:
  Seek(EncodeUint32(start)), walk forward, stop when key > EncodeUint32(end).

That single-pass design REQUIRES on-disk byte order to match numeric
order, which is what BE encoding gives us.
With LE encoding the iterator silently returns scrambled and polluted
results (as shown above) — wrong data, no error.

The only way to keep LE correct is to abandon the iterator and issue
(end - start + 1) individual Get() calls — for a 10k-ledger range
that's ~10k random IO lookups instead of one contiguous scan.

-------------------------------------------------------------------------
Why this is not a concern for any other Layer-2 facade
-------------------------------------------------------------------------
The events cold-segment design (#665) uses the same iterator pattern
for ledger-range scans, and so do other facades that key by uint32 /
uint64.

None of them have to make this choice.
The single source of truth for storage byte order is the unexported
byteOrder var in pkg/rocksdb's encoding.go.
Every Layer-2 facade goes through EncodeUint32 / DecodeUint32 /
EncodeUint64 / DecodeUint64 and inherits big-endian for free.
A facade author never picks an endianness, never imports
encoding/binary for a key, never even thinks about it.
=========================================================================
*/
