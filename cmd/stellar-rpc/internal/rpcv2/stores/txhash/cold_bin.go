package txhash

// cold_bin.go owns the on-disk format of the cold txhash chunk: the
// sorted per-chunk `<chunkID:08d>.bin` file the cold ingester publishes and
// the deferred streamhash index builder consumes. Keeping the writer and
// the filename helper next to the index builder's pre-scan in this package
// gives the format a single owner — producer (ingest) and consumer (index
// build) import a compile-time-linked codec instead of byte-matching a
// convention.
//
// File layout:
//
//	header  uint64 LE           entry count
//	        stores.SecretLen B  index secret the keys were blinded with
//	entry   ColdKeySize B       blinded txhash[:ColdKeySize]
//	        uint32 LE           absolute ledger seq
//
// Entries are lex-sorted by (blinded) key. Duplicate keys are written
// verbatim, but the downstream streamhash build fails on them — with
// 16-byte blinded keys a collision is astronomically unlikely, and
// if one ever occurs the index build rejects it loudly rather than
// serving an ambiguous key.

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

const (
	// ColdKeySize is the blinded routing-key width stored in the cold
	// .bin file. It is pinned to streamhash.MinKeySize: the deferred
	// streamhash index builder routes/hashes on the first MinKeySize bytes
	// of each key, so the .bin producer's blinded keys are exactly that
	// width for the round-trip to hold.
	ColdKeySize = streamhash.MinKeySize
	// coldBinSeqSize is the per-entry ledger seq width (uint32 LE).
	coldBinSeqSize = 4
	// coldBinEntrySize is the per-entry width in the cold .bin file:
	// ColdKeySize bytes of blinded key + the ledger seq.
	coldBinEntrySize = ColdKeySize + coldBinSeqSize
	// coldBinCountSize is the leading uint64 LE entry count.
	coldBinCountSize = 8
	// coldBinHeaderSize is the count followed by the index secret the keys were
	// blinded with (stores.SecretLen). The build reads the secret back and
	// adopts it, so an index can never be built under a secret that disagrees
	// with the one its .bin keys were keyed with (see BuildColdIndex).
	coldBinHeaderSize = coldBinCountSize + stores.SecretLen
)

// ColdEntry is one (blinded key, ledger seq) tuple in a cold .bin file.
type ColdEntry struct {
	Key [ColdKeySize]byte
	Seq uint32
}

// ColdBinName returns the .bin filename for chunkID (`<chunkID:08d>.bin`).
// Bucket-directory composition ({bucketID:05d}/) is the orchestrator's job,
// mirroring the event store cold-format split.
func ColdBinName(chunkID chunk.ID) string {
	return chunkID.String() + ".bin"
}

// WriteColdBin writes the .bin file directly to path, truncating any prior
// attempt's file (os.Create is O_TRUNC). There is no tmp+rename step: the
// orchestrator's completion record — written only after WriteColdBin returns —
// is the sole authority on whether the artifact exists, so a partial file
// from a failed or crashed attempt is inert scratch the retry overwrites
// (and scanBinHeader's header-vs-size check rejects loudly if one is
// ever opened).
//
// secret is the index secret entries' keys were blinded with; it is recorded in
// the header so the deferred build adopts it instead of re-deriving one that
// might disagree.
//
// entries must already be sorted (lex by Key, non-decreasing); this function
// writes them verbatim.
//
// Sync runs before Close, and the Close error is explicitly checked: the
// completion record must only be written once the data is durable, and on
// many filesystems ENOSPC/EIO only surface at fd close — a silently
// truncated .bin would produce a wrong index without any signal.
func WriteColdBin(path string, secret [stores.SecretLen]byte, entries []ColdEntry) error {
	f, cerr := os.Create(path)
	if cerr != nil {
		return fmt.Errorf("txhash: create %s: %w", path, cerr)
	}
	// closed guards the deferred Close against double-closing after the
	// explicit error-checked Close below.
	closed := false
	defer func() {
		if !closed {
			_ = f.Close()
		}
	}()

	bw := bufio.NewWriterSize(f, 1<<20)
	var header [coldBinHeaderSize]byte
	binary.LittleEndian.PutUint64(header[:coldBinCountSize], uint64(len(entries)))
	copy(header[coldBinCountSize:], secret[:])
	if _, werr := bw.Write(header[:]); werr != nil {
		return fmt.Errorf("txhash: write header: %w", werr)
	}
	var entryBuf [coldBinEntrySize]byte
	for _, e := range entries {
		copy(entryBuf[:ColdKeySize], e.Key[:])
		binary.LittleEndian.PutUint32(entryBuf[ColdKeySize:], e.Seq)
		if _, werr := bw.Write(entryBuf[:]); werr != nil {
			return fmt.Errorf("txhash: write entry: %w", werr)
		}
	}
	if ferr := bw.Flush(); ferr != nil {
		return fmt.Errorf("txhash: flush: %w", ferr)
	}
	if serr := f.Sync(); serr != nil {
		return fmt.Errorf("txhash: sync %s: %w", path, serr)
	}
	closed = true
	if clerr := f.Close(); clerr != nil {
		return fmt.Errorf("txhash: close %s: %w", path, clerr)
	}
	return nil
}

// coldBinCount validates a .bin file's byte size against its declared header
// count and returns the count. size comes from a trusted Stat; count is the
// untrusted header value. It divides the trusted size rather than multiplying
// the untrusted count, so a corrupt header can't overflow the arithmetic
// (coldBinEntrySize·2^62 ≡ 0 mod 2^64 would slip a wildly wrong count past a
// naive `size == header + count*entry` check and hand it to the index builder
// as an allocation). The index builder's pre-scan (scanBinHeader) gates on it.
func coldBinCount(path string, size int64, count uint64) (uint64, error) {
	body := size - coldBinHeaderSize
	if body < 0 || body%coldBinEntrySize != 0 {
		return 0, fmt.Errorf("txhash: %s is %d bytes, not a %d-byte header plus whole %d-byte entries",
			path, size, coldBinHeaderSize, coldBinEntrySize)
	}
	if want := uint64(body) / coldBinEntrySize; count != want {
		return 0, fmt.Errorf("txhash: %s header claims %d entries but its %d bytes hold %d",
			path, count, size, want)
	}
	return count, nil
}

// coldBinStream writes a .bin incrementally: header with a placeholder
// count (the secret is written up front — it is known before the first
// entry), streamed entries, then finish patches the leading count and syncs
// — byte-identical to WriteColdBin's output without holding a chunk's
// entries in memory. The freeze is its user; the walk path keeps the
// slice-based writer (its accumulator exists anyway for the finalize sort).
type coldBinStream struct {
	f     *os.File
	bw    *bufio.Writer
	count uint64
	done  bool
}

func newColdBinStream(path string, secret [stores.SecretLen]byte) (*coldBinStream, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("txhash: create %s: %w", path, err)
	}
	bw := bufio.NewWriterSize(f, 1<<20)
	var header [coldBinHeaderSize]byte // count patched in finish; secret final
	copy(header[coldBinCountSize:], secret[:])
	if _, werr := bw.Write(header[:]); werr != nil {
		_ = f.Close()
		return nil, fmt.Errorf("txhash: write header: %w", werr)
	}
	return &coldBinStream{f: f, bw: bw}, nil
}

func (w *coldBinStream) append(e ColdEntry) error {
	var buf [coldBinEntrySize]byte
	copy(buf[:ColdKeySize], e.Key[:])
	binary.LittleEndian.PutUint32(buf[ColdKeySize:], e.Seq)
	if _, err := w.bw.Write(buf[:]); err != nil {
		return fmt.Errorf("txhash: write entry: %w", err)
	}
	w.count++
	return nil
}

// finish flushes, patches the header count, syncs, and closes — the same
// durability ladder as WriteColdBin (sync before close, close error checked).
func (w *coldBinStream) finish() error {
	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("txhash: flush: %w", err)
	}
	// Patch ONLY the count field: the secret was written at create time and
	// a whole-header rewrite would wipe it.
	var count [coldBinCountSize]byte
	binary.LittleEndian.PutUint64(count[:], w.count)
	if _, err := w.f.WriteAt(count[:], 0); err != nil {
		return fmt.Errorf("txhash: patch header count: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("txhash: sync %s: %w", w.f.Name(), err)
	}
	// done only once Close consumes the fd — an earlier error must leave
	// abort() responsible for releasing it.
	w.done = true
	return w.f.Close()
}

// abort releases the file on the error path; a no-op after finish. The
// partial file is inert scratch per the artifact model (retry overwrites).
func (w *coldBinStream) abort() {
	if !w.done {
		_ = w.f.Close()
	}
}
