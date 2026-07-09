package txhash

// cold_bin.go owns the on-disk format of the RAW cold txhash chunk: the
// sorted per-chunk `<chunkID:08d>.bin` file the cold ingester publishes and
// the deferred streamhash index builder consumes. Keeping the writer and
// the filename helper next to the index builder's pre-scan in this package
// gives the format a single owner — producer (ingest) and consumer (index
// build) import a compile-time-linked codec instead of byte-matching a
// convention.
//
// File layout:
//
//	header  uint64 LE      entry count
//	entry   ColdKeySize B  txhash[:ColdKeySize]
//	        uint32 LE      absolute ledger seq
//
// Entries are lex-sorted by key. Duplicate truncated keys are written
// verbatim, but the downstream streamhash build fails on them — with
// 16-byte truncated hashes a collision is astronomically unlikely, and
// if one ever occurs the index build rejects it loudly rather than
// serving an ambiguous key.

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

const (
	// ColdKeySize is the truncated tx-hash key width stored in the cold
	// .bin file. It is pinned to streamhash.MinKeySize: the deferred
	// streamhash index builder routes/hashes on the first MinKeySize bytes
	// of each key, so the .bin producer must truncate to exactly that
	// width for the round-trip to hold.
	ColdKeySize = streamhash.MinKeySize
	// coldBinSeqSize is the per-entry ledger seq width (uint32 LE).
	coldBinSeqSize = 4
	// coldBinEntrySize is the per-entry width in the cold .bin file:
	// ColdKeySize bytes of truncated hash + the ledger seq.
	coldBinEntrySize = ColdKeySize + coldBinSeqSize
	// coldBinHeaderSize is the leading uint64 LE entry count.
	coldBinHeaderSize = 8
)

// ColdEntry is one (truncated txhash, ledger seq) tuple in a cold .bin file.
type ColdEntry struct {
	Key [ColdKeySize]byte
	Seq uint32
}

// ColdBinName returns the .bin filename for chunkID (`<chunkID:08d>.bin`).
// Bucket-directory composition ({bucketID:05d}/) is the orchestrator's job,
// mirroring the eventstore cold-format split.
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
// entries must already be sorted (lex by Key, non-decreasing); this function
// writes them verbatim.
//
// Sync runs before Close, and the Close error is explicitly checked: the
// completion record must only be written once the data is durable, and on
// many filesystems ENOSPC/EIO only surface at fd close — a silently
// truncated .bin would produce a wrong index without any signal.
func WriteColdBin(path string, entries []ColdEntry) error {
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
	binary.LittleEndian.PutUint64(header[:], uint64(len(entries)))
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
