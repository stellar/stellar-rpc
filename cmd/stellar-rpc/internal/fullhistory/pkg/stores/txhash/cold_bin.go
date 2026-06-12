package txhash

// cold_bin.go owns the on-disk format of the RAW cold txhash chunk: the
// sorted per-chunk `<chunkID:08d>.bin` file the cold ingester publishes and
// the deferred streamhash index builder consumes. Keeping the writer, the
// reader, and the filename helper next to each other in this package gives
// the format a single owner — producer (ingest) and consumer (index build)
// import a compile-time-linked codec instead of byte-matching a convention.
//
// File layout:
//
//	header  uint64 LE      entry count
//	entry   ColdKeySize B  txhash[:ColdKeySize]
//	        uint32 LE      absolute ledger seq
//
// Entries are lex-sorted by key (non-decreasing; duplicate truncated keys
// are possible and preserved).

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

const (
	// ColdKeySize is the truncated tx-hash key width stored in the cold
	// .bin file. It is pinned to streamhash.MinKeySize: the deferred
	// streamhash index builder routes/hashes on the first MinKeySize bytes
	// of each key, so the .bin producer must truncate to exactly that
	// width for the round-trip to hold.
	ColdKeySize = streamhash.MinKeySize
	// coldBinEntrySize is the per-entry width in the cold .bin file:
	// ColdKeySize bytes of truncated hash + a uint32 LE ledger seq.
	coldBinEntrySize = ColdKeySize + 4
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
// (and ReadColdBin's header-vs-size check rejects loudly if one is ever
// opened).
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

// ReadColdBin reads back a .bin file written by WriteColdBin, verifying the
// header count against the file size. The index-build step iterates these
// entries; tests use it to assert the writer's on-disk contract.
func ReadColdBin(path string) ([]ColdEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("txhash: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	br := bufio.NewReaderSize(f, 1<<20)
	var header [coldBinHeaderSize]byte
	if _, err := io.ReadFull(br, header[:]); err != nil {
		return nil, fmt.Errorf("txhash: read header of %s: %w", path, err)
	}
	count := binary.LittleEndian.Uint64(header[:])

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("txhash: stat %s: %w", path, err)
	}
	size := uint64(info.Size()) //nolint:gosec // Stat sizes are non-negative
	if count > size {           // each entry is >1 byte; also guards the multiply below
		return nil, fmt.Errorf("txhash: %s: implausible header count %d for %d-byte file", path, count, size)
	}
	want := coldBinHeaderSize + count*coldBinEntrySize
	if size != want {
		return nil, fmt.Errorf("txhash: %s: header claims %d entries (%d bytes), file has %d bytes",
			path, count, want, size)
	}

	entries := make([]ColdEntry, count)
	var entryBuf [coldBinEntrySize]byte
	for i := range entries {
		if _, err := io.ReadFull(br, entryBuf[:]); err != nil {
			return nil, fmt.Errorf("txhash: read entry %d of %s: %w", i, path, err)
		}
		copy(entries[i].Key[:], entryBuf[:ColdKeySize])
		entries[i].Seq = binary.LittleEndian.Uint32(entryBuf[ColdKeySize:])
	}
	return entries, nil
}
