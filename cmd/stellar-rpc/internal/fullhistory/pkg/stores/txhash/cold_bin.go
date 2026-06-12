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

// WriteColdBin atomically publishes the .bin file: it writes to a
// "<path>.tmp" sibling and os.Renames it onto the final path only after a
// successful flush + close. On ANY write/flush/close error it removes the
// temp file and returns the error, so a failed write never leaves a stray or
// truncated final .bin behind (the header claims the full count, so a
// partial final file would silently feed a wrong index to the build step).
//
// entries must already be sorted (lex by Key, non-decreasing); this function
// writes them verbatim.
//
// The Close error is explicitly checked: on many filesystems ENOSPC/EIO only
// surface at fd close, and a silently truncated .bin would produce a wrong
// index without any signal.
func WriteColdBin(path string, entries []ColdEntry) error {
	tmp := path + ".tmp"
	f, cerr := os.Create(tmp)
	if cerr != nil {
		return fmt.Errorf("txhash: create %s: %w", tmp, cerr)
	}
	var err error
	// On any error past this point, drop the temp file so no partial/stray
	// artifact survives. The nil-ed f guards against double-close after the
	// explicit Close below.
	defer func() {
		if err != nil {
			if f != nil {
				_ = f.Close()
			}
			_ = os.Remove(tmp)
		}
	}()

	bw := bufio.NewWriterSize(f, 1<<20)
	var header [coldBinHeaderSize]byte
	binary.LittleEndian.PutUint64(header[:], uint64(len(entries)))
	if _, werr := bw.Write(header[:]); werr != nil {
		err = fmt.Errorf("txhash: write header: %w", werr)
		return err
	}
	var entryBuf [coldBinEntrySize]byte
	for _, e := range entries {
		copy(entryBuf[:ColdKeySize], e.Key[:])
		binary.LittleEndian.PutUint32(entryBuf[ColdKeySize:], e.Seq)
		if _, werr := bw.Write(entryBuf[:]); werr != nil {
			err = fmt.Errorf("txhash: write entry: %w", werr)
			return err
		}
	}
	if ferr := bw.Flush(); ferr != nil {
		err = fmt.Errorf("txhash: flush: %w", ferr)
		return err
	}
	if serr := f.Sync(); serr != nil {
		err = fmt.Errorf("txhash: sync %s: %w", tmp, serr)
		return err
	}
	if clerr := f.Close(); clerr != nil {
		f = nil // already closed; let the deferred cleanup just Remove
		err = fmt.Errorf("txhash: close %s: %w", tmp, clerr)
		return err
	}
	f = nil // closed cleanly; deferred cleanup must not touch it
	if rerr := os.Rename(tmp, path); rerr != nil {
		err = fmt.Errorf("txhash: rename %s -> %s: %w", tmp, path, rerr)
		return err
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
