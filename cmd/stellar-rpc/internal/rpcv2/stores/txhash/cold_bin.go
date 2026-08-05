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

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
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
// mirroring the event store cold-format split.
func ColdBinName(chunkID chunk.ID) string {
	return chunkID.String() + ".bin"
}

// WriteColdBin writes the .bin file at path from entries the caller already
// holds whole — the walk path's finalize accumulator, which exists for its
// sort anyway. It is a loop over the same coldBinStream the freeze drives, so
// walk-path and freeze-path bytes are identical by construction; see that
// type for the create semantics and commit for the durability ladder.
//
// entries must already be sorted (lex by Key, non-decreasing); this function
// writes them verbatim.
func WriteColdBin(path string, entries []ColdEntry) error {
	w, err := newColdBinStream(path)
	if err != nil {
		return err
	}
	defer w.close() // a no-op once commit has consumed the fd
	for i := range entries {
		if aerr := w.append(entries[i]); aerr != nil {
			return aerr
		}
	}
	return w.commit()
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

// coldBinStream is THE .bin writer: placeholder header, entries appended in
// caller order, then commit patches the leading count and makes the file
// durable. It has two entry points and no second implementation — the freeze
// streams its merge output straight in, WriteColdBin loops a whole slice
// through it — so the freeze's byte parity with the walk path is structural
// rather than a property two serializers have to keep agreeing on.
//
// The file is created with os.Create, truncating any prior attempt (O_TRUNC).
// There is no tmp+rename step: the artifact's completion record — written
// only after the writer returns — is the sole authority on whether the
// artifact exists, so a partial file from a failed or crashed attempt is
// inert scratch the retry overwrites (and scanBinHeader's header-vs-size
// check rejects loudly if one is ever opened).
//
// Two-phase commit/close like the domain's other writers — runspill.RunWriter
// carries the pattern doc.
type coldBinStream struct {
	f  *os.File
	bw *bufio.Writer
	// entryBuf is the per-append encode scratch. It lives on the writer
	// because bufio can hand the slice through to the underlying io.Writer,
	// which escapes a local array to the heap — an allocation per entry over
	// a ~3M-entry chunk.
	entryBuf [coldBinEntrySize]byte
	count    uint64
	done     bool
}

func newColdBinStream(path string) (*coldBinStream, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("txhash: create %s: %w", path, err)
	}
	bw := bufio.NewWriterSize(f, 1<<20)
	var header [coldBinHeaderSize]byte // count patched in commit
	if _, werr := bw.Write(header[:]); werr != nil {
		_ = f.Close()
		return nil, fmt.Errorf("txhash: write header: %w", werr)
	}
	return &coldBinStream{f: f, bw: bw}, nil
}

func (w *coldBinStream) append(e ColdEntry) error {
	copy(w.entryBuf[:ColdKeySize], e.Key[:])
	binary.LittleEndian.PutUint32(w.entryBuf[ColdKeySize:], e.Seq)
	if _, err := w.bw.Write(w.entryBuf[:]); err != nil {
		return fmt.Errorf("txhash: write entry: %w", err)
	}
	w.count++
	return nil
}

// commit completes the file: flush the buffer, patch the leading count now
// that it is known, then the durability ladder every .bin depends on — Sync
// before Close, with the Close error explicitly checked. Every step of that
// order is load-bearing. The flush precedes the patch because a short chunk
// leaves the placeholder header sitting in the buffer, and flushing after the
// WriteAt would lay those zeros back over the count. The patch precedes the
// Sync so one Sync covers the header and the entries. The Sync precedes the
// Close, and the Close error is not discarded, because the artifact's
// completion record must only be written once the data is durable and on many
// filesystems ENOSPC/EIO only surface at fd close — a silently truncated .bin
// would produce a wrong index without any signal.
func (w *coldBinStream) commit() error {
	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("txhash: flush: %w", err)
	}
	var header [coldBinHeaderSize]byte
	binary.LittleEndian.PutUint64(header[:], w.count)
	if _, err := w.f.WriteAt(header[:], 0); err != nil {
		return fmt.Errorf("txhash: patch header: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("txhash: sync %s: %w", w.f.Name(), err)
	}
	// done only once Close consumes the fd — an earlier error must leave
	// close() responsible for releasing it.
	w.done = true
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("txhash: close %s: %w", w.f.Name(), err)
	}
	return nil
}

// close releases the file on the error path; a no-op after commit. The
// partial file is inert scratch per the artifact model (retry overwrites).
func (w *coldBinStream) close() {
	if !w.done {
		_ = w.f.Close()
	}
}
