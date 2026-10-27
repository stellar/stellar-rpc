package txhash

// cold_bin.go owns the on-disk format of the cold txhash chunk: the
// sorted per-chunk `<chunkID:08d>.bin` file the cold ingester publishes and
// the deferred streamhash index builder consumes. Keeping the writer and
// the filename helper next to the index builder's pre-scan in this package
// gives the format a single owner — producer (ingest) and consumer (index
// build) import a compile-time-linked codec instead of byte-matching a
// convention.
//
// File layout, streamhash's sorted-file format:
//
//	header  uint32 LE           magic 0x53545346 ("STSF")
//	        uint16 LE           version (1)
//	        uint8               payload size (ColdPayloadSize)
//	        uint8               reserved (zero)
//	        uint64 LE           entry count
//	        uint32 LE           metadata length (coldBinMetadataSize)
//	        uint8               metadata: version (1)
//	        uint32 LE           metadata: chunk ID
//	        stores.SecretLen B  metadata: index secret the keys were blinded with
//	        zero padding to coldBinHeaderSize
//	entry   ColdKeySize B       blinded txhash[:ColdKeySize]
//	        ColdPayloadSize B   LE ledger seq minus the first ledger of the
//	                            chunk's index window
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
	// coldBinEntrySize is the per-entry width in the cold .bin file:
	// ColdKeySize bytes of blinded key + the payload.
	coldBinEntrySize = ColdKeySize + ColdPayloadSize
	// coldBinMagic and coldBinVersion are streamhash's sorted-file magic and
	// version; the header scan rejects a mis-pointed or foreign file on them.
	coldBinMagic   uint32 = 0x53545346
	coldBinVersion uint16 = 1
	// coldBinPreludeSize is the magic, version, payload size and reserved byte.
	coldBinPreludeSize = 8
	// coldBinCountSize is the uint64 LE entry count after the prelude.
	coldBinCountSize = 8
	// coldBinMetadataSize is the metadata's version, chunk ID and index secret.
	// The build reads the secret back and adopts it, so an index can never be
	// built under a secret that disagrees with the one its .bin keys were keyed
	// with (see BuildColdIndex).
	coldBinMetadataSize = 1 + 4 + stores.SecretLen
	// coldBinPrefixSize is the header up to the end of the metadata.
	coldBinPrefixSize = coldBinPreludeSize + coldBinCountSize + 4 + coldBinMetadataSize
	// coldBinHeaderSize is the header padded to 4096 bytes, where the entries start.
	coldBinHeaderSize = 4096
	// coldBinSecretOffset is where the metadata's index secret starts.
	coldBinSecretOffset = coldBinPrefixSize - stores.SecretLen
)

// checkBinPrelude validates the magic, version, payload size, reserved byte
// and metadata shape at the start of a .bin header (hdr holds at least
// coldBinPrefixSize bytes). Both readers call it, so a format bump cannot
// leave one of them reading a newer header as entry bytes.
func checkBinPrelude(path string, hdr []byte) error {
	if magic := binary.LittleEndian.Uint32(hdr[:4]); magic != coldBinMagic {
		return fmt.Errorf("txhash: %s is not a cold txhash .bin (magic %#x, want %#x)", path, magic, coldBinMagic)
	}
	if v := binary.LittleEndian.Uint16(hdr[4:6]); v != coldBinVersion {
		return fmt.Errorf("txhash: %s has .bin version %d unsupported by this binary "+
			"(written by a newer stellar-rpc?)", path, v)
	}
	if hdr[6] != ColdPayloadSize || hdr[7] != 0 ||
		binary.LittleEndian.Uint32(hdr[16:20]) != coldBinMetadataSize || hdr[20] != 1 {
		return fmt.Errorf("txhash: %s has an unexpected .bin header "+
			"(written by a newer stellar-rpc, or corrupted)", path)
	}
	return nil
}

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
// might disagree. Each entry is written as its ledger minus base, the first
// ledger of the chunk's index window.
//
// entries must already be sorted (lex by Key, non-decreasing); this function
// writes them verbatim.
//
// Sync runs before Close, and the Close error is explicitly checked: the
// completion record must only be written once the data is durable, and on
// many filesystems ENOSPC/EIO only surface at fd close — a silently
// truncated .bin would produce a wrong index without any signal.
func WriteColdBin(path string, secret [stores.SecretLen]byte, c chunk.ID, base uint32, entries []ColdEntry) error {
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
	binary.LittleEndian.PutUint32(header[:4], coldBinMagic)
	binary.LittleEndian.PutUint16(header[4:6], coldBinVersion)
	header[6] = ColdPayloadSize
	binary.LittleEndian.PutUint64(header[coldBinPreludeSize:], uint64(len(entries)))
	binary.LittleEndian.PutUint32(header[16:20], coldBinMetadataSize)
	header[20] = 1
	binary.LittleEndian.PutUint32(header[21:25], uint32(c))
	copy(header[coldBinSecretOffset:], secret[:])
	if _, werr := bw.Write(header[:]); werr != nil {
		return fmt.Errorf("txhash: write header: %w", werr)
	}
	var entryBuf [coldBinEntrySize + 1]byte // PutUint32 room for the 3-byte payload
	for _, e := range entries {
		if e.Seq < base || uint64(e.Seq-base) > coldPayloadMax {
			return fmt.Errorf("txhash: ledger %d is outside the window starting at %d", e.Seq, base)
		}
		copy(entryBuf[:ColdKeySize], e.Key[:])
		binary.LittleEndian.PutUint32(entryBuf[ColdKeySize:], e.Seq-base)
		if _, werr := bw.Write(entryBuf[:coldBinEntrySize]); werr != nil {
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
