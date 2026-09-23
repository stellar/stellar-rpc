package packfile

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
)

// writeLenPrefixed writes [4-byte little-endian length][item bytes] into h.
// This is the wire format for one item in a chunked content hash; both the
// writer's hashGoroutine and the reader's contentHasher emit identical bytes
// via this helper, so the format lives in exactly one place.
func writeLenPrefixed(h hash.Hash, item []byte) {
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(item))) //nolint:gosec // caller bounds-checks
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write(item)
}

// contentHasher computes the chunked SHA-256 content hash:
//
//	digest_i = SHA-256([4B len][item_{i*K}] ... [4B len][item_{i*K+K-1}])
//	final   = SHA-256(digest_0 || digest_1 || ...)
//
// where K is chunkSize (typically itemsPerRecord). Items stream through Add;
// the final hash is produced by Sum. Matches the writer's per-record digest
// scheme so the reader's Verify can replay it.
type contentHasher struct {
	chunk     hash.Hash // current chunk's SHA-256 (length-prefixed items)
	final     hash.Hash // SHA-256 over chunk digests, ready for Sum
	count     int       // items in current chunk
	chunkSize int
}

// newContentHasher creates a contentHasher with the given chunk size.
// Panics if chunkSize <= 0.
func newContentHasher(chunkSize int) *contentHasher {
	if chunkSize <= 0 {
		panic(fmt.Sprintf("packfile: newContentHasher chunkSize must be > 0, got %d", chunkSize))
	}
	return &contentHasher{
		chunk:     sha256.New(),
		final:     sha256.New(),
		chunkSize: chunkSize,
	}
}

// Add appends one logical item.
func (h *contentHasher) Add(item []byte) {
	writeLenPrefixed(h.chunk, item)
	h.count++
	if h.count == h.chunkSize {
		h.flushChunk()
	}
}

// Sum flushes any partial chunk and returns the final hash.
// After calling Sum the hasher must not be reused.
func (h *contentHasher) Sum() [sha256.Size]byte {
	if h.count > 0 {
		h.flushChunk()
	}
	var out [sha256.Size]byte
	h.final.Sum(out[:0])
	return out
}

// flushChunk finalizes the current chunk's digest, feeds it into the outer
// hasher, and resets chunk state for the next chunk.
func (h *contentHasher) flushChunk() {
	var d [sha256.Size]byte
	h.chunk.Sum(d[:0])
	_, _ = h.final.Write(d[:])
	h.chunk.Reset()
	h.count = 0
}

// AuxHasher computes the auxiliary hash WriterOptions.AuxHashExtract feeds:
//
//	digest_i = SHA-256([4B LE len][item_i])
//	Sum      = SHA-256(digest_0 || digest_1 || ...)
//
// one chunk per record, in record order. The writer folds digests its workers
// computed; a verifier replaying the records calls Add with each record's item
// instead. Both reach Sum through the same arithmetic, which is why this type
// exists rather than two open-coded hashers.
//
// A zero-length item is a real item: a record that carried no sidecar still
// contributes a digest, so "no sidecar anywhere" and "no records" are
// different hashes.
type AuxHasher struct{ final hash.Hash }

// NewAuxHasher returns an empty auxiliary hasher.
func NewAuxHasher() *AuxHasher { return &AuxHasher{final: sha256.New()} }

// Add folds one record's auxiliary item, in record order.
func (h *AuxHasher) Add(item []byte) { h.addDigest(auxItemDigest(item)) }

// Sum returns the hash of everything added so far. It does not finalize the
// hasher: adding more records after it is well defined.
func (h *AuxHasher) Sum() [sha256.Size]byte {
	var out [sha256.Size]byte
	h.final.Sum(out[:0])
	return out
}

// addDigest folds an already-computed chunk digest, which is what the writer
// has: its workers compute each record's digest off the writing goroutine.
func (h *AuxHasher) addDigest(d [sha256.Size]byte) { _, _ = h.final.Write(d[:]) }

// auxItemDigest is one item's chunk digest, SHA-256([4B LE len][item]). It
// shares writeLenPrefixed with the content hash, so the two schemes cannot
// drift.
func auxItemDigest(item []byte) [sha256.Size]byte {
	h := sha256.New()
	writeLenPrefixed(h, item)
	var d [sha256.Size]byte
	h.Sum(d[:0])
	return d
}
