package txhash

// cold_format.go defines the on-disk format for a cold txhash index
// and the streamhash MPHF wrapper the cold reader queries through.
//
// Geometry — per-index, not global. Cold tx hashes are grouped into
// indexes of DefaultChunksPerIndex consecutive chunks (configurable).
// Each group gets ONE streamhash MPHF, in its own file, over the
// (txhash, ledgerSeq) pairs whose ledgers fall in that group's chunk
// range. A query maps a candidate ledger to its group, opens that
// group's index, and looks up the hash there. (Contrast the cold
// ledger/events stores, which key off a single chunk; the txhash
// index amortizes one MPHF across many chunks because a point lookup
// has no ledger context to narrow on.)
//
// Index payload — self-contained, no sidecar. Each tx hash maps to a
// fixed ledger seq stored directly in streamhash's per-key payload
// slot, as a 3-byte offset from the group's MinLedger anchor. Unlike
// the eventstore (whose variable-length roaring bitmaps need a
// separate index.pack), the txhash payload is fixed-width and fits
// inline — one streamhash file is the whole artifact.
//
// Build options (ColdBuildOptions):
//
//   - WithPayload(ColdPayloadSize=3): stores ledgerSeq - MinLedger.
//     3 bytes covers offsets up to 2^24 ≈ 16.7M ledgers, comfortably
//     above a group's span (DefaultChunksPerIndex × LedgersPerChunk
//     = 10M ledgers at the default).
//   - WithFingerprint(ColdFingerprintSize=1): streamhash verifies a
//     1-byte fingerprint on every Query. Residual false-positive rate
//     ≈ 1/256 for unseen keys; downstream txhash-in-LCM verification
//     (the read-assembly layer, out of scope here) rejects the
//     survivors at the cost of one wasted ledger fetch.
//   - WithMetadata(EncodeMinLedger(...)): 4-byte LE MinLedger anchor
//     embedded in the index so the reader recovers absolute seqs with
//     no external metadata.
//
// The build feeds keys pre-sorted by their big-endian prefix in a
// single pass via streamhash.NewSortedBuilder. Build logic lives in
// cold_index.go; the reader in cold_reader.go.

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// DefaultChunksPerIndex is the default number of consecutive chunks
// covered by one cold txhash index. It is configurable per
// deployment, but the build and the index-locating read path must
// agree on the value used to produce a given store.
//
// Distinct from chunk.ChunksPerBucket (the on-disk directory
// grouping) even though both currently default to 1000 — this one
// controls index fan-out (how many chunks share an MPHF), the other
// controls filesystem layout. They are free to diverge.
const DefaultChunksPerIndex uint32 = 1000

// ColdPayloadSize is the per-key payload width in bytes. Each payload
// stores ledgerSeq - MinLedger, so a 3-byte slot caps a single
// index's supported ledger span at 2^24 ≈ 16.7M ledgers from
// MinLedger. The build enforces that ceiling; widening it requires
// bumping this constant and rebuilding affected indexes.
const ColdPayloadSize = 3

// ColdFingerprintSize is the per-key fingerprint width in bytes.
// streamhash verifies it on Query, rejecting ~255/256 of unseen keys
// outright with stores.ErrNotFound.
const ColdFingerprintSize = 1

// coldPayloadMax is the largest ledger offset that fits in
// ColdPayloadSize bytes. The build rejects any offset above this so a
// silent high-byte truncation can never corrupt a stored seq.
const coldPayloadMax = uint64(1)<<(ColdPayloadSize*8) - 1

// coldMetadataSize is the size of the WithUserMetadata blob: 4 bytes
// LE holding the MinLedger anchor.
const coldMetadataSize = 4

// ErrInvalidMetadata is returned when an opened cold index's user
// metadata is not exactly coldMetadataSize bytes — the index was
// built by something other than ColdBuildOptions, or it is corrupt.
var ErrInvalidMetadata = errors.New("txhash: cold index user metadata malformed")

// IndexBaseChunk returns the first chunk ID of the index group that
// contains c, given chunksPerIndex chunks per group. The group spans
// chunk IDs [base, base+chunksPerIndex).
//
// Panics if chunksPerIndex is 0 — a zero group size is a configuration
// error, and surfacing it loudly beats a bare integer-divide-by-zero
// panic (matches chunk.IDFromLedger's guard convention).
func IndexBaseChunk(c chunk.ID, chunksPerIndex uint32) chunk.ID {
	if chunksPerIndex == 0 {
		panic("txhash: IndexBaseChunk called with chunksPerIndex 0")
	}
	return chunk.ID(uint32(c) / chunksPerIndex * chunksPerIndex)
}

// IndexFileName returns the conventional filename for the cold txhash
// index whose group begins at baseChunk. Producers and consumers
// compose the full path as {coldRoot}/{IndexFileName(base)}; the
// reader (OpenColdReader) takes that path directly.
func IndexFileName(baseChunk chunk.ID) string {
	return baseChunk.String() + "-txhash.idx"
}

// EncodeMinLedger packs minLedger as the 4-byte LE blob stored in the
// streamhash user-metadata slot.
func EncodeMinLedger(minLedger uint32) []byte {
	buf := make([]byte, coldMetadataSize)
	binary.LittleEndian.PutUint32(buf, minLedger)
	return buf
}

// ParseMinLedger recovers the MinLedger anchor from a streamhash
// user-metadata blob, rejecting anything that is not exactly
// coldMetadataSize bytes with ErrInvalidMetadata.
func ParseMinLedger(metadata []byte) (uint32, error) {
	if len(metadata) != coldMetadataSize {
		return 0, fmt.Errorf("%w: got %d bytes, want %d", ErrInvalidMetadata, len(metadata), coldMetadataSize)
	}
	return binary.LittleEndian.Uint32(metadata), nil
}

// ColdBuildOptions returns the streamhash.BuildOption set that pins a
// cold txhash index's payload size, fingerprint size, and MinLedger
// anchor. Callers append other options (e.g. WithWorkers) as needed;
// BuildColdIndex applies these for every build.
func ColdBuildOptions(minLedger uint32) []streamhash.BuildOption {
	return []streamhash.BuildOption{
		streamhash.WithPayload(ColdPayloadSize),
		streamhash.WithFingerprint(ColdFingerprintSize),
		streamhash.WithMetadata(EncodeMinLedger(minLedger)),
	}
}

// coldMPHF wraps an open streamhash payload index for the txhash cold
// lookup path. It caches the MinLedger anchor recovered from user
// metadata so lookup can return absolute seqs.
type coldMPHF struct {
	idx       *streamhash.PayloadIndex
	minLedger uint32
}

// openColdMPHF mmaps the streamhash index at path (OpenPayload errors
// if it was built without a payload region), validates its payload
// width and user metadata, and returns a query-ready wrapper.
func openColdMPHF(path string) (*coldMPHF, error) {
	idx, err := streamhash.OpenPayload(path)
	if err != nil {
		return nil, fmt.Errorf("txhash: open cold index %s: %w", path, err)
	}
	// Guard the payload width: lookup narrows the payload to uint32 on
	// the assumption it is ColdPayloadSize bytes. An index built with a
	// wider payload (a future format change, or a non-conforming
	// producer) would silently truncate to a wrong-but-plausible seq,
	// so reject the mismatch here rather than serving bad data.
	if got := idx.PayloadSize(); got != ColdPayloadSize {
		_ = idx.Close()
		return nil, fmt.Errorf("txhash: cold index %s payload size %d, want %d", path, got, ColdPayloadSize)
	}
	minLedger, err := ParseMinLedger(idx.UserMetadata())
	if err != nil {
		_ = idx.Close()
		return nil, fmt.Errorf("txhash: open cold index %s: %w", path, err)
	}
	return &coldMPHF{idx: idx, minLedger: minLedger}, nil
}

// lookup returns the absolute ledgerSeq stored under hash, or
// stores.ErrNotFound when streamhash's fingerprint check proves hash
// was not in the build set. streamhash routes and fingerprints on the
// first 16 bytes only, so passing the full 32-byte hash is equivalent
// to passing hash[:16] (the width the build keys on). Concurrent
// calls are safe — a streamhash.PayloadIndex supports concurrent reads.
func (m *coldMPHF) lookup(hash [32]byte) (uint32, error) {
	_, payload, err := m.idx.QueryPayload(hash[:])
	if err != nil {
		if errors.Is(err, streamhash.ErrNotFound) {
			return 0, stores.ErrNotFound
		}
		return 0, fmt.Errorf("txhash: cold index query: %w", err)
	}
	// payload is a ColdPayloadSize (3) byte value — streamhash
	// zero-extends to uint64, and openColdMPHF rejects any index whose
	// payload width isn't ColdPayloadSize — so it always fits uint32 and
	// the build kept it within the index's MinLedger span.
	return m.minLedger + uint32(payload), nil //nolint:gosec // 24-bit payload, width enforced at open
}

// close releases the underlying mmap. Idempotent via the index's own
// close guard.
func (m *coldMPHF) close() error {
	if err := m.idx.Close(); err != nil {
		return fmt.Errorf("txhash: close cold index: %w", err)
	}
	return nil
}
