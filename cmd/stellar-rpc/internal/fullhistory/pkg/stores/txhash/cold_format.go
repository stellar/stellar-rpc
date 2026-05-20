package txhash

// cold_format.go defines the on-disk format for the cold txhash index
// and the streamhash MPHF wrapper shared by the cold reader.
//
// The cold index is a single global file covering the entire cold
// ledger store: one streamhash MPHF over every (txhash, ledgerSeq)
// pair. Unlike the eventstore (which needs a separate index.pack for
// variable-length roaring bitmaps), the txhash payload is a fixed
// ledger-seq offset that fits natively in streamhash's per-key payload
// slot. One file is enough.
//
// Build is two-phase (lives in cmd/.../scripts/bench-fullhistory):
//
//	phase 1   ingest-raw-txhash    one .bin file per cold chunk, sorted
//	phase 2   build-txhash-index   merge .bin files into one .idx via
//	                               streamhash.NewSortedBuilder
//
// On-disk layout (streamhash-managed):
//
//   - Sorted-mode build: caller pre-sorts keys ascending by
//     big-endian uint64 prefix; the build is one-pass and avoids the
//     temp-partition writes of unsorted mode.
//   - WithPayload(ColdPayloadSize) — per-key payload is a 3-byte
//     ledger-seq offset from MinLedger. 3 bytes covers any range up
//     to 2^24 ≈ 16.7M ledgers, comfortably above mainnet history.
//   - WithFingerprint(ColdFingerprintSize) — 1-byte fingerprint
//     streamhash verifies internally on Query. Residual FPR ≈ 1/256
//     for unseen keys; downstream txhash-in-LCM verification rejects
//     false positives at the cost of one wasted ledger fetch.
//   - WithUserMetadata(EncodeMinLedger(...)) — 4-byte LE MinLedger
//     anchor embedded in the index, so the reader recovers absolute
//     seqs without external metadata.

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/tamirms/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// ColdIndexName is the conventional filename for the cold txhash
// index. Producers and consumers compose the full path as
// {coldRoot}/{ColdIndexName}.
const ColdIndexName = "txhash.idx"

// ColdPayloadSize — bytes of per-key payload in the MPHF. Each
// payload stores ledgerSeq - MinLedger, so a 3-byte slot caps the
// supported ledger span at 2^24 ≈ 16.7 M ledgers from MinLedger.
// Build-time and read-time both enforce that ceiling; widening the
// payload requires bumping this constant and rebuilding the index.
const ColdPayloadSize = 3

// ColdFingerprintSize — bytes of per-key fingerprint streamhash
// verifies on Query.
const ColdFingerprintSize = 1

// coldMetadataSize — size of the WithUserMetadata blob: 4 bytes LE
// containing the MinLedger anchor.
const coldMetadataSize = 4

// ErrInvalidMetadata is returned when an opened cold index's
// UserMetadata is not exactly coldMetadataSize bytes — the cold index
// was either built by something other than ColdBuildOptions or is
// corrupt.
var ErrInvalidMetadata = errors.New("txhash: cold index user metadata malformed")

// EncodeMinLedger packs minLedger as the 4-byte LE blob stored in
// the streamhash UserMetadata slot.
func EncodeMinLedger(minLedger uint32) []byte {
	buf := make([]byte, coldMetadataSize)
	binary.LittleEndian.PutUint32(buf, minLedger)
	return buf
}

// ParseMinLedger recovers the minLedger from a streamhash
// UserMetadata blob.
func ParseMinLedger(metadata []byte) (uint32, error) {
	if len(metadata) != coldMetadataSize {
		return 0, fmt.Errorf("%w: got %d bytes, want %d", ErrInvalidMetadata, len(metadata), coldMetadataSize)
	}
	return binary.LittleEndian.Uint32(metadata), nil
}

// ColdBuildOptions returns the streamhash.BuildOption set used to
// build a cold txhash index. The options pin payload size,
// fingerprint size, and the MinLedger anchor embedded as user
// metadata. Callers append other options (WithWorkers, WithAlgorithm)
// as needed.
func ColdBuildOptions(minLedger uint32) []streamhash.BuildOption {
	return []streamhash.BuildOption{
		streamhash.WithPayload(ColdPayloadSize),
		streamhash.WithFingerprint(ColdFingerprintSize),
		streamhash.WithMetadata(EncodeMinLedger(minLedger)),
	}
}

// coldMPHF wraps a streamhash PayloadIndex for the txhash cold
// lookup path. It stores the MinLedger recovered from the index's
// UserMetadata so Lookup can return absolute seqs.
type coldMPHF struct {
	idx       *streamhash.PayloadIndex
	minLedger uint32
}

// openColdMPHF mmaps the cold-index streamhash file at path,
// validates its UserMetadata, and returns a query-ready wrapper.
func openColdMPHF(path string) (*coldMPHF, error) {
	idx, err := streamhash.Open(path)
	if err != nil {
		return nil, fmt.Errorf("txhash: open cold index %s: %w", path, err)
	}
	minLedger, err := ParseMinLedger(idx.UserMetadata())
	if err != nil {
		_ = idx.Close()
		return nil, fmt.Errorf("txhash: open cold index %s: %w", path, err)
	}
	pidx, err := idx.WithPayload()
	if err != nil {
		_ = idx.Close()
		return nil, fmt.Errorf("txhash: cold index %s payload view: %w", path, err)
	}
	return &coldMPHF{idx: pidx, minLedger: minLedger}, nil
}

// lookup returns the absolute ledgerSeq stored under hash, or
// stores.ErrNotFound when streamhash's fingerprint check proves the
// hash was not in the build set. Concurrent calls are safe —
// streamhash.PayloadIndex supports concurrent reads.
func (m *coldMPHF) lookup(hash [32]byte) (uint32, error) {
	_, payload, err := m.idx.QueryPayload(hash[:])
	if err != nil {
		if errors.Is(err, streamhash.ErrNotFound) {
			return 0, stores.ErrNotFound
		}
		return 0, fmt.Errorf("txhash: cold MPHF query: %w", err)
	}
	if payload > 0xFFFFFF {
		// Payload is ColdPayloadSize (3) bytes; streamhash zero-extends
		// to uint64. A value above 2^24 means the on-disk representation
		// is wider than configured — corruption.
		return 0, fmt.Errorf("%w: txhash: ledger offset %d exceeds 24 bits", stores.ErrCorrupt, payload)
	}
	return m.minLedger + uint32(payload), nil
}

func (m *coldMPHF) close() error {
	if err := m.idx.Close(); err != nil {
		return fmt.Errorf("txhash: close cold MPHF: %w", err)
	}
	return nil
}
