package txhash

// cold_format.go defines the on-disk format for a cold txhash index: a
// streamhash MPHF over (txhash, ledgerSeq) for one group of
// DefaultChunksPerIndex chunks. Each tx hash maps to a 3-byte ledger-seq
// offset from the group's MinLedger, stored inline in the per-key payload.
// One MPHF spans many chunks because a hash lookup has no ledger to narrow
// on. The reader is in cold_reader.go, the build in cold_index.go.

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/stellar/streamhash"
)

// DefaultChunksPerIndex is the default number of chunks per cold txhash index.
const DefaultChunksPerIndex uint32 = 1000

// ColdPayloadSize is the per-key payload width: ledgerSeq - MinLedger, so
// 3 bytes caps an index's ledger span at 2^24 (~16.7M).
const ColdPayloadSize = 3

// ColdFingerprintSize is the per-key fingerprint width; streamhash checks it
// on Query, rejecting ~255/256 of unseen keys.
const ColdFingerprintSize = 1

// coldPayloadMax is the largest offset that fits ColdPayloadSize bytes.
const coldPayloadMax = uint64(1)<<(ColdPayloadSize*8) - 1

// coldMetadataSize is the metadata blob width: two 4-byte LE values,
// [MinLedger, MaxLedger].
const coldMetadataSize = 8

// ErrInvalidMetadata is returned when a cold index's metadata is not a valid
// [MinLedger, MaxLedger] blob.
var ErrInvalidMetadata = errors.New("txhash: cold index user metadata malformed")

// EncodeLedgerRange packs [minLedger, maxLedger] into the metadata blob.
func EncodeLedgerRange(minLedger, maxLedger uint32) []byte {
	buf := make([]byte, coldMetadataSize)
	binary.LittleEndian.PutUint32(buf[:4], minLedger)
	binary.LittleEndian.PutUint32(buf[4:], maxLedger)
	return buf
}

// ParseLedgerRange recovers [minLedger, maxLedger] from the metadata blob,
// rejecting a wrong size or maxLedger < minLedger with ErrInvalidMetadata.
func ParseLedgerRange(metadata []byte) (uint32, uint32, error) {
	if len(metadata) != coldMetadataSize {
		return 0, 0, fmt.Errorf("%w: got %d bytes, want %d", ErrInvalidMetadata, len(metadata), coldMetadataSize)
	}
	minLedger := binary.LittleEndian.Uint32(metadata[:4])
	maxLedger := binary.LittleEndian.Uint32(metadata[4:])
	if maxLedger < minLedger {
		return 0, 0, fmt.Errorf("%w: maxLedger %d < minLedger %d", ErrInvalidMetadata, maxLedger, minLedger)
	}
	return minLedger, maxLedger, nil
}

// ColdBuildOptions pins a cold index's payload size, fingerprint size, and
// [minLedger, maxLedger] anchor.
func ColdBuildOptions(minLedger, maxLedger uint32) []streamhash.BuildOption {
	return []streamhash.BuildOption{
		streamhash.WithPayload(ColdPayloadSize),
		streamhash.WithFingerprint(ColdFingerprintSize),
		streamhash.WithMetadata(EncodeLedgerRange(minLedger, maxLedger)),
	}
}
