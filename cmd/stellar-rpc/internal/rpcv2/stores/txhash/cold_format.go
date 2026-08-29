package txhash

// cold_format.go defines the on-disk format for a cold txhash index: a
// streamhash MPHF over (txhash, ledgerSeq) for one group of
// DefaultChunksPerIndex chunks. Each tx hash maps to a 3-byte ledger-seq
// offset from the group's MinLedger, stored inline in the per-key payload.
// One MPHF spans many chunks because a hash lookup has no ledger to narrow
// on. The reader is in cold_reader.go, the build in cold_index.go.
//
// Routing is secret-keyed: the .bin producer stores
// stores.BlindKey(secret, txhash[:ColdKeySize]) as each entry's key, the
// build feeds those keys verbatim, and the reader keys its queries the same
// way. secret = ColdIndexSecret(catalogSecret, indexID) — deterministic, and
// stored in the index metadata so queries never need the master key.

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
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

// coldMetadataVersion is the metadata blob's leading version byte. Every
// app-metadata blob leads with its own version byte so it is self-describing
// on its own, independent of the streamhash container version.
const coldMetadataVersion byte = 0x01

// coldMetadataSize is the metadata blob width:
// [version:1][MinLedger:4 LE][MaxLedger:4 LE][routing secret:16].
const coldMetadataSize = 1 + 8 + stores.SecretLen

// coldRoutingDomain is the DeriveIndexSecret domain for txhash cold indexes.
const coldRoutingDomain = "txhash"

// ErrInvalidMetadata is returned when a cold index's metadata is not a valid
// [MinLedger, MaxLedger, secret] blob.
var ErrInvalidMetadata = errors.New("txhash: cold index user metadata malformed")

// ColdIndexSecret derives index indexID's routing secret from the
// build-side master key. The single derivation both the .bin producer and
// BuildColdIndex use, so ingest-time keys always match the built index.
func ColdIndexSecret(catalogSecret []byte, indexID uint32) [stores.SecretLen]byte {
	return stores.DeriveIndexSecret(catalogSecret, coldRoutingDomain, indexID)
}

// EncodeColdMetadata packs [version, minLedger, maxLedger, secret] into the
// metadata blob.
func EncodeColdMetadata(minLedger, maxLedger uint32, secret [stores.SecretLen]byte) []byte {
	buf := make([]byte, coldMetadataSize)
	buf[0] = coldMetadataVersion
	binary.LittleEndian.PutUint32(buf[1:5], minLedger)
	binary.LittleEndian.PutUint32(buf[5:9], maxLedger)
	copy(buf[9:], secret[:])
	return buf
}

// ParseColdMetadata recovers [minLedger, maxLedger, secret] from the metadata
// blob, rejecting a wrong size or maxLedger < minLedger with
// ErrInvalidMetadata and an unknown version byte with its own message.
func ParseColdMetadata(metadata []byte) (uint32, uint32, [stores.SecretLen]byte, error) {
	var secret [stores.SecretLen]byte
	if err := stores.CheckBlobVersion(metadata, coldMetadataVersion); err != nil {
		return 0, 0, secret, fmt.Errorf("txhash: cold index metadata: %w", err)
	}
	if len(metadata) != coldMetadataSize {
		return 0, 0, secret, fmt.Errorf("%w: got %d bytes, want %d", ErrInvalidMetadata, len(metadata), coldMetadataSize)
	}
	minLedger := binary.LittleEndian.Uint32(metadata[1:5])
	maxLedger := binary.LittleEndian.Uint32(metadata[5:9])
	if maxLedger < minLedger {
		return 0, 0, secret, fmt.Errorf("%w: maxLedger %d < minLedger %d", ErrInvalidMetadata, maxLedger, minLedger)
	}
	copy(secret[:], metadata[9:])
	return minLedger, maxLedger, secret, nil
}

// ColdBuildOptions pins a cold index's payload size, fingerprint size, and
// [minLedger, maxLedger, secret] metadata.
func ColdBuildOptions(minLedger, maxLedger uint32, secret [stores.SecretLen]byte) []streamhash.BuildOption {
	return []streamhash.BuildOption{
		streamhash.WithPayload(ColdPayloadSize),
		streamhash.WithFingerprint(ColdFingerprintSize),
		streamhash.WithMetadata(EncodeColdMetadata(minLedger, maxLedger, secret)),
	}
}
