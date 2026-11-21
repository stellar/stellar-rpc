package txhash

// cold_format.go defines the on-disk format for a cold txhash index: a
// streamhash MPHF over (txhash, ledgerSeq) for one group of
// DefaultChunksPerIndex chunks. Each tx hash maps to its ledger as a 3-byte
// offset from the payload base, the window's first ledger (see cold_bin.go),
// stored inline in the per-key payload. One MPHF spans many chunks because a
// hash lookup has no ledger to narrow on. The reader is in cold_reader.go, the
// build in cold_index.go.
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

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// DefaultChunksPerIndex is the default number of chunks per cold txhash index.
const DefaultChunksPerIndex uint32 = 1000

// ColdPayloadSize is the per-key payload width: ledgerSeq minus the payload
// base, so 3 bytes caps an index's ledgers at 2^24 (~16.7M) from its base.
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
// ColdMetadata blob.
var ErrInvalidMetadata = errors.New("txhash: cold index user metadata malformed")

// ColdMetadata is what a cold index records in its metadata blob: MinLedger,
// the first ledger of its window, which its payloads count from; MaxLedger,
// the last ledger it covers; and the routing secret.
type ColdMetadata struct {
	MinLedger, MaxLedger uint32
	Secret               [stores.SecretLen]byte
}

// ColdIndexSecret derives index indexID's routing secret from the
// build-side master key. The single derivation both the .bin producer and
// BuildColdIndex use, so ingest-time keys always match the built index.
func ColdIndexSecret(catalogSecret []byte, indexID uint32) [stores.SecretLen]byte {
	return stores.DeriveIndexSecret(catalogSecret, coldRoutingDomain, indexID)
}

// EncodeColdMetadata packs m into the metadata blob.
func EncodeColdMetadata(m ColdMetadata) []byte {
	buf := make([]byte, 0, coldMetadataSize)
	buf = append(buf, coldMetadataVersion)
	buf = binary.LittleEndian.AppendUint32(buf, m.MinLedger)
	buf = binary.LittleEndian.AppendUint32(buf, m.MaxLedger)
	return append(buf, m.Secret[:]...)
}

// ParseColdMetadata recovers the ColdMetadata from the metadata blob. Every
// refusal (empty, unknown version, wrong size, fields out of order) wraps
// ErrInvalidMetadata.
func ParseColdMetadata(metadata []byte) (ColdMetadata, error) {
	if err := stores.CheckBlobVersion(metadata, coldMetadataVersion); err != nil {
		return ColdMetadata{}, fmt.Errorf("%w: %w", ErrInvalidMetadata, err)
	}
	if len(metadata) != coldMetadataSize {
		return ColdMetadata{}, fmt.Errorf("%w: got %d bytes, want %d", ErrInvalidMetadata, len(metadata), coldMetadataSize)
	}
	m := ColdMetadata{
		MinLedger: binary.LittleEndian.Uint32(metadata[1:5]),
		MaxLedger: binary.LittleEndian.Uint32(metadata[5:9]),
		Secret:    [stores.SecretLen]byte(metadata[9:]),
	}
	if m.MinLedger > m.MaxLedger {
		return ColdMetadata{}, fmt.Errorf("%w: ledgers [%d, %d] out of order", ErrInvalidMetadata, m.MinLedger, m.MaxLedger)
	}
	return m, nil
}
