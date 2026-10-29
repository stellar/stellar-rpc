package txhash

// cold_bin.go owns what a chunk's cold txhash .bin holds. The .bin is a
// streamhash sorted file. Each record is one of the chunk's routing keys,
// stores.BlindKey(secret, txhash[:ColdKeySize]), with the ledger it was
// committed in as a ColdPayloadSize-byte payload counted from the payload
// base, the first ledger of the chunk's index window. The base stays fixed
// for the window's life, so the payloads are final when the .bin is written
// and BuildColdIndex builds the window's index from the files as they are.
//
// The header's metadata blob, written by ColdBin.SortedFileConfig, is:
//
//	uint8     version (1)
//	uint32 LE payload base
//	uint32 LE chunk ID
//	16 bytes  index secret the keys were blinded with

import (
	"encoding/binary"
	"fmt"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// ColdKeySize is the routing key width: the first streamhash.MinKeySize
// bytes of a key are all the index uses.
const ColdKeySize = streamhash.MinKeySize

const (
	coldBinVersion      byte = 0x01
	coldBinMetadataSize      = 1 + 4 + stores.SecretLen
)

// ColdEntry is one of a chunk's routing keys with the ledger it was
// committed in.
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

// RunsDirName names the directory a build of chunkID's .bin spills its sorted
// runs into.
func RunsDirName(chunkID chunk.ID) string {
	return chunkID.String() + "-txhash.runs"
}

// ColdBin describes a chunk's .bin: the index secret its keys are blinded
// with and the chunk, which its header records, and the payload base its
// payloads count from, which it does not.
type ColdBin struct {
	Secret [stores.SecretLen]byte
	Base   uint32
	Chunk  chunk.ID
}

// SortedFileConfig returns the config the .bin, and the runs it is merged
// from, are written with.
func (b ColdBin) SortedFileConfig(bytesPerSync int) streamhash.SortedFileConfig {
	metadata := make([]byte, 0, coldBinMetadataSize)
	metadata = append(metadata, coldBinVersion)
	metadata = binary.LittleEndian.AppendUint32(metadata, uint32(b.Chunk))
	metadata = append(metadata, b.Secret[:]...)
	return streamhash.SortedFileConfig{PayloadSize: ColdPayloadSize, Metadata: metadata, BytesPerSync: bytesPerSync}
}

// Payload returns the payload a key committed in ledger seq is written with,
// refusing a ledger outside the chunk.
func (b ColdBin) Payload(seq uint32) (uint64, error) {
	if seq < b.Chunk.FirstLedger() || seq > b.Chunk.LastLedger() {
		return 0, fmt.Errorf("txhash: ledger %d is outside chunk %s", seq, b.Chunk)
	}
	return uint64(seq - b.Base), nil
}

// ReadColdBinHeader reads and checks the header of the .bin at path and
// returns the secret and the chunk it records.
func ReadColdBinHeader(path string) (ColdBin, error) {
	h, err := streamhash.ReadSortedFileHeader(path)
	if err != nil {
		return ColdBin{}, fmt.Errorf("txhash: %w", err)
	}
	if h.PayloadSize != ColdPayloadSize {
		return ColdBin{}, fmt.Errorf("txhash: %s has %d-byte payloads, want %d", path, h.PayloadSize, ColdPayloadSize)
	}
	if err := stores.CheckBlobVersion(h.Metadata, coldBinVersion); err != nil {
		return ColdBin{}, fmt.Errorf("txhash: %s metadata: %w", path, err)
	}
	if len(h.Metadata) != coldBinMetadataSize {
		return ColdBin{}, fmt.Errorf("txhash: %s metadata is %d bytes, want %d", path, len(h.Metadata), coldBinMetadataSize)
	}
	m := h.Metadata
	return ColdBin{
		Chunk:  chunk.ID(binary.LittleEndian.Uint32(m[1:5])),
		Secret: [stores.SecretLen]byte(m[5:]),
	}, nil
}
