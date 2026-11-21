package txhash

// cold_bin.go owns what a chunk's cold txhash .bin holds. The .bin is a
// streamhash sorted file. Each record is one of the chunk's routing keys,
// RoutingKey(secret, txhash), with the ledger it was committed in as a
// ColdPayloadSize-byte payload counted from the payload base, the first
// ledger of the chunk's index window. The base stays fixed for the window's
// life, so the payloads are final when the .bin is written and
// BuildColdIndex builds the window's index from the files as they are.
//
// The header's metadata blob, written by ColdBin.SortedFileConfig, is:
//
//	uint8     version (1)
//	uint32 LE chunk ID
//	uint32 LE payload base
//	16 bytes  index secret the keys were blinded with

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// ColdKeySize is the routing key width: the first streamhash.MinKeySize
// bytes of a key are all the index uses.
const ColdKeySize = streamhash.MinKeySize

// RoutingKey is hash's key in the tx-hash indexes: the whole hash keyed with the index secret.
func RoutingKey(secret [stores.SecretLen]byte, hash [32]byte) [ColdKeySize]byte {
	return stores.BlindKey(secret, hash[:])
}

const (
	coldBinVersion      byte = 0x01
	coldBinMetadataSize      = 1 + 4 + 4 + stores.SecretLen
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
// with, the chunk, and the payload base its payloads count from, all of which
// its header records.
type ColdBin struct {
	Secret [stores.SecretLen]byte
	Base   uint32
	Chunk  chunk.ID
}

// SortedFileConfig returns the config the .bin, and the runs it is merged
// from, are written with.
func (b ColdBin) SortedFileConfig(bytesPerSync int) streamhash.SortedFileConfig {
	return streamhash.SortedFileConfig{PayloadSize: ColdPayloadSize, Metadata: b.metadata(), BytesPerSync: bytesPerSync}
}

// Payload returns the payload a key committed in ledger seq is written with,
// refusing a ledger outside the chunk.
func (b ColdBin) Payload(seq uint32) (uint64, error) {
	if seq < b.Chunk.FirstLedger() || seq > b.Chunk.LastLedger() {
		return 0, fmt.Errorf("txhash: ledger %d is outside chunk %s", seq, b.Chunk)
	}
	return uint64(seq - b.Base), nil
}

func (b ColdBin) metadata() []byte {
	m := make([]byte, 0, coldBinMetadataSize)
	m = append(m, coldBinVersion)
	m = binary.LittleEndian.AppendUint32(m, uint32(b.Chunk))
	m = binary.LittleEndian.AppendUint32(m, b.Base)
	return append(m, b.Secret[:]...)
}

// CheckColdBinHeader checks that the header of the .bin at path is want's:
// its chunk's, counted from its base, under its secret.
func CheckColdBinHeader(path string, want ColdBin) error {
	h, err := streamhash.ReadSortedFileHeader(path)
	if err != nil {
		return fmt.Errorf("txhash: %w", err)
	}
	if h.PayloadSize != ColdPayloadSize {
		return fmt.Errorf("txhash: %s has %d-byte payloads, want %d", path, h.PayloadSize, ColdPayloadSize)
	}
	if err := stores.CheckBlobVersion(h.Metadata, coldBinVersion); err != nil {
		return fmt.Errorf("txhash: %s metadata: %w", path, err)
	}
	if !bytes.Equal(h.Metadata, want.metadata()) {
		return fmt.Errorf("%w: txhash: %s is not chunk %s's .bin counted from ledger %d under this secret",
			stores.ErrCorrupt, path, want.Chunk, want.Base)
	}
	return nil
}

// ReadColdBinHeader reads the header of the .bin at path and returns what it
// records.
func ReadColdBinHeader(path string) (ColdBin, error) {
	h, err := streamhash.ReadSortedFileHeader(path)
	if err != nil {
		return ColdBin{}, fmt.Errorf("txhash: %w", err)
	}
	if err := stores.CheckBlobVersion(h.Metadata, coldBinVersion); err != nil {
		return ColdBin{}, fmt.Errorf("txhash: %s metadata: %w", path, err)
	}
	if len(h.Metadata) != coldBinMetadataSize {
		return ColdBin{}, fmt.Errorf("txhash: %s metadata is %d bytes, want %d", path, len(h.Metadata), coldBinMetadataSize)
	}
	return ColdBin{
		Secret: [stores.SecretLen]byte(h.Metadata[9:]),
		Base:   binary.LittleEndian.Uint32(h.Metadata[5:9]),
		Chunk:  chunk.ID(binary.LittleEndian.Uint32(h.Metadata[1:5])),
	}, nil
}

// WriteColdBin sorts entries by key, in place, and writes them as the .bin
// at path. Each entry's ledger must be in bin's chunk.
func WriteColdBin(path string, bin ColdBin, entries []ColdEntry, bytesPerSync int) error {
	slices.SortFunc(entries, func(a, b ColdEntry) int { return bytes.Compare(a.Key[:], b.Key[:]) })
	w, err := streamhash.NewSortedFileWriter(path, bin.SortedFileConfig(bytesPerSync))
	if err != nil {
		return err
	}
	defer w.Close()
	for _, e := range entries {
		payload, err := bin.Payload(e.Seq)
		if err == nil {
			err = w.Add(e.Key[:], payload)
		}
		if err != nil {
			return fmt.Errorf("write %s: %w", path, err)
		}
	}
	return w.Finish()
}
