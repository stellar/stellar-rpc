package packfile

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	magic       = 0x48434C53 // "SLCH" in the on-disk (little-endian) byte order
	version     = 1          // on-disk format version; bump on any breaking trailer/index change
	trailerSize = 76
)

// Trailer field byte offsets. The 76-byte trailer is the single source of
// truth that the writer emits and the reader parses; both sides MUST use
// these constants instead of hard-coded indices so the layout can never
// drift between encode and decode.
//
//	0:4   magic           uint32
//	4:5   version         uint8
//	5:6   flags           uint8
//	6:8   reserved
//	8:12  format          uint32   (caller-assigned)
//	12:16 recordCount     uint32
//	16:20 totalItems      uint32
//	20:24 itemsPerRecord  uint32
//	24:26 indexGroupSize  uint16
//	26:28 reserved
//	28:32 indexSize       uint32
//	32:36 appDataSize     uint32
//	36:68 contentHash     [32]byte (zero when flagContentHash unset)
//	68:72 reserved
//	72:76 crc32c          uint32   (over trailer[:trailerCRCEnd])
const (
	tOffMagic          = 0
	tOffVersion        = 4
	tOffFlags          = 5
	tOffFormat         = 8
	tOffRecordCount    = 12
	tOffTotalItems     = 16
	tOffItemsPerRecord = 20
	tOffIndexGroupSize = 24
	tOffIndexSize      = 28
	tOffAppDataSize    = 32
	tOffContentHash    = 36
	tEndContentHash    = 68
	tOffCRC            = 72
	trailerCRCEnd      = tOffCRC // bytes [0:trailerCRCEnd] are CRC-covered
)

// On-disk flag bits (uint8 at trailer offset 5). Only one flag is currently
// defined; the remaining bits are reserved for future use.
const (
	flagContentHash uint8 = 1 << 0
)

// ErrContentHashMismatch is returned when a file's content hash does not match
// the hash stored in the trailer.
var ErrContentHashMismatch = errors.New("packfile: content hash mismatch")

// Trailer holds the parsed trailer fields of an open packfile. The fields
// mirror the on-disk trailer: a caller can introspect the file's metadata
// (e.g. for diagnostic dumps or for verifying a stored Checksum against an
// independent recomputation).
//
// HasContentHash is the typed view of the only currently-defined flag bit;
// the raw flags byte itself is not exposed because no caller can act on
// unknown bits (Open rejects them via knownFlags).
type Trailer struct {
	Version           uint8
	Format            Format
	RecordCount       uint32
	TotalItems        uint32
	ItemsPerRecord    uint32
	IndexForGroupSize uint16
	IndexSize         uint32
	AppDataSize       uint32
	ContentHash       [32]byte
	HasContentHash    bool
	Checksum          uint32 // CRC32C over the leading bytes of the on-disk trailer; validated by unmarshalTrailer
}

// marshal writes the trailer into dst[0:trailerSize], including the CRC32C
// over dst[0:trailerCRCEnd]. dst must have at least trailerSize bytes.
// The Trailer's Checksum field is ignored — the on-disk CRC is recomputed.
func (t Trailer) marshal(dst []byte) {
	var flags uint8
	if t.HasContentHash {
		flags |= flagContentHash
	}
	binary.LittleEndian.PutUint32(dst[tOffMagic:], magic)
	dst[tOffVersion] = t.Version
	dst[tOffFlags] = flags
	binary.LittleEndian.PutUint32(dst[tOffFormat:], uint32(t.Format))
	binary.LittleEndian.PutUint32(dst[tOffRecordCount:], t.RecordCount)
	binary.LittleEndian.PutUint32(dst[tOffTotalItems:], t.TotalItems)
	binary.LittleEndian.PutUint32(dst[tOffItemsPerRecord:], t.ItemsPerRecord)
	binary.LittleEndian.PutUint16(dst[tOffIndexGroupSize:], t.IndexForGroupSize)
	binary.LittleEndian.PutUint32(dst[tOffIndexSize:], t.IndexSize)
	binary.LittleEndian.PutUint32(dst[tOffAppDataSize:], t.AppDataSize)
	copy(dst[tOffContentHash:tEndContentHash], t.ContentHash[:])
	binary.LittleEndian.PutUint32(dst[tOffCRC:], crc32c(dst[:trailerCRCEnd]))
}

// unmarshalTrailer parses a 76-byte trailer from the tail of src (src must be
// at least trailerSize bytes). Validates magic, version, CRC32C, unknown flag
// bits, and indexGroupSize. Returns ErrMagic, ErrVersion, ErrChecksum, or a
// wrapped ErrCorrupt on validation failure.
func unmarshalTrailer(src []byte) (Trailer, error) {
	if len(src) < trailerSize {
		return Trailer{}, fmt.Errorf("%w: trailer slice too short: %d < %d",
			ErrCorrupt, len(src), trailerSize)
	}
	tb := src[len(src)-trailerSize:]

	if m := binary.LittleEndian.Uint32(tb[tOffMagic:]); m != magic {
		return Trailer{}, ErrMagic
	}
	v := tb[tOffVersion]
	if v != version {
		return Trailer{}, ErrVersion
	}
	storedCRC := binary.LittleEndian.Uint32(tb[tOffCRC:])
	if crc32c(tb[:trailerCRCEnd]) != storedCRC {
		return Trailer{}, ErrChecksum
	}
	flags := tb[tOffFlags]
	if flags&^knownFlags != 0 {
		return Trailer{}, fmt.Errorf("%w: unknown trailer flags: %02x", ErrCorrupt, flags)
	}
	indexGroupSize := binary.LittleEndian.Uint16(tb[tOffIndexGroupSize:])
	if int(indexGroupSize) != groupSize {
		return Trailer{}, fmt.Errorf("%w: unsupported index group size %d (expected %d)",
			ErrCorrupt, indexGroupSize, groupSize)
	}

	hasContentHash := flags&flagContentHash != 0
	var contentHash [32]byte
	if hasContentHash {
		copy(contentHash[:], tb[tOffContentHash:tEndContentHash])
	}

	return Trailer{
		Version:           v,
		Format:            Format(binary.LittleEndian.Uint32(tb[tOffFormat:])),
		RecordCount:       binary.LittleEndian.Uint32(tb[tOffRecordCount:]),
		TotalItems:        binary.LittleEndian.Uint32(tb[tOffTotalItems:]),
		ItemsPerRecord:    binary.LittleEndian.Uint32(tb[tOffItemsPerRecord:]),
		IndexForGroupSize: indexGroupSize,
		IndexSize:         binary.LittleEndian.Uint32(tb[tOffIndexSize:]),
		AppDataSize:       binary.LittleEndian.Uint32(tb[tOffAppDataSize:]),
		ContentHash:       contentHash,
		HasContentHash:    hasContentHash,
		Checksum:          storedCRC,
	}, nil
}
