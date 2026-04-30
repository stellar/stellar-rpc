package packfile

import "errors"

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
