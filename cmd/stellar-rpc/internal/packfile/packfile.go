package packfile

import "errors"

const (
	magic       = 0x48434C53 // "SLCH" in the on-disk (little-endian) byte order
	version     = 1          // on-disk format version; bump on any breaking trailer/index change
	trailerSize = 76
)

// On-disk flag bits (uint8 at trailer offset 5). Only one flag is currently
// defined; the remaining bits are reserved for future use.
const (
	flagContentHash uint8 = 1 << 0
)

// ErrContentHashMismatch is returned when a file's content hash does not match
// the hash stored in the trailer.
var ErrContentHashMismatch = errors.New("packfile: content hash mismatch")
