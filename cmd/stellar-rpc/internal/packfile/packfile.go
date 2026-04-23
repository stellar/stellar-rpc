package packfile

import "fmt"

const (
	magic       = 0x48434C53 // "SLCH" in the on-disk (little-endian) byte order
	version     = 1          // on-disk format version; bump on any breaking trailer/index change
	trailerSize = 64
)

// RecordFormat describes how records are stored on disk.
type RecordFormat int

const (
	// Compressed: zstd-compressed records (default). Integrity is provided
	// by zstd's built-in content checksum.
	Compressed RecordFormat = iota

	// Uncompressed: records stored as-is with a trailing 4-byte CRC32C.
	Uncompressed

	// Raw: records stored as-is with no integrity wrapper. Use when items
	// are already compressed or checksummed.
	Raw
)

func (f RecordFormat) String() string {
	switch f {
	case Compressed:
		return "Compressed"
	case Uncompressed:
		return "Uncompressed"
	case Raw:
		return "Raw"
	default:
		return fmt.Sprintf("RecordFormat(%d)", int(f))
	}
}

// On-disk flag bits (uint8 at trailer offset 5).
const (
	flagNoCompression uint8 = 1 << 0
	flagContentHash   uint8 = 1 << 1
	flagNoCRC         uint8 = 1 << 2
)
