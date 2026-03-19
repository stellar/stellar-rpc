package backfill

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/cf"
)

// =============================================================================
// Bin File Reader
// =============================================================================
//
// BinFileReader reads raw txhash entries from a single .bin file. Each entry
// is exactly 36 bytes: [txhash:32][ledgerSeq:4 BE]. The file has no header
// or footer — entry count is derived from file size / 36.
//
// Used by RangeBinScanner to iterate over all .bin files in a range, and by
// the RecSplit flow to count and add entries per CF.

const (
	// BinEntrySize is the size of a single txhash entry in bytes.
	// Format: [txhash:32 bytes][ledgerSeq:4 bytes big-endian]
	BinEntrySize = 36
)

// BinFileReader reads TxHashEntry values from a single .bin file.
type BinFileReader struct {
	file    *os.File
	buf     [BinEntrySize]byte
	entries int64 // total entries in file
	read    int64 // entries read so far
}

// NewBinFileReader opens a .bin file and prepares it for reading.
// Returns an error if the file size is not a multiple of 36 bytes.
func NewBinFileReader(path string) (*BinFileReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open bin file %s: %w", path, err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("stat bin file %s: %w", path, err)
	}

	if info.Size()%BinEntrySize != 0 {
		file.Close()
		return nil, fmt.Errorf("bin file %s has invalid size %d (not a multiple of %d)",
			path, info.Size(), BinEntrySize)
	}

	return &BinFileReader{
		file:    file,
		entries: info.Size() / BinEntrySize,
	}, nil
}

// Next reads the next entry from the file.
// Returns (entry, true, nil) for each entry.
// Returns (empty, false, nil) when all entries have been read.
// Returns (empty, false, err) on read error.
func (r *BinFileReader) Next() (TxHashEntry, bool, error) {
	var entry TxHashEntry

	_, err := io.ReadFull(r.file, r.buf[:])
	if err == io.EOF {
		return entry, false, nil
	}
	if err != nil {
		return entry, false, fmt.Errorf("read entry at position %d: %w", r.read, err)
	}

	copy(entry.TxHash[:], r.buf[:32])
	entry.LedgerSeq = binary.BigEndian.Uint32(r.buf[32:36])

	r.read++
	return entry, true, nil
}

// TotalEntries returns the total number of entries in the file.
func (r *BinFileReader) TotalEntries() int64 {
	return r.entries
}

// Close closes the underlying file.
func (r *BinFileReader) Close() error {
	return r.file.Close()
}

// =============================================================================
// Range Bin Scanner
// =============================================================================
//
// RangeBinScanner iterates over all 1000 .bin files in a range, optionally
// filtering entries by CF nibble. This is used during RecSplit building where
// each of 16 goroutines reads all .bin files but only processes entries
// matching its CF (txhash[0] >> 4 == cfIndex).
//
// Filtering happens during iteration, not upfront — the scanner reads all
// entries but only yields those matching the filter.

// RangeBinScanner iterates over .bin files in a range, filtered by CF.
type RangeBinScanner struct {
	txhashBase   string
	indexID      uint32
	firstChunkID uint32
	lastChunkID  uint32
	cfFilter     int // -1 for no filter, 0-15 for specific CF

	currentChunkID uint32
	reader         *BinFileReader
	done           bool
	totalYielded   int64
	totalScanned   int64
}

// RangeBinScannerConfig holds configuration for creating a RangeBinScanner.
type RangeBinScannerConfig struct {
	// TxHashBase is the base directory for txhash files.
	TxHashBase string

	// IndexID is the index to scan.
	IndexID uint32

	// FirstChunkID is the first chunk ID to read (inclusive).
	FirstChunkID uint32

	// LastChunkID is the last chunk ID to read (inclusive).
	LastChunkID uint32

	// CFFilter is the CF index to filter by (-1 for no filter).
	// Only entries where txhash[0]>>4 == CFFilter are yielded.
	CFFilter int
}

// NewRangeBinScanner creates a scanner over all .bin files in the given chunk range.
func NewRangeBinScanner(cfg RangeBinScannerConfig) *RangeBinScanner {
	return &RangeBinScanner{
		txhashBase:     cfg.TxHashBase,
		indexID:        cfg.IndexID,
		firstChunkID:   cfg.FirstChunkID,
		lastChunkID:    cfg.LastChunkID,
		cfFilter:       cfg.CFFilter,
		currentChunkID: cfg.FirstChunkID,
	}
}

// Next returns the next entry matching the CF filter.
// Returns (entry, true, nil) for each matching entry.
// Returns (empty, false, nil) when all files have been exhausted.
func (s *RangeBinScanner) Next() (TxHashEntry, bool, error) {
	for {
		if s.done {
			return TxHashEntry{}, false, nil
		}

		// Open next file if needed
		if s.reader == nil {
			if s.currentChunkID > s.lastChunkID {
				s.done = true
				return TxHashEntry{}, false, nil
			}

			path := RawTxHashPath(s.txhashBase, s.indexID, s.currentChunkID)
			reader, err := NewBinFileReader(path)
			if err != nil {
				return TxHashEntry{}, false, fmt.Errorf("open chunk %d: %w", s.currentChunkID, err)
			}
			s.reader = reader
		}

		// Read next entry from current file
		entry, hasMore, err := s.reader.Next()
		if err != nil {
			return TxHashEntry{}, false, fmt.Errorf("read chunk %d: %w", s.currentChunkID, err)
		}

		if !hasMore {
			// Current file exhausted, move to next
			s.reader.Close()
			s.reader = nil
			s.currentChunkID++
			continue
		}

		s.totalScanned++

		// Apply CF filter
		if s.cfFilter >= 0 && cf.Index(entry.TxHash[:]) != s.cfFilter {
			continue
		}

		s.totalYielded++
		return entry, true, nil
	}
}

// TotalYielded returns the number of entries that passed the CF filter.
func (s *RangeBinScanner) TotalYielded() int64 {
	return s.totalYielded
}

// TotalScanned returns the total number of entries read (before filtering).
func (s *RangeBinScanner) TotalScanned() int64 {
	return s.totalScanned
}

// Close releases any open file handles.
func (s *RangeBinScanner) Close() {
	if s.reader != nil {
		s.reader.Close()
		s.reader = nil
	}
}

