package lfs

import (
	"fmt"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/tamir/events-analysis/packfile"
)

// =============================================================================
// Timing Structures
// =============================================================================

// LFSLedgerTiming holds granular timing for reading a single ledger from LFS store.
type LFSLedgerTiming struct {
	IndexLookupTime time.Duration // Time to read offsets from index file
	DataReadTime    time.Duration // Time to read compressed data from data file
	DecompressTime  time.Duration // Time to decompress
	UnmarshalTime   time.Duration // Time to unmarshal XDR
	TotalTime       time.Duration // Total time
}

// =============================================================================
// Ledger Iterator — Packfile-Based
// =============================================================================
//
// LFSLedgerIterator reads ledgers sequentially from packfile-based LFS chunks.
// Each chunk is a single .pack file where item N is the zstd-compressed XDR
// for the Nth ledger in the chunk.
//
// The iterator uses packfile.ReadRange for sequential access, which coalesces
// reads for efficiency.

// LFSLedgerIterator efficiently iterates over a range of ledgers from LFS store.
// Uses packfile.Reader for sequential reads through chunks.
type LFSLedgerIterator struct {
	immutableBase string
	startSeq      uint32
	endSeq        uint32
	currentSeq    uint32

	// Current chunk state
	currentChunkID uint32
	reader         *packfile.Reader
	chunkItems     int // total items in current chunk's packfile
	decoder        *zstd.Decoder
}

// NewLFSLedgerIterator creates a new iterator for the given range.
// The immutableBase is the root of the index-first directory layout.
// Ledgers are read from packfiles at:
//
//	{immutableBase}/index-{indexID:08d}/ledgers/{chunkID:08d}.pack
//
// Note: The iterator determines the indexID from the chunk ID using the
// default geometry. For non-default geometries, use NewLFSLedgerIteratorWithIndex.
func NewLFSLedgerIterator(immutableBase string, startSeq, endSeq uint32) (*LFSLedgerIterator, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}

	return &LFSLedgerIterator{
		immutableBase:  immutableBase,
		startSeq:       startSeq,
		endSeq:         endSeq,
		currentSeq:     startSeq,
		currentChunkID: ^uint32(0), // Invalid chunk ID to force initial load
		decoder:        decoder,
	}, nil
}

// Next returns the next ledger in the range.
// Returns (lcm, ledgerSeq, timing, true, nil) for each ledger.
// Returns (empty, 0, timing, false, nil) when iteration is complete.
// Returns (empty, seq, timing, false, err) on error.
func (it *LFSLedgerIterator) Next() (xdr.LedgerCloseMeta, uint32, LFSLedgerTiming, bool, error) {
	var lcm xdr.LedgerCloseMeta
	var timing LFSLedgerTiming
	totalStart := time.Now()

	if it.currentSeq > it.endSeq {
		return lcm, 0, timing, false, nil
	}

	ledgerSeq := it.currentSeq
	chunkID := LedgerToChunkID(ledgerSeq)

	// Load new chunk if needed
	if chunkID != it.currentChunkID {
		if err := it.loadChunk(chunkID); err != nil {
			return lcm, ledgerSeq, timing, false, err
		}
	}

	// Index lookup — determine the local index within the chunk
	indexStart := time.Now()
	localIndex := int(LedgerToLocalIndex(ledgerSeq))
	timing.IndexLookupTime = time.Since(indexStart)

	// Data read — read compressed data from packfile
	dataReadStart := time.Now()
	var compressed []byte
	err := it.reader.ReadItem(localIndex, func(data []byte) error {
		compressed = make([]byte, len(data))
		copy(compressed, data)
		return nil
	})
	if err != nil {
		return lcm, ledgerSeq, timing, false, fmt.Errorf("failed to read data for ledger %d: %w", ledgerSeq, err)
	}
	timing.DataReadTime = time.Since(dataReadStart)

	// Decompress
	decompressStart := time.Now()
	uncompressed, err := it.decoder.DecodeAll(compressed, nil)
	if err != nil {
		return lcm, ledgerSeq, timing, false, fmt.Errorf("failed to decompress ledger %d: %w", ledgerSeq, err)
	}
	timing.DecompressTime = time.Since(decompressStart)

	// Unmarshal
	unmarshalStart := time.Now()
	if err := lcm.UnmarshalBinary(uncompressed); err != nil {
		return lcm, ledgerSeq, timing, false, fmt.Errorf("failed to unmarshal ledger %d: %w", ledgerSeq, err)
	}
	timing.UnmarshalTime = time.Since(unmarshalStart)

	timing.TotalTime = time.Since(totalStart)

	it.currentSeq++
	return lcm, ledgerSeq, timing, true, nil
}

// loadChunk opens the packfile for a new chunk.
// Uses chunkID / 1000 as the indexID (default geometry).
func (it *LFSLedgerIterator) loadChunk(chunkID uint32) error {
	// Close previous chunk reader if open
	it.closeChunkFiles()

	// Determine indexID from chunkID using default geometry (1000 chunks per index).
	// This is a simplification — the iterator doesn't receive geometry config.
	// For production use with non-default geometry, the caller should set up
	// paths appropriately.
	indexID := chunkID / 1000

	packPath := GetPackPath(it.immutableBase, indexID, chunkID)

	// Open the packfile — returns immediately, I/O in background
	reader := packfile.Open(packPath)

	// Verify it opened successfully by checking total items
	totalItems, err := reader.TotalItems()
	if err != nil {
		reader.Close()
		return fmt.Errorf("failed to open packfile for chunk %d: %w", chunkID, err)
	}

	it.reader = reader
	it.currentChunkID = chunkID
	it.chunkItems = totalItems

	return nil
}

// closeChunkFiles closes the current chunk's packfile reader.
func (it *LFSLedgerIterator) closeChunkFiles() {
	if it.reader != nil {
		it.reader.Close()
		it.reader = nil
	}
}

// Close closes the iterator and releases resources.
func (it *LFSLedgerIterator) Close() {
	it.closeChunkFiles()
	if it.decoder != nil {
		it.decoder.Close()
		it.decoder = nil
	}
}

// CurrentSequence returns the current ledger sequence (next to be read).
func (it *LFSLedgerIterator) CurrentSequence() uint32 {
	return it.currentSeq
}

// Progress returns the progress as a fraction (0.0 to 1.0).
func (it *LFSLedgerIterator) Progress() float64 {
	total := float64(it.endSeq - it.startSeq + 1)
	done := float64(it.currentSeq - it.startSeq)
	if total <= 0 {
		return 1.0
	}
	return done / total
}

// =============================================================================
// Raw Ledger Iterator (for parallel processing)
// =============================================================================

// RawLedgerData holds compressed ledger data without decompressing.
// This is used for parallel processing where workers handle decompression.
type RawLedgerData struct {
	LedgerSeq      uint32        // Ledger sequence number
	CompressedData []byte        // Raw zstd-compressed LCM data
	ReadTime       time.Duration // Time spent reading from disk
}

// LFSRawLedgerIterator reads compressed ledger data from LFS without
// decompressing or unmarshaling. Uses packfile.Reader for sequential reads.
type LFSRawLedgerIterator struct {
	immutableBase  string
	startSeq       uint32
	endSeq         uint32
	currentSeq     uint32
	currentChunkID uint32
	reader         *packfile.Reader
}

// NewLFSRawLedgerIterator creates a new iterator for reading raw compressed
// ledger data from the given range.
func NewLFSRawLedgerIterator(immutableBase string, startSeq, endSeq uint32) (*LFSRawLedgerIterator, error) {
	return &LFSRawLedgerIterator{
		immutableBase:  immutableBase,
		startSeq:       startSeq,
		endSeq:         endSeq,
		currentSeq:     startSeq,
		currentChunkID: ^uint32(0),
	}, nil
}

// Next returns the next ledger's compressed data.
func (it *LFSRawLedgerIterator) Next() (RawLedgerData, bool, error) {
	var data RawLedgerData
	readStart := time.Now()

	if it.currentSeq > it.endSeq {
		return data, false, nil
	}

	ledgerSeq := it.currentSeq
	data.LedgerSeq = ledgerSeq
	chunkID := LedgerToChunkID(ledgerSeq)

	// Load new chunk if needed
	if chunkID != it.currentChunkID {
		if err := it.loadChunk(chunkID); err != nil {
			return data, false, err
		}
	}

	localIndex := int(LedgerToLocalIndex(ledgerSeq))

	// Read compressed data from packfile
	err := it.reader.ReadItem(localIndex, func(d []byte) error {
		data.CompressedData = make([]byte, len(d))
		copy(data.CompressedData, d)
		return nil
	})
	if err != nil {
		return data, false, fmt.Errorf("failed to read data for ledger %d: %w", ledgerSeq, err)
	}

	data.ReadTime = time.Since(readStart)
	it.currentSeq++
	return data, true, nil
}

// loadChunk opens the packfile for a new chunk.
func (it *LFSRawLedgerIterator) loadChunk(chunkID uint32) error {
	it.closeChunkFiles()

	indexID := chunkID / 1000
	packPath := GetPackPath(it.immutableBase, indexID, chunkID)

	reader := packfile.Open(packPath)
	if _, err := reader.TotalItems(); err != nil {
		reader.Close()
		return fmt.Errorf("failed to open packfile for chunk %d: %w", chunkID, err)
	}

	it.reader = reader
	it.currentChunkID = chunkID
	return nil
}

func (it *LFSRawLedgerIterator) closeChunkFiles() {
	if it.reader != nil {
		it.reader.Close()
		it.reader = nil
	}
}

func (it *LFSRawLedgerIterator) Close() {
	it.closeChunkFiles()
}

func (it *LFSRawLedgerIterator) CurrentSequence() uint32 {
	return it.currentSeq
}

func (it *LFSRawLedgerIterator) Progress() float64 {
	total := float64(it.endSeq - it.startSeq + 1)
	done := float64(it.currentSeq - it.startSeq)
	if total <= 0 {
		return 1.0
	}
	return done / total
}
