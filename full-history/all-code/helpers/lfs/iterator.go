package lfs

import (
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stellar/go-stellar-sdk/xdr"
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
// Ledger Iterator
// =============================================================================

// LFSLedgerIterator efficiently iterates over a range of ledgers from LFS store,
// minimizing file I/O by keeping chunk files open while reading
// ledgers from the same chunk.
type LFSLedgerIterator struct {
	dataDir    string
	startSeq   uint32
	endSeq     uint32
	currentSeq uint32

	// Current chunk state
	currentChunkID uint32
	indexFile      *os.File
	dataFile       *os.File
	offsets        []uint64
	offsetSize     uint8
	chunkStartSeq  uint32
	chunkEndSeq    uint32

	// Decoder (reused)
	decoder *zstd.Decoder
}

// NewLFSLedgerIterator creates a new iterator for the given range.
func NewLFSLedgerIterator(dataDir string, startSeq, endSeq uint32) (*LFSLedgerIterator, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}

	return &LFSLedgerIterator{
		dataDir:        dataDir,
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

	// Load new chunk if needed (this includes reading all offsets)
	if chunkID != it.currentChunkID {
		if err := it.loadChunk(chunkID); err != nil {
			return lcm, ledgerSeq, timing, false, err
		}
	}

	// Index lookup - get offsets for this ledger
	indexStart := time.Now()
	localIndex := LedgerToLocalIndex(ledgerSeq)
	startOffset := it.offsets[localIndex]
	endOffset := it.offsets[localIndex+1]
	recordSize := endOffset - startOffset
	timing.IndexLookupTime = time.Since(indexStart)

	// Data read - read compressed data
	dataReadStart := time.Now()
	compressed := make([]byte, recordSize)
	if _, err := it.dataFile.ReadAt(compressed, int64(startOffset)); err != nil {
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

// loadChunk loads a new chunk's index and opens its data file.
func (it *LFSLedgerIterator) loadChunk(chunkID uint32) error {
	// Close previous chunk files if open
	it.closeChunkFiles()

	// Open index file
	indexPath := GetIndexPath(it.dataDir, chunkID)
	indexFile, err := os.Open(indexPath)
	if err != nil {
		return fmt.Errorf("failed to open index file for chunk %d: %w", chunkID, err)
	}
	it.indexFile = indexFile

	// Read header
	header := make([]byte, IndexHeaderSize)
	if _, err := indexFile.ReadAt(header, 0); err != nil {
		return fmt.Errorf("failed to read index header for chunk %d: %w", chunkID, err)
	}

	version := header[0]
	it.offsetSize = header[1]

	if version != IndexVersion {
		return fmt.Errorf("unsupported index version %d for chunk %d", version, chunkID)
	}

	if it.offsetSize != 4 && it.offsetSize != 8 {
		return fmt.Errorf("invalid offset size %d for chunk %d", it.offsetSize, chunkID)
	}

	// Get file size to determine number of offsets
	fileInfo, err := indexFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat index file for chunk %d: %w", chunkID, err)
	}

	numOffsets := (fileInfo.Size() - IndexHeaderSize) / int64(it.offsetSize)

	// Read all offsets for this chunk
	offsetsData := make([]byte, numOffsets*int64(it.offsetSize))
	if _, err := indexFile.ReadAt(offsetsData, IndexHeaderSize); err != nil {
		return fmt.Errorf("failed to read offsets for chunk %d: %w", chunkID, err)
	}

	// Parse offsets
	it.offsets = make([]uint64, numOffsets)
	for i := int64(0); i < numOffsets; i++ {
		if it.offsetSize == 4 {
			it.offsets[i] = uint64(binary.LittleEndian.Uint32(offsetsData[i*4 : i*4+4]))
		} else {
			it.offsets[i] = binary.LittleEndian.Uint64(offsetsData[i*8 : i*8+8])
		}
	}

	// Open data file
	dataPath := GetDataPath(it.dataDir, chunkID)
	dataFile, err := os.Open(dataPath)
	if err != nil {
		return fmt.Errorf("failed to open data file for chunk %d: %w", chunkID, err)
	}
	it.dataFile = dataFile

	// Update chunk state
	it.currentChunkID = chunkID
	it.chunkStartSeq = ChunkFirstLedger(chunkID)
	it.chunkEndSeq = ChunkLastLedger(chunkID)

	return nil
}

// closeChunkFiles closes the current chunk's files.
func (it *LFSLedgerIterator) closeChunkFiles() {
	if it.indexFile != nil {
		it.indexFile.Close()
		it.indexFile = nil
	}
	if it.dataFile != nil {
		it.dataFile.Close()
		it.dataFile = nil
	}
	it.offsets = nil
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

// RawLedgerIterator reads compressed ledger data from LFS without
// decompressing or unmarshaling. This is optimized for parallel processing
// where multiple workers handle the CPU-intensive decompression and
// unmarshaling steps.
//
// USAGE:
//
//	iterator, err := NewLFSRawLedgerIterator(dataDir, startSeq, endSeq)
//	if err != nil { ... }
//	defer iterator.Close()
//
//	for {
//	    data, hasMore, err := iterator.Next()
//	    if err != nil { ... }
//	    if !hasMore { break }
//	    // Send data.CompressedData to a worker for processing
//	}
//
// THREAD SAFETY:
//
//	RawLedgerIterator is NOT thread-safe. Each goroutine should have
//	its own iterator instance for a non-overlapping range of ledgers.
type LFSRawLedgerIterator struct {
	dataDir    string
	startSeq   uint32
	endSeq     uint32
	currentSeq uint32

	// Current chunk state
	currentChunkID uint32
	indexFile      *os.File
	dataFile       *os.File
	offsets        []uint64
	offsetSize     uint8
}

// NewLFSRawLedgerIterator creates a new iterator for reading raw compressed
// ledger data from the given range.
//
// Unlike LFSLedgerIterator, this does NOT decompress or unmarshal the data.
// It returns raw zstd-compressed bytes for processing by workers.
func NewLFSRawLedgerIterator(dataDir string, startSeq, endSeq uint32) (*LFSRawLedgerIterator, error) {
	return &LFSRawLedgerIterator{
		dataDir:        dataDir,
		startSeq:       startSeq,
		endSeq:         endSeq,
		currentSeq:     startSeq,
		currentChunkID: ^uint32(0), // Invalid chunk ID to force initial load
	}, nil
}

// Next returns the next ledger's compressed data.
//
// Returns:
//   - data: The compressed ledger data (valid only if hasMore is true)
//   - hasMore: true if data is valid, false if iteration is complete
//   - err: non-nil if an error occurred
//
// The CompressedData field contains raw zstd-compressed bytes that must
// be decompressed before unmarshaling to xdr.LedgerCloseMeta.
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

	// Get offsets for this ledger
	localIndex := LedgerToLocalIndex(ledgerSeq)
	startOffset := it.offsets[localIndex]
	endOffset := it.offsets[localIndex+1]
	recordSize := endOffset - startOffset

	// Read compressed data
	compressed := make([]byte, recordSize)
	if _, err := it.dataFile.ReadAt(compressed, int64(startOffset)); err != nil {
		return data, false, fmt.Errorf("failed to read data for ledger %d: %w", ledgerSeq, err)
	}

	data.CompressedData = compressed
	data.ReadTime = time.Since(readStart)

	it.currentSeq++
	return data, true, nil
}

// loadChunk loads a new chunk's index and opens its data file.
func (it *LFSRawLedgerIterator) loadChunk(chunkID uint32) error {
	// Close previous chunk files if open
	it.closeChunkFiles()

	// Open index file
	indexPath := GetIndexPath(it.dataDir, chunkID)
	indexFile, err := os.Open(indexPath)
	if err != nil {
		return fmt.Errorf("failed to open index file for chunk %d: %w", chunkID, err)
	}
	it.indexFile = indexFile

	// Read header
	header := make([]byte, IndexHeaderSize)
	if _, err := indexFile.ReadAt(header, 0); err != nil {
		return fmt.Errorf("failed to read index header for chunk %d: %w", chunkID, err)
	}

	version := header[0]
	it.offsetSize = header[1]

	if version != IndexVersion {
		return fmt.Errorf("unsupported index version %d for chunk %d", version, chunkID)
	}

	if it.offsetSize != 4 && it.offsetSize != 8 {
		return fmt.Errorf("invalid offset size %d for chunk %d", it.offsetSize, chunkID)
	}

	// Get file size to determine number of offsets
	fileInfo, err := indexFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat index file for chunk %d: %w", chunkID, err)
	}

	numOffsets := (fileInfo.Size() - IndexHeaderSize) / int64(it.offsetSize)

	// Read all offsets for this chunk
	offsetsData := make([]byte, numOffsets*int64(it.offsetSize))
	if _, err := indexFile.ReadAt(offsetsData, IndexHeaderSize); err != nil {
		return fmt.Errorf("failed to read offsets for chunk %d: %w", chunkID, err)
	}

	// Parse offsets
	it.offsets = make([]uint64, numOffsets)
	for i := int64(0); i < numOffsets; i++ {
		if it.offsetSize == 4 {
			it.offsets[i] = uint64(binary.LittleEndian.Uint32(offsetsData[i*4 : i*4+4]))
		} else {
			it.offsets[i] = binary.LittleEndian.Uint64(offsetsData[i*8 : i*8+8])
		}
	}

	// Open data file
	dataPath := GetDataPath(it.dataDir, chunkID)
	dataFile, err := os.Open(dataPath)
	if err != nil {
		return fmt.Errorf("failed to open data file for chunk %d: %w", chunkID, err)
	}
	it.dataFile = dataFile

	// Update chunk state
	it.currentChunkID = chunkID

	return nil
}

// closeChunkFiles closes the current chunk's files.
func (it *LFSRawLedgerIterator) closeChunkFiles() {
	if it.indexFile != nil {
		it.indexFile.Close()
		it.indexFile = nil
	}
	if it.dataFile != nil {
		it.dataFile.Close()
		it.dataFile = nil
	}
	it.offsets = nil
}

// Close closes the iterator and releases resources.
func (it *LFSRawLedgerIterator) Close() {
	it.closeChunkFiles()
}

// CurrentSequence returns the current ledger sequence (next to be read).
func (it *LFSRawLedgerIterator) CurrentSequence() uint32 {
	return it.currentSeq
}

// Progress returns the progress as a fraction (0.0 to 1.0).
func (it *LFSRawLedgerIterator) Progress() float64 {
	total := float64(it.endSeq - it.startSeq + 1)
	done := float64(it.currentSeq - it.startSeq)
	if total <= 0 {
		return 1.0
	}
	return done / total
}
