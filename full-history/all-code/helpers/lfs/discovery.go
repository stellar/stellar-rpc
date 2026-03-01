package lfs

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// =============================================================================
// Range Discovery
// =============================================================================

// LedgerRange represents a contiguous range of ledgers.
type LedgerRange struct {
	StartLedger uint32
	EndLedger   uint32
	StartChunk  uint32
	EndChunk    uint32
	TotalChunks uint32
}

// TotalLedgers returns the total number of ledgers in the range.
func (r LedgerRange) TotalLedgers() uint32 {
	return r.EndLedger - r.StartLedger + 1
}

// DiscoverLedgerRange efficiently finds the available ledger range in an LFS store.
//
// This function is optimized to minimize filesystem operations:
//   - First chunk is always 0 (Stellar starts at ledger 2, which is in chunk 0)
//   - Last chunk is found by checking the highest-numbered parent directory
//     and then finding the highest-numbered chunk file within it
//
// This reduces discovery from O(N) file reads to O(1) directory reads.
func DiscoverLedgerRange(dataDir string) (LedgerRange, error) {
	var result LedgerRange

	chunksDir := filepath.Join(dataDir, "chunks")
	if _, err := os.Stat(chunksDir); os.IsNotExist(err) {
		return result, fmt.Errorf("chunks directory not found: %s", chunksDir)
	}

	// First chunk is always 0 (ledger 2 starts in chunk 0)
	// Stellar blockchain starts at ledger 2, and chunk 0 contains ledgers 2-10001
	if !ChunkExists(dataDir, 0) {
		return result, fmt.Errorf("first chunk (000000) not found in %s", chunksDir)
	}
	firstChunk := uint32(0)

	// Find last chunk efficiently by looking at highest parent dir + highest file
	lastChunk, err := findLastChunk(dataDir, chunksDir)
	if err != nil {
		return result, err
	}

	result.StartChunk = firstChunk
	result.EndChunk = lastChunk
	result.StartLedger = ChunkFirstLedger(firstChunk)
	result.EndLedger = ChunkLastLedger(lastChunk)
	result.TotalChunks = lastChunk - firstChunk + 1

	return result, nil
}

// findLastChunk efficiently finds the highest complete chunk in the store.
//
// Algorithm:
//  1. List parent directories (0000, 0001, ...), find the highest valid one
//  2. List files in that directory, find the highest chunk ID
//  3. Verify the chunk is complete (both .data and .index exist)
//  4. If incomplete, scan backwards to find the last complete chunk
func findLastChunk(dataDir, chunksDir string) (uint32, error) {
	// List parent directories
	parentDirs, err := os.ReadDir(chunksDir)
	if err != nil {
		return 0, fmt.Errorf("failed to read chunks directory: %w", err)
	}

	// Find highest valid parent directory (4-digit number like "0051")
	highestParent, highestParentNum := findHighestValidParentDir(parentDirs)
	if highestParent == "" {
		return 0, fmt.Errorf("no valid parent directories found in %s", chunksDir)
	}

	// List files in highest parent directory
	parentPath := filepath.Join(chunksDir, highestParent)
	files, err := os.ReadDir(parentPath)
	if err != nil {
		return 0, fmt.Errorf("failed to read parent directory %s: %w", parentPath, err)
	}

	// Find highest chunk ID in this directory
	highestChunkID, found := findHighestChunkInDir(files)
	if !found {
		// No valid chunks in highest parent dir, try previous parent
		if highestParentNum == 0 {
			return 0, fmt.Errorf("no valid chunk files found")
		}
		// Fall back to scanning all chunks (rare edge case)
		return findLastChunkFallback(dataDir, chunksDir)
	}

	// Verify chunk is complete, scan backwards if not
	for chunkID := highestChunkID; ; chunkID-- {
		if ChunkExists(dataDir, chunkID) {
			return chunkID, nil
		}
		if chunkID == 0 {
			break
		}
	}

	return 0, fmt.Errorf("no complete chunks found")
}

// findHighestValidParentDir finds the highest valid 4-digit parent directory.
// Returns the directory name and its numeric value.
func findHighestValidParentDir(entries []os.DirEntry) (string, int) {
	highestName := ""
	highestNum := -1

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		name := entry.Name()
		if len(name) != 4 {
			continue
		}

		num, err := strconv.Atoi(name)
		if err != nil {
			continue
		}

		if num > highestNum {
			highestNum = num
			highestName = name
		}
	}

	return highestName, highestNum
}

// findHighestChunkInDir finds the highest chunk ID from .data files in a directory.
// Returns the chunk ID and whether a valid chunk was found.
func findHighestChunkInDir(entries []os.DirEntry) (uint32, bool) {
	highestChunkID := uint32(0)
	found := false

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".data") {
			continue
		}

		// Parse chunk ID from filename (NNNNNN.data)
		baseName := strings.TrimSuffix(name, ".data")
		if len(baseName) != 6 {
			continue
		}

		chunkID, err := strconv.ParseUint(baseName, 10, 32)
		if err != nil {
			continue
		}

		if !found || uint32(chunkID) > highestChunkID {
			highestChunkID = uint32(chunkID)
			found = true
		}
	}

	return highestChunkID, found
}

// findLastChunkFallback scans all chunks to find the last complete one.
// This is used as a fallback when the optimized approach fails.
func findLastChunkFallback(dataDir, chunksDir string) (uint32, error) {
	chunkIDs, err := findAllChunkIDs(chunksDir)
	if err != nil {
		return 0, err
	}

	if len(chunkIDs) == 0 {
		return 0, fmt.Errorf("no chunks found in: %s", chunksDir)
	}

	// Sort descending to find highest first
	sort.Slice(chunkIDs, func(i, j int) bool { return chunkIDs[i] > chunkIDs[j] })

	// Find first complete chunk (starting from highest)
	for _, chunkID := range chunkIDs {
		if ChunkExists(dataDir, chunkID) {
			return chunkID, nil
		}
	}

	return 0, fmt.Errorf("no complete chunks found")
}

// findAllChunkIDs scans the chunks directory structure and returns all chunk IDs.
func findAllChunkIDs(chunksDir string) ([]uint32, error) {
	var chunkIDs []uint32

	// List parent directories (0000, 0001, etc.)
	parentDirs, err := os.ReadDir(chunksDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunks directory: %w", err)
	}

	for _, parentDir := range parentDirs {
		if !parentDir.IsDir() {
			continue
		}

		// Check if directory name is a 4-digit number
		if len(parentDir.Name()) != 4 {
			continue
		}
		if _, err := strconv.Atoi(parentDir.Name()); err != nil {
			continue
		}

		// List chunk files in this parent directory
		parentPath := filepath.Join(chunksDir, parentDir.Name())
		chunkFiles, err := os.ReadDir(parentPath)
		if err != nil {
			continue // Skip unreadable directories
		}

		for _, chunkFile := range chunkFiles {
			if chunkFile.IsDir() {
				continue
			}

			// Look for .data files (each chunk has .data and .index)
			if !strings.HasSuffix(chunkFile.Name(), ".data") {
				continue
			}

			// Parse chunk ID from filename (NNNNNN.data)
			name := strings.TrimSuffix(chunkFile.Name(), ".data")
			if len(name) != 6 {
				continue
			}

			chunkID, err := strconv.ParseUint(name, 10, 32)
			if err != nil {
				continue
			}

			chunkIDs = append(chunkIDs, uint32(chunkID))
		}
	}

	return chunkIDs, nil
}

// ValidateLfsStore checks if the LFS store path is valid.
func ValidateLfsStore(dataDir string) error {
	// Check if directory exists
	info, err := os.Stat(dataDir)
	if os.IsNotExist(err) {
		return fmt.Errorf("LFS store directory does not exist: %s", dataDir)
	}
	if err != nil {
		return fmt.Errorf("failed to access LFS store directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("LFS store path is not a directory: %s", dataDir)
	}

	// Check if chunks subdirectory exists
	chunksDir := filepath.Join(dataDir, "chunks")
	if _, err := os.Stat(chunksDir); os.IsNotExist(err) {
		return fmt.Errorf("chunks subdirectory not found: %s", chunksDir)
	}

	return nil
}

// CountAvailableChunks returns the number of complete chunks in the store.
func CountAvailableChunks(dataDir string) (int, error) {
	chunksDir := filepath.Join(dataDir, "chunks")
	chunkIDs, err := findAllChunkIDs(chunksDir)
	if err != nil {
		return 0, err
	}

	// Count only complete chunks (both .data and .index exist)
	count := 0
	for _, chunkID := range chunkIDs {
		if ChunkExists(dataDir, chunkID) {
			count++
		}
	}

	return count, nil
}

// FindContiguousRange finds the largest contiguous range of chunks.
// Returns the range that should be used for processing.
func FindContiguousRange(dataDir string) (LedgerRange, int, error) {
	var result LedgerRange

	chunksDir := filepath.Join(dataDir, "chunks")
	chunkIDs, err := findAllChunkIDs(chunksDir)
	if err != nil {
		return result, 0, err
	}

	if len(chunkIDs) == 0 {
		return result, 0, fmt.Errorf("no chunks found")
	}

	// Sort chunk IDs
	sort.Slice(chunkIDs, func(i, j int) bool { return chunkIDs[i] < chunkIDs[j] })

	// Find gaps
	gaps := 0
	for i := 1; i < len(chunkIDs); i++ {
		if chunkIDs[i] != chunkIDs[i-1]+1 {
			gaps++
		}
	}

	// For now, just use first to last (warn about gaps)
	result.StartChunk = chunkIDs[0]
	result.EndChunk = chunkIDs[len(chunkIDs)-1]
	result.StartLedger = ChunkFirstLedger(result.StartChunk)
	result.EndLedger = ChunkLastLedger(result.EndChunk)
	result.TotalChunks = uint32(len(chunkIDs))

	return result, gaps, nil
}
