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
// Range Discovery — Type-Separated Layout with Bucket Directories
// =============================================================================
//
// Discovery functions scan the bucket directory structure to find
// available ledger data. The layout is:
//
//	{ledgersPath}/{bucketID:05d}/{chunkID:08d}.pack

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

// DiscoverLedgerRange finds the available ledger range by scanning index directories.
// It looks for .pack files in the ledgers/ subdirectory of each index directory.
func DiscoverLedgerRange(immutableBase string) (LedgerRange, error) {
	var result LedgerRange

	// Find all chunk IDs across all index directories
	chunkIDs, err := findAllPackChunkIDs(immutableBase)
	if err != nil {
		return result, err
	}
	if len(chunkIDs) == 0 {
		return result, fmt.Errorf("no pack files found in %s", immutableBase)
	}

	sort.Slice(chunkIDs, func(i, j int) bool { return chunkIDs[i] < chunkIDs[j] })

	result.StartChunk = chunkIDs[0]
	result.EndChunk = chunkIDs[len(chunkIDs)-1]
	result.StartLedger = ChunkFirstLedger(result.StartChunk)
	result.EndLedger = ChunkLastLedger(result.EndChunk)
	result.TotalChunks = uint32(len(chunkIDs))

	return result, nil
}

// findAllPackChunkIDs scans the bucket directory structure for .pack files
// and returns all discovered chunk IDs.
func findAllPackChunkIDs(ledgersPath string) ([]uint32, error) {
	var chunkIDs []uint32

	bucketDirs, err := os.ReadDir(ledgersPath)
	if err != nil {
		return nil, fmt.Errorf("read ledgers path %s: %w", ledgersPath, err)
	}

	for _, entry := range bucketDirs {
		if !entry.IsDir() {
			continue
		}

		// Scan bucket directory for .pack files
		bucketPath := filepath.Join(ledgersPath, entry.Name())
		packFiles, err := os.ReadDir(bucketPath)
		if err != nil {
			continue
		}

		for _, pf := range packFiles {
			if pf.IsDir() || !strings.HasSuffix(pf.Name(), ".pack") {
				continue
			}
			baseName := strings.TrimSuffix(pf.Name(), ".pack")
			chunkID, err := strconv.ParseUint(baseName, 10, 32)
			if err != nil {
				continue
			}
			chunkIDs = append(chunkIDs, uint32(chunkID))
		}
	}

	return chunkIDs, nil
}

// ValidateLfsStore checks if the immutable store path is valid.
func ValidateLfsStore(immutableBase string) error {
	info, err := os.Stat(immutableBase)
	if os.IsNotExist(err) {
		return fmt.Errorf("immutable store directory does not exist: %s", immutableBase)
	}
	if err != nil {
		return fmt.Errorf("failed to access immutable store directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("immutable store path is not a directory: %s", immutableBase)
	}
	return nil
}

// CountAvailableChunks returns the number of .pack files found.
func CountAvailableChunks(immutableBase string) (int, error) {
	chunkIDs, err := findAllPackChunkIDs(immutableBase)
	if err != nil {
		return 0, err
	}
	return len(chunkIDs), nil
}

// FindContiguousRange finds the largest contiguous range of chunks.
func FindContiguousRange(immutableBase string) (LedgerRange, int, error) {
	var result LedgerRange

	chunkIDs, err := findAllPackChunkIDs(immutableBase)
	if err != nil {
		return result, 0, err
	}
	if len(chunkIDs) == 0 {
		return result, 0, fmt.Errorf("no chunks found")
	}

	sort.Slice(chunkIDs, func(i, j int) bool { return chunkIDs[i] < chunkIDs[j] })

	gaps := 0
	for i := 1; i < len(chunkIDs); i++ {
		if chunkIDs[i] != chunkIDs[i-1]+1 {
			gaps++
		}
	}

	result.StartChunk = chunkIDs[0]
	result.EndChunk = chunkIDs[len(chunkIDs)-1]
	result.StartLedger = ChunkFirstLedger(result.StartChunk)
	result.EndLedger = ChunkLastLedger(result.EndChunk)
	result.TotalChunks = uint32(len(chunkIDs))

	return result, gaps, nil
}
