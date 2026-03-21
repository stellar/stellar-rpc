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
// Range Discovery — Index-First Layout
// =============================================================================
//
// Discovery functions scan the index-first directory structure to find
// available ledger data. The layout is:
//
//	{immutableBase}/index-{indexID:08d}/ledgers/{chunkID:08d}.pack

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

// findAllPackChunkIDs scans the index-first directory structure for .pack files
// and returns all discovered chunk IDs.
func findAllPackChunkIDs(immutableBase string) ([]uint32, error) {
	var chunkIDs []uint32

	entries, err := os.ReadDir(immutableBase)
	if err != nil {
		return nil, fmt.Errorf("read immutable base %s: %w", immutableBase, err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		// Match index-NNNNNNNN directories
		name := entry.Name()
		if !strings.HasPrefix(name, "index-") || len(name) != 14 {
			continue
		}

		// Scan ledgers/ subdirectory for .pack files
		ledgersDir := filepath.Join(immutableBase, name, "ledgers")
		packFiles, err := os.ReadDir(ledgersDir)
		if err != nil {
			continue // Skip unreadable directories
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
