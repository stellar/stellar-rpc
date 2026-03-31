// =============================================================================
// pkg/store/store.go - RocksDB TxHash Store Implementation
// =============================================================================
//
// This package implements the TxHashStore interface using RocksDB with 16 column
// families partitioned by the first hex character of the transaction hash.
//
// =============================================================================

package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/helpers"
	"github.com/stellar/stellar-rpc/full-history/all-code/parked-code/txhash-ingestion-workflow/pkg/cf"
	"github.com/stellar/stellar-rpc/full-history/all-code/parked-code/txhash-ingestion-workflow/pkg/interfaces"
	"github.com/stellar/stellar-rpc/full-history/all-code/parked-code/txhash-ingestion-workflow/pkg/types"
	"github.com/linxGnu/grocksdb"
)

// =============================================================================
// RocksDBTxHashStore - Implementation of TxHashStore Interface
// =============================================================================

// RocksDBTxHashStore implements the TxHashStore interface using RocksDB.
type RocksDBTxHashStore struct {
	// mu protects concurrent access to the store
	mu sync.RWMutex

	// db is the RocksDB database instance
	db *grocksdb.DB

	// opts is the database options
	opts *grocksdb.Options

	// cfHandles holds all column family handles (index 0 = "default", 1-16 = "0"-"f")
	cfHandles []*grocksdb.ColumnFamilyHandle

	// cfOpts holds options for each column family
	cfOpts []*grocksdb.Options

	// writeOpts is the write options for batch writes
	writeOpts *grocksdb.WriteOptions

	// readOpts is the shared read options for all reads
	readOpts *grocksdb.ReadOptions

	// blockCache is the shared block cache across all CFs
	blockCache *grocksdb.Cache

	// path is the filesystem path to the RocksDB store
	path string

	// cfIndexMap maps CF name to index in cfHandles
	cfIndexMap map[string]int

	// logger for logging operations
	logger interfaces.Logger

	// readOnly indicates if the store was opened in read-only mode
	readOnly bool

	// settings holds the RocksDB settings for this store
	settings *types.RocksDBSettings
}

// OpenRocksDBTxHashStore opens or creates a RocksDB store at the specified path.
//
// If the store exists, it is opened with the existing column families.
// If it doesn't exist, it is created with 16 column families (unless ReadOnly is true).
//
// READ-ONLY MODE:
// When settings.ReadOnly is true, the store is opened in read-only mode using
// OpenDbForReadOnlyColumnFamilies. In this mode:
//   - The database must already exist (CreateIfMissing is ignored)
//   - Write operations (WriteBatch, FlushAll, CompactAll, CompactCF) will panic
//   - Multiple processes can open the same database simultaneously
//   - No write-ahead log (WAL) recovery is performed
//   - Ideal for tools that only read data (RecSplit building, verification, queries)
//
// RECOVERY BEHAVIOR (read-write mode only):
// When opening an existing store, RocksDB automatically replays the WAL (Write-Ahead Log)
// to recover any data that was written but not yet flushed to SST files. This can take
// significant time (observed ~300 MB/s WAL replay speed). The function logs detailed
// timing and statistics to help diagnose recovery performance.
func OpenRocksDBTxHashStore(path string, settings *types.RocksDBSettings, logger interfaces.Logger) (*RocksDBTxHashStore, error) {
	// Create Shared Block Cache
	var blockCache *grocksdb.Cache
	if settings.BlockCacheSizeMB > 0 {
		blockCache = grocksdb.NewLRUCache(uint64(settings.BlockCacheSizeMB * types.MB))
	}

	// Create Database Options
	opts := grocksdb.NewDefaultOptions()
	if !settings.ReadOnly {
		opts.SetCreateIfMissing(true)
		opts.SetCreateIfMissingColumnFamilies(true)
	} else {
		opts.SetCreateIfMissing(false)
	}
	opts.SetErrorIfExists(false)

	// Global settings
	opts.SetMaxBackgroundJobs(settings.MaxBackgroundJobs)
	opts.SetMaxOpenFiles(settings.MaxOpenFiles)

	// Logging settings (reduce RocksDB log noise)
	opts.SetInfoLogLevel(grocksdb.WarnInfoLogLevel)
	opts.SetMaxLogFileSize(20 * types.MB)
	opts.SetKeepLogFileNum(3)

	// Prepare Column Family Names and Options
	// Note: "default" CF is required by RocksDB, but we don't use it for data
	cfNames := []string{"default"}
	cfNames = append(cfNames, cf.Names...)

	// Create options for each CF
	cfOptsList := make([]*grocksdb.Options, len(cfNames))

	// Default CF uses minimal options (we don't use it)
	cfOptsList[0] = grocksdb.NewDefaultOptions()

	// Data CFs (0-f) use full configuration
	for i := 1; i < len(cfNames); i++ {
		cfOptsList[i] = createCFOptions(settings, blockCache)
	}

	// Open Database with Column Families
	// This includes WAL replay if there's pending data from a previous crash (read-write mode only)
	if settings.ReadOnly {
		logger.Info("Opening RocksDB store at: %s (READ-ONLY)", path)
	} else {
		logger.Info("Opening RocksDB store at: %s", path)
		logger.Info("  (WAL recovery will occur if there's uncommitted data)")
	}

	openStart := time.Now()
	var db *grocksdb.DB
	var cfHandles []*grocksdb.ColumnFamilyHandle
	var err error

	if settings.ReadOnly {
		// Open in read-only mode - no WAL recovery, multiple processes can read simultaneously
		db, cfHandles, err = grocksdb.OpenDbForReadOnlyColumnFamilies(opts, path, cfNames, cfOptsList, false)
	} else {
		// Open in read-write mode - includes WAL recovery
		db, cfHandles, err = grocksdb.OpenDbColumnFamilies(opts, path, cfNames, cfOptsList)
	}
	openDuration := time.Since(openStart)

	if err != nil {
		// Cleanup on failure
		opts.Destroy()
		for _, cfOpt := range cfOptsList {
			if cfOpt != nil {
				cfOpt.Destroy()
			}
		}
		if blockCache != nil {
			blockCache.Destroy()
		}
		return nil, fmt.Errorf("failed to open RocksDB store at %s: %w", path, err)
	}

	// Create Write and Read Options
	var writeOpts *grocksdb.WriteOptions
	if !settings.ReadOnly {
		writeOpts = grocksdb.NewDefaultWriteOptions()
		writeOpts.SetSync(false) // Async writes for performance (WAL handles durability)
	}

	readOpts := grocksdb.NewDefaultReadOptions()

	// Build CF Index Map
	cfIndexMap := make(map[string]int)
	for i, name := range cfNames {
		cfIndexMap[name] = i
	}

	// Create the store instance
	store := &RocksDBTxHashStore{
		db:         db,
		opts:       opts,
		cfHandles:  cfHandles,
		cfOpts:     cfOptsList,
		writeOpts:  writeOpts,
		readOpts:   readOpts,
		blockCache: blockCache,
		path:       path,
		cfIndexMap: cfIndexMap,
		logger:     logger,
		readOnly:   settings.ReadOnly,
		settings:   settings,
	}

	// Log Configuration
	logger.Info("RocksDB store opened successfully in %s:", helpers.FormatDuration(openDuration))
	logger.Info("  Mode:            %s", map[bool]string{true: "READ-ONLY", false: "READ-WRITE"}[settings.ReadOnly])
	logger.Info("  Column Families: %d", len(cfHandles)-1) // -1 for default
	logger.Info("")
	logger.Info("  ROCKSDB SETTINGS:")
	logger.Info("    MaxBackgroundJobs:    %d", settings.MaxBackgroundJobs)
	logger.Info("    MaxOpenFiles:         %d", settings.MaxOpenFiles)
	logger.Info("    TargetFileSizeMB:     %d", settings.TargetFileSizeMB)
	logger.Info("    BloomFilterBitsPerKey: %d", settings.BloomFilterBitsPerKey)
	logger.Info("    BlockCacheSizeMB:     %d", settings.BlockCacheSizeMB)
	if !settings.ReadOnly {
		logger.Info("    WriteBufferSizeMB:    %d", settings.WriteBufferSizeMB)
		logger.Info("    MaxWriteBufferNumber: %d", settings.MaxWriteBufferNumber)
		logger.Info("    MinWriteBufferNumberToMerge: %d", settings.MinWriteBufferNumberToMerge)
		memtables := settings.WriteBufferSizeMB * settings.MaxWriteBufferNumber * len(cf.Names)
		logger.Info("    Total MemTable RAM:   %d MB (%d MB × %d buffers × %d CFs)",
			memtables, settings.WriteBufferSizeMB, settings.MaxWriteBufferNumber, len(cf.Names))
		logger.Info("    WAL:                  %s", map[bool]string{true: "DISABLED", false: "ENABLED"}[settings.DisableWAL])
		logger.Info("    Auto-Compaction:      DISABLED (manual phase)")
	}

	// Log detailed store stats (SST files, WAL size, etc.)
	store.logStoreStats(openDuration)

	return store, nil
}

// createCFOptions creates options for a data column family.
func createCFOptions(settings *types.RocksDBSettings, blockCache *grocksdb.Cache) *grocksdb.Options {
	opts := grocksdb.NewDefaultOptions()

	// MemTable Configuration
	opts.SetWriteBufferSize(uint64(settings.WriteBufferSizeMB * types.MB))
	opts.SetMaxWriteBufferNumber(settings.MaxWriteBufferNumber)
	opts.SetMinWriteBufferNumberToMerge(settings.MinWriteBufferNumberToMerge)

	// Compaction Configuration - CRITICAL: Disable auto-compaction during ingestion
	opts.SetDisableAutoCompactions(true)
	opts.SetCompactionStyle(grocksdb.LevelCompactionStyle)
	opts.SetTargetFileSizeBase(uint64(settings.TargetFileSizeMB * types.MB))
	opts.SetTargetFileSizeMultiplier(1)

	// These settings apply during explicit compaction
	opts.SetMaxBytesForLevelBase(uint64(settings.TargetFileSizeMB * types.MB * 10))
	opts.SetMaxBytesForLevelMultiplier(10)

	// Compression: No compression - values are only 4 bytes
	opts.SetCompression(grocksdb.NoCompression)

	// Block-Based Table Options
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()

	// Bloom filter for fast negative lookups
	if settings.BloomFilterBitsPerKey > 0 {
		bbto.SetFilterPolicy(grocksdb.NewBloomFilter(float64(settings.BloomFilterBitsPerKey)))
	}

	// Shared block cache
	if blockCache != nil {
		bbto.SetBlockCache(blockCache)
	}

	opts.SetBlockBasedTableFactory(bbto)

	return opts
}

// =============================================================================
// TxHashStore Interface Implementation
// =============================================================================

// WriteBatch writes entries to column families without holding a lock, returning timing info.
func (s *RocksDBTxHashStore) WriteBatch(entriesByCF map[string][]types.Entry) (map[string]time.Duration, error) {
	if s.readOnly {
		panic("WriteBatch called on read-only store")
	}

	timings := make(map[string]time.Duration)

	batchStart := time.Now()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for cfName, entries := range entriesByCF {
		if len(entries) == 0 {
			continue
		}
		cfHandle := s.getCFHandleByName(cfName)
		for _, entry := range entries {
			batch.PutCF(cfHandle, entry.Key, entry.Value)
		}
	}
	timings["batch_build"] = time.Since(batchStart)

	writeStart := time.Now()
	wo := grocksdb.NewDefaultWriteOptions()
	wo.SetSync(false)
	wo.DisableWAL(s.settings.DisableWAL)
	defer wo.Destroy()

	if err := s.db.Write(wo, batch); err != nil {
		return timings, err
	}
	timings["total"] = time.Since(writeStart)

	return timings, nil
}

// Get retrieves the ledger sequence for a transaction hash.
func (s *RocksDBTxHashStore) Get(txHash []byte) (value []byte, found bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cfHandle := s.getCFHandle(txHash)

	slice, err := s.db.GetCF(s.readOpts, cfHandle, txHash)
	if err != nil {
		return nil, false, err
	}
	defer slice.Free()

	if !slice.Exists() {
		return nil, false, nil
	}

	// Make a copy since slice data is invalidated after Free()
	value = make([]byte, slice.Size())
	copy(value, slice.Data())
	return value, true, nil
}

// NewIteratorCF creates a new iterator for a specific column family.
func (s *RocksDBTxHashStore) NewIteratorCF(cfName string) interfaces.Iterator {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cfHandle := s.getCFHandleByName(cfName)
	iter := s.db.NewIteratorCF(s.readOpts, cfHandle)

	return &rocksDBIterator{iter: iter}
}

// NewScanIteratorCF creates a scan-optimized iterator for a specific column family.
//
// This iterator is optimized for full sequential scans (e.g., counting entries):
//   - 2MB readahead buffer for prefetching sequential reads
//   - Does not fill block cache (avoids cache pollution during scans)
//   - Auto-tunes readahead size based on access patterns
//
// Use this for operations that iterate through all entries in a CF, such as
// count verification after compaction. For point lookups or small range scans,
// use NewIteratorCF() instead.
func (s *RocksDBTxHashStore) NewScanIteratorCF(cfName string) interfaces.Iterator {
	s.mu.RLock()
	cfHandle := s.getCFHandleByName(cfName)
	s.mu.RUnlock()

	// Create scan-optimized read options
	scanOpts := grocksdb.NewDefaultReadOptions()
	scanOpts.SetReadaheadSize(2 * 1024 * 1024) // 2MB readahead for sequential access
	scanOpts.SetFillCache(false)               // Don't pollute block cache with scan data

	iter := s.db.NewIteratorCF(scanOpts, cfHandle)

	return &rocksDBScanIterator{
		iter:     iter,
		scanOpts: scanOpts, // Must destroy when iterator closes
	}
}

// FlushAll flushes all column family MemTables to SST files on disk.
// Panics if the store was opened in read-only mode.
func (s *RocksDBTxHashStore) FlushAll() error {
	if s.readOnly {
		panic("FlushAll called on read-only store")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	flushOpts := grocksdb.NewDefaultFlushOptions()
	defer flushOpts.Destroy()
	flushOpts.SetWait(true) // Block until flush completes

	s.logger.Info("Flushing all column families to disk...")

	for i, cfName := range cf.Names {
		cfHandle := s.cfHandles[i+1] // +1 because index 0 is "default"
		if err := s.db.FlushCF(cfHandle, flushOpts); err != nil {
			return fmt.Errorf("failed to flush CF %s: %w", cfName, err)
		}
	}

	s.logger.Info("All column families flushed successfully")
	return nil
}

// CompactAll performs full compaction on all 16 column families in parallel.
//
// NOTE: This method does NOT hold a lock during compaction. RocksDB is thread-safe
// and handles concurrent reads/writes during compaction internally. All 16 CFs are
// compacted in parallel using goroutines, with SetExclusiveManualCompaction(false)
// to allow concurrent manual compactions. Queries continue to work during compaction.
//
// Panics if the store was opened in read-only mode.
func (s *RocksDBTxHashStore) CompactAll() time.Duration {
	if s.readOnly {
		panic("CompactAll called on read-only store")
	}
	// Get all CF handles under read lock (quick operation)
	s.mu.RLock()
	cfHandles := make([]*grocksdb.ColumnFamilyHandle, len(cf.Names))
	for i := range cf.Names {
		cfHandles[i] = s.cfHandles[i+1] // +1 because index 0 is "default"
	}
	s.mu.RUnlock()

	s.logger.Separator()
	s.logger.Info("                    COMPACTING ALL COLUMN FAMILIES (PARALLEL)")
	s.logger.Separator()
	s.logger.Info("")
	s.logger.Info("Starting parallel compaction of all 16 column families...")

	totalStart := time.Now()

	// Compact all CFs in parallel - RocksDB is thread-safe
	var wg sync.WaitGroup
	for i, cfName := range cf.Names {
		wg.Add(1)
		go func(idx int, name string) {
			defer wg.Done()

			// Create options that allow parallel manual compactions
			opts := grocksdb.NewCompactRangeOptions()
			opts.SetExclusiveManualCompaction(false) // Allow concurrent compactions
			defer opts.Destroy()

			start := time.Now()
			s.db.CompactRangeCFOpt(cfHandles[idx], grocksdb.Range{Start: nil, Limit: nil}, opts)
			s.logger.Info("  CF [%s] compacted in %s", name, helpers.FormatDuration(time.Since(start)))
		}(i, cfName)
	}
	wg.Wait()

	totalTime := time.Since(totalStart)
	s.logger.Info("")
	s.logger.Info("All column families compacted in %s (parallel)", helpers.FormatDuration(totalTime))

	return totalTime
}

// CompactCF compacts a single column family by name.
//
// NOTE: This method does NOT hold a lock during compaction. RocksDB is thread-safe
// and handles concurrent reads/writes during compaction internally. Uses
// SetExclusiveManualCompaction(false) to allow concurrent compactions with other CFs.
//
// Panics if the store was opened in read-only mode.
func (s *RocksDBTxHashStore) CompactCF(cfName string) time.Duration {
	if s.readOnly {
		panic("CompactCF called on read-only store")
	}
	// Get CF handle under read lock (quick operation)
	s.mu.RLock()
	cfHandle := s.getCFHandleByName(cfName)
	s.mu.RUnlock()

	// Create options that allow parallel manual compactions
	opts := grocksdb.NewCompactRangeOptions()
	opts.SetExclusiveManualCompaction(false) // Allow concurrent compactions
	defer opts.Destroy()

	// Compaction runs WITHOUT holding any lock - RocksDB is thread-safe
	start := time.Now()
	s.db.CompactRangeCFOpt(cfHandle, grocksdb.Range{Start: nil, Limit: nil}, opts)

	return time.Since(start)
}

// GetAllCFStats returns statistics for all 16 column families.
func (s *RocksDBTxHashStore) GetAllCFStats() []types.CFStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := make([]types.CFStats, len(cf.Names))

	for i, cfName := range cf.Names {
		stats[i] = s.getCFStatsLocked(cfName)
	}

	return stats
}

// GetCFStats returns statistics for a specific column family.
func (s *RocksDBTxHashStore) GetCFStats(cfName string) types.CFStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.getCFStatsLocked(cfName)
}

// getCFStatsLocked returns CF stats (caller must hold lock).
func (s *RocksDBTxHashStore) getCFStatsLocked(cfName string) types.CFStats {
	cfHandle := s.getCFHandleByName(cfName)

	// Get estimated key count
	keyCountStr := s.db.GetPropertyCF("rocksdb.estimate-num-keys", cfHandle)
	var keyCount int64
	fmt.Sscanf(keyCountStr, "%d", &keyCount)

	// Get SST file stats (simplified - could add per-level breakdown)
	sstFileCountStr := s.db.GetPropertyCF("rocksdb.num-files-at-level0", cfHandle)
	var l0Files int64
	fmt.Sscanf(sstFileCountStr, "%d", &l0Files)

	// Get level stats
	levelStats := make([]types.CFLevelStats, 7) // L0-L6
	var totalFiles, totalSize int64

	for level := 0; level <= 6; level++ {
		filesProp := fmt.Sprintf("rocksdb.num-files-at-level%d", level)
		filesStr := s.db.GetPropertyCF(filesProp, cfHandle)
		var files int64
		fmt.Sscanf(filesStr, "%d", &files)

		levelStats[level] = types.CFLevelStats{
			Level:     level,
			FileCount: files,
		}

		totalFiles += files
	}

	// Get total size
	totalSizeStr := s.db.GetPropertyCF("rocksdb.total-sst-files-size", cfHandle)
	fmt.Sscanf(totalSizeStr, "%d", &totalSize)

	return types.CFStats{
		Name:          cfName,
		EstimatedKeys: keyCount,
		TotalSize:     totalSize,
		TotalFiles:    totalFiles,
		LevelStats:    levelStats,
	}
}

// Close releases all resources associated with the store.
func (s *RocksDBTxHashStore) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("Closing RocksDB store...")

	// Destroy write options
	if s.writeOpts != nil {
		s.writeOpts.Destroy()
		s.writeOpts = nil
	}

	// Destroy read options
	if s.readOpts != nil {
		s.readOpts.Destroy()
		s.readOpts = nil
	}

	// Destroy column family handles
	for _, cfHandle := range s.cfHandles {
		if cfHandle != nil {
			cfHandle.Destroy()
		}
	}
	s.cfHandles = nil

	// Close database
	if s.db != nil {
		s.db.Close()
		s.db = nil
	}

	// Destroy database options
	if s.opts != nil {
		s.opts.Destroy()
		s.opts = nil
	}

	// Destroy CF options
	for _, cfOpt := range s.cfOpts {
		if cfOpt != nil {
			cfOpt.Destroy()
		}
	}
	s.cfOpts = nil

	// Destroy block cache
	if s.blockCache != nil {
		s.blockCache.Destroy()
		s.blockCache = nil
	}

	s.logger.Info("RocksDB store closed")
}

// Path returns the filesystem path to the RocksDB store.
func (s *RocksDBTxHashStore) Path() string {
	return s.path
}

// IsReadOnly returns true if the store was opened in read-only mode.
func (s *RocksDBTxHashStore) IsReadOnly() bool {
	return s.readOnly
}

// ============================================================================
// Store Statistics and Recovery Logging
// =============================================================================

// logStoreStats logs detailed RocksDB statistics after opening.
// This is especially useful for diagnosing recovery performance.
func (s *RocksDBTxHashStore) logStoreStats(openDuration time.Duration) {
	s.logger.Info("")
	s.logger.Info("ROCKSDB STORE STATISTICS (after open):")

	// Get database-wide properties
	// Note: Some properties are CF-specific, so we'll aggregate across CFs

	// WAL Statistics (database-wide)
	walFilesNum := s.db.GetProperty("rocksdb.num-live-versions")
	curSizeActiveMemTable := s.db.GetProperty("rocksdb.cur-size-active-mem-table")
	curSizeAllMemTables := s.db.GetProperty("rocksdb.cur-size-all-mem-tables")
	sizeAllMemTables := s.db.GetProperty("rocksdb.size-all-mem-tables")
	numEntriesActiveMemTable := s.db.GetProperty("rocksdb.num-entries-active-mem-table")
	numEntriesImmMemTables := s.db.GetProperty("rocksdb.num-entries-imm-mem-tables")

	s.logger.Info("")
	s.logger.Info("  MEMTABLE STATE:")
	s.logger.Info("    Active MemTable Size:     %s", formatPropertyBytes(curSizeActiveMemTable))
	s.logger.Info("    All MemTables Size:       %s", formatPropertyBytes(curSizeAllMemTables))
	s.logger.Info("    MemTables + Pending Flush: %s", formatPropertyBytes(sizeAllMemTables))
	s.logger.Info("    Active MemTable Entries:  %s", numEntriesActiveMemTable)
	s.logger.Info("    Immutable MemTable Entries: %s", numEntriesImmMemTables)
	s.logger.Info("    Live Versions:            %s", walFilesNum)

	// Aggregate SST stats across all CFs and collect per-CF data
	var totalSSTFiles, totalSSTSize, totalKeys int64
	var totalL0Files int64

	// Per-CF statistics for detailed breakdown
	type cfSSTStats struct {
		name  string
		size  int64
		files int64
		keys  int64
	}
	cfStats := make([]cfSSTStats, len(cf.Names))

	for i, cfName := range cf.Names {
		cfHandle := s.getCFHandleByName(cfName)

		// SST files
		sstFilesSize := s.db.GetPropertyCF("rocksdb.total-sst-files-size", cfHandle)
		numSSTFiles := s.db.GetPropertyCF("rocksdb.num-files-at-level0", cfHandle)
		estimatedKeys := s.db.GetPropertyCF("rocksdb.estimate-num-keys", cfHandle)

		var size, l0Files, keys int64
		fmt.Sscanf(sstFilesSize, "%d", &size)
		fmt.Sscanf(numSSTFiles, "%d", &l0Files)
		fmt.Sscanf(estimatedKeys, "%d", &keys)

		totalSSTSize += size
		totalL0Files += l0Files
		totalKeys += keys

		// Count files at all levels for this CF
		var cfFileCount int64
		for level := 0; level <= 6; level++ {
			numAtLevel := s.db.GetPropertyCF(fmt.Sprintf("rocksdb.num-files-at-level%d", level), cfHandle)
			var n int64
			fmt.Sscanf(numAtLevel, "%d", &n)
			cfFileCount += n
			totalSSTFiles += n
		}

		cfStats[i] = cfSSTStats{
			name:  cfName,
			size:  size,
			files: cfFileCount,
			keys:  keys,
		}
	}

	s.logger.Info("")
	s.logger.Info("  SST FILE STATISTICS (all CFs):")
	s.logger.Info("    Total SST Files:          %s", helpers.FormatNumber(totalSSTFiles))
	s.logger.Info("    L0 Files (unflushed):     %s", helpers.FormatNumber(totalL0Files))
	s.logger.Info("    Total SST Size:           %s", helpers.FormatBytes(totalSSTSize))
	s.logger.Info("    Estimated Total Keys:     %s", helpers.FormatNumber(totalKeys))

	// Log timing analysis
	s.logger.Info("")
	s.logger.Info("  OPEN TIMING ANALYSIS:")
	s.logger.Info("    Total Open Time:          %s", helpers.FormatDuration(openDuration))

	// Estimate if WAL replay occurred
	// WAL replay at ~300 MB/s, so 583 MB WAL would take ~2 seconds
	// If open time is significantly more, there might be other factors
	if openDuration > 5*time.Second {
		s.logger.Info("    NOTE: Long open time may indicate WAL recovery")
		s.logger.Info("          WAL recovery typically processes ~300 MB/s")
		s.logger.Info("          Check the MemTable entries above for recovered data")
	}

	// Log per-CF breakdown with SST sizes if there are any keys
	if totalKeys > 0 {
		s.logger.Info("")
		s.logger.Info("  PER-CF SST BREAKDOWN:")
		s.logger.Info("    %-4s %15s %12s %10s", "CF", "Est. Keys", "SST Size", "Files")
		s.logger.Info("    %-4s %15s %12s %10s", "----", "---------------", "------------", "----------")
		for _, stat := range cfStats {
			if stat.keys > 0 || stat.size > 0 {
				s.logger.Info("    %-4s %15s %12s %10d",
					stat.name,
					helpers.FormatNumber(stat.keys),
					helpers.FormatBytes(stat.size),
					stat.files)
			}
		}
		s.logger.Info("    %-4s %15s %12s %10s", "----", "---------------", "------------", "----------")
		s.logger.Info("    %-4s %15s %12s %10d", "TOT", helpers.FormatNumber(totalKeys), helpers.FormatBytes(totalSSTSize), totalSSTFiles)
	}

	s.logger.Info("")
}

// LogMemTableAndWALStats displays MemTable statistics and actual WAL file sizes from the filesystem.
func (s *RocksDBTxHashStore) LogMemTableAndWALStats(logger interfaces.Logger, rocksdbPath string, label string) {
	logger.Info("")
	logger.Info("================================================================================")
	logger.Info("                    %s", label)
	logger.Info("================================================================================")

	curSizeActiveMemTable := s.db.GetProperty("rocksdb.cur-size-active-mem-table")
	curSizeAllMemTables := s.db.GetProperty("rocksdb.cur-size-all-mem-tables")
	sizeAllMemTables := s.db.GetProperty("rocksdb.size-all-mem-tables")
	numEntriesActiveMemTable := s.db.GetProperty("rocksdb.num-entries-active-mem-table")
	numEntriesImmMemTables := s.db.GetProperty("rocksdb.num-entries-imm-mem-tables")

	logger.Info("")
	logger.Info("  MEMTABLE STATE:")
	logger.Info("    Active MemTable Size:     %s", formatPropertyBytes(curSizeActiveMemTable))
	logger.Info("    All MemTables Size:       %s", formatPropertyBytes(curSizeAllMemTables))
	logger.Info("    MemTables + Pending Flush: %s", formatPropertyBytes(sizeAllMemTables))

	var activeEntries, immEntries int64
	fmt.Sscanf(numEntriesActiveMemTable, "%d", &activeEntries)
	fmt.Sscanf(numEntriesImmMemTables, "%d", &immEntries)

	logger.Info("    Active MemTable Entries:  %s", helpers.FormatNumber(activeEntries))
	logger.Info("    Immutable MemTable Entries: %s", helpers.FormatNumber(immEntries))

	var walSize int64
	var walCount int

	entries, err := os.ReadDir(rocksdbPath)
	if err != nil {
		logger.Error("Failed to read RocksDB directory: %v", err)
		logger.Info("")
		return
	}

	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".log") {
			walCount++
			info, err := os.Stat(filepath.Join(rocksdbPath, entry.Name()))
			if err == nil {
				walSize += info.Size()
			}
		}
	}

	logger.Info("")
	logger.Info("  WAL FILES:")
	logger.Info("    Count:                    %s", helpers.FormatNumber(int64(walCount)))
	logger.Info("    Total Size:               %s", helpers.FormatBytes(walSize))
	logger.Info("")
}

// GetLiveDataSize returns the total size of live data in the store.
// This includes MemTables and SST files.
func (s *RocksDBTxHashStore) GetLiveDataSize() int64 {
	var totalSize int64

	for _, cfName := range cf.Names {
		cfHandle := s.getCFHandleByName(cfName)
		sstFilesSize := s.db.GetPropertyCF("rocksdb.total-sst-files-size", cfHandle)
		var size int64
		fmt.Sscanf(sstFilesSize, "%d", &size)
		totalSize += size
	}

	// Add MemTable size
	memTableSize := s.db.GetProperty("rocksdb.cur-size-all-mem-tables")
	var memSize int64
	fmt.Sscanf(memTableSize, "%d", &memSize)
	totalSize += memSize

	return totalSize
}

// formatPropertyBytes formats a RocksDB property value (string bytes) as human-readable size.
func formatPropertyBytes(propValue string) string {
	if propValue == "" {
		return "N/A"
	}
	var bytes int64
	_, err := fmt.Sscanf(propValue, "%d", &bytes)
	if err != nil {
		return propValue
	}
	return helpers.FormatBytes(bytes)
}

// =============================================================================
// Helper Methods
// =============================================================================

// getCFHandle returns the CF handle for a transaction hash.
func (s *RocksDBTxHashStore) getCFHandle(txHash []byte) *grocksdb.ColumnFamilyHandle {
	cfName := cf.GetName(txHash)
	return s.getCFHandleByName(cfName)
}

// getCFHandleByName returns the CF handle by name.
func (s *RocksDBTxHashStore) getCFHandleByName(cfName string) *grocksdb.ColumnFamilyHandle {
	idx, ok := s.cfIndexMap[cfName]
	if !ok {
		// Fall back to default CF (should never happen for valid cfName)
		return s.cfHandles[0]
	}
	return s.cfHandles[idx]
}

// =============================================================================
// RocksDB Iterator Wrapper
// =============================================================================

// rocksDBIterator wraps grocksdb.Iterator to implement our Iterator interface.
type rocksDBIterator struct {
	iter *grocksdb.Iterator
}

func (it *rocksDBIterator) SeekToFirst() {
	it.iter.SeekToFirst()
}

func (it *rocksDBIterator) Valid() bool {
	return it.iter.Valid()
}

func (it *rocksDBIterator) Next() {
	it.iter.Next()
}

func (it *rocksDBIterator) Key() []byte {
	return it.iter.Key().Data()
}

func (it *rocksDBIterator) Value() []byte {
	return it.iter.Value().Data()
}

func (it *rocksDBIterator) Error() error {
	return it.iter.Err()
}

func (it *rocksDBIterator) Close() {
	it.iter.Close()
}

// =============================================================================
// RocksDB Scan Iterator Wrapper (optimized for sequential scans)
// =============================================================================

// rocksDBScanIterator wraps grocksdb.Iterator with scan-optimized ReadOptions.
// Unlike rocksDBIterator, this also owns and destroys the ReadOptions on Close().
type rocksDBScanIterator struct {
	iter     *grocksdb.Iterator
	scanOpts *grocksdb.ReadOptions
}

func (it *rocksDBScanIterator) SeekToFirst() {
	it.iter.SeekToFirst()
}

func (it *rocksDBScanIterator) Valid() bool {
	return it.iter.Valid()
}

func (it *rocksDBScanIterator) Next() {
	it.iter.Next()
}

func (it *rocksDBScanIterator) Key() []byte {
	return it.iter.Key().Data()
}

func (it *rocksDBScanIterator) Value() []byte {
	return it.iter.Value().Data()
}

func (it *rocksDBScanIterator) Error() error {
	return it.iter.Err()
}

func (it *rocksDBScanIterator) Close() {
	it.iter.Close()
	if it.scanOpts != nil {
		it.scanOpts.Destroy()
	}
}

// Compile-Time Interface Checks
var _ interfaces.TxHashStore = (*RocksDBTxHashStore)(nil)
var _ interfaces.Iterator = (*rocksDBIterator)(nil)
var _ interfaces.Iterator = (*rocksDBScanIterator)(nil)

// =============================================================================
// Utility Functions
// =============================================================================

// LogAllCFStats logs statistics for all column families.
func LogAllCFStats(store interfaces.TxHashStore, logger interfaces.Logger, label string) {
	stats := store.GetAllCFStats()

	logger.Separator()
	logger.Info("                    %s", label)
	logger.Separator()
	logger.Info("")

	var totalKeys, totalSize, totalFiles int64

	logger.Info("%-4s %15s %15s %10s", "CF", "Est. Keys", "Size", "Files")
	logger.Info("%-4s %15s %15s %10s", "----", "---------------", "---------------", "----------")

	for _, cfStats := range stats {
		logger.Info("%-4s %15s %15s %10d",
			cfStats.Name,
			helpers.FormatNumber(cfStats.EstimatedKeys),
			helpers.FormatBytes(cfStats.TotalSize),
			cfStats.TotalFiles)

		totalKeys += cfStats.EstimatedKeys
		totalSize += cfStats.TotalSize
		totalFiles += cfStats.TotalFiles
	}

	logger.Info("%-4s %15s %15s %10s", "----", "---------------", "---------------", "----------")
	logger.Info("%-4s %15s %15s %10d", "TOT", helpers.FormatNumber(totalKeys), helpers.FormatBytes(totalSize), totalFiles)
	logger.Info("")
}

// LogCFLevelStats logs the level distribution for all column families.
func LogCFLevelStats(store interfaces.TxHashStore, logger interfaces.Logger) {
	stats := store.GetAllCFStats()

	logger.Info("LEVEL DISTRIBUTION BY COLUMN FAMILY:")
	logger.Info("")

	// Header
	header := "CF  "
	for level := 0; level <= 6; level++ {
		header += fmt.Sprintf("  L%d", level)
	}
	logger.Info("%s", header)
	logger.Info("--- " + "-----" + "-----" + "-----" + "-----" + "-----" + "-----" + "-----")

	for _, cfStats := range stats {
		line := fmt.Sprintf("%-3s ", cfStats.Name)
		for _, ls := range cfStats.LevelStats {
			line += fmt.Sprintf("%5d", ls.FileCount)
		}
		logger.Info("%s", line)
	}
	logger.Info("")
}
