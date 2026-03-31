// =============================================================================
// meta_store.go - Checkpoint and Progress Tracking Store
// =============================================================================
//
// This file implements the MetaStore interface using a separate RocksDB instance
// for storing workflow state and progress information.
//
// PURPOSE:
//
//	The meta store enables crash recovery by persisting:
//	  - Configuration (start/end ledger)
//	  - Current workflow phase
//	  - Last committed ledger
//	  - Per-CF entry counts
//	  - Verification progress
//
// ATOMICITY GUARANTEES:
//
//	CommitBatchProgress() updates all progress fields atomically using a
//	RocksDB WriteBatch. This ensures that last_committed_ledger and cf_counts
//	are always consistent after a crash.
//
// STORAGE FORMAT:
//
//	Keys are string constants, values are serialized as follows:
//	  - uint32 values: 4-byte big-endian
//	  - uint64 values: 8-byte big-endian
//	  - strings: UTF-8 bytes
//	  - cf_counts: serialized as "cf:count,cf:count,..."
//
// CRASH RECOVERY SCENARIOS:
//
//	1. Crash during ingestion (before CommitBatchProgress):
//	   - Resume from last_committed_ledger + 1
//	   - Re-ingest the incomplete batch (up to 999 ledgers)
//	   - Duplicates are handled by RocksDB (compaction removes them)
//
//	2. Crash after CommitBatchProgress:
//	   - Resume from last_committed_ledger + 1
//	   - No data loss
//
//	3. Crash during compaction:
//	   - Restart compaction for all CFs
//	   - Compaction is idempotent
//
//	4. Crash during RecSplit building:
//	   - Delete all temp and index files
//	   - Rebuild from scratch
//
//	5. Crash during verification:
//	   - Resume from the CF stored in verify_cf
//	   - Restart that CF from the beginning
//
// =============================================================================

package main

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/stellar/stellar-rpc/full-history/all-code/helpers"
	"github.com/stellar/stellar-rpc/full-history/all-code/parked-code/txhash-ingestion-workflow/pkg/cf"
	"github.com/stellar/stellar-rpc/full-history/all-code/parked-code/txhash-ingestion-workflow/pkg/interfaces"
	"github.com/stellar/stellar-rpc/full-history/all-code/parked-code/txhash-ingestion-workflow/pkg/types"
	"github.com/linxGnu/grocksdb"
)

// =============================================================================
// Key Constants
// =============================================================================

const (
	// metaKeyStartLedger stores the configured start ledger
	metaKeyStartLedger = "config:start_ledger"

	// metaKeyEndLedger stores the configured end ledger
	metaKeyEndLedger = "config:end_ledger"

	// metaKeyPhase stores the current workflow phase
	metaKeyPhase = "state:phase"

	// metaKeyLastCommittedLedger stores the last fully committed ledger
	metaKeyLastCommittedLedger = "state:last_committed_ledger"

	// metaKeyCFCounts stores the per-CF entry counts
	metaKeyCFCounts = "state:cf_counts"

	// metaKeyVerifyCF stores the CF currently being verified
	metaKeyVerifyCF = "state:verify_cf"

	// metaKeyInitialized marks the store as initialized
	metaKeyInitialized = "meta:initialized"
)

// =============================================================================
// RocksDBMetaStore Implementation
// =============================================================================

// RocksDBMetaStore implements the MetaStore interface using RocksDB.
//
// This is a separate RocksDB instance from the main data store, located at:
//
//	<output-dir>/txHash-ledgerSeq/meta/
//
// DESIGN DECISIONS:
//
//  1. Separate DB: Keeps meta data isolated from main data store
//  2. Single CF: Simple key-value storage (no column families needed)
//  3. WAL enabled: Critical for crash recovery correctness
//  4. Small footprint: Only stores a few KB of data
type RocksDBMetaStore struct {
	mu sync.Mutex

	// db is the RocksDB instance
	db *grocksdb.DB

	// opts is the database options
	opts *grocksdb.Options

	// writeOpts is the write options
	writeOpts *grocksdb.WriteOptions

	// readOpts is the read options
	readOpts *grocksdb.ReadOptions

	// path is the filesystem path
	path string
}

// OpenRocksDBMetaStore opens or creates the meta store.
//
// PARAMETERS:
//   - path: Filesystem path for the meta store
//
// RETURNS:
//   - A new RocksDBMetaStore instance
//   - An error if opening/creation fails
func OpenRocksDBMetaStore(path string) (*RocksDBMetaStore, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetErrorIfExists(false)

	// Minimal settings for meta store (small data, simple access pattern)
	opts.SetWriteBufferSize(4 * types.MB)
	opts.SetMaxWriteBufferNumber(2)
	opts.SetMaxOpenFiles(10)

	// Reduce logging noise
	opts.SetInfoLogLevel(grocksdb.ErrorInfoLogLevel)

	db, err := grocksdb.OpenDb(opts, path)
	if err != nil {
		opts.Destroy()
		return nil, fmt.Errorf("failed to open meta store at %s: %w", path, err)
	}

	writeOpts := grocksdb.NewDefaultWriteOptions()
	writeOpts.SetSync(true) // Sync writes for durability

	readOpts := grocksdb.NewDefaultReadOptions()

	return &RocksDBMetaStore{
		db:        db,
		opts:      opts,
		writeOpts: writeOpts,
		readOpts:  readOpts,
		path:      path,
	}, nil
}

// =============================================================================
// Configuration Methods
// =============================================================================

// GetStartLedger returns the configured start ledger.
// Returns 0 if not set.
func (m *RocksDBMetaStore) GetStartLedger() (uint32, error) {
	return m.getUint32(metaKeyStartLedger)
}

// GetEndLedger returns the configured end ledger.
// Returns 0 if not set.
func (m *RocksDBMetaStore) GetEndLedger() (uint32, error) {
	return m.getUint32(metaKeyEndLedger)
}

// SetConfig stores the start and end ledger configuration.
//
// This is called once at the beginning of a fresh ingestion.
// If config already exists and differs, returns an error.
func (m *RocksDBMetaStore) SetConfig(startLedger, endLedger uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if config already exists
	existingStart, _ := m.getUint32Locked(metaKeyStartLedger)
	existingEnd, _ := m.getUint32Locked(metaKeyEndLedger)

	if existingStart != 0 || existingEnd != 0 {
		// Config exists, check for mismatch
		if existingStart != startLedger || existingEnd != endLedger {
			return fmt.Errorf(
				"configuration mismatch: stored config has start=%d end=%d, "+
					"but command line has start=%d end=%d; "+
					"cannot resume with different parameters",
				existingStart, existingEnd, startLedger, endLedger)
		}
		// Config matches, nothing to do
		return nil
	}

	// Store new config
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	batch.Put([]byte(metaKeyStartLedger), encodeUint32(startLedger))
	batch.Put([]byte(metaKeyEndLedger), encodeUint32(endLedger))
	batch.Put([]byte(metaKeyInitialized), []byte("true"))
	batch.Put([]byte(metaKeyPhase), []byte(types.PhaseIngesting))

	return m.db.Write(m.writeOpts, batch)
}

// =============================================================================
// Phase Tracking Methods
// =============================================================================

// GetPhase returns the current workflow phase.
// Returns PhaseIngesting if not set.
func (m *RocksDBMetaStore) GetPhase() (types.Phase, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	value, err := m.getStringLocked(metaKeyPhase)
	if err != nil {
		return types.PhaseIngesting, err
	}
	if value == "" {
		return types.PhaseIngesting, nil
	}

	return types.Phase(value), nil
}

// SetPhase updates the current workflow phase.
func (m *RocksDBMetaStore) SetPhase(phase types.Phase) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.db.Put(m.writeOpts, []byte(metaKeyPhase), []byte(phase.String()))
}

// =============================================================================
// Ingestion Progress Methods
// =============================================================================

// GetLastCommittedLedger returns the last fully committed ledger sequence.
// Returns 0 if no batches have been committed yet.
func (m *RocksDBMetaStore) GetLastCommittedLedger() (uint32, error) {
	return m.getUint32(metaKeyLastCommittedLedger)
}

// GetCFCounts returns the entry count for each column family.
// Returns empty map if no batches have been committed yet.
func (m *RocksDBMetaStore) GetCFCounts() (map[string]uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	value, err := m.getStringLocked(metaKeyCFCounts)
	if err != nil {
		return nil, err
	}

	if value == "" {
		// Return empty counts
		counts := make(map[string]uint64)
		for _, cfName := range cf.Names {
			counts[cfName] = 0
		}
		return counts, nil
	}

	return parseCFCounts(value), nil
}

// CommitBatchProgress atomically updates progress after a successful batch.
//
// ATOMICITY:
//
//	All updates are written in a single RocksDB WriteBatch.
//	Either all updates succeed or none do.
//
// CRASH RECOVERY GUARANTEE:
//
//	After a crash, last_committed_ledger and cf_counts are always consistent.
//	They represent the state after the last fully-committed batch.
func (m *RocksDBMetaStore) CommitBatchProgress(lastLedger uint32, cfCounts map[string]uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	// Update last committed ledger
	batch.Put([]byte(metaKeyLastCommittedLedger), encodeUint32(lastLedger))

	// Update CF counts
	batch.Put([]byte(metaKeyCFCounts), []byte(serializeCFCounts(cfCounts)))

	return m.db.Write(m.writeOpts, batch)
}

// =============================================================================
// Verification Progress Methods
// =============================================================================

// GetVerifyCF returns the column family currently being verified.
// Returns empty string if verification hasn't started.
func (m *RocksDBMetaStore) GetVerifyCF() (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.getStringLocked(metaKeyVerifyCF)
}

// SetVerifyCF updates the column family currently being verified.
func (m *RocksDBMetaStore) SetVerifyCF(cf string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.db.Put(m.writeOpts, []byte(metaKeyVerifyCF), []byte(cf))
}

// =============================================================================
// Utility Methods
// =============================================================================

// Exists returns true if the meta store has been initialized.
func (m *RocksDBMetaStore) Exists() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	value, _ := m.getStringLocked(metaKeyInitialized)
	return value == "true"
}

// LogState logs the current meta store state for debugging/monitoring.
func (m *RocksDBMetaStore) LogState(logger interfaces.Logger) {
	m.mu.Lock()
	defer m.mu.Unlock()

	startLedger, _ := m.getUint32Locked(metaKeyStartLedger)
	endLedger, _ := m.getUint32Locked(metaKeyEndLedger)
	phase, _ := m.getStringLocked(metaKeyPhase)
	lastCommitted, _ := m.getUint32Locked(metaKeyLastCommittedLedger)
	cfCountsStr, _ := m.getStringLocked(metaKeyCFCounts)
	verifyCF, _ := m.getStringLocked(metaKeyVerifyCF)

	logger.Info("")
	logger.Info("META STORE STATE:")
	logger.Info("  Path:                  %s", m.path)
	logger.Info("  Start Ledger:          %d", startLedger)
	logger.Info("  End Ledger:            %d", endLedger)
	logger.Info("  Phase:                 %s", phase)
	logger.Info("  Last Committed Ledger: %d", lastCommitted)
	logger.Info("  Verify CF:             %s", verifyCF)

	if cfCountsStr != "" {
		cfCounts := parseCFCounts(cfCountsStr)
		var total uint64
		for _, count := range cfCounts {
			total += count
		}
		logger.Info("  Total CF Counts:       %s", helpers.FormatNumber(int64(total)))

		// Show breakdown if requested (verbose)
		logger.Info("  CF Counts:")
		for _, cfName := range cf.Names {
			logger.Info("    %s: %s", cfName, helpers.FormatNumber(int64(cfCounts[cfName])))
		}
	}
	logger.Info("")
}

// Close releases all resources associated with the meta store.
func (m *RocksDBMetaStore) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.writeOpts != nil {
		m.writeOpts.Destroy()
		m.writeOpts = nil
	}

	if m.readOpts != nil {
		m.readOpts.Destroy()
		m.readOpts = nil
	}

	if m.db != nil {
		m.db.Close()
		m.db = nil
	}

	if m.opts != nil {
		m.opts.Destroy()
		m.opts = nil
	}
}

// =============================================================================
// Internal Helper Methods
// =============================================================================

// getUint32 reads a uint32 value from the store.
func (m *RocksDBMetaStore) getUint32(key string) (uint32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.getUint32Locked(key)
}

// getUint32Locked reads a uint32 value (caller must hold lock).
func (m *RocksDBMetaStore) getUint32Locked(key string) (uint32, error) {
	slice, err := m.db.Get(m.readOpts, []byte(key))
	if err != nil {
		return 0, err
	}
	defer slice.Free()

	if !slice.Exists() || slice.Size() != 4 {
		return 0, nil
	}

	return binary.BigEndian.Uint32(slice.Data()), nil
}

// getStringLocked reads a string value (caller must hold lock).
func (m *RocksDBMetaStore) getStringLocked(key string) (string, error) {
	slice, err := m.db.Get(m.readOpts, []byte(key))
	if err != nil {
		return "", err
	}
	defer slice.Free()

	if !slice.Exists() {
		return "", nil
	}

	return string(slice.Data()), nil
}

// encodeUint32 encodes a uint32 as big-endian bytes.
func encodeUint32(v uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, v)
	return buf
}

// encodeUint64 encodes a uint64 as big-endian bytes.
func encodeUint64(v uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return buf
}

// =============================================================================
// CF Counts Serialization
// =============================================================================

// serializeCFCounts serializes CF counts to a string.
// Format: "cf:count,cf:count,..."
// Example: "0:125000,1:123500,2:124000,...,f:126000"
func serializeCFCounts(counts map[string]uint64) string {
	var parts []string
	for _, cfName := range cf.Names {
		count := counts[cfName]
		parts = append(parts, fmt.Sprintf("%s:%d", cfName, count))
	}
	return strings.Join(parts, ",")
}

// parseCFCounts parses CF counts from a string.
func parseCFCounts(s string) map[string]uint64 {
	counts := make(map[string]uint64)

	// Initialize with zeros
	for _, cfName := range cf.Names {
		counts[cfName] = 0
	}

	if s == "" {
		return counts
	}

	parts := strings.Split(s, ",")
	for _, part := range parts {
		kv := strings.SplitN(part, ":", 2)
		if len(kv) != 2 {
			continue
		}
		cf := kv[0]
		count, err := strconv.ParseUint(kv[1], 10, 64)
		if err != nil {
			continue
		}
		counts[cf] = count
	}

	return counts
}

// =============================================================================
// Compile-Time Interface Check
// =============================================================================

var _ interfaces.MetaStore = (*RocksDBMetaStore)(nil)

// =============================================================================
// Resume Helper Functions
// =============================================================================

// CheckResumability checks if the workflow can be resumed.
//
// Returns:
//   - canResume: true if this is a resume (meta store exists)
//   - startFrom: the ledger to start from (last_committed + 1 or config start)
//   - phase: the phase to resume from
//   - error: if there's a configuration mismatch or other issue
func CheckResumability(meta interfaces.MetaStore, configStart, configEnd uint32) (canResume bool, startFrom uint32, phase types.Phase, err error) {
	if !meta.Exists() {
		// Fresh start
		return false, configStart, types.PhaseIngesting, nil
	}

	// Check config match
	storedStart, err := meta.GetStartLedger()
	if err != nil {
		return false, 0, "", fmt.Errorf("failed to read stored start ledger: %w", err)
	}

	storedEnd, err := meta.GetEndLedger()
	if err != nil {
		return false, 0, "", fmt.Errorf("failed to read stored end ledger: %w", err)
	}

	if storedStart != configStart || storedEnd != configEnd {
		return false, 0, "", fmt.Errorf(
			"configuration mismatch: stored config has start=%d end=%d, "+
				"but command line has start=%d end=%d; "+
				"cannot resume with different parameters",
			storedStart, storedEnd, configStart, configEnd)
	}

	// Get current phase
	phase, err = meta.GetPhase()
	if err != nil {
		return false, 0, "", fmt.Errorf("failed to read phase: %w", err)
	}

	// Determine start point based on phase
	switch phase {
	case types.PhaseIngesting:
		lastCommitted, err := meta.GetLastCommittedLedger()
		if err != nil {
			return false, 0, "", fmt.Errorf("failed to read last committed ledger: %w", err)
		}
		if lastCommitted == 0 {
			startFrom = configStart
		} else {
			startFrom = lastCommitted + 1
		}

	case types.PhaseCompacting, types.PhaseBuildingRecsplit, types.PhaseVerifyingRecsplit:
		// These phases restart from the beginning of the phase
		startFrom = 0 // Not applicable for these phases

	case types.PhaseComplete:
		// Already done
		startFrom = 0
	}

	return true, startFrom, phase, nil
}

// LogResumeState logs information about resuming a workflow.
// This provides detailed information about the checkpoint state for debugging.
func LogResumeState(meta interfaces.MetaStore, logger interfaces.Logger, resumeFrom uint32, phase types.Phase) {
	logger.Separator()
	logger.Info("                        RESUMING WORKFLOW")
	logger.Separator()
	logger.Info("")
	logger.Info("Previous run detected. Resuming from checkpoint.")
	logger.Info("")
	logger.Info("  Resume Phase:          %s", phase)

	// Get stored configuration
	storedStart, _ := meta.GetStartLedger()
	storedEnd, _ := meta.GetEndLedger()

	logger.Info("")
	logger.Info("STORED CONFIGURATION:")
	logger.Info("  Start Ledger:          %s", helpers.FormatNumber(int64(storedStart)))
	logger.Info("  End Ledger:            %s", helpers.FormatNumber(int64(storedEnd)))
	logger.Info("  Total Ledgers:         %s", helpers.FormatNumber(int64(storedEnd-storedStart+1)))

	switch phase {
	case types.PhaseIngesting:
		logger.Info("")
		logger.Info("INGESTION PROGRESS:")
		logger.Info("  Resume From Ledger:    %s", helpers.FormatNumber(int64(resumeFrom)))
		lastCommitted, _ := meta.GetLastCommittedLedger()
		if lastCommitted > 0 {
			cfCounts, _ := meta.GetCFCounts()
			var total uint64
			for _, count := range cfCounts {
				total += count
			}
			logger.Info("  Last Committed:        %s", helpers.FormatNumber(int64(lastCommitted)))
			logger.Info("  Ledgers Processed:     %s", helpers.FormatNumber(int64(lastCommitted-storedStart+1)))
			logger.Info("  Ledgers Remaining:     %s", helpers.FormatNumber(int64(storedEnd-lastCommitted)))
			percentDone := float64(lastCommitted-storedStart+1) / float64(storedEnd-storedStart+1) * 100
			logger.Info("  Progress:              %.1f%%", percentDone)
			logger.Info("  Entries Ingested:      %s", helpers.FormatNumber(int64(total)))

			// Show per-CF breakdown
			logger.Info("")
			logger.Info("PER-CF ENTRY COUNTS (from checkpoint):")
			for _, cfName := range cf.Names {
				count := cfCounts[cfName]
				logger.Info("  CF [%s]: %s", cfName, helpers.FormatNumber(int64(count)))
			}
		} else {
			logger.Info("  Last Committed:        (none - starting fresh)")
		}

	case types.PhaseCompacting:
		logger.Info("")
		logger.Info("COMPACTION RECOVERY:")
		logger.Info("  Action:                Restart compaction for all 16 CFs")
		logger.Info("  Note:                  Compaction is idempotent, safe to restart")

		// Show CF counts that will be compacted
		cfCounts, _ := meta.GetCFCounts()
		var total uint64
		for _, count := range cfCounts {
			total += count
		}
		logger.Info("  Total Entries:         %s", helpers.FormatNumber(int64(total)))

	case types.PhaseBuildingRecsplit:
		logger.Info("")
		logger.Info("RECSPLIT RECOVERY:")
		logger.Info("  Action:                Delete temp/index files, rebuild from scratch")
		logger.Info("  Note:                  RecSplit building is not resumable mid-build")

		cfCounts, _ := meta.GetCFCounts()
		var total uint64
		for _, count := range cfCounts {
			total += count
		}
		logger.Info("  Total Keys to Index:   %s", helpers.FormatNumber(int64(total)))

	case types.PhaseVerifyingRecsplit:
		logger.Info("")
		logger.Info("VERIFICATION RECOVERY:")
		verifyCF, _ := meta.GetVerifyCF()
		if verifyCF != "" {
			logger.Info("  Resume From CF:        %s", verifyCF)
		}
		logger.Info("  Action:                Re-run verification for all 16 CFs")
		logger.Info("  Note:                  Verification is idempotent")

	case types.PhaseComplete:
		logger.Info("")
		logger.Info("STATUS:")
		logger.Info("  Status:                Already complete")
		logger.Info("  Action:                No work needed")
	}

	logger.Info("")
}
