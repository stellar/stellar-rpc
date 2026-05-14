package rocksdb

// Tuning — per-store RocksDB knobs. Zero means "leave grocksdb's
// default alone" (wrapper skips the setter). BloomFilterBitsPerKey == 0
// is the documented "install no bloom filter" sentinel.
//
// Wrapper-pinned values not exposed here (applied to every facade):
// MinWriteBufferNumberToMerge=1, CompactionStyle=Level,
// TargetFileSizeMultiplier=1, MaxBytesForLevelMultiplier=10,
// Compression=None, WAL=on, per-write Sync=on.
type Tuning struct {
	// WriteBufferMB sizes the active memtable per CF, in MB.
	WriteBufferMB int

	// MaxWriteBufferNumber caps the active + immutable memtable count
	// per CF before writes back-pressure.
	MaxWriteBufferNumber int

	// Level0FileNumCompactionTrigger — L0 file count that starts
	// an L0→L1 compaction.
	Level0FileNumCompactionTrigger int

	// Level0SlowdownWritesTrigger — L0 file count that slows writes.
	Level0SlowdownWritesTrigger int

	// Level0StopWritesTrigger — L0 file count that stalls writes
	// entirely.
	Level0StopWritesTrigger int

	// DisableAutoCompactions turns automatic compaction off per CF —
	// for write-once, point-lookup stores where compaction would
	// rewrite the same data with no reordering benefit.
	DisableAutoCompactions bool

	// TargetFileSizeMB — size at which compaction produces new SSTs.
	TargetFileSizeMB int

	// MaxBytesForLevelBaseMB — byte budget for level 1; each later
	// level is this × MaxBytesForLevelMultiplier (10, pinned).
	MaxBytesForLevelBaseMB int

	// MaxBackgroundJobs caps background threads for compactions
	// and flushes combined. Orthogonal to DisableAutoCompactions:
	// this rate-limits work, that turns compaction off entirely.
	MaxBackgroundJobs int

	// MaxOpenFiles caps concurrent open SST files.
	MaxOpenFiles int

	// BlockCacheMB sizes the shared LRU block cache, per store.
	BlockCacheMB int

	// BloomFilterBitsPerKey — per-CF bloom filter. 0 = no filter
	// installed; positive values install one with that many bits.
	// Typical: 10 (~1% false positive), 12 (~0.4%).
	BloomFilterBitsPerKey int

	// MaxTotalWalSizeMB caps total live WAL size. Crash-recovery
	// replay scales with this cap; graceful Close drains the
	// memtable so this only bounds ungraceful shutdowns.
	MaxTotalWalSizeMB int
}
