package rocksdb

import "github.com/linxGnu/grocksdb"

// CFOptions — per-CF overrides applied after the shared pinned
// defaults and global Tuning. Zero means "inherit the pinned
// default" (NoCompression for Compression, RocksDB's BBTO default
// block size for BlockSize). Use to opt specific CFs into
// compression or non-default block sizes while leaving the rest at
// the wrapper-pinned defaults.
type CFOptions struct {
	// Compression overrides the CF's compression type. The wrapper's
	// pinned default is NoCompression; CFs that hold compressible
	// payloads (e.g. XDR events) should opt into ZSTDCompression here.
	// A zero value (NoCompression) leaves the pinned default in
	// place — semantically a no-op since the pinned default is also
	// NoCompression.
	Compression grocksdb.CompressionType

	// BlockSize overrides the per-CF SST block size in bytes. Zero
	// means "leave RocksDB's BBTO default (16 KiB)". Small-value CFs
	// (sparse-key indexes, dense-key offset maps) benefit from
	// smaller blocks because random Get only needs the block holding
	// the target key; larger blocks waste I/O per cache miss.
	BlockSize int
}

// Tuning — per-store RocksDB knobs. Zero means "leave grocksdb's
// default alone" (wrapper skips the setter). BloomFilterBitsPerKey == 0
// is the documented "install no bloom filter" sentinel.
//
// Wrapper-pinned values not exposed here (applied to every facade):
// MinWriteBufferNumberToMerge=1, CompactionStyle=Level,
// TargetFileSizeMultiplier=1, MaxBytesForLevelMultiplier=10,
// Compression=None, WAL=on, per-write Sync=on.
// Per-CF overrides for Compression and BlockSize live in
// Config.PerCFOptions.
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
