package rocksdb

import "github.com/linxGnu/grocksdb"

// CFOptions — per-CF overrides applied after the shared pinned defaults.
// Zero means "inherit the default" for every field (NoCompression, RocksDB's
// BBTO block size, no bloom, grocksdb's memtable/compaction defaults). The
// knobs are per-CF because each hot workload differs — one CF's tuning must
// not leak onto the others.
type CFOptions struct {
	// Compression overrides the CF's compression type. The wrapper's
	// pinned default is NoCompression; CFs that hold compressible
	// payloads (e.g. XDR events) should opt into ZSTDCompression here.
	// A zero value (NoCompression) leaves the pinned default in
	// place — semantically a no-op since the pinned default is also
	// NoCompression.
	Compression grocksdb.CompressionType

	// BlockSize overrides the per-CF SST block size in bytes. Zero
	// means "leave RocksDB's BBTO default (4 KiB)" — so an explicit
	// 4-KiB value pins the default rather than changing it.
	// Small-value CFs (sparse-key indexes, dense-key offset maps)
	// benefit from smaller blocks because random Get only needs the
	// block holding the target key; larger blocks waste I/O per
	// cache miss.
	BlockSize int

	// WriteBufferMB sizes the active memtable for this CF, in MB.
	WriteBufferMB int

	// MaxWriteBufferNumber caps the active + immutable memtable count
	// for this CF before writes back-pressure.
	MaxWriteBufferNumber int

	// Level0FileNumCompactionTrigger — L0 file count that starts
	// an L0→L1 compaction.
	Level0FileNumCompactionTrigger int

	// Level0SlowdownWritesTrigger — L0 file count that slows writes.
	Level0SlowdownWritesTrigger int

	// Level0StopWritesTrigger — L0 file count that stalls writes entirely.
	Level0StopWritesTrigger int

	// DisableAutoCompactions turns automatic compaction off for this CF —
	// for write-once, point-lookup CFs where compaction would rewrite the
	// same data with no reordering benefit.
	DisableAutoCompactions bool

	// TargetFileSizeMB — size at which compaction produces new SSTs.
	TargetFileSizeMB int

	// BloomFilterBitsPerKey installs this CF's bloom filter; 0 = none —
	// right for a CF never probed for keys it may not hold.
	BloomFilterBitsPerKey int
}

// Tuning — DB-wide RocksDB knobs shared across every CF of one store. Zero
// means "leave grocksdb's default alone" (wrapper skips the setter). Per-CF
// knobs (memtables, compaction, bloom, block size, compression) live in
// Config.PerCFOptions, not here.
//
// Wrapper-pinned per-CF values not exposed anywhere (applied to every CF):
// MinWriteBufferNumberToMerge=1, CompactionStyle=Level,
// TargetFileSizeMultiplier=1, MaxBytesForLevelMultiplier=10,
// Compression=None, WAL=on, per-write Sync=on.
type Tuning struct {
	// MaxBackgroundJobs caps background threads for compactions
	// and flushes combined, across the whole DB.
	MaxBackgroundJobs int

	// MaxOpenFiles caps concurrent open SST files, across the whole DB.
	MaxOpenFiles int

	// BlockCacheMB sizes the LRU block cache shared across every CF in
	// the store.
	BlockCacheMB int

	// MaxTotalWalSizeMB caps total live WAL size. Crash-recovery
	// replay scales with this cap; graceful Close drains the
	// memtable so this only bounds ungraceful shutdowns.
	MaxTotalWalSizeMB int
}
