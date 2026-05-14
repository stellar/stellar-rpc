package rocksdb

// Tuning is the per-store set of RocksDB performance knobs that a
// Layer-2 facade pins for its own workload.
// Layer-2 facades populate one of these inside their own New(cfg) and
// hand it to this wrapper through Config.Tuning; the wrapper applies
// the non-zero fields to grocksdb's underlying Options at Open time.
//
// Zero is "leave the grocksdb internal default alone".
// For each field below, a zero value means the wrapper does NOT call
// the corresponding grocksdb setter — whatever grocksdb's own default
// is for that knob is what the store ends up with.
// This lets a facade specify only the knobs it cares about and leave
// the rest at grocksdb's defaults.
//
// One semantic-zero exception is BloomFilterBitsPerKey == 0.
// That value tells the wrapper to skip installing a bloom filter at
// all — useful for stores like the meta store whose keyspace is small
// enough that a bloom filter would be pure overhead.
// Any positive value installs a standard rocksdb bloom filter with
// that many bits per key.
//
// Values pinned wrapper-wide are NOT exposed on Tuning.
// The wrapper applies these to every store unconditionally at Open,
// because they should never vary by facade:
//
//	MinWriteBufferNumberToMerge = 2
//	CompactionStyle             = LevelCompactionStyle
//	TargetFileSizeMultiplier    = 1
//	MaxBytesForLevelMultiplier  = 10
//	Compression                 = NoCompression
//	WAL                         = on   (WriteOptions.DisableWAL = false)
//	Sync                        = on   (WriteOptions.Sync       = true)
//
// Compression is off at the rocksdb block level for every store.
// Stores that benefit from value compression (e.g., the hot ledger
// store) apply zstd at the value level themselves before calling Put,
// rather than relying on rocksdb's block-level compression — that
// keeps the compressed bytes available for forward / proxy paths
// without round-tripping decompress + recompress.
//
// WAL and per-write Sync are non-negotiable across every store.
// The streaming-side ingestion contract is: write to each hot store,
// wait for success, only then update the meta-store checkpoint.
// For "AddEntries returned nil" to mean "this batch is durably on
// disk", every Put / Batch must fsync the WAL before returning.
// There is no facade in the codebase that wants to flip either knob,
// so the Tuning struct deliberately does not expose them.
type Tuning struct {
	// WriteBufferMB sizes the active memtable for each column family,
	// in megabytes.
	// Larger memtables mean fewer (and larger) SST files at flush time
	// but more RAM resident per CF.
	WriteBufferMB int

	// MaxWriteBufferNumber is the maximum number of memtables (active
	// + immutable-being-flushed) the engine allows per CF before it
	// starts back-pressuring writes.
	MaxWriteBufferNumber int

	// Level0FileNumCompactionTrigger is the L0 file count at which
	// rocksdb starts an L0→L1 compaction.
	Level0FileNumCompactionTrigger int

	// Level0SlowdownWritesTrigger is the L0 file count at which
	// rocksdb starts artificially slowing writes to let compaction
	// catch up.
	Level0SlowdownWritesTrigger int

	// Level0StopWritesTrigger is the L0 file count at which rocksdb
	// stalls writes entirely until compaction catches up.
	Level0StopWritesTrigger int

	// DisableAutoCompactions, when true, turns automatic compaction
	// off for every CF.
	// Used by stores whose access pattern (e.g., a write-once point-
	// lookup store like the hot txhash store) does not benefit from
	// compaction and prefers the L0 file count to grow naturally.
	DisableAutoCompactions bool

	// TargetFileSizeMB sets the size at which compaction produces new
	// SST files, in megabytes.
	// Smaller files mean more, smaller compactions; larger files mean
	// fewer, larger compactions and more bloom filter coverage per
	// file.
	TargetFileSizeMB int

	// MaxBytesForLevelBaseMB sets the byte budget for level 1, in
	// megabytes; each subsequent level's budget is this value times
	// the wrapper-pinned MaxBytesForLevelMultiplier (10).
	MaxBytesForLevelBaseMB int

	// MaxBackgroundJobs caps the total background-thread budget the
	// engine can use for compactions and flushes.
	MaxBackgroundJobs int

	// MaxOpenFiles caps the number of SST files rocksdb keeps open
	// concurrently.
	// Stores with very large SST counts can hit OS-level file-handle
	// limits if this isn't raised.
	MaxOpenFiles int

	// BlockCacheMB sizes the shared block cache for the store, in
	// megabytes.
	// One LRU cache is created per store and shared across every CF;
	// it caches recently-used data and index blocks read from SST
	// files.
	BlockCacheMB int

	// BloomFilterBitsPerKey installs a bloom filter with this many
	// bits per key on every CF.
	// Zero is the documented exception: the wrapper installs no
	// filter at all (so the meta store can opt out of paying RAM for
	// a tiny keyspace).
	// Typical values: 10 (~1% false-positive rate), 12 (~0.4%).
	BloomFilterBitsPerKey int

	// MaxTotalWalSizeMB caps the total size of all live WAL files,
	// in megabytes.
	// When the cap is exceeded, rocksdb forces a memtable flush to
	// recover WAL space.
	// Crash-recovery WAL replay at startup scales with this cap —
	// a smaller cap caps the worst-case replay work, at the price
	// of more frequent memtable flushes and more SST files.
	// On a graceful Close (where the facade Flushes before tearing
	// down) the WAL is recyclable on next open and replay is zero;
	// this cap only fires for ungraceful shutdowns (kernel panic,
	// power loss, OOM kill).
	MaxTotalWalSizeMB int
}
