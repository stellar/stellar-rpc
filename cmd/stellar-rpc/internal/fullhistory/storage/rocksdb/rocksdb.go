// Package rocksdb wraps grocksdb: Layer-1 generic store +
// Layer-2 typed facades. Schema-agnostic primitives here; key
// shapes and tunings owned by each facade.
package rocksdb

import (
	"bytes"
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/linxGnu/grocksdb"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

var (
	ErrInvalidConfig = errors.New("rocksdb: invalid config")
	ErrCFNotFound    = errors.New("rocksdb: column family not configured at open")
	ErrStoreClosed   = errors.New("rocksdb: store is closed")
)

const (
	dirPerm       os.FileMode = 0o700
	defaultCFName             = "default"
)

// Config — per-store knobs. Each Layer-2 facade owns one, built in
// code (never from operator TOML).
type Config struct {
	// Path is the on-disk directory the store occupies. Required.
	// Created (with parents) by New if missing.
	Path string

	// ColumnFamilies — full CF list, fixed for the store's lifetime.
	// nil or [] means default-CF only. "default" is implicitly added
	// if the caller leaves it out. Empty string at the call site
	// normalizes to "default".
	ColumnFamilies []string

	// Logger receives the on-open state line and the close-time
	// Flush warning. Required.
	Logger *supportlog.Entry

	// Tuning — per-facade RocksDB knobs. Zero-valued fields fall
	// back to grocksdb defaults (the wrapper skips the setter).
	Tuning Tuning

	// PerCFOptions — per-CF overrides applied after the pinned
	// defaults and global Tuning. nil or absent CF name means
	// "inherit the pinned defaults"; see CFOptions docstring for
	// the per-knob inherit/override semantics.
	PerCFOptions map[string]CFOptions

	// ReadOnly opens the store read-only (dir never created, no writes, no
	// flush-on-close). An un-flushed WAL IS recovered into in-memory memtables
	// on open (RocksDB OpenForReadOnly semantics; nothing is persisted), so
	// reads see every synced write, not just SST/MANIFEST state. Used by the
	// freeze source.
	ReadOnly bool

	// MustExist opens read-WRITE but with create-if-missing OFF, so opening a
	// missing or gutted DB fails instead of silently fabricating a fresh empty one
	// — the "never auto-heal" hot-DB open under a "ready" key, a DB the filesystem
	// should already hold. (RocksDB's env layer may still leave a stub leaf dir with
	// a LOG file behind on the failed open; correctness holds — every retry still
	// fails on the missing CURRENT — but no usable DB is created.) Ignored when
	// ReadOnly is set (read-only never creates regardless).
	MustExist bool
}

// Store is the Layer-1 RocksDB handle. Concrete struct: one impl,
// every test runs against a real RocksDB in a tempdir.
//
// Lifecycle: New returns a fully-open store ready to use; Close
// flushes and tears down. Close is idempotent. Each Layer-2 facade
// owns its own Store; two Stores on the same Path collide on
// grocksdb's LOCK by design.
type Store struct {
	cfg Config

	db        *grocksdb.DB
	opts      *grocksdb.Options
	cfOpts    []*grocksdb.Options
	cfHandles map[string]*grocksdb.ColumnFamilyHandle
	ro        *grocksdb.ReadOptions
	wo        *grocksdb.WriteOptions

	// cache is the block cache shared across every CF in this store,
	// created in applyTuning when BlockCacheMB is set. bbtos are the
	// per-CF block-based-table options (one per CF that has a cache,
	// bloom filter, or block-size override); each may own a moved-in
	// bloom filter. Both are destroyed in Close after opts/cfOpts,
	// which hold C-side refs we must drop first.
	cache *grocksdb.Cache
	bbtos []*grocksdb.BlockBasedTableOptions

	// mu is a lifecycle / memory-safety lock at the C boundary, not
	// a data-consistency lock (RocksDB is already thread-safe).
	// Every op (Put/Get/Delete/Iterate/Batch/Flush) takes RLock for
	// the duration of its C call. Close takes Lock after flipping
	// closed=true so it waits for in-flight ops to drain before
	// freeing the C++ DB. Without this, a Put already past checkOpen
	// would segfault against a torn-down DB.
	mu sync.RWMutex

	closed atomic.Bool
}

// New validates cfg and returns a fully-open Store. On any failure
// no Store is returned and every C resource allocated as a side
// effect is destroyed before this returns. Caller retries by calling
// New again — failed attempts are not cached anywhere.
func New(cfg Config) (*Store, error) {
	if cfg.Path == "" {
		return nil, ErrInvalidConfig
	}
	if cfg.Logger == nil {
		return nil, ErrInvalidConfig
	}
	s := &Store{cfg: cfg}
	if err := s.constructAndOpen(); err != nil {
		return nil, err
	}
	return s, nil
}

// resolveCFNames returns the final CF list. "default" is appended
// if the caller left it out (RocksDB requires it).
func resolveCFNames(cfg Config) []string {
	if len(cfg.ColumnFamilies) == 0 {
		return []string{defaultCFName}
	}
	out := make([]string, 0, len(cfg.ColumnFamilies)+1)
	hasDefault := false
	for _, n := range cfg.ColumnFamilies {
		if n == defaultCFName {
			hasDefault = true
		}
		out = append(out, n)
	}
	if !hasDefault {
		out = append(out, defaultCFName)
	}
	return out
}

// Put writes one (key, value) to cf. cf == "" normalizes to "default".
// For atomic multi-write, use Batch.
func (s *Store) Put(cf string, key, value []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.checkOpen(); err != nil {
		return err
	}
	cfh, err := s.resolveCF(cf)
	if err != nil {
		return err
	}
	return s.db.PutCF(s.wo, cfh, key, value)
}

// Get returns (value, true, nil) on hit, (nil, false, nil) on miss.
// Returned value is a fresh copy the caller owns.
func (s *Store) Get(cf string, key []byte) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.checkOpen(); err != nil {
		return nil, false, err
	}
	cfh, err := s.resolveCF(cf)
	if err != nil {
		return nil, false, err
	}
	handle, err := s.db.GetPinnedCFV2(s.ro, cfh, key)
	if err != nil {
		return nil, false, err
	}
	defer handle.Destroy()
	if !handle.Exists() {
		return nil, false, nil
	}
	// handle.Data() points into the pinned cache page; copy before
	// Destroy invalidates it.
	out := bytes.Clone(handle.Data())
	return out, true, nil
}

// BatchMultiGet reads many keys from cf in a single batched call.
// Returns a [][]byte of length len(keys) where result[i] is the
// value for keys[i] (a fresh copy the caller owns), or nil if that
// key is absent. An empty keys slice returns (nil, nil).
//
// keys must be sorted ascending — the underlying RocksDB
// BatchedMultiGetCF is invoked with sortedInput=true, which lets it
// merge adjacent SST seeks across the input set. Behavior on
// unsorted input is undefined per RocksDB semantics.
//
// Uses async_io read options so the kernel can issue overlapping
// I/Os under the hood (notable on EBS / high random-latency
// storage). The batched call is a single CGO crossing; callers
// needing cancellation between individual key reads should not use
// this API — split into multiple calls or use Get in a loop.
func (s *Store) BatchMultiGet(cf string, keys [][]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	cfh, err := s.resolveCF(cf)
	if err != nil {
		return nil, err
	}

	// Fresh ReadOptions: mutating s.ro would surface async_io to
	// every concurrent reader on this Store.
	ro := grocksdb.NewDefaultReadOptions()
	ro.SetAsyncIO(true)
	defer ro.Destroy()

	pinned, err := s.db.BatchedMultiGetCF(ro, cfh, true /* sortedInput */, keys...)
	if err != nil {
		return nil, fmt.Errorf("rocksdb: batched multi get on %q: %w", cf, err)
	}
	defer pinned.Destroy()

	results := make([][]byte, len(keys))
	for i, p := range pinned {
		if !p.Exists() {
			continue
		}
		// p.Data() points into the pinned cache page; copy before
		// Destroy invalidates it.
		results[i] = bytes.Clone(p.Data())
	}
	return results, nil
}

// Delete removes key from cf. Idempotent: no error on miss.
func (s *Store) Delete(cf string, key []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.checkOpen(); err != nil {
		return err
	}
	cfh, err := s.resolveCF(cf)
	if err != nil {
		return err
	}
	return s.db.DeleteCF(s.wo, cfh, key)
}

// Entry is one (key, value) yielded by Iterate / IterateRange.
// Key and Value are zero-copy refs into the iterator's internal
// buffer — valid ONLY during the current iteration step. Copy
// before retaining past the next step.
type Entry struct {
	Key, Value []byte
}

// Iterate yields (key, value) for every key in cf that starts with
// prefix, in byte-lex order. Empty prefix iterates the whole CF.
// Up-front errors (closed/never-opened/unknown CF) and mid-walk
// RocksDB errors yield once with (Entry{}, err).
func (s *Store) Iterate(cf string, prefix []byte) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		s.mu.RLock()
		defer s.mu.RUnlock()

		if err := s.checkOpen(); err != nil {
			yield(Entry{}, err)
			return
		}
		cfh, err := s.resolveCF(cf)
		if err != nil {
			yield(Entry{}, err)
			return
		}

		it := s.db.NewIteratorCF(s.ro, cfh)
		defer it.Close()

		// Copy prefix: the caller may mutate its buffer while ranging.
		pcopy := bytes.Clone(prefix)
		it.Seek(pcopy)

		for ; it.Valid(); it.Next() {
			kSlice := it.KeySlice()
			if !bytes.HasPrefix(kSlice.Data(), pcopy) {
				return
			}
			vSlice := it.ValueSlice()
			if !yield(Entry{Key: kSlice.Data(), Value: vSlice.Data()}, nil) {
				return
			}
		}
		if err := it.Err(); err != nil {
			yield(Entry{}, err)
		}
	}
}

// LastKey returns the largest key in cf. If cf has no keys this is not an
// error: it returns (nil, false, nil), so callers detect emptiness via ok.
// (cf == "" selects the default column family; an unregistered cf name
// returns ErrCFNotFound.)
// Cheap: a single boundary seek (no scan).
func (s *Store) LastKey(cf string) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := s.checkOpen(); err != nil {
		return nil, false, err
	}
	cfh, err := s.resolveCF(cf)
	if err != nil {
		return nil, false, err
	}

	it := s.db.NewIteratorCF(s.ro, cfh)
	defer it.Close()
	it.SeekToLast()
	if !it.Valid() {
		// Empty CF (it.Err() is nil) or a mid-seek RocksDB error.
		return nil, false, it.Err()
	}
	// Copy: the KeySlice is freed when the iterator closes.
	return bytes.Clone(it.KeySlice().Data()), true, it.Err()
}

// IterateRange yields (key, value) for keys in [start, end] byte-lex
// inclusive. nil or empty start means "from the first key in the CF";
// nil or empty end means "walk to the end of the CF". Right tool for
// range scans over EncodeUint32 keys where numeric order matches
// byte-lex order.
//
// Gap handling: yields every key that exists in [start, end]. Holes
// (e.g., 100, 102, 105) are silent — callers comparing consecutive
// yielded keys decide whether a gap is fatal.
func (s *Store) IterateRange(cf string, start, end []byte) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		s.mu.RLock()
		defer s.mu.RUnlock()

		if err := s.checkOpen(); err != nil {
			yield(Entry{}, err)
			return
		}
		cfh, err := s.resolveCF(cf)
		if err != nil {
			yield(Entry{}, err)
			return
		}

		it := s.db.NewIteratorCF(s.ro, cfh)
		defer it.Close()

		if len(start) == 0 {
			it.SeekToFirst()
		} else {
			sk := bytes.Clone(start)
			it.Seek(sk)
		}

		// Copy end: caller may mutate its buffer mid-iteration.
		endCopy := bytes.Clone(end)
		hasUpperBound := len(endCopy) > 0

		for ; it.Valid(); it.Next() {
			kSlice := it.KeySlice()
			if hasUpperBound && bytes.Compare(kSlice.Data(), endCopy) > 0 {
				return
			}
			vSlice := it.ValueSlice()
			if !yield(Entry{Key: kSlice.Data(), Value: vSlice.Data()}, nil) {
				return
			}
		}
		if err := it.Err(); err != nil {
			yield(Entry{}, err)
		}
	}
}

// Flush drains the active memtable to an SST. Callers do NOT need
// to call this before Close — Close auto-Flushes internally.
func (s *Store) Flush() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.checkOpen(); err != nil {
		return err
	}
	return s.doFlush()
}

// IsClosed reports whether Close has been called. Used by Layer-2
// facade methods that short-circuit before reaching the wrapper.
func (s *Store) IsClosed() bool {
	return s.closed.Load()
}

// Close shuts the store down. Idempotent. Waits for any in-flight
// ops to settle, then auto-Flushes the active memtable (best-effort)
// so the next graceful New on the same path replays zero WAL. On
// Flush failure: logged, teardown proceeds; data stays durable in
// the WAL and replays on the next New.
func (s *Store) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil
	}

	// A read-only store has nothing to flush (and the RocksDB read-only handle
	// would reject it); only a writable store flushes its memtable on close.
	if !s.cfg.ReadOnly {
		if err := s.doFlush(); err != nil {
			s.cfg.Logger.WithError(err).Warnf(
				"rocksdb: graceful close Flush failed at %s; next Open will replay WAL", s.cfg.Path)
		}
	}

	for _, cfh := range s.cfHandles {
		cfh.Destroy()
	}
	s.ro.Destroy()
	s.wo.Destroy()
	s.db.Close()
	s.opts.Destroy()
	for _, o := range s.cfOpts {
		o.Destroy()
	}
	// Tear down the per-CF BBTOs and the shared cache AFTER opts/cfOpts.
	// SetBlockBasedTableFactory copies the BBTO into the CF's factory, so
	// destroying the BBTO frees the copy we own (Options.Destroy does not);
	// a BBTO with a moved-in bloom filter frees that filter too.
	for _, bbto := range s.bbtos {
		bbto.Destroy()
	}
	s.bbtos = nil
	if s.cache != nil {
		s.cache.Destroy()
		s.cache = nil
	}
	return nil
}

// doFlush is the lock-less core of Flush. Caller holds at least
// mu.RLock and has confirmed s.db != nil. Flushes every CF: the
// data CFs are all named, and DB.Flush drains only the (always
// empty) default CF, so a plain Flush would persist nothing.
func (s *Store) doFlush() error {
	fo := grocksdb.NewDefaultFlushOptions()
	defer fo.Destroy()
	cfs := make([]*grocksdb.ColumnFamilyHandle, 0, len(s.cfHandles))
	for _, cfh := range s.cfHandles {
		cfs = append(cfs, cfh)
	}
	return s.db.FlushCFs(cfs, fo)
}

func (s *Store) checkOpen() error {
	if s.closed.Load() {
		return ErrStoreClosed
	}
	return nil
}

// resolveCF normalizes "" → "default" and looks up the CF handle.
func (s *Store) resolveCF(cf string) (*grocksdb.ColumnFamilyHandle, error) {
	if cf == "" {
		cf = defaultCFName
	}
	cfh, ok := s.cfHandles[cf]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrCFNotFound, cf)
	}
	return cfh, nil
}

// constructAndOpen does the full open work — validation, mkdir,
// options setup, applyTuning (which allocates the shared cache and
// filter as side effects on s), then OpenDbColumnFamilies. On
// failure, every C resource allocated as a side effect is destroyed
// before returning. New is the thin public wrapper that returns nil
// on error so callers never observe a half-built Store. Keeping
// this method package-private lets the leak-regression test build a
// Store directly and call constructAndOpen to inspect post-failure
// state on the half-built receiver, which the New caller can't.
func (s *Store) constructAndOpen() error {
	abs, err := filepath.Abs(s.cfg.Path)
	if err != nil {
		return fmt.Errorf("rocksdb: canonicalize path %s: %w", s.cfg.Path, err)
	}
	// Read-only and must-exist opens require a pre-existing DB; neither creates
	// the directory. Only a plain read-write open (create-if-missing) does.
	if !s.cfg.ReadOnly && !s.cfg.MustExist {
		if err := os.MkdirAll(abs, dirPerm); err != nil {
			return fmt.Errorf("mkdir %s: %w", abs, err)
		}
	}

	cfNames := resolveCFNames(s.cfg)
	opts := grocksdb.NewDefaultOptions()
	if !s.cfg.ReadOnly && !s.cfg.MustExist {
		opts.SetCreateIfMissing(true)
		opts.SetCreateIfMissingColumnFamilies(true)
	}

	cfOpts := make([]*grocksdb.Options, len(cfNames))
	for i := range cfOpts {
		cfOpts[i] = grocksdb.NewDefaultOptions()
	}

	s.applyTuning(opts, cfNames, cfOpts)

	start := time.Now()
	var (
		db        *grocksdb.DB
		cfHandles []*grocksdb.ColumnFamilyHandle
	)
	if s.cfg.ReadOnly {
		// errorIfWalFileExists=false: a cleanly-closed DB has no WAL; if a crash ever
		// left one, the open recovers it into in-memory memtables (see Config.ReadOnly)
		// rather than failing, so reads still see every synced write.
		db, cfHandles, err = grocksdb.OpenDbForReadOnlyColumnFamilies(opts, abs, cfNames, cfOpts, false)
	} else {
		db, cfHandles, err = grocksdb.OpenDbColumnFamilies(opts, abs, cfNames, cfOpts)
	}
	elapsed := time.Since(start)
	if err != nil {
		opts.Destroy()
		for _, o := range cfOpts {
			o.Destroy()
		}
		// applyTuning allocated the per-CF BBTOs and the shared cache
		// as side effects before the open attempt; without this
		// teardown they leak (no Close path can reach them since New
		// returns nil to the caller on failure).
		for _, bbto := range s.bbtos {
			bbto.Destroy()
		}
		s.bbtos = nil
		if s.cache != nil {
			s.cache.Destroy()
			s.cache = nil
		}
		return fmt.Errorf("rocksdb: open %s: %w", abs, err)
	}

	cfMap := make(map[string]*grocksdb.ColumnFamilyHandle, len(cfHandles))
	for i, name := range cfNames {
		cfMap[name] = cfHandles[i]
	}

	s.db = db
	s.opts = opts
	s.cfOpts = cfOpts
	s.cfHandles = cfMap
	s.ro = grocksdb.NewDefaultReadOptions()
	s.wo = grocksdb.NewDefaultWriteOptions()

	// WAL on + per-write Sync on — non-negotiable across every
	// fullhistory store, so pinned here on the shared wo rather
	// than exposed via Tuning. The ingestion contract
	// requires "the ledger batch committed" to mean "durable on disk";
	// one fsync per Put/Batch regardless of size.
	s.wo.DisableWAL(false)
	s.wo.SetSync(true)

	logOpenState(s.cfg.Logger, abs, s, elapsed)
	return nil
}

// applyTuning splits configuration into wrapper-pinned values
// (every CF, unconditional), per-facade Tuning fields (applied
// only when non-zero), and per-CF overrides (applied last so they
// win over the pinned defaults). BloomFilterBitsPerKey == 0 is
// the documented "no bloom filter" sentinel.
func (s *Store) applyTuning(opts *grocksdb.Options, cfNames []string, cfOpts []*grocksdb.Options) {
	t := s.cfg.Tuning
	for i, o := range cfOpts {
		applyPinnedCFOptions(o)
		applyCFTuning(o, t)
		applyCFOverride(o, s.cfg.PerCFOptions[cfNames[i]])
	}
	applyDBTuning(opts, t)
	s.applySharedTableOptions(cfNames, cfOpts, t)
}

// applyCFOverride applies the per-CF Compression override. BlockSize
// is applied inside applySharedTableOptions when the BBTO is built.
func applyCFOverride(o *grocksdb.Options, override CFOptions) {
	// CompressionType is an int alias; NoCompression (the pinned
	// default) is 0. A zero-value override is therefore a no-op and
	// leaves the pinned NoCompression in place. Non-zero values
	// (Snappy, ZSTD, ...) replace it.
	if override.Compression != grocksdb.NoCompression {
		o.SetCompression(override.Compression)
	}
}

// applyPinnedCFOptions sets the per-CF values that hold for every
// facade — applied unconditionally so a future facade can't drift
// off these defaults.
func applyPinnedCFOptions(o *grocksdb.Options) {
	o.SetMinWriteBufferNumberToMerge(1)
	o.SetCompactionStyle(grocksdb.LevelCompactionStyle)
	o.SetTargetFileSizeMultiplier(1)
	o.SetMaxBytesForLevelMultiplier(10)
	o.SetCompression(grocksdb.NoCompression)
}

func applyCFTuning(o *grocksdb.Options, t Tuning) {
	if t.WriteBufferMB > 0 {
		o.SetWriteBufferSize(uint64(t.WriteBufferMB) << 20)
	}
	if t.MaxWriteBufferNumber > 0 {
		o.SetMaxWriteBufferNumber(t.MaxWriteBufferNumber)
	}
	if t.Level0FileNumCompactionTrigger > 0 {
		o.SetLevel0FileNumCompactionTrigger(t.Level0FileNumCompactionTrigger)
	}
	if t.Level0SlowdownWritesTrigger > 0 {
		o.SetLevel0SlowdownWritesTrigger(t.Level0SlowdownWritesTrigger)
	}
	if t.Level0StopWritesTrigger > 0 {
		o.SetLevel0StopWritesTrigger(t.Level0StopWritesTrigger)
	}
	if t.DisableAutoCompactions {
		o.SetDisableAutoCompactions(true)
	}
	if t.TargetFileSizeMB > 0 {
		o.SetTargetFileSizeBase(uint64(t.TargetFileSizeMB) << 20)
	}
	if t.MaxBytesForLevelBaseMB > 0 {
		o.SetMaxBytesForLevelBase(uint64(t.MaxBytesForLevelBaseMB) << 20)
	}
}

func applyDBTuning(opts *grocksdb.Options, t Tuning) {
	if t.MaxBackgroundJobs > 0 {
		opts.SetMaxBackgroundJobs(t.MaxBackgroundJobs)
	}
	if t.MaxOpenFiles > 0 {
		opts.SetMaxOpenFiles(t.MaxOpenFiles)
	}
	if t.MaxTotalWalSizeMB > 0 {
		opts.SetMaxTotalWalSize(uint64(t.MaxTotalWalSizeMB) << 20)
	}
}

// applySharedTableOptions builds one BBTO per CF, referencing the
// shared block cache (when set), a fresh per-CF bloom filter (when
// set), and the per-CF BlockSize override (when set). The Store
// retains every BBTO and the cache; Close destroys them after
// opts/cfOpts.
//
// A BBTO is installed on a CF iff any of the cache, a bloom filter,
// or that CF's BlockSize override is configured — preserving the
// previous behavior of leaving RocksDB's default BBTO untouched when
// no table-level knob is set.
//
// The bloom filter is built per CF because SetFilterPolicy MOVES the
// policy into the BBTO (it nils the source pointer), so a single
// shared filter would install on the first CF only and every later
// CF would silently get none.
func (s *Store) applySharedTableOptions(cfNames []string, cfOpts []*grocksdb.Options, t Tuning) {
	if t.BlockCacheMB > 0 {
		s.cache = grocksdb.NewLRUCache(uint64(t.BlockCacheMB) << 20)
	}
	for i, o := range cfOpts {
		override := s.cfg.PerCFOptions[cfNames[i]]
		if s.cache == nil && t.BloomFilterBitsPerKey == 0 && override.BlockSize == 0 {
			continue
		}
		bbto := grocksdb.NewDefaultBlockBasedTableOptions()
		if s.cache != nil {
			bbto.SetBlockCache(s.cache)
		}
		if t.BloomFilterBitsPerKey > 0 {
			bbto.SetFilterPolicy(grocksdb.NewBloomFilter(float64(t.BloomFilterBitsPerKey)))
		}
		if override.BlockSize > 0 {
			bbto.SetBlockSize(override.BlockSize)
		}
		o.SetBlockBasedTableFactory(bbto)
		s.bbtos = append(s.bbtos, bbto)
	}
}

// logOpenState emits one Info line summarizing on-disk state and
// open elapsed. Diagnoses slow restarts: big WAL → replay; high L0
// → pending compaction; large memtable → crash with unflushed data.
func logOpenState(log *supportlog.Entry, abs string, s *Store, elapsed time.Duration) {
	// These are per-CF properties; a DB-level GetProperty resolves against the
	// always-empty default CF, so sum each across every CF instead.
	memtable := s.sumUintPropertyCF("rocksdb.cur-size-active-mem-table")
	l0Count := s.sumUintPropertyCF("rocksdb.num-files-at-level0")
	sstSize := s.sumUintPropertyCF("rocksdb.total-sst-files-size")
	walSize := walDirSize(abs) // DB-level: one WAL spans all CFs.

	log.Infof(
		"[ROCKSDB:OPEN] path=%s elapsed=%s WAL size=%s L0 file count=%d data size=%s memtable size=%s",
		abs,
		// Round to microseconds: a fast open like 350µs stays
		// "350µs" rather than "350.812µs" (operator-noise nanos);
		// Round(time.Millisecond) would round to "0s".
		elapsed.Round(time.Microsecond).String(),
		humanize.Bytes(walSize),
		l0Count,
		humanize.Bytes(sstSize),
		humanize.Bytes(memtable),
	)
}

// sumUintPropertyCF sums an unsigned-integer RocksDB property across every CF.
// Empty (ParseUint of "") or unparseable values count as zero.
func (s *Store) sumUintPropertyCF(name string) uint64 {
	var total uint64
	for _, cfh := range s.cfHandles {
		if n, err := strconv.ParseUint(s.db.GetPropertyCF(name, cfh), 10, 64); err == nil {
			total += n
		}
	}
	return total
}

// walDirSize sums *.log file sizes in dir (RocksDB exposes no
// single "WAL size" property).
func walDirSize(dir string) uint64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var total uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) < 4 || name[len(name)-4:] != ".log" {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if size := info.Size(); size > 0 {
			total += uint64(size)
		}
	}
	return total
}
