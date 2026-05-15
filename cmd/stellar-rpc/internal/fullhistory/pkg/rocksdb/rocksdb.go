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

	// cache and filter are shared across every CF in this store.
	// Created in applyTuning when the corresponding Tuning knob is
	// set; destroyed in Close after opts/cfOpts (their BBTOs hold
	// C-side refs we must drop first).
	cache  *grocksdb.Cache
	filter *grocksdb.NativeFilterPolicy

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
	out := append([]byte(nil), handle.Data()...)
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
		results[i] = append([]byte(nil), p.Data()...)
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
		pcopy := append([]byte(nil), prefix...)
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

// IterateRange yields (key, value) for keys in [start, end] byte-lex
// inclusive. nil or empty start means "from the first key in the CF";
// nil or empty end means "walk to the end of the CF". Right tool for
// range scans over EncodeUint32 / EncodeUint64 keys where numeric
// order matches byte-lex order.
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
			sk := append([]byte(nil), start...)
			it.Seek(sk)
		}

		// Copy end: caller may mutate its buffer mid-iteration.
		endCopy := append([]byte(nil), end...)
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

	if err := s.doFlush(); err != nil {
		s.cfg.Logger.WithError(err).Warnf("rocksdb: graceful close Flush failed at %s; next Open will replay WAL", s.cfg.Path)
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
	// Tear down cache and filter AFTER opts/cfOpts: each cfOpts'
	// BBTO holds a C-side ref; releasing those first makes the
	// final Destroy here safe.
	if s.cache != nil {
		s.cache.Destroy()
		s.cache = nil
	}
	if s.filter != nil {
		s.filter.Destroy()
		s.filter = nil
	}
	return nil
}

// doFlush is the lock-less core of Flush. Caller holds at least
// mu.RLock and has confirmed s.db != nil.
func (s *Store) doFlush() error {
	fo := grocksdb.NewDefaultFlushOptions()
	defer fo.Destroy()
	return s.db.Flush(fo)
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
	if err := os.MkdirAll(abs, dirPerm); err != nil {
		return fmt.Errorf("rocksdb: mkdir %s: %w", abs, err)
	}

	cfNames := resolveCFNames(s.cfg)
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)

	cfOpts := make([]*grocksdb.Options, len(cfNames))
	for i := range cfOpts {
		cfOpts[i] = grocksdb.NewDefaultOptions()
	}

	s.applyTuning(opts, cfOpts)

	start := time.Now()
	db, cfHandles, err := grocksdb.OpenDbColumnFamilies(opts, abs, cfNames, cfOpts)
	elapsed := time.Since(start)
	if err != nil {
		opts.Destroy()
		for _, o := range cfOpts {
			o.Destroy()
		}
		// applyTuning allocated the shared cache and filter as side
		// effects before the open attempt; without this teardown
		// they leak (no Close path can reach them since New returns
		// nil to the caller on failure).
		if s.cache != nil {
			s.cache.Destroy()
			s.cache = nil
		}
		if s.filter != nil {
			s.filter.Destroy()
			s.filter = nil
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
	// than exposed via Tuning. The streaming ingestion contract
	// requires "AddEntries returned nil" to mean "durable on disk";
	// one fsync per Put/Batch regardless of size.
	s.wo.DisableWAL(false)
	s.wo.SetSync(true)

	logOpenState(s.cfg.Logger, abs, s, elapsed)
	return nil
}

// applyTuning splits configuration into wrapper-pinned values
// (every CF, unconditional) and per-facade Tuning fields (applied
// only when non-zero). BloomFilterBitsPerKey == 0 is the documented
// "no bloom filter" sentinel.
func (s *Store) applyTuning(opts *grocksdb.Options, cfOpts []*grocksdb.Options) {
	t := s.cfg.Tuning
	for _, o := range cfOpts {
		applyPinnedCFOptions(o)
		applyCFTuning(o, t)
	}
	applyDBTuning(opts, t)
	s.applySharedTableOptions(cfOpts, t)
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

// applySharedTableOptions builds one BBTO per CF, all referencing
// the shared cache + filter (when set). The Store owns cache and
// filter; Close destroys them after opts/cfOpts.
func (s *Store) applySharedTableOptions(cfOpts []*grocksdb.Options, t Tuning) {
	if t.BlockCacheMB > 0 {
		s.cache = grocksdb.NewLRUCache(uint64(t.BlockCacheMB) << 20)
	}
	if t.BloomFilterBitsPerKey > 0 {
		s.filter = grocksdb.NewBloomFilter(float64(t.BloomFilterBitsPerKey))
	}
	if s.cache == nil && s.filter == nil {
		return
	}
	for _, o := range cfOpts {
		bbto := grocksdb.NewDefaultBlockBasedTableOptions()
		if s.cache != nil {
			bbto.SetBlockCache(s.cache)
		}
		if s.filter != nil {
			bbto.SetFilterPolicy(s.filter)
		}
		o.SetBlockBasedTableFactory(bbto)
	}
}

// logOpenState emits one Info line summarizing on-disk state and
// open elapsed. Diagnoses slow restarts: big WAL → replay; high L0
// → pending compaction; large memtable → crash with unflushed data.
func logOpenState(log *supportlog.Entry, abs string, s *Store, elapsed time.Duration) {
	memtable := readUintProperty(s.db, "rocksdb.cur-size-active-mem-table")
	l0Count := readIntProperty(s.db, "rocksdb.num-files-at-level0")
	sstSize := readUintProperty(s.db, "rocksdb.total-sst-files-size")
	walSize := walDirSize(abs)

	log.Infof(
		"[ROCKSDB:OPEN] path=%s elapsed=%s WAL size=%s L0 file count=%s data size=%s memtable size=%s",
		abs,
		// Round to microseconds: a fast open like 350µs stays
		// "350µs" rather than "350.812µs" (operator-noise nanos);
		// Round(time.Millisecond) would round to "0s".
		elapsed.Round(time.Microsecond).String(),
		humanize.Bytes(walSize),
		humanize.Comma(l0Count),
		humanize.Bytes(sstSize),
		humanize.Bytes(memtable),
	)
}

func readIntProperty(db *grocksdb.DB, name string) int64 {
	v := db.GetProperty(name)
	if v == "" {
		return 0
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func readUintProperty(db *grocksdb.DB, name string) uint64 {
	v := db.GetProperty(name)
	if v == "" {
		return 0
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0
	}
	return n
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
