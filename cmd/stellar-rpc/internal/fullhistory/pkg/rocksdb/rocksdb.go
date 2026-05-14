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
	ErrInvalidConfig  = errors.New("rocksdb: invalid config")
	ErrCFNotFound     = errors.New("rocksdb: column family not configured at open")
	ErrStoreClosed    = errors.New("rocksdb: store is closed")
	ErrStoreNotOpened = errors.New("rocksdb: store has not been opened")
	// ErrNotFound — collapsed-shape miss sentinel used by Layer-2
	// facades whose Get returns (value, error). Layer-1 Store.Get
	// itself reports a miss with (nil, false, nil).
	ErrNotFound = errors.New("rocksdb: key not found")
)

const (
	dirPerm       os.FileMode = 0o700
	defaultCFName             = "default"
)

// Config — per-store knobs. Each Layer-2 facade owns one, built in
// code (never from operator TOML).
type Config struct {
	// Path is the on-disk directory the store occupies. Required.
	// Created (with parents) on Open if missing.
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
// Lifecycle: construct with New, then Open. Both Open and Close are
// idempotent. Each Layer-2 facade owns its own Store; two Stores on
// the same Path collide on grocksdb's LOCK by design.
type Store struct {
	cfg Config

	openOnce sync.Once
	openErr  error

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

// New validates cfg and returns a Store ready to be Opened.
func New(cfg Config) (*Store, error) {
	if cfg.Path == "" {
		return nil, ErrInvalidConfig
	}
	if cfg.Logger == nil {
		return nil, ErrInvalidConfig
	}
	return &Store{cfg: cfg}, nil
}

// Open creates/opens the RocksDB instance and logs an on-open state
// summary. Idempotent. Auto-mkdir's Path (parents, mode 0700).
// Returns ErrStoreClosed if Close ran concurrently.
func (s *Store) Open() error {
	s.openOnce.Do(func() {
		s.openErr = s.doOpen()
	})
	if s.openErr != nil {
		return s.openErr
	}
	if s.closed.Load() {
		return ErrStoreClosed
	}
	return nil
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

// Entry is one (key, value) yielded by Iterate/IterateFrom.
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

// IterateFrom yields (key, value) for keys >= startKey, byte-lex
// order, unbounded forward. Empty startKey == SeekToFirst.
// Right tool for range scans over EncodeUint32/EncodeUint64 keys
// where prefix-matching would terminate after one key.
//
// Gap handling: yields every key that exists in [startKey, end-of-CF).
// Holes (e.g., 100, 102, 105) are silent — callers comparing
// consecutive yielded keys decide whether a gap is fatal.
func (s *Store) IterateFrom(cf string, startKey []byte) iter.Seq2[Entry, error] {
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

		if len(startKey) == 0 {
			it.SeekToFirst()
		} else {
			sk := append([]byte(nil), startKey...)
			it.Seek(sk)
		}

		for ; it.Valid(); it.Next() {
			kSlice := it.KeySlice()
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

// FirstLastKey returns the smallest and largest keys in cf, in
// byte-lex order, or (nil, nil, false, nil) when cf is empty.
// O(1) — SeekToFirst + SeekToLast hit SST file metadata, no walk.
// Returned slices are fresh copies the caller owns.
func (s *Store) FirstLastKey(cf string) ([]byte, []byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := s.checkOpen(); err != nil {
		return nil, nil, false, err
	}
	cfh, err := s.resolveCF(cf)
	if err != nil {
		return nil, nil, false, err
	}

	it := s.db.NewIteratorCF(s.ro, cfh)
	defer it.Close()

	it.SeekToFirst()
	if !it.Valid() {
		if itErr := it.Err(); itErr != nil {
			return nil, nil, false, itErr
		}
		return nil, nil, false, nil
	}
	first := append([]byte(nil), it.KeySlice().Data()...)

	it.SeekToLast()
	if !it.Valid() {
		if itErr := it.Err(); itErr != nil {
			return nil, nil, false, itErr
		}
		return nil, nil, false, errors.New("rocksdb: FirstLastKey: SeekToFirst yielded a key but SeekToLast did not")
	}
	last := append([]byte(nil), it.KeySlice().Data()...)

	return first, last, true, nil
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
// Open and in-flight ops to settle, then auto-Flushes the active
// memtable (best-effort) so the next graceful Open replays zero
// WAL. On Flush failure: logged, teardown proceeds; data stays
// durable in the WAL and replays on next Open.
func (s *Store) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	// Wait for any in-flight Open. If another goroutine is inside
	// openOnce.Do, Do(noop) blocks until it finishes. If no Open
	// has started, our Do(noop) claims it — a later Open then sees
	// closed=true and returns ErrStoreClosed without touching disk.
	s.openOnce.Do(func() {})

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
	if s.db == nil {
		return ErrStoreNotOpened
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

func (s *Store) doOpen() error {
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
	o.SetMinWriteBufferNumberToMerge(2)
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
