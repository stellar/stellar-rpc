// Package rocksdb is the generic, schema-agnostic RocksDB wrapper
// (Layer-1) used by every RocksDB-backed store in the unified
// stellar-rpc fullhistory codebase.
//
// Each store on top of this wrapper — the backfill meta store, the
// hot ledger store, the hot txhash store, the hot events store — is a
// "Layer-2 facade".
// A facade owns its own key schema, its own typed read/write methods,
// and a Config struct (built in code, never read from operator TOML)
// that pins this wrapper's behavior for that store.
//
// What this wrapper handles for every facade:
//
//   - Opening a RocksDB instance with zero, one, or many column
//     families.
//   - Auto-creating the on-disk directory at Open time.
//   - Holding the cross-process LOCK file so two services can't
//     stomp on the same directory.
//   - Keeping the WAL on (configurable in code, but no facade flips
//     it off).
//   - Plain bytes-in / bytes-out Put / Get / Delete / Iterate /
//     Batch / Flush / Close.
//
// Each facade gets its own dedicated RocksDB directory, flock, and
// Config — nothing is shared between facades.
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

// ErrInvalidConfig is returned by New when the supplied Config is
// missing a required field.
var ErrInvalidConfig = errors.New("rocksdb: invalid config")

// ErrCFNotFound is returned by Put / Get / Delete / Iterate / Batch
// when the caller passes a CF name that wasn't configured at New.
var ErrCFNotFound = errors.New("rocksdb: column family not configured at open")

// ErrStoreClosed is returned by Store methods called after Close.
var ErrStoreClosed = errors.New("rocksdb: store is closed")

// ErrStoreNotOpened is returned by Put / Get / Delete / Iterate / Batch
// called on a Store that hasn't been Opened yet (or whose Open failed).
var ErrStoreNotOpened = errors.New("rocksdb: store has not been opened")

// dirPerm is the default permission set on directories that Open
// creates when Path is missing. Owner-only.
const dirPerm os.FileMode = 0o700

// defaultCFName is the always-present CF in any RocksDB instance.
const defaultCFName = "default"

// Config configures a single Layer-1 RocksDB store. Each Layer-2 facade
// (meta store, hot ledger store, hot txhash store, hot events store)
// owns one Config, hardcoded in code — never read from operator TOML.
type Config struct {
	// Path is the on-disk directory the store occupies. Required.
	// Created (with parents) on Open if missing.
	Path string

	// ColumnFamilies is the full list of CFs to open with — fixed for
	// the store's lifetime. nil or [] means default-CF only. "default"
	// is implicitly added if the caller leaves it out. Empty string
	// passed to Put / Get / Delete / Iterate normalizes to "default".
	ColumnFamilies []string

	// Logger is where the wrapper writes its on-open state log line.
	// Required.
	Logger *supportlog.Entry
}

// Store is the Layer-1 RocksDB handle.
//
// Concrete struct, not an interface — by design.
// Layer-2 facades (meta store, hot ledger store, hot txhash store, hot
// events store) hold a *Store directly and call its methods.
//
// Reasoning: there is exactly one implementation, and tests at every
// layer (Layer-1 here, every Layer-2 facade) run against a real
// RocksDB opened in a temporary directory.
// Swapping the real Store for a mock in Layer-2 tests would just
// verify the mock — it wouldn't catch any actual RocksDB interaction
// bugs, which is the only reason those tests exist.
//
// Most fake-backend pluggability for the codebase belongs at Layer-2,
// not here.
// Layer-2 facades (meta store, hot ledger store, hot txhash store, hot
// events store) each expose their own typed interface to their
// callers, so test fakes / dry-run versions / alternate impls slot in
// at THAT level — without Layer-1 needing to be an interface.
//
// The Layer-1 interface only becomes worth introducing if a need
// shows up for pluggability AT this specific boundary — i.e., a
// caller wants to hold "anything that quacks like *Store" rather
// than the concrete type itself.
// That's narrow; we haven't hit it yet.
//
// And if it does come up, the refactor is cheap.
// The exported methods on *Store (Put / Get / Delete / Iterate /
// Batch / Open / Close / Flush) ARE the eventual interface.
// Extracting `type Store interface { ... }` and renaming the concrete
// type to e.g. `rocksDBStore` is a mechanical one-PR change.
// Every caller already speaks through this method set, so no call
// site has to change.
//
// Lifecycle: construct with New, then call Open to establish the
// underlying RocksDB instance.
// Open and Close are both idempotent — extra calls return the same
// result without re-opening / re-closing.
//
// Each Layer-2 facade owns its own Store with its own RocksDB
// directory + flock + Config — nothing is shared across facades.
// Within a single process, two separate Stores opened against the
// same Path collide on grocksdb's LOCK file — by design.
// Sharing a directory means sharing one Store instance, not creating
// two.
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

	// mu protects against tearing down the C-side RocksDB instance
	// while another goroutine has an in-flight C call into it.
	//
	// What this lock IS for: lifecycle / memory safety at the C
	// boundary. A goroutine that has passed the open-check and is
	// about to call a C function like rocksdb_put_cf must not have
	// the underlying C++ DB object freed underneath it by a
	// concurrent Close. Without this lock the race is:
	//
	//   goroutine A: passes checkOpen(); about to call rocksdb_put_cf...
	//   goroutine B: calls Close(); rocksdb_close frees the C++ DB.
	//   goroutine A: rocksdb_put_cf runs against freed memory -> SEGFAULT.
	//
	// Every operation (Put, Get, Delete, Iterate, Batch, Flush) takes
	// RLock for the duration of its C-side work. Close takes the
	// exclusive Lock after flipping the closed flag, which waits for
	// every in-flight RLock holder to release before teardown begins.
	//
	// What this lock is NOT for: data consistency. RocksDB itself is
	// thread-safe for concurrent reads and writes; idempotent
	// application data plus the atomic Batch primitive cover ordering
	// at the call site. The lock adds no serialization between Put
	// and Get, and no serialization between two Puts: both take
	// RLock, and many RLocks can be held simultaneously. The
	// exclusive Lock is taken by Close only.
	//
	// This is an explicit design choice over the alternative of
	// documenting a "drain in-flight operations before Close"
	// contract on every Layer-2 facade (zero runtime cost, relies on
	// orderly shutdown). We picked the wrapper-level lock because:
	//
	//   - Cost is one atomic CAS per op (~5ns on modern hardware,
	//     well under 0.1% CPU overhead at our expected write rate of
	//     ~100k puts/sec into the events store), and a single CAS
	//     spans an entire Batch.
	//   - Reads do not block writes; writes do not block reads. Only
	//     Close serializes, and Close is a once-per-process event.
	//   - The failure mode if drain ordering is violated (e.g., during
	//     panic-induced shutdown) is a clean ErrStoreClosed return
	//     rather than a C segfault that destroys the diagnostic
	//     stack.
	//   - Layer-2 facades do not have to re-implement drain
	//     coordination.
	mu sync.RWMutex

	closed atomic.Bool
}

// New validates cfg and returns a Store ready to be Opened.
// Returns ErrInvalidConfig if cfg.Path or cfg.Logger is missing.
func New(cfg Config) (*Store, error) {
	if cfg.Path == "" {
		return nil, ErrInvalidConfig
	}
	if cfg.Logger == nil {
		return nil, ErrInvalidConfig
	}
	return &Store{cfg: cfg}, nil
}

// Open opens (or creates) the underlying RocksDB instance backing
// this Store and writes a one-line summary of the store's state to
// the configured Logger.
// On a slow restart that summary tells an operator at a glance why
// it was slow — pending L0 compactions, a large memtable from a
// crash, a big WAL to replay.
//
// Idempotent: only the first Open call actually does the grocksdb
// work; every later call (from any goroutine) returns that first
// call's result without re-opening anything.
//
// Auto-mkdir: if Path doesn't exist, Open creates it (with parents)
// using mode 0700.
//
// Concurrent Close: if Close runs at the same time as Open and wins,
// Open returns ErrStoreClosed even if the underlying RocksDB
// instance itself opened cleanly — Close will tear it down before
// the caller can use it.
// A genuine open failure (e.g., a disk error from grocksdb) takes
// precedence over ErrStoreClosed because it explains WHY the open
// didn't yield a usable store.
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

// resolveCFNames returns the final CF list to open against.
// Caller may supply an empty slice (defaults to ["default"]) or a
// list missing "default" (we append it; RocksDB requires it).
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

// Put writes a single key/value pair to the named CF.
// cf "" is normalized to "default".
// Returns ErrCFNotFound if cf was not configured at New.
// For atomic multi-write commits, use Batch.
//
// Holds the lifecycle read-lock for the duration of the underlying C
// call. See the mu field doc on Store for what that lock is and is
// not for.
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

// Get retrieves the value for key from the named CF.
// Returns (value, true, nil) if found, (nil, false, nil) if not.
// The returned value is a fresh copy owned by the caller.
//
// Uses grocksdb's zero-copy pinned-read variant (GetPinnedCFV2):
// the returned handle points directly into the pinned block-cache
// page rather than into a separate C-side buffer that RocksDB would
// otherwise allocate and memcpy into. Total copies on the read
// path: one (cache page -> Go-owned byte slice), down from two
// (cache page -> C buffer -> Go-owned slice) with the older GetCF
// path. grocksdb's own docs flag the *V2 variants as the
// recommended migration target for performance.
//
// Holds the lifecycle read-lock for the duration of the underlying C
// call. See the mu field doc on Store for what that lock is and is
// not for.
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
	// Copy because handle.Data() points into the pinned cache page
	// and is invalidated by handle.Destroy().
	out := append([]byte(nil), handle.Data()...)
	return out, true, nil
}

// Delete removes key from the named CF.
// Idempotent at the wrapper level: returns no error if the key didn't
// exist.
//
// Holds the lifecycle read-lock for the duration of the underlying C
// call. See the mu field doc on Store for what that lock is and is
// not for.
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

// Entry is one key/value pair yielded by Store.Iterate.
//
// Both Key and Value are zero-copy refs into the iterator's internal
// buffer. They are valid ONLY during the current iteration step;
// callers that need to retain a slice past the next range step (or
// past the end of the range loop) MUST copy it. Examples of correct
// retention:
//
//	for e, err := range store.Iterate(cf, prefix) {
//	    if err != nil { return err }
//	    savedKey := string(e.Key)                  // safe — new allocation
//	    savedVal := append([]byte(nil), e.Value...) // safe — new allocation
//	}
type Entry struct {
	Key, Value []byte
}

// Iterate returns a Go 1.23+ range-over-func sequence over the
// key/value pairs in cf whose key starts with prefix.
// An empty prefix iterates every key in the CF.
// Keys come back in sorted byte order.
//
// Up-front failures (closed store, never-opened store, unknown CF)
// surface by yielding once with (Entry{}, err). Mid-walk RocksDB
// errors surface the same way after the last successfully-yielded
// entry. Either way the caller's loop sees the error in the
// iteration tuple — there is no separate Err() method to check.
//
// Resource lifecycle is handled inside the producer closure: the
// underlying RocksDB iterator is created on entry and Close-d via
// defer on every exit path (normal completion, range break, caller's
// early return, panic). Callers cannot forget cleanup.
//
// Lifecycle read-lock: held for the duration of the iteration, from
// the moment the caller starts ranging until the loop exits. While
// the caller is mid-loop, Close cannot tear down the C-side DB.
// See the mu field doc on Store for what that lock is and is not for.
//
// Typical use:
//
//	for e, err := range store.Iterate("default", []byte("ledger:")) {
//	    if err != nil { return err }
//	    process(e.Key, e.Value)
//	}
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

		// Copy the caller's prefix so we own the bytes for the whole
		// iteration; the caller may reuse / mutate its prefix buffer
		// while ranging.
		pcopy := append([]byte(nil), prefix...)
		it.Seek(pcopy)

		for ; it.Valid(); it.Next() {
			// KeySlice / ValueSlice return OptimizedSlice (a value
			// type) rather than *Slice (heap-allocated). For a 10k-key
			// scan this eliminates ~30k transient heap allocations vs.
			// the Key() / Value() variants.
			kSlice := it.KeySlice()
			if !bytes.HasPrefix(kSlice.Data(), pcopy) {
				return
			}
			vSlice := it.ValueSlice()
			if !yield(Entry{Key: kSlice.Data(), Value: vSlice.Data()}, nil) {
				// Caller broke out of the range loop. The deferred
				// it.Close() and s.mu.RUnlock() fire as we return.
				return
			}
		}
		if err := it.Err(); err != nil {
			yield(Entry{}, err)
		}
	}
}

// Flush forces the active memtable to be written to an SST file (and
// fsynced).
// Used at graceful shutdown to ensure no WAL replay is needed on the
// next Open — a startup-latency optimization, not a durability
// requirement (the WAL replay path handles durability regardless).
//
// Typical Layer-2 facade shutdown sequence:
//
//	if err := store.Flush(); err != nil { ... }
//	if err := store.Close(); err != nil { ... }
//
// Calling Flush on a closed or never-Opened Store returns the
// corresponding error.
//
// Holds the lifecycle read-lock for the duration of the underlying C
// call. See the mu field doc on Store for what that lock is and is
// not for.
func (s *Store) Flush() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.checkOpen(); err != nil {
		return err
	}
	fo := grocksdb.NewDefaultFlushOptions()
	defer fo.Destroy()
	return s.db.Flush(fo)
}

// Close cleanly shuts down the underlying RocksDB and releases the
// flock.
// Idempotent: a second Close is a no-op.
// Calling Close on a Store that was never successfully Opened is also
// a no-op.
//
// Concurrent Open: Close waits for any in-flight Open to complete
// before tearing down, so it sees a stable view of s.db (either
// nil-because-Open-failed-or-never-ran, or non-nil-because-Open-
// succeeded).
// This avoids the race where Close fires while Open is still
// constructing the grocksdb DB, returns early on s.db == nil, and
// leaves the just-opened DB (and its flock) leaked.
//
// Close does NOT auto-Flush.
// Data is durable either way (WAL replay handles it on next Open),
// but callers wanting a clean shutdown with no WAL replay should call
// Flush first.
func (s *Store) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	// Wait for any in-flight Open to finish before we look at s.db.
	//
	// sync.Once.Do has two behaviors that combine perfectly here:
	//
	//   - If another goroutine is currently inside the original
	//     openOnce.Do(realOpen), our Do(noop) blocks until that
	//     finishes.
	//     After it returns we have a stable view of s.db (either set,
	//     because Open succeeded, or still nil, because it failed).
	//   - If no one has called openOnce.Do yet, our Do(noop) "claims"
	//     it.
	//     A later Open call will see openOnce already done, the noop
	//     having been the once-fn; that Open returns ErrStoreClosed
	//     because s.closed is true — without ever touching disk.
	s.openOnce.Do(func() {})

	// Wait for any in-flight Put / Get / Delete / Iterate / Batch /
	// Flush to release its read-lock before tearing the C-side DB
	// down. Without this, an op that already passed checkOpen but is
	// still inside its C call (e.g., rocksdb_put_cf) would run
	// against freed memory and segfault the process.
	//
	// New ops that arrive after this point will block on RLock until
	// we return, then see s.closed == true and return ErrStoreClosed
	// without touching the (now torn-down) DB.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		// Either Open never ran, or it failed before setting s.db.
		// Nothing to clean up.
		return nil
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
	return nil
}

// checkOpen returns ErrStoreClosed / ErrStoreNotOpened where appropriate.
func (s *Store) checkOpen() error {
	if s.closed.Load() {
		return ErrStoreClosed
	}
	if s.db == nil {
		return ErrStoreNotOpened
	}
	return nil
}

// resolveCF normalizes empty CF name to "default" and looks up the
// underlying CF handle.
// Returns ErrCFNotFound if cf was not configured at New.
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

// doOpen is the actual grocksdb-side open.
// Called at most once per Store via openOnce.Do.
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

	// Time the grocksdb open call.
	// A store with a populated WAL to replay or many L0 files to
	// reconcile can take seconds to come up; the elapsed time goes
	// straight into the on-open log line so a slow restart is visible
	// in operator logs without further instrumentation.
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
	// Pin "WAL on" explicitly.
	// RocksDB doesn't have a DB-level switch to disable the
	// write-ahead log — the only knob is per-write, on WriteOptions,
	// via DisableWAL(true).
	// We never want any facade to flip that, so we set it here on the
	// shared WriteOptions that every Put / Delete / Batch will use.
	// The "false" makes the intent obvious to anyone reading this
	// file: WAL stays on for every store.
	s.wo.DisableWAL(false)

	logOpenState(s.cfg.Logger, abs, s, elapsed)
	return nil
}

// logOpenState emits a single Info line at Open time summarizing the
// store's on-disk + in-memory state and how long the grocksdb open
// itself took.
// Critical for diagnosing slow restarts: large WAL → many in-flight
// commits to replay (drives elapsed up); high L0 count → pending
// compaction; large memtable → recent crash with unflushed data.
func logOpenState(log *supportlog.Entry, abs string, s *Store, elapsed time.Duration) {
	memtable := readUintProperty(s.db, "rocksdb.cur-size-active-mem-table")
	l0Count := readIntProperty(s.db, "rocksdb.num-files-at-level0")
	sstSize := readUintProperty(s.db, "rocksdb.total-sst-files-size")
	walSize := walDirSize(abs)

	log.Infof(
		"[ROCKSDB:OPEN] path=%s elapsed=%s WAL size=%s L0 file count=%s data size=%s memtable size=%s",
		abs,
		// Round to microseconds so a fast open like 350µs renders as
		// "350µs" rather than "350.812µs" (operator-noise nanoseconds),
		// without losing precision on legitimately fast opens (a plain
		// Round(time.Millisecond) would log the same fast open as
		// "0s", erasing operator-useful signal).
		elapsed.Round(time.Microsecond).String(),
		humanize.Bytes(walSize),
		humanize.Comma(l0Count),
		humanize.Bytes(sstSize),
		humanize.Bytes(memtable),
	)
}

// readIntProperty parses a RocksDB property string as int64.
// Used for non-negative counts (e.g., L0 file count) that we then
// pass to humanize.Comma, which itself takes int64.
// Empty or unparseable values degrade to zero so the on-open log
// stays informative when a property name isn't supported by the
// linked grocksdb version.
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

// readUintProperty parses a RocksDB property string as uint64.
// Used for byte-size properties (e.g., active memtable size, total
// SST size) that we pass to humanize.Bytes, which takes uint64.
// Returning uint64 here means no signed→unsigned conversion at the
// call site, which avoids gosec's int64→uint64 overflow warning.
// Empty or unparseable values degrade to zero, same rationale as
// readIntProperty.
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

// walDirSize sums the byte sizes of *.log files (RocksDB's WAL
// segments) in the store directory.
// RocksDB does not expose a single "WAL size" property.
// Returns uint64 because byte sizes are inherently non-negative;
// also matches what humanize.Bytes takes, so no conversion at the
// call site.
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
		// info.Size() returns int64 but file sizes are inherently
		// non-negative; clamp at 0 just in case (a negative would
		// indicate a stat-layer bug rather than a real file size).
		if size := info.Size(); size > 0 {
			total += uint64(size)
		}
	}
	return total
}
