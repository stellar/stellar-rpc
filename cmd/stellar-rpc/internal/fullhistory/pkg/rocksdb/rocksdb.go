package rocksdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linxGnu/grocksdb"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/format"
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

// Store is the Layer-1 RocksDB handle. Construct one via New, then
// call Open to establish the underlying RocksDB instance. Open and
// Close are both idempotent — extra calls return the same result
// without re-opening / re-closing.
//
// Each Layer-2 facade owns its own Store with its own RocksDB
// directory + flock + Config. Within a single process, two separate
// Stores opened against the same Path collide on grocksdb's LOCK file
// — by design; sharing a directory means sharing a Store, not creating
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

	closed atomic.Bool
}

// New validates cfg and returns a Store ready to be Opened. Returns
// ErrInvalidConfig if cfg.Path or cfg.Logger is missing.
func New(cfg Config) (*Store, error) {
	if cfg.Path == "" {
		return nil, ErrInvalidConfig
	}
	if cfg.Logger == nil {
		return nil, ErrInvalidConfig
	}
	return &Store{cfg: cfg}, nil
}

// Open establishes the underlying RocksDB instance and runs the
// on-open state log. Idempotent: the first call performs the actual
// open; subsequent calls return the same result without re-opening.
//
// Auto-mkdir: if Path doesn't exist, Open creates it (with parents)
// using mode 0700.
func (s *Store) Open() error {
	s.openOnce.Do(func() {
		s.openErr = s.doOpen()
	})
	return s.openErr
}

// resolveCFNames returns the final CF list to open against. Caller may
// supply an empty slice (defaults to ["default"]) or a list missing
// "default" (we append it; RocksDB requires it).
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

// Put writes a single key/value pair to the named CF. cf "" is
// normalized to "default". Returns ErrCFNotFound if cf was not
// configured at New. For atomic multi-write commits, use Batch.
func (s *Store) Put(cf string, key, value []byte) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	cfh, err := s.resolveCF(cf)
	if err != nil {
		return err
	}
	return s.db.PutCF(s.wo, cfh, key, value)
}

// Get retrieves the value for key from the named CF. Returns
// (value, true, nil) if found, (nil, false, nil) if not. The returned
// value is a fresh copy owned by the caller.
func (s *Store) Get(cf string, key []byte) ([]byte, bool, error) {
	if err := s.checkOpen(); err != nil {
		return nil, false, err
	}
	cfh, err := s.resolveCF(cf)
	if err != nil {
		return nil, false, err
	}
	slice, err := s.db.GetCF(s.ro, cfh, key)
	if err != nil {
		return nil, false, err
	}
	defer slice.Free()
	if !slice.Exists() {
		return nil, false, nil
	}
	// Copy because slice.Data() is owned by RocksDB and freed by
	// slice.Free().
	out := make([]byte, slice.Size())
	copy(out, slice.Data())
	return out, true, nil
}

// Delete removes key from the named CF. Idempotent at the wrapper
// level: returns no error if the key didn't exist.
func (s *Store) Delete(cf string, key []byte) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	cfh, err := s.resolveCF(cf)
	if err != nil {
		return err
	}
	return s.db.DeleteCF(s.wo, cfh, key)
}

// Iterate returns an iterator over keys in the named CF that share
// prefix. An empty prefix iterates every key in the CF. Keys come back
// in sorted byte order. Caller MUST Close the iterator.
func (s *Store) Iterate(cf string, prefix []byte) Iter {
	if err := s.checkOpen(); err != nil {
		return &errIter{err: err}
	}
	cfh, err := s.resolveCF(cf)
	if err != nil {
		return &errIter{err: err}
	}
	it := s.db.NewIteratorCF(s.ro, cfh)
	pcopy := make([]byte, len(prefix))
	copy(pcopy, prefix)
	it.Seek(pcopy)
	return &prefixIter{it: it, prefix: pcopy}
}

// Close cleanly shuts down the underlying RocksDB and releases the
// flock. Idempotent: a second Close is a no-op. Calling Close on a
// Store that was never successfully Opened is also a no-op.
func (s *Store) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	if s.db == nil {
		// New'd but Open never succeeded — nothing to clean up.
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

// doOpen is the actual grocksdb-side open. Called at most once per
// Store via openOnce.Do.
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

	// Time the actual grocksdb open. An existing store with a
	// populated WAL or many L0 files can take seconds to recover;
	// surface that in the on-open log so a slow restart is visible.
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

	logOpenState(s.cfg.Logger, abs, s, elapsed)
	return nil
}

// logOpenState emits a single Info line at Open time summarizing the
// store's on-disk + in-memory state and how long the grocksdb open
// itself took. Critical for diagnosing slow restarts: large WAL → many
// in-flight commits to replay (drives elapsed up); high L0 count →
// pending compaction; large memtable → recent crash with unflushed data.
func logOpenState(log *supportlog.Entry, abs string, s *Store, elapsed time.Duration) {
	memtable := readIntProperty(s.db, "rocksdb.cur-size-active-mem-table")
	l0Count := readIntProperty(s.db, "rocksdb.num-files-at-level0")
	sstSize := readIntProperty(s.db, "rocksdb.total-sst-files-size")
	walSize := walDirSize(abs)

	log.Infof(
		"[ROCKSDB:OPEN] path=%s elapsed=%s WAL size=%s L0 file count=%s data size=%s memtable size=%s",
		abs,
		format.Duration(elapsed),
		format.Bytes(walSize),
		format.Number(l0Count),
		format.Bytes(sstSize),
		format.Bytes(memtable),
	)
}

// readIntProperty parses a RocksDB property string as int64. Empty or
// unparseable values degrade to zero so the on-open log stays
// informative when a property name isn't supported by the linked
// grocksdb version.
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

// walDirSize sums the byte sizes of *.log files (RocksDB's WAL
// segments) in the store directory. RocksDB does not expose a single
// "WAL size" property.
func walDirSize(dir string) int64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var total int64
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
		total += info.Size()
	}
	return total
}

// Iter is the prefix-scan iterator returned by Store.Iterate.
//
// Lifetime: Close MUST be called when iteration is done. Key / Value
// byte slices are valid only between Next-returning-true and the next
// Next call (RocksDB owns the buffer); copy them to retain past that.
type Iter interface {
	Next() bool
	Key() []byte
	Value() []byte
	Err() error
	Close() error
}

// prefixIter wraps a grocksdb.Iterator with manual prefix-bounds
// checking. Independent of CF tuning, which matches the wrapper's
// schema-agnostic role.
type prefixIter struct {
	it      *grocksdb.Iterator
	prefix  []byte
	started bool // first Next() doesn't advance — Seek already positioned
}

func (i *prefixIter) Next() bool {
	if !i.started {
		i.started = true
	} else {
		i.it.Next()
	}
	if !i.it.Valid() {
		return false
	}
	k := i.it.Key()
	defer k.Free()
	return hasPrefix(k.Data(), i.prefix)
}

func (i *prefixIter) Key() []byte {
	k := i.it.Key()
	defer k.Free()
	out := make([]byte, k.Size())
	copy(out, k.Data())
	return out
}

func (i *prefixIter) Value() []byte {
	v := i.it.Value()
	defer v.Free()
	out := make([]byte, v.Size())
	copy(out, v.Data())
	return out
}

func (i *prefixIter) Err() error { return i.it.Err() }

func (i *prefixIter) Close() error {
	if i.it != nil {
		i.it.Close()
		i.it = nil
	}
	return nil
}

// errIter is returned when Iterate fails up-front (closed store,
// unknown CF). Lets callers drive the standard iterator loop without
// branching.
type errIter struct{ err error }

func (i *errIter) Next() bool    { return false }
func (i *errIter) Key() []byte   { return nil }
func (i *errIter) Value() []byte { return nil }
func (i *errIter) Err() error    { return i.err }
func (i *errIter) Close() error  { return nil }

// hasPrefix is a local equivalent of bytes.HasPrefix kept inline to
// avoid the import.
func hasPrefix(s, prefix []byte) bool {
	if len(s) < len(prefix) {
		return false
	}
	for i := range prefix {
		if s[i] != prefix[i] {
			return false
		}
	}
	return true
}
