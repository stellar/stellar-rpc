package streaming

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// Single-process enforcement (design "Single-process enforcement"). The daemon
// holds a kernel flock on a LOCK file under EVERY independently configurable
// storage root — the catalog, each immutable_storage tree, AND the
// hot_storage tree. A second daemon that touches any shared root fails fast.
//
// Why all roots and not just the catalog: [catalog], each
// [immutable_storage.*] path, and [streaming.hot_storage] are independently
// configurable, so two daemons with DIFFERENT catalogs could still share an
// artifact tree or a hot-DB tree. The hot root matters most — its hot/{chunk}
// DBs are the only copy of recently-ingested ledgers, independently
// created/opened/deleted by ingestion and discard, so two daemons sharing it
// would corrupt or delete that sole copy.
//
// A kernel flock is the right primitive: it releases on ANY process exit
// (including kill -9 / a crash), so a stale lock never strands the next start —
// nothing on disk to clean up.

// ErrRootLocked is returned when a LOCK file in a configured root is already
// held by another process. It wraps the offending root so the daemon can name
// it in the operator-facing error.
var ErrRootLocked = errors.New("streaming: storage root is locked by another process")

// lockFileName is the per-root lock file. Kept distinct from RocksDB's own
// "LOCK" so the catalog root's flock and RocksDB's internal lock never
// collide — the catalog root carries both, on different files.
const lockFileName = "stellar-rpc-fullhistory.lock"

// RootLocks holds the flock handles for every configured storage root. Release
// (defer'd by the daemon for the process's whole life) unlocks and closes them
// all; the kernel also drops them on any process exit.
type RootLocks struct {
	files []*os.File
}

// LockRoots takes a non-blocking exclusive flock on the LOCK file in each
// distinct root in roots, in the order given. Duplicate paths (e.g. the
// immutable trees all defaulting under default_data_dir is NOT a duplicate —
// they are distinct subdirs — but a caller passing the same root twice) are
// de-duplicated so one root is locked once. On the FIRST root that is already
// held by another process it releases everything acquired so far and returns
// ErrRootLocked naming that root — fail fast, leak nothing.
//
// Each root directory is created if absent (MkdirAll): a fresh deployment locks
// before any store opens, and the lock file must have a directory to live in.
func LockRoots(roots ...string) (*RootLocks, error) {
	locks := &RootLocks{}
	seen := make(map[string]struct{}, len(roots))
	for _, root := range roots {
		if root == "" {
			continue
		}
		abs, err := filepath.Abs(root)
		if err != nil {
			locks.Release()
			return nil, fmt.Errorf("streaming: resolve lock root %q: %w", root, err)
		}
		if _, dup := seen[abs]; dup {
			continue
		}
		seen[abs] = struct{}{}

		f, err := lockOne(abs)
		if err != nil {
			locks.Release()
			return nil, err
		}
		locks.files = append(locks.files, f)
	}
	return locks, nil
}

// lockOne creates root (if absent), opens its LOCK file, and takes a
// non-blocking exclusive flock. An EWOULDBLOCK means another live process holds
// it — surfaced as ErrRootLocked, the fail-fast case. Any other error (mkdir,
// open, a non-contention flock failure) surfaces verbatim.
func lockOne(root string) (*os.File, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("streaming: create lock root %q: %w", root, err)
	}
	path := filepath.Join(root, lockFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("streaming: open lock file %q: %w", path, err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %q (another daemon is using it)", ErrRootLocked, root)
		}
		return nil, fmt.Errorf("streaming: flock %q: %w", path, err)
	}
	return f, nil
}

// Release unlocks and closes every held lock file. Idempotent: a second call is
// a no-op. Closing the fd drops the flock; the explicit unix.Flock(LOCK_UN) is
// belt-and-suspenders so the lock is gone the instant Release returns rather
// than whenever the fd's last reference is collected.
func (l *RootLocks) Release() {
	if l == nil {
		return
	}
	for _, f := range l.files {
		_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
		_ = f.Close()
	}
	l.files = nil
}
