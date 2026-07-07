package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/durable"
)

// Single-process enforcement (design "Single-process enforcement"). The daemon
// holds an exclusive OS lock on a LOCK file under EVERY independently configurable
// storage root — the catalog, each immutable artifact tree, AND the
// hot-storage tree. A second daemon touching any shared root fails fast.
//
// All roots, not just the catalog: every [storage] path (catalog, the immutable
// artifact trees, and hot) is independently configurable, so two daemons with
// DIFFERENT catalogs could still share an artifact or hot-DB tree. The hot
// root matters most — its hot/{chunk} DBs are the only copy of recently-ingested
// ledgers (created/opened/deleted by ingestion and discard), so sharing it would
// corrupt or delete that sole copy.
//
// A kernel flock is the right primitive: it releases on ANY process exit
// (including kill -9 / a crash), so a stale lock never strands the next start —
// nothing on disk to clean up. flock — and the directory fsyncs the one-write
// protocol relies on — are unix-only, so the daemon is a unix-only build (we
// ship no Windows binary).

// ErrRootLocked is returned when a LOCK file in a configured root is already
// held by another process. It wraps the offending root so the daemon can name
// it in the operator-facing error.
var ErrRootLocked = errors.New("fullhistory: storage root is locked by another process")

// lockFileName is the per-root lock file. Distinct from RocksDB's own "LOCK"
// so they never collide on the catalog root, which carries both.
const lockFileName = "stellar-rpc-fullhistory.lock"

// RootLocks holds the flock handles for every configured storage root. Release
// (defer'd by the daemon for the process's whole life) unlocks and closes them
// all; the kernel also drops them on any process exit.
type RootLocks struct {
	files []*os.File
}

// LockRoots takes a non-blocking exclusive flock on the LOCK file in each
// distinct root, in the order given. Duplicate paths are de-duplicated so one
// root is locked once (note: immutable trees defaulting under default_data_dir
// are distinct subdirs, NOT duplicates; this only catches the same root passed
// twice). On the FIRST root already held by another process it releases
// everything acquired so far and returns ErrRootLocked naming that root — fail
// fast, leak nothing.
//
// Each root is created if absent (MkdirAll) so a fresh deployment locks before
// any store opens and the lock file has a directory to live in.
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
			return nil, fmt.Errorf("fullhistory: resolve lock root %q: %w", root, err)
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
// non-blocking exclusive flock. EWOULDBLOCK (another live process holds it)
// surfaces as ErrRootLocked, the fail-fast case; any other error (mkdir, open,
// non-contention flock failure) surfaces verbatim.
func lockOne(root string) (*os.File, error) {
	existing := durable.DeepestExistingDir(root)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("fullhistory: create lock root %q: %w", root, err)
	}
	// Persist the just-created directory chain. MkdirAll does not fsync the
	// direntries naming the new dirs, and the one-write protocol's grandparent
	// fsync only reaches a root's contents, not the root's own link — so without
	// this a fresh-deploy crash could lose a whole storage tree while the synced
	// catalog still advertises a "frozen" artifact under it.
	if err := durable.FsyncNewDirs(existing, root); err != nil {
		return nil, fmt.Errorf("fullhistory: fsync lock root %q: %w", root, err)
	}
	path := filepath.Join(root, lockFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("fullhistory: open lock file %q: %w", path, err)
	}
	if err := acquireLock(f); err != nil {
		_ = f.Close()
		if errors.Is(err, ErrRootLocked) {
			return nil, fmt.Errorf("%w: %q (another daemon is using it)", ErrRootLocked, root)
		}
		return nil, fmt.Errorf("fullhistory: lock %q: %w", path, err)
	}
	return f, nil
}

// Release unlocks and closes every held lock file. Idempotent: a second call is
// a no-op. Closing the fd drops the OS lock; the explicit releaseLock is
// belt-and-suspenders so the lock is gone the instant Release returns, not when
// the fd's last reference is collected.
func (l *RootLocks) Release() {
	if l == nil {
		return
	}
	for _, f := range l.files {
		releaseLock(f)
		_ = f.Close()
	}
	l.files = nil
}

// fdInt narrows an *os.File's descriptor to the int unix.Flock wants. A file
// descriptor always fits in an int, so gosec's G115 overflow check is moot.
func fdInt(f *os.File) int {
	return int(f.Fd()) //nolint:gosec // G115: a file descriptor always fits in an int
}

// acquireLock takes a non-blocking exclusive flock on f. It returns ErrRootLocked
// when another live process already holds it (the fail-fast case); any other
// error surfaces verbatim. The kernel drops the lock on the fd's close and on
// any process exit (incl. kill -9 / a crash).
func acquireLock(f *os.File) error {
	if err := unix.Flock(fdInt(f), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) {
			return ErrRootLocked
		}
		return err
	}
	return nil
}

// releaseLock drops the flock explicitly (LOCK_UN); closing the fd would too.
func releaseLock(f *os.File) {
	_ = unix.Flock(fdInt(f), unix.LOCK_UN)
}
