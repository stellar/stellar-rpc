//go:build windows

package streaming

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

// lockBytes is the byte range each LOCK file's lock covers. The file's content
// is irrelevant — locking one byte at offset 0 is enough for whole-file mutual
// exclusion (a byte-range lock may extend past EOF on Windows).
const lockBytes = 1

// acquireLock is the Windows equivalent of a non-blocking exclusive flock:
// LockFileEx with LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY. It returns
// errLockHeld when another live process already holds it (ERROR_LOCK_VIOLATION,
// the fail-fast case); any other error surfaces verbatim. Like flock, the lock
// releases when the handle is closed and on any process exit (incl. a crash), so
// the same crash-release contract holds.
func acquireLock(f *os.File) error {
	err := windows.LockFileEx(
		windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0, lockBytes, 0, &windows.Overlapped{},
	)
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return errLockHeld
	}
	return err
}

// releaseLock drops the lock explicitly; closing the handle would too.
func releaseLock(f *os.File) {
	_ = windows.UnlockFileEx(windows.Handle(f.Fd()), 0, lockBytes, 0, &windows.Overlapped{})
}
