//go:build unix

package streaming

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// acquireLock takes a non-blocking exclusive flock on f. It returns errLockHeld
// when another live process already holds it (the fail-fast case); any other
// error surfaces verbatim. The kernel drops the lock on the fd's close and on
// any process exit (incl. kill -9 / a crash).
func acquireLock(f *os.File) error {
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) {
			return errLockHeld
		}
		return err
	}
	return nil
}

// releaseLock drops the flock explicitly (LOCK_UN); closing the fd would too.
func releaseLock(f *os.File) {
	_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
}
