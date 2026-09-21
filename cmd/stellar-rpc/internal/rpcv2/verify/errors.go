package verify

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"syscall"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// isInfrastructure reports whether err is a fault of the run's environment
// rather than a verdict on the data read: a canceled run, or a file that is
// missing, unreadable, failing at the operating-system level, or served by
// a store that has been torn down. Everything else an artifact reader
// returns (a bad magic number, a truncated record, a failed checksum, a
// foreign format, offsets that do not add up) describes the bytes that are
// there, and is recorded as a mismatch.
func isInfrastructure(err error) bool {
	var pathErr *fs.PathError
	var sysErr *os.SyscallError
	var errno syscall.Errno
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrPermission) ||
		errors.Is(err, stores.ErrStoreClosed) ||
		errors.As(err, &pathErr) || errors.As(err, &sysErr) || errors.As(err, &errno)
}
