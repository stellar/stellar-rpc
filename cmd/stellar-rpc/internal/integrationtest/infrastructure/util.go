package infrastructure

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/txnbuild"
)

//go:noinline
func GetCurrentDirectory() string {
	_, currentFilename, _, _ := runtime.Caller(1)
	return filepath.Dir(currentFilename)
}

// Captive core reads fixed port numbers from its config file, so the harness
// has to choose them before core starts. They come from a range the kernel
// never uses for outgoing connections or for Docker's dynamic host ports
// (32768-60999 on Linux, 49152-65535 on macOS). Asking the kernel for port 0
// handed out ports from that range, and a client socket sometimes took the
// port back before core bound it: core then died with "bind: Address already
// in use" and the test failed.
//
// A CI leg runs two test binaries at once (the shared package and the
// daemon's own), and two processes walking the same range collide. Each
// process therefore claims one of 25 ranges of 500 ports by holding an
// exclusive flock on a lock file in the temp dir for its whole lifetime. The
// kernel drops the lock when the process ends, so a crashed run leaves no
// stale claim.
const (
	testPortRangeCount = 25
	testPortRangeSize  = 500
)

var (
	testPortBase     uint32
	testPortBaseOnce sync.Once
	testPortNext     atomic.Uint32
	// Only ever written: the open file is what keeps the flock alive for the
	// life of the process.
	testPortLock *os.File //nolint:unused // see above
)

func claimTestPortRange(t require.TestingT) {
	for n := range testPortRangeCount {
		name := filepath.Join(os.TempDir(), fmt.Sprintf("stellar-rpc-itest-ports-%d.lock", n))
		f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o666)
		if err != nil {
			continue
		}
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			_ = f.Close()
			continue
		}
		testPortLock = f
		testPortBase = uint32(20000 + n*testPortRangeSize)
		return
	}
	require.Fail(t, "no free test port range: every lock file in the temp dir is held")
}

// getFreeTCPPorts hands out n distinct ports that nothing on this host is
// listening on.
func getFreeTCPPorts(t require.TestingT, n int) []uint16 {
	testPortBaseOnce.Do(func() { claimTestPortRange(t) })
	ports := make([]uint16, 0, n)
	for len(ports) < n {
		port := testPortBase + testPortNext.Add(1) - 1
		require.Less(t, port, testPortBase+testPortRangeSize, "ran out of test ports")
		l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			continue // something else already listens here, skip it
		}
		require.NoError(t, l.Close())
		ports = append(ports, uint16(port))
	}
	return ports
}

func CreateTransactionParams(account txnbuild.Account, op txnbuild.Operation) txnbuild.TransactionParams {
	return txnbuild.TransactionParams{
		SourceAccount:        account,
		IncrementSequenceNum: true,
		Operations:           []txnbuild.Operation{op},
		BaseFee:              txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	}
}
