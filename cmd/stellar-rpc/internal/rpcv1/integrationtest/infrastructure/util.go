package infrastructure

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"

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
// in use" and the test failed. The range is offset by process id so two test
// binaries on one machine do not walk the same ports.
var (
	testPortBase = uint32(20000 + (os.Getpid()%10)*1000)
	testPortNext atomic.Uint32
)

// getFreeTCPPorts hands out n distinct ports that nothing on this host is
// listening on.
func getFreeTCPPorts(t require.TestingT, n int) []uint16 {
	ports := make([]uint16, 0, n)
	for len(ports) < n {
		port := testPortBase + testPortNext.Add(1) - 1
		require.Less(t, port, testPortBase+1000, "ran out of test ports")
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
