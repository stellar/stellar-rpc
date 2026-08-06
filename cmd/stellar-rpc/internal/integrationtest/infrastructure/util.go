package infrastructure

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/txnbuild"
)

//go:noinline
func GetCurrentDirectory() string {
	_, currentFilename, _, _ := runtime.Caller(1)
	return filepath.Dir(currentFilename)
}

// Ports reserved for captive core are picked from this range, which sits below
// the kernel's ephemeral port range (32768+ on Linux, 49152+ on macOS). Sockets
// bound to "localhost:0" (e.g. the RPC daemon's endpoints) always get ephemeral
// ports, so they can never receive a port reserved here — even after the
// reservation listeners below are closed. Reserving straight from "localhost:0"
// caused exactly that collision: core would fail to start with "bind: Address
// already in use" because the daemon had been handed the reserved port.
const (
	reservedPortRangeStart = 20000
	reservedPortRangeEnd   = 30000
)

// getFreeTCPPorts allocates n distinct free TCP ports from the reserved range.
// It keeps all listeners open until all ports are assigned, preventing the OS
// from handing out the same port twice.
func getFreeTCPPorts(t require.TestingT, n int) []uint16 {
	const maxAttempts = 1000
	listeners := make([]*net.TCPListener, 0, n)
	defer func() {
		for _, l := range listeners {
			l.Close()
		}
	}()
	ports := make([]uint16, 0, n)
	for range maxAttempts {
		if len(ports) == n {
			break
		}
		port := reservedPortRangeStart + rand.IntN(reservedPortRangeEnd-reservedPortRangeStart)
		a, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("localhost:%d", port))
		require.NoError(t, err)
		l, err := net.ListenTCP("tcp", a)
		if err != nil {
			if errors.Is(err, syscall.EADDRINUSE) {
				// Port already in use (possibly by a previous iteration of this
				// loop, since we hold our listeners open). Try another one.
				continue
			}
			// Any other error (permissions, fd exhaustion, ...) will not be
			// fixed by retrying, so report it instead of burning all attempts.
			require.NoError(t, err)
		}
		listeners = append(listeners, l)
		ports = append(ports, uint16(port))
	}
	require.Len(t, ports, n, "could not find %d free ports in [%d, %d)",
		n, reservedPortRangeStart, reservedPortRangeEnd)
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
