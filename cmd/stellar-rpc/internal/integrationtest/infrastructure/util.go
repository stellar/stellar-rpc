package infrastructure

import (
	"net"
	"path/filepath"
	"runtime"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/txnbuild"
)

//go:noinline
func GetCurrentDirectory() string {
	_, currentFilename, _, _ := runtime.Caller(1)
	return filepath.Dir(currentFilename)
}

// getFreeTCPPorts allocates n distinct free TCP ports. It keeps all listeners
// open until all ports are assigned, preventing the OS from handing out the
// same port twice.
func getFreeTCPPorts(t require.TestingT, n int) []uint16 {
	listeners := make([]*net.TCPListener, n)
	ports := make([]uint16, n)
	for i := 0; i < n; i++ {
		a, err := net.ResolveTCPAddr("tcp", "localhost:0")
		require.NoError(t, err)
		l, err := net.ListenTCP("tcp", a)
		require.NoError(t, err)
		listeners[i] = l
		ports[i] = uint16(l.Addr().(*net.TCPAddr).Port)
	}
	for _, l := range listeners {
		l.Close()
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
