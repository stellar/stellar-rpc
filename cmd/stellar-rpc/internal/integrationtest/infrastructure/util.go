package infrastructure

import (
	"net"
	"path/filepath"
	"runtime"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go/txnbuild"
)

//go:noinline
func GetCurrentDirectory() string {
	_, currentFilename, _, _ := runtime.Caller(1)
	return filepath.Dir(currentFilename)
}

func getFreeTCPPort(t require.TestingT) uint16 {
	var a *net.TCPAddr
	a, err := net.ResolveTCPAddr("tcp", "localhost:0")
	require.NoError(t, err)
	var l *net.TCPListener
	l, err = net.ListenTCP("tcp", a)
	require.NoError(t, err)
	defer l.Close()
	return uint16(l.Addr().(*net.TCPAddr).Port)
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
