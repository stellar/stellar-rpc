package integrationtest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
)

func TestUpgradeFrom20To21(t *testing.T) {
	if infrastructure.GetCoreMaxSupportedProtocol() != 21 {
		t.Skip("Only test this for protocol 21")
	}
	test := infrastructure.NewTest(t, &infrastructure.TestConfig{
		ProtocolVersion: 20,
	})

	test.UploadHelloWorldContract()

	// Upgrade to protocol 21 and re-upload the contract, which should cause a
	// caching of the contract estimations
	test.UpgradeProtocol(21)
	// Wait for the ledger to advance, so that the simulation library passes the
	// right protocol number
	rpcDB := test.GetDaemon().GetDB()
	initialLedgerSequence, err := db.NewLedgerReader(rpcDB).GetLatestLedgerSequence(t.Context())
	require.NoError(t, err)
	require.Eventually(t,
		func() bool {
			newLedgerSequence, err := db.NewLedgerReader(rpcDB).GetLatestLedgerSequence(t.Context())
			require.NoError(t, err)
			return newLedgerSequence > initialLedgerSequence
		},
		time.Minute,
		time.Second,
	)

	_, contractID, _ := test.CreateHelloWorldContract()

	contractFnParameterSym := xdr.ScSymbol("world")
	test.InvokeHostFunc(
		contractID,
		"hello",
		xdr.ScVal{
			Type: xdr.ScValTypeScvSymbol,
			Sym:  &contractFnParameterSym,
		},
	)
}
