package integrationtest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"

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
	initial, err := test.GetRPCLient().GetLatestLedger(t.Context())
	require.NoError(t, err)
	require.Eventually(t,
		func() bool {
			latest, err := test.GetRPCLient().GetLatestLedger(t.Context())
			require.NoError(t, err)
			return latest.Sequence > initial.Sequence
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
