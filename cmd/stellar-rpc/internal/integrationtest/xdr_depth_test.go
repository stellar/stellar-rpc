package integrationtest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	client "github.com/stellar/go-stellar-sdk/clients/rpcclient"
	"github.com/stellar/go-stellar-sdk/keypair"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/protocols/stellarcore"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
)

// coreMaxScVecNesting is the deepest ScVec nesting of a contract-call argument
// that stellar-core admits in a transaction envelope.
const coreMaxScVecNesting = 246

// TestCoreAdmissionDepthLimit checks that Core admits a contract-call argument
// nested coreMaxScVecNesting ScVec levels deep and rejects one more level as
// malformed.
func TestCoreAdmissionDepthLimit(t *testing.T) {
	test := infrastructure.NewTest(t, nil)
	client := test.GetRPCLient()

	payer := keypair.MustRandom()
	test.SendMasterOperation(&txnbuild.CreateAccount{
		Destination: payer.Address(),
		Amount:      "1000",
	})

	tooDeep := signAndSend(t, client, loadAccount(t, client, payer), payer,
		deepInvokeOperation(coreMaxScVecNesting+1))
	require.Equal(t, stellarcore.TXStatusError, tooDeep.Status)
	var tooDeepResult xdr.TransactionResult
	require.NoError(t, xdr.SafeUnmarshalBase64(tooDeep.ErrorResultXDR, &tooDeepResult))
	require.Equal(t, xdr.TransactionResultCodeTxMalformed, tooDeepResult.Result.Code)

	deep := signAndSend(t, client, test.MasterAccount(), test.MasterKey(),
		deepInvokeOperation(coreMaxScVecNesting))
	requirePending(t, deep)
}

// deepInvokeOperation calls a contract that does not exist with one argument:
// a u32 wrapped in depth single-element vectors. It carries its own Soroban
// resources, so it needs no simulation.
func deepInvokeOperation(depth int) *txnbuild.InvokeHostFunction {
	u := xdr.Uint32(7)
	arg := xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &u}
	for range depth {
		vec := &xdr.ScVec{arg}
		arg = xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &vec}
	}

	contractID := xdr.ContractId{0xde, 0xad}
	contract := xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &contractID}
	instanceKey := xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeContractData,
		ContractData: &xdr.LedgerKeyContractData{
			Contract:   contract,
			Key:        xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance},
			Durability: xdr.ContractDataDurabilityPersistent,
		},
	}
	return &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
			InvokeContract: &xdr.InvokeContractArgs{
				ContractAddress: contract,
				FunctionName:    "f",
				Args:            xdr.ScVec{arg},
			},
		},
		// These resources exceed Core's initial Soroban limits, so they rely on
		// the unlimited-limits upgrade that NewTest(t, nil) applies.
		Ext: xdr.TransactionExt{V: 1, SorobanData: &xdr.SorobanTransactionData{
			Resources: xdr.SorobanResources{
				Footprint:     xdr.LedgerFootprint{ReadOnly: []xdr.LedgerKey{instanceKey}},
				Instructions:  50_000_000,
				DiskReadBytes: 10_000,
			},
			ResourceFee: 2_000_000,
		}},
	}
}

func loadAccount(t *testing.T, client *client.Client, kp *keypair.Full) txnbuild.Account {
	account, err := client.LoadAccount(t.Context(), kp.Address())
	require.NoError(t, err)
	return account
}

func signAndSend(
	t *testing.T, client *client.Client, account txnbuild.Account, kp *keypair.Full, op txnbuild.Operation,
) protocol.SendTransactionResponse {
	tx, err := txnbuild.NewTransaction(infrastructure.CreateTransactionParams(account, op))
	require.NoError(t, err)
	tx, err = tx.Sign(infrastructure.StandaloneNetworkPassphrase, kp)
	require.NoError(t, err)
	b64, err := tx.Base64()
	require.NoError(t, err)
	return infrastructure.SendTransaction(t, client, b64)
}

func requirePending(t *testing.T, result protocol.SendTransactionResponse) {
	if !assert.Equal(t, stellarcore.TXStatusPending, result.Status) {
		var txResult xdr.TransactionResult
		require.NoError(t, xdr.SafeUnmarshalBase64(result.ErrorResultXDR, &txResult))
		t.Logf("error: %#v\n", txResult)
		t.FailNow()
	}
}
