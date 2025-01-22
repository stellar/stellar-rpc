package integrationtest

import (
	"context"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
	"github.com/stellar/stellar-rpc/protocol"
)

func TestGetLedgerEntriesNotFound(t *testing.T) {
	test := infrastructure.NewTest(t, nil)
	client := test.GetRPCLient()

	hash := xdr.Hash{0xa, 0xb}
	keyB64, err := xdr.MarshalBase64(xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeContractData,
		ContractData: &xdr.LedgerKeyContractData{
			Contract: xdr.ScAddress{
				Type:       xdr.ScAddressTypeScAddressTypeContract,
				ContractId: &hash,
			},
			Key: xdr.ScVal{
				Type: xdr.ScValTypeScvLedgerKeyContractInstance,
			},
			Durability: xdr.ContractDataDurabilityPersistent,
		},
	})
	require.NoError(t, err)

	var keys []string
	keys = append(keys, keyB64)
	request := protocol.GetLedgerEntriesRequest{
		Keys: keys,
	}

	result, err := client.GetLedgerEntries(context.Background(), request)
	require.NoError(t, err)

	assert.Equal(t, 0, len(result.Entries))
	assert.Greater(t, result.LatestLedger, uint32(0))
}

func TestGetLedgerEntriesInvalidParams(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	client := test.GetRPCLient()

	var keys []string
	keys = append(keys, "<>@@#$")
	request := protocol.GetLedgerEntriesRequest{
		Keys: keys,
	}

	_, jsonRPCErr := client.GetLedgerEntries(context.Background(), request)
	assert.Contains(t, jsonRPCErr.(*jrpc2.Error).Message, "cannot unmarshal key value")
	assert.Equal(t, jrpc2.InvalidParams, jsonRPCErr.(*jrpc2.Error).Code)
}

func TestGetLedgerEntriesSucceeds(t *testing.T) {
	test := infrastructure.NewTest(t, nil)
	_, contractID, contractHash := test.CreateHelloWorldContract()

	contractCodeKeyB64, err := xdr.MarshalBase64(xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeContractCode,
		ContractCode: &xdr.LedgerKeyContractCode{
			Hash: contractHash,
		},
	})

	// Doesn't exist.
	notFoundKeyB64, err := xdr.MarshalBase64(getCounterLedgerKey(contractID))
	require.NoError(t, err)

	contractIDHash := xdr.Hash(contractID)
	contractInstanceKeyB64, err := xdr.MarshalBase64(xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeContractData,
		ContractData: &xdr.LedgerKeyContractData{
			Contract: xdr.ScAddress{
				Type:       xdr.ScAddressTypeScAddressTypeContract,
				ContractId: &contractIDHash,
			},
			Key: xdr.ScVal{
				Type: xdr.ScValTypeScvLedgerKeyContractInstance,
			},
			Durability: xdr.ContractDataDurabilityPersistent,
		},
	})
	require.NoError(t, err)

	keys := []string{contractCodeKeyB64, notFoundKeyB64, contractInstanceKeyB64}
	request := protocol.GetLedgerEntriesRequest{
		Keys: keys,
	}

	result, err := test.GetRPCLient().GetLedgerEntries(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 2, len(result.Entries))
	require.Greater(t, result.LatestLedger, uint32(0))

	require.Greater(t, result.Entries[0].LastModifiedLedger, uint32(0))
	require.LessOrEqual(t, result.Entries[0].LastModifiedLedger, result.LatestLedger)
	require.NotNil(t, result.Entries[0].LiveUntilLedgerSeq)
	require.Greater(t, *result.Entries[0].LiveUntilLedgerSeq, result.LatestLedger)
	require.Equal(t, contractCodeKeyB64, result.Entries[0].KeyXDR)
	var firstEntry xdr.LedgerEntryData
	require.NoError(t, xdr.SafeUnmarshalBase64(result.Entries[0].DataXDR, &firstEntry))
	require.Equal(t, xdr.LedgerEntryTypeContractCode, firstEntry.Type)
	require.Equal(t, infrastructure.GetHelloWorldContract(), firstEntry.MustContractCode().Code)

	require.Greater(t, result.Entries[1].LastModifiedLedger, uint32(0))
	require.LessOrEqual(t, result.Entries[1].LastModifiedLedger, result.LatestLedger)
	require.NotNil(t, result.Entries[1].LiveUntilLedgerSeq)
	require.Greater(t, *result.Entries[1].LiveUntilLedgerSeq, result.LatestLedger)
	require.Equal(t, contractInstanceKeyB64, result.Entries[1].KeyXDR)
	var secondEntry xdr.LedgerEntryData
	require.NoError(t, xdr.SafeUnmarshalBase64(result.Entries[1].DataXDR, &secondEntry))
	require.Equal(t, xdr.LedgerEntryTypeContractData, secondEntry.Type)
	require.True(t, secondEntry.MustContractData().Key.Equals(xdr.ScVal{
		Type: xdr.ScValTypeScvLedgerKeyContractInstance,
	}))
}
