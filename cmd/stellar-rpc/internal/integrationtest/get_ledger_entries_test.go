package integrationtest

import (
	"context"
	"os"
	"path"
	"strings"
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

	hash := xdr.ContractId{0xa, 0xb}
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

	assert.Empty(t, result.Entries)
	assert.Positive(t, result.LatestLedger)
}

func TestGetLedgerEntriesInvalidParams(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	client := test.GetRPCLient()

	var keys []string
	keys = append(keys, "<>@@#$")
	request := protocol.GetLedgerEntriesRequest{
		Keys: keys,
	}

	_, err := client.GetLedgerEntries(context.Background(), request)
	var jsonRPCErr *jrpc2.Error
	require.ErrorAs(t, err, &jsonRPCErr)
	assert.Contains(t, jsonRPCErr.Message, "cannot unmarshal key value")
	assert.Equal(t, jrpc2.InvalidParams, jsonRPCErr.Code)
}

func TestGetLedgerEntriesSucceeds(t *testing.T) {
	test := infrastructure.NewTest(t, nil)
	accountID := test.MasterAccount().GetAccountID()
	_, contractID, contractHash := test.CreateHelloWorldContract()

	// ContractCode entry, exists, with a TTL entry, so will have a LiveUntilLedgerSeq.
	contractCodeKeyB64, err := xdr.MarshalBase64(xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeContractCode,
		ContractCode: &xdr.LedgerKeyContractCode{
			Hash: contractHash,
		},
	})
	require.NoError(t, err)

	// Doesn't exist, won't be returned in response.
	notFoundKeyB64, err := xdr.MarshalBase64(getCounterLedgerKey(contractID))
	require.NoError(t, err)

	// ContractData entry exists, with a TTL entry, so will have a LiveUntilLedgerSeq.
	contractIDHash := xdr.ContractId(contractID)
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

	// Account entry, exists, but does not have a TTL entry, so no LiveUntilLedgerSeq.
	accountKeyB64, err := xdr.MarshalBase64(xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeAccount,
		Account: &xdr.LedgerKeyAccount{
			AccountId: xdr.MustAddress(accountID),
		},
	})
	require.NoError(t, err)

	keys := []string{contractCodeKeyB64, notFoundKeyB64, contractInstanceKeyB64, accountKeyB64}
	request := protocol.GetLedgerEntriesRequest{
		Keys: keys,
	}

	result, err := test.GetRPCLient().GetLedgerEntries(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, result.Entries, 3)
	require.Positive(t, result.LatestLedger)

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

	require.Greater(t, result.Entries[2].LastModifiedLedger, uint32(0))
	require.LessOrEqual(t, result.Entries[2].LastModifiedLedger, result.LatestLedger)
	require.Nil(t, result.Entries[2].LiveUntilLedgerSeq)
	require.Equal(t, accountKeyB64, result.Entries[2].KeyXDR)
	var thirdEntry xdr.LedgerEntryData
	require.NoError(t, xdr.SafeUnmarshalBase64(result.Entries[2].DataXDR, &thirdEntry))
	require.Equal(t, xdr.LedgerEntryTypeAccount, thirdEntry.Type)
}

func BenchmarkGetLedgerEntries(b *testing.B) {
	sqlitePath := ""
	captivecoreStoragePath := ""
	// Needed to test performance on specific storage types (e.g. AWS EBS directories)
	dir := os.Getenv("STELLAR_RPC_BENCH_BASE_STORAGE_DIR")
	if dir != "" {
		tmp, err := os.MkdirTemp(dir, "rpcbench") //nolint:usetesting
		require.NoError(b, err)
		b.Cleanup(func() { _ = os.RemoveAll(tmp) })
		sqlitePath = path.Join(tmp, "stellar_rpc.sqlite")
		captivecoreStoragePath = tmp
	}
	// Workaround to test manually on Kubernetes (which doesn't support docker-compose)
	hostPortsEnv := os.Getenv("STELLAR_RPC_BENCH_CORE_HOST_PORTS")
	var testOnlyRPC *infrastructure.TestOnlyRPCConfig
	if hostPortsEnv != "" {
		hostPorts := strings.Split(hostPortsEnv, ";")
		require.Len(b, hostPorts, 3)
		testOnlyRPC = &infrastructure.TestOnlyRPCConfig{
			CorePorts: infrastructure.TestCorePorts{
				CoreHostPort:        hostPorts[0],
				CoreArchiveHostPort: hostPorts[1],
				CoreHTTPHostPort:    hostPorts[2],
			},
			DontWait: false,
		}
	}
	test := infrastructure.NewTest(b, &infrastructure.TestConfig{
		SQLitePath:             sqlitePath,
		CaptiveCoreStoragePath: captivecoreStoragePath,
		OnlyRPC:                testOnlyRPC,
	})
	_, contractID, contractHash := test.CreateHelloWorldContract()

	contractCodeKeyB64, err := xdr.MarshalBase64(xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeContractCode,
		ContractCode: &xdr.LedgerKeyContractCode{
			Hash: contractHash,
		},
	})
	require.NoError(b, err)

	// Doesn't exist.
	notFoundKeyB64, err := xdr.MarshalBase64(getCounterLedgerKey(contractID))
	require.NoError(b, err)

	contractIDHash := xdr.ContractId(contractID)
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
	require.NoError(b, err)

	keys := []string{contractCodeKeyB64, notFoundKeyB64, contractInstanceKeyB64}
	request := protocol.GetLedgerEntriesRequest{
		Keys: keys,
	}

	client := test.GetRPCLient()

	for b.Loop() {
		result, err := client.GetLedgerEntries(b.Context(), request)
		b.StopTimer()
		require.NoError(b, err)
		require.Len(b, result.Entries, 2)
		// False positive lint error: see https://github.com/Antonboom/testifylint/pull/236
		//nolint:testifylint
		require.Positive(b, result.LatestLedger)
		b.StartTimer()
	}
}
