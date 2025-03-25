package integrationtest

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
	"github.com/stellar/stellar-rpc/protocol"
)

func TestGetLedgerEntriesNotFound(t *testing.T) {
	t.Run("WithCore", func(t *testing.T) {
		testGetLedgerEntriesNotFound(t, true)
	})
	t.Run("WithoutCore", func(t *testing.T) {
		testGetLedgerEntriesNotFound(t, false)
	})
}

func testGetLedgerEntriesNotFound(t *testing.T, useCore bool) {
	test := infrastructure.NewTest(t, &infrastructure.TestConfig{
		EnableCoreHTTPQueryServer: useCore,
	})
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

	assert.Empty(t, result.Entries)
	assert.Positive(t, result.LatestLedger)
}

func TestGetLedgerEntriesInvalidParams(t *testing.T) {
	t.Run("WithCore", func(t *testing.T) {
		testGetLedgerEntriesInvalidParams(t, true)
	})
	t.Run("WithoutCore", func(t *testing.T) {
		testGetLedgerEntriesInvalidParams(t, false)
	})
}

func testGetLedgerEntriesInvalidParams(t *testing.T, useCore bool) {
	test := infrastructure.NewTest(t, &infrastructure.TestConfig{
		EnableCoreHTTPQueryServer: useCore,
	})

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
	t.Run("WithCore", func(t *testing.T) {
		testGetLedgerEntriesSucceeds(t, true)
	})
	t.Run("WithoutCore", func(t *testing.T) {
		testGetLedgerEntriesSucceeds(t, false)
	})
}

func testGetLedgerEntriesSucceeds(t testing.TB, useCore bool) {
	test := infrastructure.NewTest(t, &infrastructure.TestConfig{
		EnableCoreHTTPQueryServer: useCore,
	})
	_, contractID, contractHash := test.CreateHelloWorldContract()

	contractCodeKeyB64, err := xdr.MarshalBase64(xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeContractCode,
		ContractCode: &xdr.LedgerKeyContractCode{
			Hash: contractHash,
		},
	})
	require.NoError(t, err)

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
	require.Len(t, result.Entries, 2)
	require.Positive(t, result.LatestLedger)

	require.Positive(t, result.Entries[0].LastModifiedLedger)
	require.LessOrEqual(t, result.Entries[0].LastModifiedLedger, result.LatestLedger)
	require.NotNil(t, result.Entries[0].LiveUntilLedgerSeq)
	require.Greater(t, *result.Entries[0].LiveUntilLedgerSeq, result.LatestLedger)
	require.Equal(t, contractCodeKeyB64, result.Entries[0].KeyXDR)
	var firstEntry xdr.LedgerEntryData
	require.NoError(t, xdr.SafeUnmarshalBase64(result.Entries[0].DataXDR, &firstEntry))
	require.Equal(t, xdr.LedgerEntryTypeContractCode, firstEntry.Type)
	require.Equal(t, infrastructure.GetHelloWorldContract(), firstEntry.MustContractCode().Code)

	require.Positive(t, result.Entries[1].LastModifiedLedger)
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

func benchmarkGetLedgerEntries(b *testing.B, useCore bool) {
	sqlitePath := ""
	captivecoreStoragePath := ""
	dir := os.Getenv("STELLAR_RPC_BENCH_BASE_STORAGE_DIR")
	if dir != "" {
		tmp, err := os.MkdirTemp(dir, "rpcbench") //nolint:usetesting
		require.NoError(b, err)
		b.Cleanup(func() { _ = os.RemoveAll(tmp) })
		sqlitePath = path.Join(tmp, "stellar_rpc.sqlite")
		captivecoreStoragePath = tmp
	}

	test := infrastructure.NewTest(b, &infrastructure.TestConfig{
		EnableCoreHTTPQueryServer: useCore,
		SQLitePath:                sqlitePath,
		CaptiveCoreStoragePath:    captivecoreStoragePath,
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
	require.NoError(b, err)

	keys := []string{contractCodeKeyB64, notFoundKeyB64, contractInstanceKeyB64}
	request := protocol.GetLedgerEntriesRequest{
		Keys: keys,
	}

	client := test.GetRPCLient()

	b.ResetTimer()

	for range b.N {
		result, err := client.GetLedgerEntries(context.Background(), request)
		b.StopTimer()
		require.NoError(b, err)
		require.Len(b, result.Entries, 2)
		require.Positive(b, result.LatestLedger)
		b.StartTimer()
	}
}

func BenchmarkGetLedgerEntries(b *testing.B) {
	b.Run("WithCore", func(b *testing.B) {
		benchmarkGetLedgerEntries(b, true)
	})
	b.Run("WithoutCore", func(b *testing.B) {
		benchmarkGetLedgerEntries(b, false)
	})
}
