package preflight

import (
	"context"
	"crypto/sha256"
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerentries"
)

var (
	mockContractID   = xdr.Hash{0xa, 0xb, 0xc}
	mockContractHash = xdr.Hash{0xd, 0xe, 0xf}
)

const (
	latestSimulateTransactionLedgerSeq = 2
	// Make sure it doesn't ttl
	entryTTLValue = 1000
)

var contractCostParams = func() *xdr.ContractCostParams {
	var result xdr.ContractCostParams

	for i := range 23 {
		result = append(result, xdr.ContractCostParamEntry{
			Ext:        xdr.ExtensionPoint{},
			ConstTerm:  xdr.Int64((i + 1) * 10),
			LinearTerm: xdr.Int64(i),
		})
	}

	return &result
}()

var mockLedgerEntriesWithoutTTLs = []xdr.LedgerEntry{
	{
		LastModifiedLedgerSeq: latestSimulateTransactionLedgerSeq - 1,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeContractData,
			ContractData: &xdr.ContractDataEntry{
				Contract: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: &mockContractID,
				},
				Key: xdr.ScVal{
					Type: xdr.ScValTypeScvLedgerKeyContractInstance,
				},
				Durability: xdr.ContractDataDurabilityPersistent,
				Val: xdr.ScVal{
					Type: xdr.ScValTypeScvContractInstance,
					Instance: &xdr.ScContractInstance{
						Executable: xdr.ContractExecutable{
							Type:     xdr.ContractExecutableTypeContractExecutableWasm,
							WasmHash: &mockContractHash,
						},
						Storage: nil,
					},
				},
			},
		},
	},
	{
		LastModifiedLedgerSeq: latestSimulateTransactionLedgerSeq,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeContractCode,
			ContractCode: &xdr.ContractCodeEntry{
				Hash: mockContractHash,
				Code: helloWorldContract,
			},
		},
	},
	{
		LastModifiedLedgerSeq: latestSimulateTransactionLedgerSeq,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId: xdr.ConfigSettingIdConfigSettingContractComputeV0,
				ContractCompute: &xdr.ConfigSettingContractComputeV0{
					LedgerMaxInstructions:           100000000,
					TxMaxInstructions:               100000000,
					FeeRatePerInstructionsIncrement: 1,
					TxMemoryLimit:                   100000000,
				},
			},
		},
	},
	{
		LastModifiedLedgerSeq: latestSimulateTransactionLedgerSeq,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId: xdr.ConfigSettingIdConfigSettingContractLedgerCostV0,
				ContractLedgerCost: &xdr.ConfigSettingContractLedgerCostV0{
					LedgerMaxReadLedgerEntries:     100,
					LedgerMaxReadBytes:             100,
					LedgerMaxWriteLedgerEntries:    100,
					LedgerMaxWriteBytes:            100,
					TxMaxReadLedgerEntries:         100,
					TxMaxReadBytes:                 100,
					TxMaxWriteLedgerEntries:        100,
					TxMaxWriteBytes:                100,
					FeeReadLedgerEntry:             100,
					FeeWriteLedgerEntry:            100,
					FeeRead1Kb:                     100,
					BucketListTargetSizeBytes:      100,
					WriteFee1KbBucketListLow:       1,
					WriteFee1KbBucketListHigh:      1,
					BucketListWriteFeeGrowthFactor: 1,
				},
			},
		},
	},
	{
		LastModifiedLedgerSeq: latestSimulateTransactionLedgerSeq,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId: xdr.ConfigSettingIdConfigSettingContractHistoricalDataV0,
				ContractHistoricalData: &xdr.ConfigSettingContractHistoricalDataV0{
					FeeHistorical1Kb: 100,
				},
			},
		},
	},
	{
		LastModifiedLedgerSeq: latestSimulateTransactionLedgerSeq,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId: xdr.ConfigSettingIdConfigSettingContractEventsV0,
				ContractEvents: &xdr.ConfigSettingContractEventsV0{
					TxMaxContractEventsSizeBytes: 10000,
					FeeContractEvents1Kb:         1,
				},
			},
		},
	},
	{
		LastModifiedLedgerSeq: 2,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId: xdr.ConfigSettingIdConfigSettingContractBandwidthV0,
				ContractBandwidth: &xdr.ConfigSettingContractBandwidthV0{
					LedgerMaxTxsSizeBytes: 100000,
					TxMaxSizeBytes:        1000,
					FeeTxSize1Kb:          1,
				},
			},
		},
	},
	{
		LastModifiedLedgerSeq: 2,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId: xdr.ConfigSettingIdConfigSettingStateArchival,
				StateArchivalSettings: &xdr.StateArchivalSettings{
					MaxEntryTtl:                    100,
					MinTemporaryTtl:                100,
					MinPersistentTtl:               100,
					PersistentRentRateDenominator:  100,
					TempRentRateDenominator:        100,
					MaxEntriesToArchive:            100,
					BucketListSizeWindowSampleSize: 100,
					EvictionScanSize:               100,
				},
			},
		},
	},
	{
		LastModifiedLedgerSeq: latestSimulateTransactionLedgerSeq,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId:            xdr.ConfigSettingIdConfigSettingContractCostParamsCpuInstructions,
				ContractCostParamsCpuInsns: contractCostParams,
			},
		},
	},
	{
		LastModifiedLedgerSeq: latestSimulateTransactionLedgerSeq,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId:            xdr.ConfigSettingIdConfigSettingContractCostParamsMemoryBytes,
				ContractCostParamsMemBytes: contractCostParams,
			},
		},
	},
	{
		LastModifiedLedgerSeq: latestSimulateTransactionLedgerSeq,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId:      xdr.ConfigSettingIdConfigSettingBucketlistSizeWindow,
				BucketListSizeWindow: &[]xdr.Uint64{100, 200},
			},
		},
	},
}

// Adds ttl entries to mockLedgerEntriesWithoutTTLs
var mockLedgerEntries = func() []xdr.LedgerEntry {
	result := make([]xdr.LedgerEntry, 0, len(mockLedgerEntriesWithoutTTLs))
	for _, entry := range mockLedgerEntriesWithoutTTLs {
		result = append(result, entry)

		if entry.Data.Type == xdr.LedgerEntryTypeContractData || entry.Data.Type == xdr.LedgerEntryTypeContractCode {
			key, err := entry.LedgerKey()
			if err != nil {
				panic(err)
			}
			bin, err := key.MarshalBinary()
			if err != nil {
				panic(err)
			}
			ttlEntry := xdr.LedgerEntry{
				LastModifiedLedgerSeq: entry.LastModifiedLedgerSeq,
				Data: xdr.LedgerEntryData{
					Type: xdr.LedgerEntryTypeTtl,
					Ttl: &xdr.TtlEntry{
						KeyHash:            sha256.Sum256(bin),
						LiveUntilLedgerSeq: entryTTLValue,
					},
				},
			}
			result = append(result, ttlEntry)
		}
	}
	return result
}()

var helloWorldContract = func() []byte {
	_, filename, _, _ := runtime.Caller(0)
	testDirName := path.Dir(filename)
	contractFile := path.Join(testDirName, "../../../../wasms/test_hello_world.wasm")
	ret, err := os.ReadFile(contractFile)
	if err != nil {
		log.Fatalf("unable to read test_hello_world.wasm (%v) please get it from `soroban-cli`", err)
	}
	return ret
}()

type inMemoryLedgerEntryGetter struct {
	entries              map[string]xdr.LedgerEntry
	latestLedgerSequence uint32
}

func (m inMemoryLedgerEntryGetter) GetLedgerEntries(
	_ context.Context,
	keys []xdr.LedgerKey,
) ([]ledgerentries.LedgerKeyAndEntry, uint32, error) {
	result := make([]ledgerentries.LedgerKeyAndEntry, 0, len(keys))
	for _, key := range keys {
		serializedKey, err := key.MarshalBinaryBase64()
		if err != nil {
			return nil, 0, err
		}
		entry, ok := m.entries[serializedKey]
		if !ok {
			continue
		}
		toAppend := ledgerentries.LedgerKeyAndEntry{
			Key:   key,
			Entry: entry,
		}
		if entry.Data.Type == xdr.LedgerEntryTypeContractData || entry.Data.Type == xdr.LedgerEntryTypeContractCode {
			// Make sure it doesn't ttl
			ttl := uint32(entryTTLValue)
			toAppend.LiveUntilLedgerSeq = &ttl
		}
		result = append(result, toAppend)
	}
	return result, m.latestLedgerSequence, nil
}

func newInMemoryLedgerEntryGetter(
	entries []xdr.LedgerEntry, latestLedgerSeq uint32,
) (inMemoryLedgerEntryGetter, error) {
	entriesMap := make(map[string]xdr.LedgerEntry, len(entries))
	for _, entry := range entries {
		key, err := entry.LedgerKey()
		if err != nil {
			return inMemoryLedgerEntryGetter{}, err
		}
		serialized, err := key.MarshalBinaryBase64()
		if err != nil {
			return inMemoryLedgerEntryGetter{}, err
		}
		entriesMap[serialized] = entry
	}
	result := inMemoryLedgerEntryGetter{
		entries:              entriesMap,
		latestLedgerSequence: latestLedgerSeq,
	}
	return result, nil
}

func (m inMemoryLedgerEntryGetter) GetLatestLedgerSequence() (uint32, error) {
	return 2, nil
}

func (m inMemoryLedgerEntryGetter) Done() error {
	return nil
}

func getPreflightParameters(t testing.TB) Parameters {
	ledgerEntryGetter, err := newInMemoryLedgerEntryGetter(mockLedgerEntries, latestSimulateTransactionLedgerSeq)
	require.NoError(t, err)
	argSymbol := xdr.ScSymbol("world")
	params := Parameters{
		EnableDebug:   true,
		Logger:        log.New(),
		SourceAccount: xdr.MustAddress("GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H"),
		OpBody: xdr.OperationBody{
			Type: xdr.OperationTypeInvokeHostFunction,
			InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
				HostFunction: xdr.HostFunction{
					Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
					InvokeContract: &xdr.InvokeContractArgs{
						ContractAddress: xdr.ScAddress{
							Type:       xdr.ScAddressTypeScAddressTypeContract,
							ContractId: &mockContractID,
						},
						FunctionName: "hello",
						Args: []xdr.ScVal{
							{
								Type: xdr.ScValTypeScvSymbol,
								Sym:  &argSymbol,
							},
						},
					},
				},
			},
		},
		NetworkPassphrase: "foo",
		LedgerEntryGetter: ledgerEntryGetter,
		BucketListSize:    200,
		// TODO: test with multiple protocol versions
		ProtocolVersion: 22,
	}
	return params
}

func TestGetPreflight(t *testing.T) {
	// in-memory
	params := getPreflightParameters(t)
	result, err := GetPreflight(context.Background(), params)
	require.NoError(t, err)
	require.Empty(t, result.Error)
}

func TestGetPreflightDebug(t *testing.T) {
	params := getPreflightParameters(t)
	// Cause an error: non-existent function
	params.OpBody.InvokeHostFunctionOp.HostFunction.InvokeContract.FunctionName = "bar"

	resultWithDebug, err := GetPreflight(context.Background(), params)
	require.NoError(t, err)
	require.NotZero(t, resultWithDebug.Error)
	require.Contains(t, resultWithDebug.Error, "Event log")
	require.Contains(t, resultWithDebug.Error, "Diagnostic Event")
	require.NotContains(t, resultWithDebug.Error, "DebugInfo not available")

	// Disable debug
	params.EnableDebug = false
	resultWithoutDebug, err := GetPreflight(context.Background(), params)
	require.NoError(t, err)
	require.NotZero(t, resultWithoutDebug.Error)
	require.NotContains(t, resultWithoutDebug.Error, "Event log")
	require.NotContains(t, resultWithoutDebug.Error, "Diagnostic Event")
	require.Contains(t, resultWithoutDebug.Error, "DebugInfo not available")
}

func BenchmarkGetPreflight(b *testing.B) {
	params := getPreflightParameters(b)

	for b.Loop() {
		result, err := GetPreflight(context.Background(), params)
		require.NoError(b, err)
		require.Empty(b, result.Error)
	}
}
