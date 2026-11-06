//nolint:prealloc // test fixture construction keeps explicit literals for readability
package preflight

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"fmt"
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerentries"
)

var (
	mockContractID   = xdr.ContractId{0xa, 0xb, 0xc}
	mockContractHash = xdr.Hash{0xd, 0xe, 0xf}
)

const (
	latestSimulateTransactionLedgerSeq = 2
	// Make sure it doesn't ttl
	entryTTLValue = 1000
)

//go:embed testnet-config.xdr
var configSettings string

var mockLedgerEntriesWithoutTTLs = func() []xdr.LedgerEntry {
	var config xdr.ConfigUpgradeSet
	err := xdr.SafeUnmarshalBase64(configSettings, &config)
	if err != nil {
		panic("Failed to unmarshal ConfigUpgradeSet from configSettings")
	}

	stateWindow := []xdr.Uint64{10, 10, 10}

	entries := []xdr.LedgerEntry{
		{
			LastModifiedLedgerSeq: latestSimulateTransactionLedgerSeq - 1,
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeConfigSetting,
				ConfigSetting: &xdr.ConfigSettingEntry{
					ConfigSettingId:            xdr.ConfigSettingIdConfigSettingLiveSorobanStateSizeWindow,
					LiveSorobanStateSizeWindow: &stateWindow,
				},
			},
		},
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
	}

	for _, configEntry := range config.UpdatedEntry {
		entry := xdr.LedgerEntry{
			LastModifiedLedgerSeq: latestSimulateTransactionLedgerSeq,
			Data: xdr.LedgerEntryData{
				Type:          xdr.LedgerEntryTypeConfigSetting,
				ConfigSetting: &configEntry,
			},
		}
		entries = append(entries, entry)
	}

	return entries
}()

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
	keys []xdr.LedgerKeyView,
) ([]ledgerentries.LedgerKeyAndEntry, uint32, error) {
	result := make([]ledgerentries.LedgerKeyAndEntry, 0, len(keys))
	for _, key := range keys {
		entry, ok := m.entries[base64.StdEncoding.EncodeToString(key)]
		if !ok {
			continue
		}
		entryXDR, err := entry.MarshalBinary()
		if err != nil {
			return nil, 0, err
		}
		toAppend := ledgerentries.LedgerKeyAndEntry{
			Key:   key,
			Entry: entryXDR,
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

// supportedProtocolVersions are the protocol versions the bundled soroban hosts
// can simulate: the previous host (prev) handles protocol 29 and the current
// host (curr) handles protocol 30. Preflight switches between them at runtime
// based on the ledger's protocol version, so the tests exercise both paths.
var supportedProtocolVersions = []uint32{29, 30}

func getPreflightParameters(t testing.TB, protocolVersion uint32) Parameters {
	ledgerEntryGetter, err := newInMemoryLedgerEntryGetter(mockLedgerEntries, latestSimulateTransactionLedgerSeq)
	require.NoError(t, err)
	sourceAccount, err := xdr.MustAddress("GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H").MarshalBinary()
	require.NoError(t, err)
	params := Parameters{
		EnableDebug:       true,
		Logger:            log.New(),
		SourceAccount:     sourceAccount,
		OpBody:            invokeContractOpBody(t, "hello"),
		NetworkPassphrase: "foo",
		LedgerEntryGetter: ledgerEntryGetter,
		BucketListSize:    200,
		LedgerTime:        1_700_000_123,
		ProtocolVersion:   protocolVersion,
		AuthMode:          protocol.AuthModeRecord,
	}
	return params
}

// invokeContractOpBody returns the XDR of an operation calling function on the mock contract.
func invokeContractOpBody(t testing.TB, function xdr.ScSymbol) xdr.OperationBodyView {
	argSymbol := xdr.ScSymbol("world")
	opBody, err := xdr.OperationBody{
		Type: xdr.OperationTypeInvokeHostFunction,
		InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
			HostFunction: xdr.HostFunction{
				Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
				InvokeContract: &xdr.InvokeContractArgs{
					ContractAddress: xdr.ScAddress{
						Type:       xdr.ScAddressTypeScAddressTypeContract,
						ContractId: &mockContractID,
					},
					FunctionName: function,
					Args: []xdr.ScVal{
						{
							Type: xdr.ScValTypeScvSymbol,
							Sym:  &argSymbol,
						},
					},
				},
			},
		},
	}.MarshalBinary()
	require.NoError(t, err)
	return opBody
}

func TestGetLedgerInfoUsesProvidedLedgerTime(t *testing.T) {
	params := Parameters{
		NetworkPassphrase: "Standalone Network ; February 2017",
		LedgerSeq:         42,
		LedgerTime:        1_700_000_123,
		ProtocolVersion:   25,
		BucketListSize:    200,
	}

	ledgerInfo := getLedgerInfo(params)
	defer freeLedgerInfo(ledgerInfo)

	require.EqualValues(t, params.LedgerTime, ledgerInfo.timestamp)
	require.EqualValues(t, params.LedgerSeq, ledgerInfo.sequence_number)
	require.EqualValues(t, params.ProtocolVersion, ledgerInfo.protocol_version)
	require.EqualValues(t, params.BucketListSize, ledgerInfo.bucket_list_size)
}

func TestGetPreflight(t *testing.T) {
	for _, protocolVersion := range supportedProtocolVersions {
		t.Run(fmt.Sprintf("protocol %d", protocolVersion), func(t *testing.T) {
			// in-memory
			params := getPreflightParameters(t, protocolVersion)
			result, err := GetPreflight(t.Context(), params)
			require.NoError(t, err)
			require.Empty(t, result.Error)
		})
	}
}

func TestGetPreflightDebug(t *testing.T) {
	for _, protocolVersion := range supportedProtocolVersions {
		t.Run(fmt.Sprintf("protocol %d", protocolVersion), func(t *testing.T) {
			params := getPreflightParameters(t, protocolVersion)
			// Cause an error: non-existent function
			params.OpBody = invokeContractOpBody(t, "bar")

			resultWithDebug, err := GetPreflight(t.Context(), params)
			require.NoError(t, err)
			require.NotEmpty(t, resultWithDebug.Error)
			require.NotEmpty(t, resultWithDebug.Events)

			// Disable debug
			params.EnableDebug = false
			resultWithoutDebug, err := GetPreflight(t.Context(), params)
			require.NoError(t, err)
			require.Empty(t, resultWithoutDebug.Events)

			// The error is only the host error code. The events are returned as XDR.
			require.Equal(t, "HostError: Error(WasmVm, MissingValue)", resultWithDebug.Error)
			require.Equal(t, resultWithDebug.Error, resultWithoutDebug.Error)
		})
	}
}

// TestGetPreflightUnsupportedProtocol checks that simulating on a protocol
// neither bundled host supports fails with an error that names the network's
// protocol and the supported ones, instead of the host's opaque
// "Error(Context, InternalError)".
func TestGetPreflightUnsupportedProtocol(t *testing.T) {
	oldest := supportedProtocolVersions[0]
	newest := supportedProtocolVersions[len(supportedProtocolVersions)-1]
	supported := fmt.Sprintf("only supports protocols %d and %d", oldest, newest)

	for _, protocolVersion := range []uint32{oldest - 1, newest + 1} {
		t.Run(fmt.Sprintf("protocol %d", protocolVersion), func(t *testing.T) {
			networkProtocol := fmt.Sprintf("the network is on protocol %d", protocolVersion)

			params := getPreflightParameters(t, protocolVersion)
			result, err := GetPreflight(t.Context(), params)
			require.NoError(t, err)
			require.Contains(t, result.Error, networkProtocol)
			require.Contains(t, result.Error, supported)

			opBody, err := xdr.OperationBody{
				Type:                 xdr.OperationTypeExtendFootprintTtl,
				ExtendFootprintTtlOp: &xdr.ExtendFootprintTtlOp{ExtendTo: 100},
			}.MarshalBinary()
			require.NoError(t, err)
			footprint, err := xdr.LedgerFootprint{ReadOnly: []xdr.LedgerKey{{
				Type:         xdr.LedgerEntryTypeContractCode,
				ContractCode: &xdr.LedgerKeyContractCode{Hash: mockContractHash},
			}}}.MarshalBinary()
			require.NoError(t, err)
			params.OpBody = opBody
			params.Footprint = footprint
			result, err = GetPreflight(t.Context(), params)
			require.NoError(t, err)
			require.Contains(t, result.Error, networkProtocol)
			require.Contains(t, result.Error, supported)
		})
	}
}

func BenchmarkGetPreflight(b *testing.B) {
	params := getPreflightParameters(b, supportedProtocolVersions[len(supportedProtocolVersions)-1])

	for b.Loop() {
		result, err := GetPreflight(b.Context(), params)
		require.NoError(b, err)
		require.Empty(b, result.Error)
	}
}

// TestGetPreflightUseUpgradedAuthOnPrevProtocol locks in the behavior that
// requesting v2 (AddressV2) credentials on the protocol served by the prev
// soroban-env host succeeds. Every prev host since 27.1.0 supports v2
// credentials (UseUpgradedAuth), so the flag passes through to the host rather
// than being silently dropped. (Asserting the credential version itself
// requires an auth-recording contract; that is covered by the integration
// tests.)
func TestGetPreflightUseUpgradedAuthOnPrevProtocol(t *testing.T) {
	prevHostProtocol := supportedProtocolVersions[0]
	params := getPreflightParameters(t, prevHostProtocol)
	params.UseUpgradedAuth = true
	result, err := GetPreflight(t.Context(), params)
	require.NoError(t, err)
	require.Empty(t, result.Error)
}

func TestGetPreflightExtendFootprintTTL(t *testing.T) {
	key, err := mockLedgerEntriesWithoutTTLs[1].LedgerKey()
	require.NoError(t, err)
	footprint, err := xdr.LedgerFootprint{ReadOnly: []xdr.LedgerKey{key}}.MarshalBinary()
	require.NoError(t, err)
	opBody, err := xdr.OperationBody{
		Type:                 xdr.OperationTypeExtendFootprintTtl,
		ExtendFootprintTtlOp: &xdr.ExtendFootprintTtlOp{ExtendTo: 2000},
	}.MarshalBinary()
	require.NoError(t, err)

	for _, protocolVersion := range supportedProtocolVersions {
		params := getPreflightParameters(t, protocolVersion)
		params.OpBody = opBody
		params.Footprint = footprint
		result, err := GetPreflight(t.Context(), params)
		require.NoError(t, err)
		require.Empty(t, result.Error)
		require.NotEmpty(t, result.TransactionData)
	}
}
