//nolint:prealloc // test fixture construction keeps explicit literals for readability
package preflight

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path"
	"runtime"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerentries"
)

var mockContractID = xdr.ContractId{0xa, 0xb, 0xc}

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
	}
	entries = append(entries, contractEntries(mockContractID, helloWorldContract)...)

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

// contractEntries are the ledger entries of the contract id, which runs code.
func contractEntries(id xdr.ContractId, code []byte) []xdr.LedgerEntry {
	hash := xdr.Hash(sha256.Sum256(code))
	return []xdr.LedgerEntry{
		{
			LastModifiedLedgerSeq: latestSimulateTransactionLedgerSeq,
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeContractData,
				ContractData: &xdr.ContractDataEntry{
					Contract:   xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &id},
					Key:        xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance},
					Durability: xdr.ContractDataDurabilityPersistent,
					Val: xdr.ScVal{
						Type: xdr.ScValTypeScvContractInstance,
						Instance: &xdr.ScContractInstance{
							Executable: xdr.ContractExecutable{
								Type:     xdr.ContractExecutableTypeContractExecutableWasm,
								WasmHash: &hash,
							},
						},
					},
				},
			},
		},
		{
			LastModifiedLedgerSeq: latestSimulateTransactionLedgerSeq,
			Data: xdr.LedgerEntryData{
				Type:         xdr.LedgerEntryTypeContractCode,
				ContractCode: &xdr.ContractCodeEntry{Hash: hash, Code: code},
			},
		},
	}
}

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
// host (curr), built with the `next` feature, handles protocols 30 and 31.
// Preflight switches between them at runtime based on the ledger's protocol
// version, so the tests exercise both paths.
var supportedProtocolVersions = []uint32{29, 30, 31}

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
				ContractCode: &xdr.LedgerKeyContractCode{Hash: xdr.Hash(sha256.Sum256(helloWorldContract))},
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

// extendTTLOpBody returns the XDR of an operation extending the TTL of the
// mock contract's instance, and of its footprint.
func extendTTLOpBody(t testing.TB) (xdr.OperationBodyView, xdr.LedgerFootprintView) {
	key, err := mockLedgerEntriesWithoutTTLs[1].LedgerKey()
	require.NoError(t, err)
	footprint, err := xdr.LedgerFootprint{ReadOnly: []xdr.LedgerKey{key}}.MarshalBinary()
	require.NoError(t, err)
	opBody, err := xdr.OperationBody{
		Type:                 xdr.OperationTypeExtendFootprintTtl,
		ExtendFootprintTtlOp: &xdr.ExtendFootprintTtlOp{ExtendTo: 2000},
	}.MarshalBinary()
	require.NoError(t, err)
	return opBody, footprint
}

func TestGetPreflightExtendFootprintTTL(t *testing.T) {
	opBody, footprint := extendTTLOpBody(t)
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

// readCounter counts the reads of each ledger key.
type readCounter struct {
	ledgerentries.LedgerEntryGetter

	reads map[string]int
}

func (c *readCounter) GetLedgerEntries(
	ctx context.Context, keys []xdr.LedgerKeyView,
) ([]ledgerentries.LedgerKeyAndEntry, uint32, error) {
	for _, key := range keys {
		c.reads[string(key)]++
	}
	return c.LedgerEntryGetter.GetLedgerEntries(ctx, keys)
}

func marshal(t *testing.T, v encoding.BinaryMarshaler) []byte {
	t.Helper()
	b, err := v.MarshalBinary()
	require.NoError(t, err)
	return b
}

// extendOp extends the TTLs of the entries in a footprint to ledger 2000.
var extendOp = xdr.OperationBody{
	Type:                 xdr.OperationTypeExtendFootprintTtl,
	ExtendFootprintTtlOp: &xdr.ExtendFootprintTtlOp{ExtendTo: 2000},
}

// footprintOf returns the footprint of the transaction a simulation built.
func footprintOf(t *testing.T, result Preflight) xdr.LedgerFootprint {
	t.Helper()
	var data xdr.SorobanTransactionData
	require.NoError(t, data.UnmarshalBinary(result.TransactionData))
	return data.Resources.Footprint
}

// A simulation reads each ledger entry from core once, although the host
// reads the footprint's entries again after the invocation.
func TestGetPreflightReadsEachLedgerEntryOnce(t *testing.T) {
	for _, protocolVersion := range supportedProtocolVersions {
		params := getPreflightParameters(t, protocolVersion)
		counter := &readCounter{LedgerEntryGetter: params.LedgerEntryGetter, reads: map[string]int{}}
		params.LedgerEntryGetter = counter
		result, err := GetPreflight(t.Context(), params)
		require.NoError(t, err)
		require.Empty(t, result.Error)
		footprint := footprintOf(t, result)
		require.NotEmpty(t, footprint.ReadOnly)
		for _, key := range footprint.ReadOnly {
			require.Contains(t, counter.reads, string(marshal(t, key)))
		}
		for key, reads := range counter.reads {
			require.Equal(t, 1, reads, "key %x", key)
		}
	}
}

// An extend or restore reads a key its footprint repeats once, and the
// transaction it builds holds the key once.
func TestGetPreflightReadsRepeatedFootprintKeysOnce(t *testing.T) {
	key, err := mockLedgerEntriesWithoutTTLs[1].LedgerKey()
	require.NoError(t, err)
	repeated := []xdr.LedgerKey{key, key, key}
	restore := xdr.OperationBody{Type: xdr.OperationTypeRestoreFootprint, RestoreFootprintOp: &xdr.RestoreFootprintOp{}}
	for _, protocolVersion := range supportedProtocolVersions {
		params := getPreflightParameters(t, protocolVersion)
		counter := &readCounter{LedgerEntryGetter: params.LedgerEntryGetter, reads: map[string]int{}}
		params.LedgerEntryGetter = counter

		params.OpBody = marshal(t, extendOp)
		params.Footprint = marshal(t, xdr.LedgerFootprint{ReadOnly: repeated})
		result, err := GetPreflight(t.Context(), params)
		require.NoError(t, err)
		require.Empty(t, result.Error)
		require.Equal(t, 1, counter.reads[string(marshal(t, key))])
		require.Equal(t, []xdr.LedgerKey{key}, footprintOf(t, result).ReadOnly)

		// The entry lives until ledger entryTTLValue, so it is restored after it.
		clear(counter.reads)
		params.OpBody = marshal(t, restore)
		params.Footprint = marshal(t, xdr.LedgerFootprint{ReadWrite: repeated})
		params.LedgerSeq = entryTTLValue + 1
		result, err = GetPreflight(t.Context(), params)
		require.NoError(t, err)
		require.Empty(t, result.Error)
		require.Equal(t, 1, counter.reads[string(marshal(t, key))])
		require.Equal(t, []xdr.LedgerKey{key}, footprintOf(t, result).ReadWrite)
	}
}

// simulateWithMaxFootprintEntries simulates params on the mock ledger with
// its limit on a transaction's footprint entries set to n.
func simulateWithMaxFootprintEntries(t *testing.T, params Parameters, n uint32) Preflight {
	t.Helper()
	entries := slices.Clone(mockLedgerEntries)
	for i, entry := range entries {
		setting := entry.Data.ConfigSetting
		if setting == nil || setting.ConfigSettingId != xdr.ConfigSettingIdConfigSettingContractLedgerCostExtV0 {
			continue
		}
		cost := *setting.ContractLedgerCostExt
		cost.TxMaxFootprintEntries = xdr.Uint32(n)
		limited := *setting
		limited.ContractLedgerCostExt = &cost
		entries[i].Data.ConfigSetting = &limited
	}
	getter, err := newInMemoryLedgerEntryGetter(entries, latestSimulateTransactionLedgerSeq)
	require.NoError(t, err)
	params.LedgerEntryGetter = getter
	result, err := GetPreflight(t.Context(), params)
	require.NoError(t, err)
	return result
}

// A simulation may read as many ledger entries as a transaction's footprint
// may hold, and fails when it reads one more.
func TestGetPreflightReadsAtMostTheEntriesAFootprintMayHold(t *testing.T) {
	tooMany := func(n uint32) string {
		return fmt.Sprintf("the simulation read more than %d ledger entries, "+
			"the most a transaction's footprint may hold", n)
	}
	instance, err := mockLedgerEntriesWithoutTTLs[1].LedgerKey()
	require.NoError(t, err)
	code, err := mockLedgerEntriesWithoutTTLs[2].LedgerKey()
	require.NoError(t, err)
	missingKey := xdr.Uint32(7)
	missing := xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeContractData,
		ContractData: &xdr.LedgerKeyContractData{
			Contract:   xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &mockContractID},
			Key:        xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &missingKey},
			Durability: xdr.ContractDataDurabilityPersistent,
		},
	}
	for _, protocolVersion := range supportedProtocolVersions {
		t.Run(fmt.Sprintf("protocol %d", protocolVersion), func(t *testing.T) {
			invoke := getPreflightParameters(t, protocolVersion)
			result, err := GetPreflight(t.Context(), invoke)
			require.NoError(t, err)
			footprint := footprintOf(t, result)
			n := uint32(len(footprint.ReadOnly) + len(footprint.ReadWrite))
			require.Empty(t, simulateWithMaxFootprintEntries(t, invoke, n).Error)
			require.Equal(t, tooMany(n-1), simulateWithMaxFootprintEntries(t, invoke, n-1).Error)

			params := getPreflightParameters(t, protocolVersion)
			params.OpBody = marshal(t, extendOp)
			params.Footprint = marshal(t, xdr.LedgerFootprint{ReadOnly: []xdr.LedgerKey{instance, code}})
			result = simulateWithMaxFootprintEntries(t, params, 2)
			require.Empty(t, result.Error)
			require.Len(t, footprintOf(t, result).ReadOnly, 2)
			params.Footprint = marshal(t, xdr.LedgerFootprint{ReadOnly: []xdr.LedgerKey{instance, code, missing}})
			require.Equal(t, tooMany(2), simulateWithMaxFootprintEntries(t, params, 2).Error)
		})
	}
}

// readFailingGetter fails its failAt-th read, or returns every entry twice
// when duplicate is set.
type readFailingGetter struct {
	ledgerentries.LedgerEntryGetter

	failAt    int
	err       error
	duplicate bool
	reads     int
}

func (g *readFailingGetter) GetLedgerEntries(
	ctx context.Context, keys []xdr.LedgerKeyView,
) ([]ledgerentries.LedgerKeyAndEntry, uint32, error) {
	g.reads++
	if g.reads == g.failAt {
		return nil, 0, g.err
	}
	entries, ledger, err := g.LedgerEntryGetter.GetLedgerEntries(ctx, keys)
	if g.duplicate {
		entries = append(entries, entries...)
	}
	return entries, ledger, err
}

// The library takes a failed read for a missing entry, so a simulation with
// a failed read must fail even when it would succeed without that entry.
func TestGetPreflightFailsWhenALedgerEntryReadFails(t *testing.T) {
	params := getPreflightParameters(t, supportedProtocolVersions[len(supportedProtocolVersions)-1])
	entries := params.LedgerEntryGetter
	counter := &readFailingGetter{LedgerEntryGetter: entries}
	params.LedgerEntryGetter = counter
	result, err := GetPreflight(t.Context(), params)
	require.NoError(t, err)
	require.Empty(t, result.Error)
	require.Positive(t, counter.reads)

	readErr := errors.New("could not query captive core")
	for failAt := 1; failAt <= counter.reads; failAt++ {
		params.LedgerEntryGetter = &readFailingGetter{LedgerEntryGetter: entries, failAt: failAt, err: readErr}
		result, err := GetPreflight(t.Context(), params)
		require.ErrorIs(t, err, readErr, "read %d", failAt)
		require.Equal(t, Preflight{}, result)
	}

	params.LedgerEntryGetter = &readFailingGetter{LedgerEntryGetter: entries, duplicate: true}
	_, err = GetPreflight(t.Context(), params)
	require.ErrorContains(t, err, "more than one entry")
}
