package preflight

import (
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
)

// growMemoryContract is a contract whose function grow adds as many 64 KiB
// pages to its linear memory as its u32 argument says, and returns void. It
// is this module, with the contractenvmetav0 section of a contract built for
// protocol 22:
//
//	(module
//	  (memory (export "memory") 1)
//	  (func (export "grow") (param i64) (result i64)
//	    (drop (memory.grow (i32.wrap_i64 (i64.shr_u (local.get 0) (i64.const 32)))))
//	    (i64.const 2)))
var growMemoryContract = []byte{
	0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x06, 0x01, 0x60,
	0x01, 0x7e, 0x01, 0x7e, 0x03, 0x02, 0x01, 0x00, 0x05, 0x03, 0x01, 0x00,
	0x01, 0x07, 0x11, 0x02, 0x06, 0x6d, 0x65, 0x6d, 0x6f, 0x72, 0x79, 0x02,
	0x00, 0x04, 0x67, 0x72, 0x6f, 0x77, 0x00, 0x00, 0x0a, 0x0f, 0x01, 0x0d,
	0x00, 0x20, 0x00, 0x42, 0x20, 0x88, 0xa7, 0x40, 0x00, 0x1a, 0x42, 0x02,
	0x0b, 0x00, 0x1e, 0x11, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x61, 0x63, 0x74,
	0x65, 0x6e, 0x76, 0x6d, 0x65, 0x74, 0x61, 0x76, 0x30, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x16, 0x00, 0x00, 0x00, 0x00,
}

var growMemoryContractID = xdr.ContractId{0x6, 0x7}

// growMemory is a simulation that adds pages to growMemoryContract's linear
// memory.
func growMemory(t *testing.T, pages uint32) GetterParameters {
	arg := xdr.Uint32(pages)
	opBody, err := xdr.OperationBody{
		Type: xdr.OperationTypeInvokeHostFunction,
		InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
			HostFunction: xdr.HostFunction{
				Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
				InvokeContract: &xdr.InvokeContractArgs{
					ContractAddress: xdr.ScAddress{
						Type:       xdr.ScAddressTypeScAddressTypeContract,
						ContractId: &growMemoryContractID,
					},
					FunctionName: "grow",
					Args:         []xdr.ScVal{{Type: xdr.ScValTypeScvU32, U32: &arg}},
				},
			},
		},
	}.MarshalBinary()
	require.NoError(t, err)
	params := testSimulation(t, protocol.FormatJSON)
	params.OperationBody = opBody
	return params
}

// computeSetting is the mock ContractComputeV0 setting with txMemoryLimit
// in place of its own.
func computeSetting(t *testing.T, txMemoryLimit uint32) xdr.LedgerEntry {
	for _, entry := range mockLedgerEntries {
		setting, ok := entry.Data.GetConfigSetting()
		if ok && setting.ConfigSettingId == xdr.ConfigSettingIdConfigSettingContractComputeV0 {
			compute := *setting.ContractCompute
			compute.TxMemoryLimit = xdr.Uint32(txMemoryLimit)
			setting.ContractCompute = &compute
			entry.Data.ConfigSetting = &setting
			return entry
		}
	}
	require.FailNow(t, "mockLedgerEntries has no ContractComputeV0 setting")
	return xdr.LedgerEntry{}
}

// A contract's linear memory is on the Rust heap, so a contract that grows it
// past the worker's memory limit ends the worker, and the pool replaces it.
func TestPoolReplacesWorkerThatRunsOutOfMemory(t *testing.T) {
	core := newCoreServer(t, nil, contractEntries(growMemoryContractID, growMemoryContract)...)
	pool, logs := newTestPool(t, core, defaultPoolLimits(), func(cfg *WorkerPoolConfig) {
		cfg.MemoryLimit = 8 << 20
	})
	pid := workerPID(t, logs, 0)

	encoded, err := pool.GetPreflight(t.Context(), growMemory(t, 16)) // 1 MiB
	require.NoError(t, err)
	require.False(t, encoded.Failed(), string(encoded.JSON()))
	require.EqualValues(t, 8<<20, pool.memoryLimit.Load())

	_, err = pool.GetPreflight(t.Context(), growMemory(t, 160)) // 10 MiB
	require.ErrorIs(t, err, ErrSimulationFailed)
	assert.InDelta(t, 1, exits(pool, exitMemoryLimit), 0)
	workerPID(t, logs, pid)
	requireSimulates(t, pool)
}

// On a network with stellar-core's initial per-transaction memory limit, one
// worker runs valid simulations one after another without failing any or
// exiting.
func TestPoolRunsSimulationsUnderTheInitialNetworkMemoryLimit(t *testing.T) {
	pool, _ := newTestPool(t, newCoreServer(t, nil, computeSetting(t, 2_000_000)), defaultPoolLimits())

	extendTTL := testSimulation(t, protocol.FormatJSON)
	extendTTL.OperationBody, extendTTL.Footprint = extendTTLOpBody(t)
	simulations := []GetterParameters{
		extendTTL,
		testSimulation(t, protocol.FormatJSON),
		testSimulation(t, protocol.FormatBase64),
	}
	for i := range 60 {
		encoded, err := pool.GetPreflight(t.Context(), simulations[i%len(simulations)])
		require.NoError(t, err)
		require.False(t, encoded.Failed(), string(encoded.JSON()))
		require.EqualValues(t, 8*2_000_000, pool.memoryLimit.Load())
	}
	for _, cause := range []string{exitCrash, exitDeadline, exitMemoryLimit, exitInvalidResponse} {
		require.Zero(t, exits(pool, cause), cause)
	}
}

// Only a worker counts and limits its memory. A simulation in any other
// process, such as the daemon or this test, counts nothing and sets no limit.
func TestSimulationOutsideAWorkerSetsNoMemoryLimit(t *testing.T) {
	params := getPreflightParameters(t, supportedProtocolVersions[len(supportedProtocolVersions)-1])
	resetMemoryPeak()
	result, err := GetPreflight(t.Context(), params)
	require.NoError(t, err)
	require.Empty(t, result.Error)
	require.Zero(t, memoryLimit())
	require.Zero(t, memoryPeak())
}

// convertUnderLimitEnv makes TestJSONConversionCountsTowardTheMemoryLimit
// limit the memory of its own process and convert values to JSON there.
const convertUnderLimitEnv = "PREFLIGHT_TEST_CONVERT_UNDER_LIMIT"

// convertedUnderLimit is what the child prints once a small conversion has
// succeeded under the limit.
const convertedUnderLimit = "converted a small value under the memory limit"

// Both Rust libraries allocate through one counting allocator, so a worker's
// memory limit covers the conversion of a simulation's result to JSON as well
// as the simulation. The conversions run in a child process, which the large
// one ends.
func TestJSONConversionCountsTowardTheMemoryLimit(t *testing.T) {
	if os.Getenv(convertUnderLimitEnv) != "" {
		limitMemory(4 << 20)
		convert := func(size int) error {
			data := make(xdr.ScBytes, size)
			_, err := xdr2json.ConvertInterface(xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &data})
			return err
		}
		require.NoError(t, convert(1<<10))
		_, err := os.Stdout.WriteString(convertedUnderLimit + "\n")
		require.NoError(t, err)
		require.NoError(t, convert(8<<20))
		require.Fail(t, "the conversion passed the memory limit")
		return
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestJSONConversionCountsTowardTheMemoryLimit$")
	cmd.Env = append(os.Environ(), convertUnderLimitEnv+"=1")
	output, err := cmd.CombinedOutput()
	require.Contains(t, string(output), convertedUnderLimit)
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, string(output))
	require.Equal(t, memoryLimitExitCode, exitErr.ExitCode(), string(output))
}
