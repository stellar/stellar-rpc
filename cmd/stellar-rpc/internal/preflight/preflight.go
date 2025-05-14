//nolint:lll // CGO LDFLAG definitions are long
package preflight

/*
#include "../../lib/preflight.h"
#include <stdlib.h>
// This assumes that the Rust compiler should be using a -gnu target (i.e. MinGW compiler) in Windows
// (I (fons) am not even sure if CGo supports MSVC, see https://github.com/golang/go/issues/20982)
#cgo windows,amd64 LDFLAGS: -L${SRCDIR}/../../../../target/x86_64-pc-windows-gnu/release-with-panic-unwind/ -lpreflight -lntdll -static -lws2_32 -lbcrypt -luserenv
// You cannot compile with -static in macOS (and it's not worth it in Linux, at least with glibc)
#cgo darwin,amd64 LDFLAGS: -L${SRCDIR}/../../../../target/x86_64-apple-darwin/release-with-panic-unwind/ -lpreflight -ldl -lm
#cgo darwin,arm64 LDFLAGS: -L${SRCDIR}/../../../../target/aarch64-apple-darwin/release-with-panic-unwind/ -lpreflight -ldl -lm
// In Linux, at least for now, we will be dynamically linking glibc. See https://github.com/2opremio/soroban-go-rust-preflight-poc/issues/3 for details
// I (fons) did try linking statically against musl but it caused problems catching (unwinding) Rust panics.
#cgo linux,amd64 LDFLAGS: -L${SRCDIR}/../../../../target/x86_64-unknown-linux-gnu/release-with-panic-unwind/ -lpreflight -ldl -lm
#cgo linux,arm64 LDFLAGS: -L${SRCDIR}/../../../../target/aarch64-unknown-linux-gnu/release-with-panic-unwind/ -lpreflight -ldl -lm
*/
import "C"

import (
	"context"
	"fmt"
	"runtime/cgo"
	"time"
	"unsafe"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerentries"
	"github.com/stellar/stellar-rpc/protocol"
)

type snapshotSourceHandle struct {
	ledgerEntryGetter ledgerentries.LedgerEntryGetter
	ctx               context.Context //nolint:containedctx
	logger            *log.Entry
}

// Current base reserve is 0.5XLM (in stroops)
const defaultBaseReserve = 5_000_000

// SnapshotSourceGet takes a LedgerKey XDR in base64 string and returns its matching LedgerEntry XDR in base64 string
// It's used by the Rust preflight code to obtain ledger entries.
//
//export SnapshotSourceGet
func SnapshotSourceGet(handle C.uintptr_t, cLedgerKey C.xdr_t) C.ledger_entry_and_ttl_t {
	h, ok := cgo.Handle(handle).Value().(snapshotSourceHandle)
	if !ok {
		panic("invalid handle type: expected snapshotSourceHandle")
	}
	ledgerKeyXDR := GoXDR(cLedgerKey)
	var ledgerKey xdr.LedgerKey
	if err := xdr.SafeUnmarshal(ledgerKeyXDR, &ledgerKey); err != nil {
		panic(err)
	}
	entries, _, err := h.ledgerEntryGetter.GetLedgerEntries(h.ctx, []xdr.LedgerKey{ledgerKey})
	if err != nil {
		h.logger.WithError(err).Error("SnapshotSourceGet(): GetLedgerEntries() failed")
		return C.ledger_entry_and_ttl_t{}
	}
	if len(entries) > 1 {
		h.logger.WithError(err).Error("SnapshotSourceGet(): GetLedgerEntries() returned more than one entry")
		return C.ledger_entry_and_ttl_t{}
	}
	if len(entries) == 0 {
		return C.ledger_entry_and_ttl_t{}
	}
	out, err := entries[0].Entry.MarshalBinary()
	if err != nil {
		panic(err)
	}

	result := C.ledger_entry_and_ttl_t{
		entry: C.xdr_t{
			xdr: (*C.uchar)(C.CBytes(out)),
			len: C.size_t(len(out)),
		},
		ttl: -1, // missing TTL
	}
	if entries[0].LiveUntilLedgerSeq != nil {
		result.ttl = C.int64_t(*entries[0].LiveUntilLedgerSeq)
	}
	return result
}

func FreeGoXDR(xdr C.xdr_t) {
	C.free(unsafe.Pointer(xdr.xdr))
}

//export FreeGoLedgerEntryAndTTL
func FreeGoLedgerEntryAndTTL(leTTL C.ledger_entry_and_ttl_t) {
	C.free(unsafe.Pointer(leTTL.entry.xdr))
}

type Parameters struct {
	Logger            *log.Entry
	SourceAccount     xdr.AccountId
	OpBody            xdr.OperationBody
	Footprint         xdr.LedgerFootprint
	NetworkPassphrase string
	LedgerEntryGetter ledgerentries.LedgerEntryGetter
	LedgerSeq         uint32
	BucketListSize    uint64
	ResourceConfig    protocol.ResourceConfig
	EnableDebug       bool
	EnableNonrootAuth bool
	ProtocolVersion   uint32
}

type XDRDiff struct {
	Before []byte // optional before XDR
	After  []byte // optional after XDR
}

type Preflight struct {
	Error                     string
	Events                    [][]byte // DiagnosticEvents XDR
	TransactionData           []byte   // SorobanTransactionData XDR
	MinFee                    int64
	Result                    []byte   // XDR SCVal in base64
	Auth                      [][]byte // SorobanAuthorizationEntries XDR
	CPUInstructions           uint64
	MemoryBytes               uint64
	PreRestoreTransactionData []byte // SorobanTransactionData XDR
	PreRestoreMinFee          int64
	LedgerEntryDiff           []XDRDiff
}

func CXDR(xdr []byte) C.xdr_t {
	return C.xdr_t{
		xdr: (*C.uchar)(C.CBytes(xdr)),
		len: C.size_t(len(xdr)),
	}
}

func GoXDR(xdr C.xdr_t) []byte {
	return C.GoBytes(unsafe.Pointer(xdr.xdr), C.int(xdr.len))
}

func GoXDRVector(xdrVector C.xdr_vector_t) [][]byte {
	result := make([][]byte, xdrVector.len)
	inputSlice := unsafe.Slice(xdrVector.array, xdrVector.len)
	for i, v := range inputSlice {
		result[i] = GoXDR(v)
	}
	return result
}

func GoXDRDiffVector(xdrDiffVector C.xdr_diff_vector_t) []XDRDiff {
	result := make([]XDRDiff, xdrDiffVector.len)
	inputSlice := unsafe.Slice(xdrDiffVector.array, xdrDiffVector.len)
	for i, v := range inputSlice {
		result[i].Before = GoXDR(v.before)
		result[i].After = GoXDR(v.after)
	}
	return result
}

func GetPreflight(ctx context.Context, params Parameters) (Preflight, error) {
	switch params.OpBody.Type {
	case xdr.OperationTypeInvokeHostFunction:
		return getInvokeHostFunctionPreflight(ctx, params)
	case xdr.OperationTypeExtendFootprintTtl, xdr.OperationTypeRestoreFootprint:
		return getFootprintTTLPreflight(ctx, params)
	default:
		return Preflight{}, fmt.Errorf("unsupported operation type: %s", params.OpBody.Type.String())
	}
}

func getLedgerInfo(params Parameters) C.ledger_info_t {
	return C.ledger_info_t{
		network_passphrase: C.CString(params.NetworkPassphrase),
		sequence_number:    C.uint32_t(params.LedgerSeq),
		protocol_version:   C.uint32_t(params.ProtocolVersion),
		timestamp:          C.uint64_t(time.Now().Unix()),
		base_reserve:       defaultBaseReserve,
		bucket_list_size:   C.uint64_t(params.BucketListSize),
	}
}

func getFootprintTTLPreflight(ctx context.Context, params Parameters) (Preflight, error) {
	opBodyXDR, err := params.OpBody.MarshalBinary()
	if err != nil {
		return Preflight{}, err
	}
	opBodyCXDR := CXDR(opBodyXDR)
	footprintXDR, err := params.Footprint.MarshalBinary()
	if err != nil {
		return Preflight{}, fmt.Errorf("cannot marshal footprint: %w", err)
	}
	footprintCXDR := CXDR(footprintXDR)
	ssh := snapshotSourceHandle{
		ledgerEntryGetter: params.LedgerEntryGetter,
		ctx:               ctx,
		logger:            params.Logger,
	}
	handle := cgo.NewHandle(ssh)
	defer handle.Delete()

	res := C.preflight_footprint_ttl_op(
		C.uintptr_t(handle),
		opBodyCXDR,
		footprintCXDR,
		getLedgerInfo(params),
	)

	FreeGoXDR(opBodyCXDR)
	FreeGoXDR(footprintCXDR)

	return GoPreflight(res), nil
}

func getInvokeHostFunctionPreflight(ctx context.Context, params Parameters) (Preflight, error) {
	invokeHostFunctionXDR, err := params.OpBody.MustInvokeHostFunctionOp().MarshalBinary()
	if err != nil {
		return Preflight{}, err
	}
	invokeHostFunctionCXDR := CXDR(invokeHostFunctionXDR)
	sourceAccountXDR, err := params.SourceAccount.MarshalBinary()
	if err != nil {
		return Preflight{}, err
	}
	sourceAccountCXDR := CXDR(sourceAccountXDR)

	ssh := snapshotSourceHandle{
		ledgerEntryGetter: params.LedgerEntryGetter,
		ctx:               ctx,
		logger:            params.Logger,
	}
	handle := cgo.NewHandle(ssh)
	defer handle.Delete()
	resourceConfig := C.resource_config_t{
		instruction_leeway: C.uint64_t(params.ResourceConfig.InstructionLeeway),
	}
	res := C.preflight_invoke_hf_op(
		C.uintptr_t(handle),
		invokeHostFunctionCXDR,
		sourceAccountCXDR,
		getLedgerInfo(params),
		resourceConfig,
		C.bool(params.EnableDebug),
		C.bool(params.EnableNonrootAuth),
	)
	FreeGoXDR(invokeHostFunctionCXDR)
	FreeGoXDR(sourceAccountCXDR)

	return GoPreflight(res), nil
}

func GoPreflight(result *C.preflight_result_t) Preflight {
	defer C.free_preflight_result(result)

	preflight := Preflight{
		Error:                     C.GoString(result.error),
		Events:                    GoXDRVector(result.events),
		TransactionData:           GoXDR(result.transaction_data),
		MinFee:                    int64(result.min_fee),
		Result:                    GoXDR(result.result),
		Auth:                      GoXDRVector(result.auth),
		CPUInstructions:           uint64(result.cpu_instructions),
		MemoryBytes:               uint64(result.memory_bytes),
		PreRestoreTransactionData: GoXDR(result.pre_restore_transaction_data),
		PreRestoreMinFee:          int64(result.pre_restore_min_fee),
		LedgerEntryDiff:           GoXDRDiffVector(result.ledger_entry_diff),
	}
	return preflight
}
