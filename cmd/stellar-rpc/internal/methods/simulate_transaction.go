package methods

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/preflight"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

type PreflightGetter interface {
	GetPreflight(ctx context.Context, params preflight.GetterParameters) (preflight.Response, error)
}

// simulationInput is what simulation reads from a transaction envelope. For a
// fee bump it comes from the inner transaction.
type simulationInput struct {
	opCount   int
	memoType  xdr.MemoType
	opBody    xdr.OperationBodyView // of the first operation
	opType    xdr.OperationType
	hasAuth   bool                    // the operation invokes a host function with auth entries
	source    xdr.AccountIdView       // the operation's source account, or else the transaction's
	footprint xdr.LedgerFootprintView // nil without SorobanTransactionData
}

func readSimulationInput(envelopeB64 string) (simulationInput, error) {
	envelope, err := xdr.ViewFromBase64[xdr.TransactionEnvelopeView](envelopeB64)
	if err != nil {
		return simulationInput{}, err
	}
	var in simulationInput
	err = xdr.TryVoid(func() {
		tx := envelope.MustTransaction()
		ops := tx.MustOperations().MustAll()
		in.memoType = tx.MustMemo().MustType()
		in.source = tx.MustSourceAccount().MustToAccountId()
		if ext := tx.MustExt(); ext.MustV() == 1 {
			in.footprint = ext.MustSorobanData().MustResources().MustFootprint().MustRaw()
		}
		if in.opCount = len(ops); in.opCount == 0 {
			return
		}
		in.opBody = ops[0].MustBody().MustRaw()
		in.opType = in.opBody.MustType()
		if in.opType == xdr.OperationTypeInvokeHostFunction {
			in.hasAuth = in.opBody.MustInvokeHostFunctionOp().MustAuth().MustCount() > 0
		}
		if source, ok := ops[0].MustSourceAccount().MustUnwrap(); ok {
			in.source = source.MustToAccountId()
		}
	})
	return in, err
}

// NewSimulateTransactionHandler returns a JSON rpc handler to run preflight simulations
func NewSimulateTransactionHandler(logger *log.Entry,
	ledgerReader store.LedgerReader, getter PreflightGetter,
) jrpc2.Handler {
	var preflightInfos latestMemo[latestLedgerPreflightInfo]
	return NewHandler(func(ctx context.Context, request protocol.SimulateTransactionRequest,
	) (preflight.Response, error) {
		params, err := simulationParameters(ctx, logger, ledgerReader, &preflightInfos, request)
		if err == nil {
			var response preflight.Response
			if response, err = getter.GetPreflight(ctx, params); err == nil {
				return response, nil
			}
		}
		return preflight.EncodeResponse(protocol.SimulateTransactionResponse{
			Error:        err.Error(),
			LatestLedger: params.LedgerSeq,
		})
	})
}

// simulationError is a failure that simulateTransaction reports as written.
type simulationError string

func (e simulationError) Error() string {
	return string(e)
}

// simulationParameters checks request and reads the ledger to simulate it at.
// When it fails after reading the latest ledger, the parameters it returns
// carry that ledger.
func simulationParameters(
	ctx context.Context, logger *log.Entry, ledgerReader store.LedgerReader,
	preflightInfos *latestMemo[latestLedgerPreflightInfo], request protocol.SimulateTransactionRequest,
) (preflight.GetterParameters, error) {
	var params preflight.GetterParameters
	if err := protocol.IsValidFormat(request.Format); err != nil {
		return params, err
	}
	in, err := readSimulationInput(request.Transaction)
	if err != nil {
		logger.WithError(err).WithField("request", request).
			Info("could not unmarshal simulate transaction envelope")
		return params, simulationError("Could not unmarshal transaction")
	}
	if in.opCount != 1 {
		return params, simulationError("Transaction contains more than one operation")
	}
	// Validate that transaction does not contain a memo
	if in.memoType != xdr.MemoTypeMemoNone {
		return params, simulationError("Transaction contains a memo. Soroban transactions do not support memos.")
	}

	if err := validateAuthMode(in.opType, in.hasAuth, &request.AuthMode); err != nil {
		return params, err
	}

	switch in.opType {
	case xdr.OperationTypeInvokeHostFunction: // no-op
	case xdr.OperationTypeExtendFootprintTtl, xdr.OperationTypeRestoreFootprint:
		if in.footprint == nil {
			return params, simulationError(
				"To perform a SimulateTransaction for ExtendFootprintTtl or RestoreFootprint operations," +
					" SorobanTransactionData must be provided")
		}

	default:
		return params, simulationError("Transaction contains unsupported operation type: " + in.opType.String())
	}

	latestLedger, err := ledgerReader.GetLatestLedgerSequence(ctx)
	if err != nil {
		return params, err
	}
	params.LedgerSeq = latestLedger

	ledgerInfo, err := preflightInfos.get(latestLedger, func() (latestLedgerPreflightInfo, error) {
		return getLatestLedgerPreflightInfo(ctx, ledgerReader, latestLedger)
	})
	if err != nil {
		return params, err
	}

	resourceConfig := protocol.DefaultResourceConfig()
	if request.ResourceConfig != nil {
		resourceConfig = *request.ResourceConfig
	}

	return preflight.GetterParameters{
		BucketListSize:  ledgerInfo.bucketListSize,
		SourceAccount:   in.source,
		OperationBody:   in.opBody,
		Footprint:       in.footprint,
		ResourceConfig:  resourceConfig,
		AuthMode:        request.AuthMode,
		ProtocolVersion: ledgerInfo.protocolVersion,
		UseUpgradedAuth: request.UseUpgradedAuth, //nolint:staticcheck // transitional flag the RPC must read
		LedgerSeq:       latestLedger,
		LedgerTime:      ledgerInfo.closeTime,
		Format:          request.Format,
	}, nil
}

// Ensures the given auth mode is valid for the given operation. Auth mode
// is passed by reference so that if it's omitted, it will be set to the
// appropriate value for the given operation (namely, enforcement if auth
// is present, recording otherwise).
func validateAuthMode(opType xdr.OperationType, hasAuth bool, authModeRef *string) error {
	if authModeRef == nil {
		return errors.New("invalid auth mode")
	}

	authMode := *authModeRef

	// Prior to parsing, validate auth mode.
	switch authMode {
	case "", protocol.AuthModeEnforce, protocol.AuthModeRecord, protocol.AuthModeRecordAllowNonroot:
	default:
		return fmt.Errorf(
			"optional 'authMode' must be one of %s when included",
			strings.Join([]string{
				protocol.AuthModeEnforce,
				protocol.AuthModeRecord,
				protocol.AuthModeRecordAllowNonroot,
			}, ","),
		)
	}

	switch opType {
	case xdr.OperationTypeInvokeHostFunction:
		if authMode == "" {
			// Interpret the best course of action based on the payload:
			//  - default is enforcement with auth payload, recording without
			//  - recording with an auth payload isn't allowed
			if hasAuth {
				*authModeRef = protocol.AuthModeEnforce
			} else {
				*authModeRef = protocol.AuthModeRecord
			}
		} else if hasAuth && (authMode == protocol.AuthModeRecord ||
			authMode == protocol.AuthModeRecordAllowNonroot) {
			// If the operation has auth included already, it's invalid for the
			// user to ask for recording mode. This may change in the future if
			// simulation supports partial recording.
			return fmt.Errorf(
				"cannot set authMode to '%s' with an auth footprint",
				authMode,
			)
		}

	case xdr.OperationTypeExtendFootprintTtl, xdr.OperationTypeRestoreFootprint:
		if authMode != "" {
			return errors.New("cannot set authMode with non-InvokeHostFunction operations")
		}

	default:
		return fmt.Errorf("transaction contains unsupported operation type: %s", opType.String())
	}

	return nil
}

// helper function to base64 encode slices of slices like ContractEvents
func base64EncodeSliceOfSlices(in [][][]byte) [][]string {
	xdrStrings := make([][]string, 0, len(in))
	for _, value := range in {
		encodedVal := base64EncodeSlice(value)
		xdrStrings = append(xdrStrings, encodedVal)
	}
	return xdrStrings
}

func base64EncodeSlice(in [][]byte) []string {
	result := make([]string, len(in))
	for i, v := range in {
		result[i] = base64.StdEncoding.EncodeToString(v)
	}
	return result
}

type latestLedgerPreflightInfo struct {
	bucketListSize  uint64
	protocolVersion uint32
	closeTime       uint64
}

func getLatestLedgerPreflightInfo(
	ctx context.Context,
	ledgerReader store.LedgerReader,
	latestLedger uint32,
) (latestLedgerPreflightInfo, error) {
	// Obtain every preflight ledger field from the same meta so sequence and
	// close time cannot come from different ingestion snapshots.
	var info latestLedgerPreflightInfo
	ok, err := store.WithLedgerRaw(ctx, ledgerReader, latestLedger, func(raw []byte) error {
		meta := xdr.LedgerCloseMetaView(raw)
		version, err := meta.V()
		if err != nil {
			return err
		}
		if version != 1 && version != 2 {
			return fmt.Errorf("latest ledger (%d) meta has unexpected version (%d)", latestLedger, version)
		}
		info, err = xdr.Try(func() latestLedgerPreflightInfo { // read fields off the view without decoding
			var headerEntry xdr.LedgerHeaderHistoryEntryView
			var sorobanState xdr.Uint64View
			if version == 1 {
				v1 := meta.MustV1()
				headerEntry, sorobanState = v1.MustLedgerHeader(), v1.MustTotalByteSizeOfLiveSorobanState()
			} else {
				v2 := meta.MustV2()
				headerEntry, sorobanState = v2.MustLedgerHeader(), v2.MustTotalByteSizeOfLiveSorobanState()
			}
			header := headerEntry.MustHeader()
			return latestLedgerPreflightInfo{
				bucketListSize:  sorobanState.MustValue(),
				protocolVersion: header.MustLedgerVersion().MustValue(),
				closeTime:       header.MustScpValue().MustCloseTime().MustValue(),
			}
		})
		return err
	})
	if err != nil {
		return latestLedgerPreflightInfo{}, err
	}
	if !ok {
		return latestLedgerPreflightInfo{}, fmt.Errorf(
			"missing meta for latest ledger (%d)", latestLedger,
		)
	}
	return info, nil
}
