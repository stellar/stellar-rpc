package methods

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/creachadair/jrpc2"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerentries"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/preflight"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
	"github.com/stellar/stellar-rpc/protocol"
)

type PreflightGetter interface {
	GetPreflight(ctx context.Context, params preflight.GetterParameters) (preflight.Preflight, error)
}

var errMissingDiff = errors.New("no ledger difference found")

func LedgerEntryChangeFromXDRDiff(diff preflight.XDRDiff, format string) (protocol.LedgerEntryChange, error) {
	if err := protocol.IsValidFormat(format); err != nil {
		return protocol.LedgerEntryChange{}, err
	}

	var (
		entryXDR   []byte
		changeType protocol.LedgerEntryChangeType
	)

	beforePresent := len(diff.Before) > 0
	afterPresent := len(diff.After) > 0

	switch {
	case beforePresent:
		entryXDR = diff.Before
		if afterPresent {
			changeType = protocol.LedgerEntryChangeTypeUpdated
		} else {
			changeType = protocol.LedgerEntryChangeTypeDeleted
		}

	case afterPresent:
		entryXDR = diff.After
		changeType = protocol.LedgerEntryChangeTypeCreated

	default:
		return protocol.LedgerEntryChange{}, errMissingDiff
	}

	result := protocol.LedgerEntryChange{Type: changeType}

	// We need to unmarshal the ledger entry for both b64 and json cases
	// because we need the inner ledger key.
	var entry xdr.LedgerEntry
	if err := xdr.SafeUnmarshal(entryXDR, &entry); err != nil {
		return protocol.LedgerEntryChange{}, err
	}

	key, err := entry.LedgerKey()
	if err != nil {
		return protocol.LedgerEntryChange{}, err
	}

	switch format {
	case protocol.FormatJSON:
		err = AddLedgerEntryChangeJSON(&result, diff, key)
		if err != nil {
			return protocol.LedgerEntryChange{}, err
		}

	default:
		keyB64, err := xdr.MarshalBase64(key)
		if err != nil {
			return protocol.LedgerEntryChange{}, err
		}

		result.KeyXDR = keyB64

		if beforePresent {
			before := base64.StdEncoding.EncodeToString(diff.Before)
			result.BeforeXDR = &before
		}

		if afterPresent {
			after := base64.StdEncoding.EncodeToString(diff.After)
			result.AfterXDR = &after
		}
	}

	return result, nil
}

func AddLedgerEntryChangeJSON(l *protocol.LedgerEntryChange, diff preflight.XDRDiff, key xdr.LedgerKey) error {
	var err error
	beforePresent := len(diff.Before) > 0
	afterPresent := len(diff.After) > 0

	l.KeyJSON, err = xdr2json.ConvertInterface(key)
	if err != nil {
		return err
	}

	if beforePresent {
		l.BeforeJSON, err = xdr2json.ConvertBytes(xdr.LedgerEntry{}, diff.Before)
		if err != nil {
			return err
		}
	}

	if afterPresent {
		l.BeforeJSON, err = xdr2json.ConvertBytes(xdr.LedgerEntry{}, diff.After)
		if err != nil {
			return err
		}
	}

	return nil
}

func getRestorePreamble(preflight preflight.Preflight, format string) (*protocol.RestorePreamble, error) {
	var restorePreamble *protocol.RestorePreamble
	if len(preflight.PreRestoreTransactionData) == 0 {
		return restorePreamble, nil
	}
	switch format {
	case protocol.FormatJSON:
		txDataJs, err := xdr2json.ConvertBytes(
			xdr.SorobanTransactionData{},
			preflight.PreRestoreTransactionData)
		if err != nil {
			return nil, err
		}

		restorePreamble = &protocol.RestorePreamble{
			TransactionDataJSON: txDataJs,
			MinResourceFee:      preflight.PreRestoreMinFee,
		}

	default:
		restorePreamble = &protocol.RestorePreamble{
			TransactionDataXDR: base64.StdEncoding.EncodeToString(preflight.PreRestoreTransactionData),
			MinResourceFee:     preflight.PreRestoreMinFee,
		}
	}
	return restorePreamble, nil
}

func getSimulationResults(preflight preflight.Preflight, format string) ([]protocol.SimulateHostFunctionResult, error) {
	var results []protocol.SimulateHostFunctionResult
	if len(preflight.Result) == 0 {
		return nil, nil
	}
	switch format {
	case protocol.FormatJSON:
		rvJs, err := xdr2json.ConvertBytes(xdr.ScVal{}, preflight.Result)
		if err != nil {
			return nil, err
		}

		auths, err := jsonifySlice(xdr.SorobanAuthorizationEntry{}, preflight.Auth)
		if err != nil {
			return nil, err
		}

		results = []protocol.SimulateHostFunctionResult{
			{
				ReturnValueJSON: rvJs,
				AuthJSON:        auths,
			},
		}

	default:
		rv := base64.StdEncoding.EncodeToString(preflight.Result)
		auth := base64EncodeSlice(preflight.Auth)
		results = []protocol.SimulateHostFunctionResult{
			{
				ReturnValueXDR: &rv,
				AuthXDR:        &auth,
			},
		}
	}
	return results, nil
}

func formatResponse(preflight preflight.Preflight,
	format string, latestLedger uint32,
) (protocol.SimulateTransactionResponse, error) {
	results, err := getSimulationResults(preflight, format)
	if err != nil {
		return protocol.SimulateTransactionResponse{}, err
	}

	restorePreamble, err := getRestorePreamble(preflight, format)
	if err != nil {
		return protocol.SimulateTransactionResponse{}, err
	}

	stateChanges := make([]protocol.LedgerEntryChange, 0, len(preflight.LedgerEntryDiff))
	for _, entryDiff := range preflight.LedgerEntryDiff {
		change, err := LedgerEntryChangeFromXDRDiff(entryDiff, format)
		// Intentionally ignore "no before and after" entries because they're
		// possible but shouldn't result in a full failure.
		if errors.Is(err, errMissingDiff) {
			continue
		} else if err != nil {
			return protocol.SimulateTransactionResponse{}, err
		}

		stateChanges = append(stateChanges, change)
	}

	simResp := protocol.SimulateTransactionResponse{
		Error:           preflight.Error,
		Results:         results,
		MinResourceFee:  preflight.MinFee,
		LatestLedger:    latestLedger,
		RestorePreamble: restorePreamble,
		StateChanges:    stateChanges,
	}

	switch format {
	case protocol.FormatJSON:
		simResp.TransactionDataJSON, err = xdr2json.ConvertBytes(
			xdr.SorobanTransactionData{},
			preflight.TransactionData)
		if err != nil {
			return protocol.SimulateTransactionResponse{}, err
		}

		simResp.EventsJSON, err = jsonifySlice(xdr.DiagnosticEvent{}, preflight.Events)
		if err != nil {
			return protocol.SimulateTransactionResponse{}, err
		}

	default:
		simResp.EventsXDR = base64EncodeSlice(preflight.Events)
		simResp.TransactionDataXDR = base64.StdEncoding.EncodeToString(preflight.TransactionData)
	}

	return simResp, nil
}

// NewSimulateTransactionHandler returns a JSON rpc handler to run preflight simulations
//
//nolint:cyclop
func NewSimulateTransactionHandler(logger *log.Entry,
	ledgerReader db.LedgerReader,
	coreClient interfaces.FastCoreClient, getter PreflightGetter,
) jrpc2.Handler {
	return NewHandler(func(ctx context.Context, request protocol.SimulateTransactionRequest,
	) protocol.SimulateTransactionResponse {
		if err := protocol.IsValidFormat(request.Format); err != nil {
			return protocol.SimulateTransactionResponse{Error: err.Error()}
		}
		var txEnvelope xdr.TransactionEnvelope
		if err := xdr.SafeUnmarshalBase64(request.Transaction, &txEnvelope); err != nil {
			logger.WithError(err).WithField("request", request).
				Info("could not unmarshal simulate transaction envelope")
			return protocol.SimulateTransactionResponse{
				Error: "Could not unmarshal transaction",
			}
		}
		if len(txEnvelope.Operations()) != 1 {
			return protocol.SimulateTransactionResponse{
				Error: "Transaction contains more than one operation",
			}
		}
		op := txEnvelope.Operations()[0]

		if err := validateAuthMode(op.Body, &request.AuthMode); err != nil {
			return protocol.SimulateTransactionResponse{Error: err.Error()}
		}

		var sourceAccount xdr.AccountId
		if opSourceAccount := op.SourceAccount; opSourceAccount != nil {
			sourceAccount = opSourceAccount.ToAccountId()
		} else {
			sourceAccount = txEnvelope.SourceAccount().ToAccountId()
		}

		footprint := xdr.LedgerFootprint{}
		switch op.Body.Type {
		case xdr.OperationTypeInvokeHostFunction: // no-op
		case xdr.OperationTypeExtendFootprintTtl, xdr.OperationTypeRestoreFootprint:
			if txEnvelope.Type != xdr.EnvelopeTypeEnvelopeTypeTx && txEnvelope.V1.Tx.Ext.V != 1 {
				return protocol.SimulateTransactionResponse{
					Error: "To perform a SimulateTransaction for ExtendFootprintTtl or RestoreFootprint operations," +
						" SorobanTransactionData must be provided",
				}
			}
			footprint = txEnvelope.V1.Tx.Ext.SorobanData.Resources.Footprint

		default:
			return protocol.SimulateTransactionResponse{
				Error: "Transaction contains unsupported operation type: " + op.Body.Type.String(),
			}
		}

		latestLedger, err := ledgerReader.GetLatestLedgerSequence(ctx)
		if err != nil {
			return protocol.SimulateTransactionResponse{
				Error: err.Error(),
			}
		}
		bucketListSize, protocolVersion, err := getBucketListSizeAndProtocolVersion(ctx, ledgerReader, latestLedger)
		if err != nil {
			return protocol.SimulateTransactionResponse{
				Error:        err.Error(),
				LatestLedger: latestLedger,
			}
		}

		resourceConfig := protocol.DefaultResourceConfig()
		if request.ResourceConfig != nil {
			resourceConfig = *request.ResourceConfig
		}
		ledgerEntryGetter := ledgerentries.NewLedgerEntryAtGetter(coreClient, latestLedger)

		params := preflight.GetterParameters{
			BucketListSize:    bucketListSize,
			SourceAccount:     sourceAccount,
			OperationBody:     op.Body,
			Footprint:         footprint,
			ResourceConfig:    resourceConfig,
			AuthMode:          request.AuthMode,
			ProtocolVersion:   protocolVersion,
			LedgerEntryGetter: ledgerEntryGetter,
			LedgerSeq:         latestLedger,
		}
		result, err := getter.GetPreflight(ctx, params)
		if err != nil {
			return protocol.SimulateTransactionResponse{
				Error:        err.Error(),
				LatestLedger: latestLedger,
			}
		}

		simResp, err := formatResponse(result, request.Format, latestLedger)
		if err != nil {
			return protocol.SimulateTransactionResponse{
				Error:        err.Error(),
				LatestLedger: latestLedger,
			}
		}
		return simResp
	})
}

// Ensures the given auth mode is valid for the given operation body. Auth mode
// is passed by reference so that if it's omitted, it will be set to the
// appropriate value for the given operation body (namely, enforcement if auth
// is present, recording otherwise).
func validateAuthMode(opBody xdr.OperationBody, authModeRef *string) error {
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

	switch opBody.Type {
	case xdr.OperationTypeInvokeHostFunction:
		hasAuth := len(opBody.MustInvokeHostFunctionOp().Auth) > 0

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
		return fmt.Errorf("transaction contains unsupported operation type: %s", opBody.Type.String())
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

func getBucketListSizeAndProtocolVersion(
	ctx context.Context,
	ledgerReader db.LedgerReader,
	latestLedger uint32,
) (uint64, uint32, error) {
	// obtain bucket size
	closeMeta, ok, err := ledgerReader.GetLedger(ctx, latestLedger)
	if err != nil {
		return 0, 0, err
	}
	if !ok {
		return 0, 0, fmt.Errorf("missing meta for latest ledger (%d)", latestLedger)
	}
	switch closeMeta.V {
	case 1:
		return uint64(closeMeta.V1.TotalByteSizeOfLiveSorobanState),
			uint32(closeMeta.V1.LedgerHeader.Header.LedgerVersion),
			nil
	case 2:
		return uint64(closeMeta.V2.TotalByteSizeOfLiveSorobanState),
			uint32(closeMeta.V2.LedgerHeader.Header.LedgerVersion),
			nil
	default:
		return 0, 0, fmt.Errorf("latest ledger (%d) meta has unexpected version (%d)",
			latestLedger, closeMeta.V)
	}
}
