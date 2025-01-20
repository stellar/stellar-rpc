package methods

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/creachadair/jrpc2"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/preflight"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
	"github.com/stellar/stellar-rpc/protocol"
)

type SimulateTransactionRequest struct {
	Transaction    string                    `json:"transaction"`
	ResourceConfig *preflight.ResourceConfig `json:"resourceConfig,omitempty"`
	Format         string                    `json:"xdrFormat,omitempty"`
}

// SimulateHostFunctionResult contains the simulation result of each HostFunction within the single
// InvokeHostFunctionOp allowed in a Transaction
type SimulateHostFunctionResult struct {
	AuthXDR  *[]string         `json:"auth,omitempty"`
	AuthJSON []json.RawMessage `json:"authJson,omitempty"`

	ReturnValueXDR  *string         `json:"xdr,omitempty"`
	ReturnValueJSON json.RawMessage `json:"returnValueJson,omitempty"`
}

type RestorePreamble struct {
	// TransactionDataXDR is an xdr.SorobanTransactionData in base64
	TransactionDataXDR  string          `json:"transactionData,omitempty"`
	TransactionDataJSON json.RawMessage `json:"transactionDataJson,omitempty"`

	MinResourceFee int64 `json:"minResourceFee,string"`
}
type LedgerEntryChangeType int

const (
	LedgerEntryChangeTypeCreated LedgerEntryChangeType = iota + 1
	LedgerEntryChangeTypeUpdated
	LedgerEntryChangeTypeDeleted
)

var (
	LedgerEntryChangeTypeName = map[LedgerEntryChangeType]string{
		LedgerEntryChangeTypeCreated: "created",
		LedgerEntryChangeTypeUpdated: "updated",
		LedgerEntryChangeTypeDeleted: "deleted",
	}
	LedgerEntryChangeTypeValue = map[string]LedgerEntryChangeType{
		"created": LedgerEntryChangeTypeCreated,
		"updated": LedgerEntryChangeTypeUpdated,
		"deleted": LedgerEntryChangeTypeDeleted,
	}
)

func (l LedgerEntryChangeType) String() string {
	return LedgerEntryChangeTypeName[l]
}

func (l LedgerEntryChangeType) MarshalJSON() ([]byte, error) {
	return json.Marshal(l.String())
}

func (l *LedgerEntryChangeType) Parse(s string) error {
	s = strings.TrimSpace(strings.ToLower(s))
	value, ok := LedgerEntryChangeTypeValue[s]
	if !ok {
		return fmt.Errorf("%q is not a valid ledger entry change type", s)
	}
	*l = value
	return nil
}

func (l *LedgerEntryChangeType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	return l.Parse(s)
}

func (l *LedgerEntryChange) FromXDRDiff(diff preflight.XDRDiff, format string) error {
	if err := protocol.IsValidFormat(format); err != nil {
		return err
	}

	var (
		entryXDR   []byte
		changeType LedgerEntryChangeType
	)

	beforePresent := len(diff.Before) > 0
	afterPresent := len(diff.After) > 0
	switch {
	case beforePresent:
		entryXDR = diff.Before
		if afterPresent {
			changeType = LedgerEntryChangeTypeUpdated
		} else {
			changeType = LedgerEntryChangeTypeDeleted
		}

	case afterPresent:
		entryXDR = diff.After
		changeType = LedgerEntryChangeTypeCreated

	default:
		return errors.New("missing before and after")
	}

	l.Type = changeType

	// We need to unmarshal the ledger entry for both b64 and json cases
	// because we need the inner ledger key.
	var entry xdr.LedgerEntry
	if err := xdr.SafeUnmarshal(entryXDR, &entry); err != nil {
		return err
	}

	key, err := entry.LedgerKey()
	if err != nil {
		return err
	}

	switch format {
	case protocol.FormatJSON:
		return l.jsonXdrDiff(diff, key)

	default:
		keyB64, err := xdr.MarshalBase64(key)
		if err != nil {
			return err
		}

		l.KeyXDR = keyB64

		if beforePresent {
			before := base64.StdEncoding.EncodeToString(diff.Before)
			l.BeforeXDR = &before
		}

		if afterPresent {
			after := base64.StdEncoding.EncodeToString(diff.After)
			l.AfterXDR = &after
		}
	}

	return nil
}

func (l *LedgerEntryChange) jsonXdrDiff(diff preflight.XDRDiff, key xdr.LedgerKey) error {
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

// LedgerEntryChange designates a change in a ledger entry. Before and After cannot be omitted at the same time.
// If Before is omitted, it constitutes a creation, if After is omitted, it constitutes a deletion.
type LedgerEntryChange struct {
	Type LedgerEntryChangeType `json:"type"`

	KeyXDR  string          `json:"key,omitempty"` // LedgerEntryKey in base64
	KeyJSON json.RawMessage `json:"keyJson,omitempty"`

	BeforeXDR  *string         `json:"before"` // LedgerEntry XDR in base64
	BeforeJSON json.RawMessage `json:"beforeJson,omitempty"`

	AfterXDR  *string         `json:"after"` // LedgerEntry XDR in base64
	AfterJSON json.RawMessage `json:"afterJson,omitempty"`
}

type SimulateTransactionResponse struct {
	Error string `json:"error,omitempty"`

	TransactionDataXDR  string          `json:"transactionData,omitempty"` // SorobanTransactionData XDR in base64
	TransactionDataJSON json.RawMessage `json:"transactionDataJson,omitempty"`

	EventsXDR  []string          `json:"events,omitempty"` // DiagnosticEvent XDR in base64
	EventsJSON []json.RawMessage `json:"eventsJson,omitempty"`

	MinResourceFee int64 `json:"minResourceFee,string,omitempty"`
	// an array of the individual host function call results
	Results []SimulateHostFunctionResult `json:"results,omitempty"`
	// If present, it indicates that a prior RestoreFootprint is required
	RestorePreamble *RestorePreamble `json:"restorePreamble,omitempty"`
	// If present, it indicates how the state (ledger entries) will change as a result of the transaction execution.
	StateChanges []LedgerEntryChange `json:"stateChanges,omitempty"`
	LatestLedger uint32              `json:"latestLedger"`
}

type PreflightGetter interface {
	GetPreflight(ctx context.Context, params preflight.GetterParameters) (preflight.Preflight, error)
}

// NewSimulateTransactionHandler returns a json rpc handler to run preflight simulations
func NewSimulateTransactionHandler(logger *log.Entry, ledgerEntryReader db.LedgerEntryReader, ledgerReader db.LedgerReader, daemon interfaces.Daemon, getter PreflightGetter) jrpc2.Handler {
	return NewHandler(func(ctx context.Context, request SimulateTransactionRequest) SimulateTransactionResponse {
		if err := protocol.IsValidFormat(request.Format); err != nil {
			return SimulateTransactionResponse{Error: err.Error()}
		}

		var txEnvelope xdr.TransactionEnvelope
		if err := xdr.SafeUnmarshalBase64(request.Transaction, &txEnvelope); err != nil {
			logger.WithError(err).WithField("request", request).
				Info("could not unmarshal simulate transaction envelope")
			return SimulateTransactionResponse{
				Error: "Could not unmarshal transaction",
			}
		}
		if len(txEnvelope.Operations()) != 1 {
			return SimulateTransactionResponse{
				Error: "Transaction contains more than one operation",
			}
		}
		op := txEnvelope.Operations()[0]

		var sourceAccount xdr.AccountId
		if opSourceAccount := op.SourceAccount; opSourceAccount != nil {
			sourceAccount = opSourceAccount.ToAccountId()
		} else {
			sourceAccount = txEnvelope.SourceAccount().ToAccountId()
		}

		footprint := xdr.LedgerFootprint{}
		switch op.Body.Type {
		case xdr.OperationTypeInvokeHostFunction:
		case xdr.OperationTypeExtendFootprintTtl, xdr.OperationTypeRestoreFootprint:
			if txEnvelope.Type != xdr.EnvelopeTypeEnvelopeTypeTx && txEnvelope.V1.Tx.Ext.V != 1 {
				return SimulateTransactionResponse{
					Error: "To perform a SimulateTransaction for ExtendFootprintTtl or RestoreFootprint operations," +
						" SorobanTransactionData must be provided",
				}
			}
			footprint = txEnvelope.V1.Tx.Ext.SorobanData.Resources.Footprint
		default:
			return SimulateTransactionResponse{
				Error: "Transaction contains unsupported operation type: " + op.Body.Type.String(),
			}
		}

		readTx, err := ledgerEntryReader.NewTx(ctx, true)
		if err != nil {
			return SimulateTransactionResponse{
				Error: "Cannot create read transaction",
			}
		}
		defer func() {
			_ = readTx.Done()
		}()
		latestLedger, err := readTx.GetLatestLedgerSequence()
		if err != nil {
			return SimulateTransactionResponse{
				Error: err.Error(),
			}
		}
		bucketListSize, protocolVersion, err := getBucketListSizeAndProtocolVersion(ctx, ledgerReader, latestLedger)
		if err != nil {
			return SimulateTransactionResponse{
				Error: err.Error(),
			}
		}

		resourceConfig := preflight.DefaultResourceConfig()
		if request.ResourceConfig != nil {
			resourceConfig = *request.ResourceConfig
		}
		params := preflight.GetterParameters{
			LedgerEntryReadTx: readTx,
			BucketListSize:    bucketListSize,
			SourceAccount:     sourceAccount,
			OperationBody:     op.Body,
			Footprint:         footprint,
			ResourceConfig:    resourceConfig,
			ProtocolVersion:   protocolVersion,
		}
		result, err := getter.GetPreflight(ctx, params)
		if err != nil {
			return SimulateTransactionResponse{
				Error:        err.Error(),
				LatestLedger: latestLedger,
			}
		}

		var results []SimulateHostFunctionResult
		if len(result.Result) != 0 {
			switch request.Format {
			case protocol.FormatJSON:
				rvJs, err := xdr2json.ConvertBytes(xdr.ScVal{}, result.Result)
				if err != nil {
					return SimulateTransactionResponse{
						Error:        err.Error(),
						LatestLedger: latestLedger,
					}
				}

				auths, err := jsonifySlice(xdr.SorobanAuthorizationEntry{}, result.Auth)
				if err != nil {
					return SimulateTransactionResponse{
						Error:        err.Error(),
						LatestLedger: latestLedger,
					}
				}

				results = append(results, SimulateHostFunctionResult{
					ReturnValueJSON: rvJs,
					AuthJSON:        auths,
				})

			default:
				rv := base64.StdEncoding.EncodeToString(result.Result)
				auth := base64EncodeSlice(result.Auth)
				results = append(results, SimulateHostFunctionResult{
					ReturnValueXDR: &rv,
					AuthXDR:        &auth,
				})
			}
		}

		var restorePreamble *RestorePreamble = nil
		if len(result.PreRestoreTransactionData) != 0 {
			switch request.Format {
			case protocol.FormatJSON:
				txDataJs, err := xdr2json.ConvertBytes(
					xdr.SorobanTransactionData{},
					result.PreRestoreTransactionData)
				if err != nil {
					return SimulateTransactionResponse{
						Error:        err.Error(),
						LatestLedger: latestLedger,
					}
				}

				restorePreamble = &RestorePreamble{
					TransactionDataJSON: txDataJs,
					MinResourceFee:      result.PreRestoreMinFee,
				}

			default:
				restorePreamble = &RestorePreamble{
					TransactionDataXDR: base64.StdEncoding.EncodeToString(result.PreRestoreTransactionData),
					MinResourceFee:     result.PreRestoreMinFee,
				}
			}
		}

		stateChanges := make([]LedgerEntryChange, len(result.LedgerEntryDiff))
		for i := range stateChanges {
			if err := stateChanges[i].FromXDRDiff(result.LedgerEntryDiff[i], request.Format); err != nil {
				return SimulateTransactionResponse{
					Error:        err.Error(),
					LatestLedger: latestLedger,
				}
			}
		}

		simResp := SimulateTransactionResponse{
			Error:           result.Error,
			Results:         results,
			MinResourceFee:  result.MinFee,
			LatestLedger:    latestLedger,
			RestorePreamble: restorePreamble,
			StateChanges:    stateChanges,
		}

		switch request.Format {
		case protocol.FormatJSON:
			simResp.TransactionDataJSON, err = xdr2json.ConvertBytes(
				xdr.SorobanTransactionData{},
				result.TransactionData)
			if err != nil {
				return SimulateTransactionResponse{
					Error:        err.Error(),
					LatestLedger: latestLedger,
				}
			}

			simResp.EventsJSON, err = jsonifySlice(xdr.DiagnosticEvent{}, result.Events)
			if err != nil {
				return SimulateTransactionResponse{
					Error:        err.Error(),
					LatestLedger: latestLedger,
				}
			}

		default:
			simResp.EventsXDR = base64EncodeSlice(result.Events)
			simResp.TransactionDataXDR = base64.StdEncoding.EncodeToString(result.TransactionData)
		}

		return simResp
	})
}

func base64EncodeSlice(in [][]byte) []string {
	result := make([]string, len(in))
	for i, v := range in {
		result[i] = base64.StdEncoding.EncodeToString(v)
	}
	return result
}

func jsonifySlice(xdr interface{}, values [][]byte) ([]json.RawMessage, error) {
	result := make([]json.RawMessage, len(values))
	var err error

	for i, value := range values {
		result[i], err = xdr2json.ConvertBytes(xdr, value)
		if err != nil {
			return result, err
		}
	}

	return result, nil
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
	if closeMeta.V != 1 {
		return 0, 0, fmt.Errorf("latest ledger (%d) meta has unexpected verion (%d)", latestLedger, closeMeta.V)
	}
	return uint64(closeMeta.V1.TotalByteSizeOfBucketList), uint32(closeMeta.V1.LedgerHeader.Header.LedgerVersion), nil
}
