package preflight

import (
	"encoding/base64"
	"encoding/json"
	"errors"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
)

var errMissingDiff = errors.New("no ledger difference found")

func ledgerEntryChangeFromXDRDiff(
	diff XDRDiff, format string, budget *xdr2json.Budget,
) (protocol.LedgerEntryChange, error) {
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

	keyXDR, err := xdr.LedgerEntryView(entryXDR).LedgerKey()
	if err != nil {
		return protocol.LedgerEntryChange{}, err
	}

	switch format {
	case protocol.FormatJSON:
		err = addLedgerEntryChangeJSON(&result, diff, keyXDR, budget)
		if err != nil {
			return protocol.LedgerEntryChange{}, err
		}

	default:
		result.KeyXDR = base64.StdEncoding.EncodeToString(keyXDR)

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

func addLedgerEntryChangeJSON(
	l *protocol.LedgerEntryChange, diff XDRDiff, keyXDR []byte, budget *xdr2json.Budget,
) error {
	var err error
	beforePresent := len(diff.Before) > 0
	afterPresent := len(diff.After) > 0

	l.KeyJSON, err = budget.ConvertBytes(xdr.LedgerKey{}, keyXDR)
	if err != nil {
		return err
	}

	if beforePresent {
		l.BeforeJSON, err = budget.ConvertBytes(xdr.LedgerEntry{}, diff.Before)
		if err != nil {
			return err
		}
	}

	if afterPresent {
		l.AfterJSON, err = budget.ConvertBytes(xdr.LedgerEntry{}, diff.After)
		if err != nil {
			return err
		}
	}

	return nil
}

func getRestorePreamble(
	preflight Preflight, format string, budget *xdr2json.Budget,
) (*protocol.RestorePreamble, error) {
	var restorePreamble *protocol.RestorePreamble
	if len(preflight.PreRestoreTransactionData) == 0 {
		return restorePreamble, nil
	}
	switch format {
	case protocol.FormatJSON:
		txDataJs, err := budget.ConvertBytes(
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

func getSimulationResults(
	preflight Preflight, format string, budget *xdr2json.Budget,
) ([]protocol.SimulateHostFunctionResult, error) {
	var results []protocol.SimulateHostFunctionResult
	if len(preflight.Result) == 0 {
		return nil, nil
	}
	switch format {
	case protocol.FormatJSON:
		rvJs, err := budget.ConvertBytes(xdr.ScVal{}, preflight.Result)
		if err != nil {
			return nil, err
		}

		auths, err := budget.ConvertSlice(xdr.SorobanAuthorizationEntry{}, preflight.Auth)
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

// maxSimulationJSONBytes bounds the JSON converted from XDR for one simulateTransaction response.
const maxSimulationJSONBytes = 32 * 1024 * 1024

// Response is a simulateTransaction response, encoded as JSON.
type Response struct {
	body   json.RawMessage
	failed bool
}

// EncodeResponse encodes response.
func EncodeResponse(response protocol.SimulateTransactionResponse) (Response, error) {
	body, err := json.Marshal(response)
	if err != nil {
		return Response{}, err
	}
	return Response{body: body, failed: response.Error != ""}, nil
}

// JSON returns the encoded response.
func (r Response) JSON() json.RawMessage {
	return r.body
}

// Failed reports whether the response carries an error.
func (r Response) Failed() bool {
	return r.failed
}

// MarshalJSON returns the encoded response.
func (r Response) MarshalJSON() ([]byte, error) {
	return r.body, nil
}

func formatResponse(preflight Preflight,
	format string, latestLedger uint32,
) (protocol.SimulateTransactionResponse, error) {
	budget := xdr2json.NewBudget(maxSimulationJSONBytes)
	results, err := getSimulationResults(preflight, format, budget)
	if err != nil {
		return protocol.SimulateTransactionResponse{}, err
	}

	restorePreamble, err := getRestorePreamble(preflight, format, budget)
	if err != nil {
		return protocol.SimulateTransactionResponse{}, err
	}

	stateChanges := make([]protocol.LedgerEntryChange, 0, len(preflight.LedgerEntryDiff))
	for _, entryDiff := range preflight.LedgerEntryDiff {
		change, err := ledgerEntryChangeFromXDRDiff(entryDiff, format, budget)
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
		simResp.TransactionDataJSON, err = budget.ConvertBytes(
			xdr.SorobanTransactionData{},
			preflight.TransactionData)
		if err != nil {
			return protocol.SimulateTransactionResponse{}, err
		}

		simResp.EventsJSON, err = budget.ConvertSlice(xdr.DiagnosticEvent{}, preflight.Events)
		if err != nil {
			return protocol.SimulateTransactionResponse{}, err
		}

	default:
		simResp.EventsXDR = base64EncodeSlice(preflight.Events)
		simResp.TransactionDataXDR = base64.StdEncoding.EncodeToString(preflight.TransactionData)
	}

	return simResp, nil
}

func base64EncodeSlice(in [][]byte) []string {
	result := make([]string, len(in))
	for i, v := range in {
		result[i] = base64.StdEncoding.EncodeToString(v)
	}
	return result
}
