package preflight

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
)

//nolint:funlen
func TestLedgerEntryChange(t *testing.T) {
	entry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 100,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeAccount,
			Account: &xdr.AccountEntry{
				AccountId: xdr.MustAddress("GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON"),
				Balance:   100,
				SeqNum:    1,
			},
		},
	}

	updatedEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 101,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeAccount,
			Account: &xdr.AccountEntry{
				AccountId: xdr.MustAddress("GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON"),
				Balance:   200,
				SeqNum:    2,
			},
		},
	}

	entryXDR, err := entry.MarshalBinary()
	require.NoError(t, err)
	entryB64 := base64.StdEncoding.EncodeToString(entryXDR)

	updatedEntryXDR, err := updatedEntry.MarshalBinary()
	require.NoError(t, err)
	updatedEntryB64 := base64.StdEncoding.EncodeToString(updatedEntryXDR)

	key, err := entry.LedgerKey()
	require.NoError(t, err)
	keyXDR, err := key.MarshalBinary()
	require.NoError(t, err)
	keyB64 := base64.StdEncoding.EncodeToString(keyXDR)

	keyJs, err := xdr2json.ConvertInterface(key)
	require.NoError(t, err)
	entryJs, err := xdr2json.ConvertInterface(entry)
	require.NoError(t, err)
	updatedEntryJs, err := xdr2json.ConvertInterface(updatedEntry)
	require.NoError(t, err)

	for _, test := range []struct {
		name            string
		input           XDRDiff
		expectedOutput  protocol.LedgerEntryChange
		expectedBeforeJ json.RawMessage
		expectedAfterJ  json.RawMessage
	}{
		{
			name: "creation",
			input: XDRDiff{
				Before: nil,
				After:  entryXDR,
			},
			expectedOutput: protocol.LedgerEntryChange{
				Type:      protocol.LedgerEntryChangeTypeCreated,
				KeyXDR:    keyB64,
				BeforeXDR: nil,
				AfterXDR:  &entryB64,
			},
			expectedBeforeJ: nil,
			expectedAfterJ:  entryJs,
		},
		{
			name: "deletion",
			input: XDRDiff{
				Before: entryXDR,
				After:  nil,
			},
			expectedOutput: protocol.LedgerEntryChange{
				Type:      protocol.LedgerEntryChangeTypeDeleted,
				KeyXDR:    keyB64,
				BeforeXDR: &entryB64,
				AfterXDR:  nil,
			},
			expectedBeforeJ: entryJs,
			expectedAfterJ:  nil,
		},
		{
			name: "update",
			input: XDRDiff{
				Before: entryXDR,
				After:  updatedEntryXDR,
			},
			expectedOutput: protocol.LedgerEntryChange{
				Type:      protocol.LedgerEntryChangeTypeUpdated,
				KeyXDR:    keyB64,
				BeforeXDR: &entryB64,
				AfterXDR:  &updatedEntryB64,
			},
			expectedBeforeJ: entryJs,
			expectedAfterJ:  updatedEntryJs,
		},
	} {
		var change protocol.LedgerEntryChange
		change, err := ledgerEntryChangeFromXDRDiff(test.input, "", nil)
		require.NoError(t, err, test.name)
		require.Equal(t, test.expectedOutput, change)

		// test json roundtrip
		changeJSON, err := json.Marshal(change)
		require.NoError(t, err, test.name)
		var change2 protocol.LedgerEntryChange
		require.NoError(t, json.Unmarshal(changeJSON, &change2))
		require.Equal(t, change, change2, test.name)

		// test JSON output
		changeJs, err := ledgerEntryChangeFromXDRDiff(test.input, protocol.FormatJSON, nil)
		require.NoError(t, err, test.name)

		require.Equal(t, keyJs, changeJs.KeyJSON, test.name)
		require.Equal(t, test.expectedBeforeJ, changeJs.BeforeJSON, test.name)
		require.Equal(t, test.expectedAfterJ, changeJs.AfterJSON, test.name)
	}

	// Check the error case
	change, err := ledgerEntryChangeFromXDRDiff(XDRDiff{
		Before: nil,
		After:  nil,
	}, "", nil)
	require.ErrorIs(t, err, errMissingDiff)
	require.Equal(t, protocol.LedgerEntryChange{}, change)
}

func TestSimulationJSONStaysWithinBudget(t *testing.T) {
	result, err := xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: new(xdr.Uint32)}.MarshalBinary()
	require.NoError(t, err)
	p := Preflight{Result: result}

	_, err = getSimulationResults(p, protocol.FormatJSON, xdr2json.NewBudget(4))
	require.ErrorContains(t, err, "exceeds")

	results, err := getSimulationResults(p, protocol.FormatJSON, nil)
	require.NoError(t, err)
	require.JSONEq(t, `{"u32":0}`, string(results[0].ReturnValueJSON))
}

func TestSimulationEventsShareTheJSONBudget(t *testing.T) {
	// Each "<" is six bytes in the response, so each value takes just over half the budget.
	str := xdr.ScString(strings.Repeat("<", maxSimulationJSONBytes/12+1))
	val := xdr.ScVal{Type: xdr.ScValTypeScvString, Str: &str}
	result, err := val.MarshalBinary()
	require.NoError(t, err)
	event, err := xdr.DiagnosticEvent{Event: xdr.ContractEvent{
		Type: xdr.ContractEventTypeDiagnostic,
		Body: xdr.ContractEventBody{V0: &xdr.ContractEventV0{Data: val}},
	}}.MarshalBinary()
	require.NoError(t, err)

	resp, err := formatResponse(Preflight{Events: [][]byte{event}}, protocol.FormatJSON, 0)
	require.NoError(t, err)
	require.Len(t, resp.EventsJSON, 1)

	_, err = formatResponse(Preflight{Result: result, Events: [][]byte{event}}, protocol.FormatJSON, 0)
	require.ErrorContains(t, err, "exceeds")
}
