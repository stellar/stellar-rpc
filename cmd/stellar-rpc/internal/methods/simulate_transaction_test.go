package methods

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/preflight"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
)

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
		input           preflight.XDRDiff
		expectedOutput  protocol.LedgerEntryChange
		expectedBeforeJ json.RawMessage
		expectedAfterJ  json.RawMessage
	}{
		{
			name: "creation",
			input: preflight.XDRDiff{
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
			input: preflight.XDRDiff{
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
			input: preflight.XDRDiff{
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
		change, err := LedgerEntryChangeFromXDRDiff(test.input, "")
		require.NoError(t, err, test.name)
		require.Equal(t, test.expectedOutput, change)

		// test json roundtrip
		changeJSON, err := json.Marshal(change)
		require.NoError(t, err, test.name)
		var change2 protocol.LedgerEntryChange
		require.NoError(t, json.Unmarshal(changeJSON, &change2))
		require.Equal(t, change, change2, test.name)

		// test JSON output
		changeJs, err := LedgerEntryChangeFromXDRDiff(test.input, protocol.FormatJSON)
		require.NoError(t, err, test.name)

		require.Equal(t, keyJs, changeJs.KeyJSON, test.name)
		require.Equal(t, test.expectedBeforeJ, changeJs.BeforeJSON, test.name)
		require.Equal(t, test.expectedAfterJ, changeJs.AfterJSON, test.name)
	}

	// Check the error case
	change, err := LedgerEntryChangeFromXDRDiff(preflight.XDRDiff{
		Before: nil,
		After:  nil,
	}, "")
	require.ErrorIs(t, err, errMissingDiff)
	require.Equal(t, protocol.LedgerEntryChange{}, change)
}

type panicPreflightGetter struct{}

func (panicPreflightGetter) GetPreflight(
	context.Context,
	preflight.GetterParameters,
) (preflight.Preflight, error) {
	panic("unexpected GetPreflight call")
}

func TestSimulateTransactionFeeBumpMissingSorobanData(t *testing.T) {
	logger := log.New()
	ledgerReader := &MockLedgerReader{}
	handler := NewSimulateTransactionHandler(logger, ledgerReader, nil, panicPreflightGetter{})

	txEnvelope := feeBumpExtendFootprintMissingSorobanData(t)
	txB64, err := xdr.MarshalBase64(txEnvelope)
	require.NoError(t, err)

	requestJSON := fmt.Sprintf(`{
"jsonrpc": "2.0",
"id": 1,
"method": "simulateTransaction",
"params": { "transaction": "%s" }
}`, txB64)
	requests, err := jrpc2.ParseRequests([]byte(requestJSON))
	require.NoError(t, err)
	require.Len(t, requests, 1)

	resp, err := handler(t.Context(), requests[0].ToRequest())
	require.NoError(t, err)

	simResp, ok := resp.(protocol.SimulateTransactionResponse)
	require.True(t, ok)
	require.Equal(t,
		"To perform a SimulateTransaction for ExtendFootprintTtl or RestoreFootprint operations,"+
			" SorobanTransactionData must be provided",
		simResp.Error,
	)
}

func feeBumpExtendFootprintMissingSorobanData(t *testing.T) xdr.TransactionEnvelope {
	t.Helper()

	sourceAccountID := xdr.MustAddress("GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON")
	source := (&sourceAccountID).ToMuxedAccount()
	feeSourceAccountID := xdr.MustAddress("GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON")
	feeSource := (&feeSourceAccountID).ToMuxedAccount()

	op := xdr.Operation{
		Body: xdr.OperationBody{
			Type: xdr.OperationTypeExtendFootprintTtl,
			ExtendFootprintTtlOp: &xdr.ExtendFootprintTtlOp{
				Ext:      xdr.ExtensionPoint{V: 0},
				ExtendTo: xdr.Uint32(100),
			},
		},
	}

	tx := xdr.Transaction{
		SourceAccount: source,
		Fee:           xdr.Uint32(100),
		SeqNum:        xdr.SequenceNumber(1),
		Cond:          xdr.Preconditions{Type: xdr.PreconditionTypePrecondNone},
		Memo:          xdr.Memo{Type: xdr.MemoTypeMemoNone},
		Operations:    []xdr.Operation{op},
		Ext:           xdr.TransactionExt{V: 0},
	}

	innerV1 := xdr.TransactionV1Envelope{Tx: tx}
	innerTx := xdr.FeeBumpTransactionInnerTx{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1:   &innerV1,
	}
	feeBumpTx := xdr.FeeBumpTransaction{
		FeeSource: feeSource,
		Fee:       xdr.Int64(1000),
		InnerTx:   innerTx,
		Ext:       xdr.FeeBumpTransactionExt{V: 0},
	}
	feeBumpEnvelope := xdr.FeeBumpTransactionEnvelope{Tx: feeBumpTx}

	return xdr.TransactionEnvelope{
		Type:    xdr.EnvelopeTypeEnvelopeTypeTxFeeBump,
		FeeBump: &feeBumpEnvelope,
	}
}
