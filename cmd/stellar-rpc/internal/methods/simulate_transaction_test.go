package methods

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
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

	entryXDR, err := entry.MarshalBinary()
	require.NoError(t, err)
	entryB64 := base64.StdEncoding.EncodeToString(entryXDR)

	key, err := entry.LedgerKey()
	require.NoError(t, err)
	keyXDR, err := key.MarshalBinary()
	require.NoError(t, err)
	keyB64 := base64.StdEncoding.EncodeToString(keyXDR)

	keyJs, err := xdr2json.ConvertInterface(key)
	require.NoError(t, err)
	entryJs, err := xdr2json.ConvertInterface(entry)
	require.NoError(t, err)

	for _, test := range []struct {
		name           string
		input          preflight.XDRDiff
		expectedOutput protocol.LedgerEntryChange
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
		},
		{
			name: "update",
			input: preflight.XDRDiff{
				Before: entryXDR,
				After:  entryXDR,
			},
			expectedOutput: protocol.LedgerEntryChange{
				Type:      protocol.LedgerEntryChangeTypeUpdated,
				KeyXDR:    keyB64,
				BeforeXDR: &entryB64,
				AfterXDR:  &entryB64,
			},
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

		require.Equal(t, keyJs, changeJs.KeyJSON)
		if changeJs.AfterJSON != nil {
			require.Equal(t, entryJs, changeJs.AfterJSON)
		}
		if changeJs.BeforeJSON != nil {
			require.Equal(t, entryJs, changeJs.BeforeJSON)
		}
	}

	// Check the error case
	change, err := LedgerEntryChangeFromXDRDiff(preflight.XDRDiff{
		Before: nil,
		After:  nil,
	}, "")
	require.ErrorIs(t, err, errMissingDiff)
	require.Equal(t, protocol.LedgerEntryChange{}, change)
}
