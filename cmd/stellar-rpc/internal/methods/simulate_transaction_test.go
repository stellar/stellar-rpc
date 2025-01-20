package methods

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/preflight"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
	"github.com/stellar/stellar-rpc/protocol"
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
		expectedOutput LedgerEntryChange
	}{
		{
			name: "creation",
			input: preflight.XDRDiff{
				Before: nil,
				After:  entryXDR,
			},
			expectedOutput: LedgerEntryChange{
				Type:      LedgerEntryChangeTypeCreated,
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
			expectedOutput: LedgerEntryChange{
				Type:      LedgerEntryChangeTypeDeleted,
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
			expectedOutput: LedgerEntryChange{
				Type:      LedgerEntryChangeTypeUpdated,
				KeyXDR:    keyB64,
				BeforeXDR: &entryB64,
				AfterXDR:  &entryB64,
			},
		},
	} {
		var change LedgerEntryChange
		require.NoError(t, change.FromXDRDiff(test.input, ""), test.name)
		require.Equal(t, test.expectedOutput, change)

		// test json roundtrip
		changeJSON, err := json.Marshal(change)
		require.NoError(t, err, test.name)
		var change2 LedgerEntryChange
		require.NoError(t, json.Unmarshal(changeJSON, &change2))
		require.Equal(t, change, change2, test.name)

		// test JSON output
		var changeJs LedgerEntryChange
		require.NoError(t, changeJs.FromXDRDiff(test.input, protocol.FormatJSON), test.name)

		require.Equal(t, keyJs, changeJs.KeyJSON)
		if changeJs.AfterJSON != nil {
			require.Equal(t, entryJs, changeJs.AfterJSON)
		}
		if changeJs.BeforeJSON != nil {
			require.Equal(t, entryJs, changeJs.BeforeJSON)
		}
	}
}
