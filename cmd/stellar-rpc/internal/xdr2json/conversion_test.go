package xdr2json

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestConversion(t *testing.T) {
	// Make a structure to encode
	pubkey := keypair.MustRandom()
	asset := xdr.MustNewCreditAsset("ABCD", pubkey.Address())

	// Try the all-inclusive version
	jsi, err := ConvertInterface(asset)
	require.NoError(t, err)

	// Try the byte-and-interface version
	rawBytes, err := asset.MarshalBinary()
	require.NoError(t, err)
	jsb, err := ConvertBytes(xdr.Asset{}, rawBytes)
	require.NoError(t, err)

	for _, rawJs := range []json.RawMessage{jsi, jsb} {
		var dest map[string]any
		require.NoError(t, json.Unmarshal(rawJs, &dest))

		require.Contains(t, dest, "credit_alphanum4")
		require.Contains(t, dest["credit_alphanum4"], "asset_code")
		require.Contains(t, dest["credit_alphanum4"], "issuer")
		require.IsType(t, map[string]any{}, dest["credit_alphanum4"])
		if converted, ok := dest["credit_alphanum4"].(map[string]any); assert.True(t, ok) {
			require.Equal(t, pubkey.Address(), converted["issuer"])
		}
	}
}

func TestEmptyConversion(t *testing.T) {
	js, err := ConvertBytes(xdr.SorobanTransactionData{}, []byte{})
	require.NoError(t, err)
	require.Equal(t, "", string(js))
}
