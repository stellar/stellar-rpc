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
		var dest map[string]interface{}
		require.NoError(t, json.Unmarshal(rawJs, &dest))

		require.Contains(t, dest, "credit_alphanum4")
		require.Contains(t, dest["credit_alphanum4"], "asset_code")
		require.Contains(t, dest["credit_alphanum4"], "issuer")
		require.IsType(t, map[string]interface{}{}, dest["credit_alphanum4"])
		if converted, ok := dest["credit_alphanum4"].(map[string]interface{}); assert.True(t, ok) {
			require.Equal(t, pubkey.Address(), converted["issuer"])
		}
	}
}

func TestEmptyConversion(t *testing.T) {
	js, err := ConvertBytes(xdr.SorobanTransactionData{}, []byte{})
	require.NoError(t, err)
	require.Empty(t, string(js))
}

func TestBudgetChargesEscapedLength(t *testing.T) {
	str := xdr.ScString("<&>")
	val := xdr.ScVal{Type: xdr.ScValTypeScvString, Str: &str}
	js, err := ConvertInterface(val)
	require.NoError(t, err)
	escaped, err := json.Marshal(js)
	require.NoError(t, err)
	require.Greater(t, len(escaped), len(js))

	_, err = NewBudget(len(escaped) - 1).ConvertInterface(val)
	require.ErrorContains(t, err, "exceeds")
	_, err = NewBudget(len(escaped)).ConvertInterface(val)
	require.NoError(t, err)
}

func TestBudgetCapsTheConversion(t *testing.T) {
	asset := xdr.MustNewCreditAsset("ABCD", keypair.MustRandom().Address())
	_, err := NewBudget(0).ConvertInterface(asset)
	// The Rust conversion stops at the cap, before Go checks the escaped length.
	require.EqualError(t, err, "JSON output exceeds the 0 bytes left in its budget")
}

func TestBudgetIsSharedAcrossConversions(t *testing.T) {
	asset := xdr.MustNewCreditAsset("ABCD", keypair.MustRandom().Address())
	js, err := ConvertInterface(asset)
	require.NoError(t, err)

	budget := NewBudget(len(js))
	_, err = budget.ConvertInterface(asset)
	require.NoError(t, err)
	_, err = budget.ConvertInterface(asset)
	require.ErrorContains(t, err, "exceeds")
}
