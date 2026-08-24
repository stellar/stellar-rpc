package xdr2json

import (
	"encoding/json"
	"math"
	"strings"
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
	require.Empty(t, string(js))
}

// TestConversionError exercises the panic-unwinding path: xdr_to_json still
// uses panic control flow for malformed bytes.
func TestConversionError(t *testing.T) {
	_, err := ConvertBytes(xdr.ScVal{}, []byte{0xff})
	require.ErrorContains(t, err, "xdr_to_json() failed: couldn't read ScVal")
}

// TestJSONConversionRoundTrip checks that ConvertJSON parses ConvertBytes'
// output back to the identical bytes, across every representative ScVal
// variant. TestJSONConversionDepthLimit pins where the round trip stops
// holding.
func TestJSONConversionRoundTrip(t *testing.T) {
	sym := xdr.ScSymbol("transfer")
	symVal := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
	u32 := xdr.Uint32(42)
	u32Val := xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &u32}

	boolv := true
	i32 := xdr.Int32(-42)
	u64 := xdr.Uint64(math.MaxUint64)
	i64v := xdr.Int64(math.MinInt64)
	tp := xdr.TimePoint(1700000000)
	dur := xdr.Duration(3600)
	u128 := xdr.UInt128Parts{Hi: 1, Lo: math.MaxUint64}
	i128 := xdr.Int128Parts{Hi: -1, Lo: 2}
	u256 := xdr.UInt256Parts{HiHi: 1, HiLo: 2, LoHi: 3, LoLo: 4}
	i256 := xdr.Int256Parts{HiHi: -1, HiLo: 2, LoHi: 3, LoLo: 4}
	bin := xdr.ScBytes{0xde, 0xad, 0xbe, 0xef}
	str := xdr.ScString("a string")
	vec := &xdr.ScVec{symVal, u32Val}
	m := &xdr.ScMap{{Key: symVal, Val: u32Val}}
	contractCode := xdr.Uint32(7)
	scErr := xdr.ScError{Type: xdr.ScErrorTypeSceContract, ContractCode: &contractCode}
	accountAddr := xdr.ScAddress{
		Type:      xdr.ScAddressTypeScAddressTypeAccount,
		AccountId: xdr.MustAddressPtr(keypair.MustRandom().Address()),
	}
	contractID := xdr.ContractId{0x01, 0x02, 0x03, 0x04}
	contractAddr := xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: &contractID,
	}
	instance := xdr.ScContractInstance{
		Executable: xdr.ContractExecutable{
			Type: xdr.ContractExecutableTypeContractExecutableStellarAsset,
		},
		Storage: m,
	}
	nonce := xdr.ScNonceKey{Nonce: xdr.Int64(-7)}
	execTag := xdr.ScString("tag")

	for name, val := range map[string]xdr.ScVal{
		"void":              {Type: xdr.ScValTypeScvVoid},
		"bool":              {Type: xdr.ScValTypeScvBool, B: &boolv},
		"error":             {Type: xdr.ScValTypeScvError, Error: &scErr},
		"u32":               u32Val,
		"i32":               {Type: xdr.ScValTypeScvI32, I32: &i32},
		"u64":               {Type: xdr.ScValTypeScvU64, U64: &u64},
		"i64":               {Type: xdr.ScValTypeScvI64, I64: &i64v},
		"timepoint":         {Type: xdr.ScValTypeScvTimepoint, Timepoint: &tp},
		"duration":          {Type: xdr.ScValTypeScvDuration, Duration: &dur},
		"u128":              {Type: xdr.ScValTypeScvU128, U128: &u128},
		"i128":              {Type: xdr.ScValTypeScvI128, I128: &i128},
		"u256":              {Type: xdr.ScValTypeScvU256, U256: &u256},
		"i256":              {Type: xdr.ScValTypeScvI256, I256: &i256},
		"bytes":             {Type: xdr.ScValTypeScvBytes, Bytes: &bin},
		"string":            {Type: xdr.ScValTypeScvString, Str: &str},
		"symbol":            symVal,
		"vec":               {Type: xdr.ScValTypeScvVec, Vec: &vec},
		"map":               {Type: xdr.ScValTypeScvMap, Map: &m},
		"account address":   {Type: xdr.ScValTypeScvAddress, Address: &accountAddr},
		"contract address":  {Type: xdr.ScValTypeScvAddress, Address: &contractAddr},
		"contract instance": {Type: xdr.ScValTypeScvContractInstance, Instance: &instance},
		"ledger key contract instance": {
			Type: xdr.ScValTypeScvLedgerKeyContractInstance,
		},
		"ledger key nonce": {Type: xdr.ScValTypeScvLedgerKeyNonce, NonceKey: &nonce},
		"executable tag":   {Type: xdr.ScValTypeScvExecutableTag, ExecutableTag: &execTag},
	} {
		t.Run(name, func(t *testing.T) {
			rawBytes, err := val.MarshalBinary()
			require.NoError(t, err)

			js, err := ConvertBytes(xdr.ScVal{}, rawBytes)
			require.NoError(t, err)

			back, err := ConvertJSON(xdr.ScVal{}, js)
			require.NoError(t, err)
			require.Equal(t, rawBytes, back)
		})
	}
}

// TestJSONConversionDialect pins the JSON wire form, so a stellar-xdr crate
// bump that shifts the dialect fails loudly.
func TestJSONConversionDialect(t *testing.T) {
	sym := xdr.ScSymbol("transfer")
	val := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
	rawBytes, err := val.MarshalBinary()
	require.NoError(t, err)

	emitted, err := ConvertBytes(xdr.ScVal{}, rawBytes)
	require.NoError(t, err)
	require.JSONEq(t, `{"symbol":"transfer"}`, string(emitted))

	back, err := ConvertJSON(xdr.ScVal{}, json.RawMessage(`{"symbol":"transfer"}`))
	require.NoError(t, err)
	require.Equal(t, rawBytes, back)
}

func TestJSONConversionGenericType(t *testing.T) {
	key := xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeAccount,
		Account: &xdr.LedgerKeyAccount{
			AccountId: xdr.MustAddress(keypair.MustRandom().Address()),
		},
	}
	rawBytes, err := key.MarshalBinary()
	require.NoError(t, err)

	js, err := ConvertBytes(xdr.LedgerKey{}, rawBytes)
	require.NoError(t, err)

	back, err := ConvertJSON(xdr.LedgerKey{}, js)
	require.NoError(t, err)
	require.Equal(t, rawBytes, back)
}

func TestJSONConversionErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		js          string
		errContains string
	}{
		"empty":            {``, "empty"},
		"malformed":        {`{"symbol":`, "couldn't parse ScVal"},
		"unknown variant":  {`{"symbal":"transfer"}`, "couldn't parse ScVal"},
		"wrong shape":      {`12`, "couldn't parse ScVal"},
		"trailing garbage": {`{"symbol":"transfer"} extra`, "couldn't parse ScVal"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ConvertJSON(xdr.ScVal{}, json.RawMessage(tc.js))
			require.ErrorContains(t, err, tc.errContains)
		})
	}

	type notAnXdrType struct{}
	_, err := ConvertJSON(notAnXdrType{}, json.RawMessage(`{}`))
	require.ErrorContains(t, err, "couldn't match type notAnXdrType")
}

// TestJSONConversionUnknownFields pins that unknown fields inside nested
// structures are dropped, not rejected: the crate's collecting variant
// would allocate a path string per ignored field (a large memory amplifier
// on adversarial input) and misses the untagged numeric arms anyway, so
// strict rejection is deferred to the request handler.
func TestJSONConversionUnknownFields(t *testing.T) {
	void := xdr.ScVal{Type: xdr.ScValTypeScvVoid}
	m := &xdr.ScMap{{Key: void, Val: void}}
	want, err := xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &m}.MarshalBinary()
	require.NoError(t, err)

	js := json.RawMessage(`{"map":[{"key":"void","val":"void","extra":1}]}`)
	got, err := ConvertJSON(xdr.ScVal{}, js)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func nestedScVec(depth int) xdr.ScVal {
	one := xdr.Uint32(1)
	val := xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &one}
	for range depth {
		inner := &xdr.ScVec{val}
		val = xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &inner}
	}
	return val
}

// TestJSONConversionDepthLimit pins the parse direction's nesting boundary:
// serde_json's recursion limit (128 JSON levels, two per ScVal container)
// stops the ConvertBytes->ConvertJSON round trip at 63 nested containers,
// below the 500-depth XDR read limit. Deeper values still convert from
// base64. A serde_json or stellar-xdr bump that moves the boundary should
// fail here.
func TestJSONConversionDepthLimit(t *testing.T) {
	roundTrip := func(depth int) error {
		rawBytes, err := nestedScVec(depth).MarshalBinary()
		require.NoError(t, err)
		js, err := ConvertBytes(xdr.ScVal{}, rawBytes)
		require.NoError(t, err)
		back, err := ConvertJSON(xdr.ScVal{}, js)
		if err == nil {
			require.Equal(t, rawBytes, back)
		}
		return err
	}

	require.NoError(t, roundTrip(63))
	require.ErrorContains(t, roundTrip(64), "recursion limit exceeded")
}

func TestJSONConversionSizeLimit(t *testing.T) {
	js := []byte(`{"string":"` + strings.Repeat("a", 32<<20) + `"}`)
	_, err := ConvertJSON(xdr.ScVal{}, js)
	require.ErrorContains(t, err, "-byte limit")
}
