package eventsapi

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// Test helpers shared with the handler package's own tests (internal/eventsapi).

func testContractRaw(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }

func testContractStrkey(t *testing.T, b byte) string {
	t.Helper()
	s, err := strkey.Encode(strkey.VersionByteContract, testContractRaw(b))
	require.NoError(t, err)
	return s
}

// symbolScVal returns an ScVal symbol and its canonical XDR bytes, the form
// the index is keyed on.
func symbolScVal(t *testing.T, s string) (xdr.ScVal, []byte) {
	t.Helper()
	val := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: (*xdr.ScSymbol)(&s)}
	raw, err := val.MarshalBinary()
	require.NoError(t, err)
	return val, raw
}

// requestTopic carries ScVal bytes the way a base64-format request does:
// a JSON string of their base64.
func requestTopic(t *testing.T, raw []byte) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(raw)
	require.NoError(t, err)
	return b
}

func eventTypePtr(v xdr.ContractEventType) *xdr.ContractEventType { return new(v) }
