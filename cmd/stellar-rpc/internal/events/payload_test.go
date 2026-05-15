package events

import (
	"encoding/hex"
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"
)

func makeTxHash(b byte) xdr.Hash {
	var h xdr.Hash
	for i := range h {
		h[i] = b
	}
	return h
}

func makeContractEvent(t *testing.T, sym string) xdr.ContractEvent {
	t.Helper()
	contractIDBytes, err := hex.DecodeString("df06d62447fd25da07c0135eed7557e5a5497ee7d15b7fe345bd47e191d8f577")
	require.NoError(t, err)
	var contractID xdr.ContractId
	copy(contractID[:], contractIDBytes)
	scSym := xdr.ScSymbol(sym)
	return xdr.ContractEvent{
		ContractId: &contractID,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: []xdr.ScVal{{Type: xdr.ScValTypeScvSymbol, Sym: &scSym}},
				Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &scSym},
			},
		},
	}
}

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		p    Payload
	}{
		{
			name: "typical event",
			p: Payload{
				TxHash:         makeTxHash(0xab),
				LedgerSequence: 50_002,
				TxIdx:          7,
				OpIdx:          2,
				LedgerClosedAt: 1_700_000_000,
				EventIdx:       5,
				ContractEvent:  makeContractEvent(t, "TRANSFER"),
			},
		},
		{
			name: "zero values",
			p: Payload{
				TxHash:         xdr.Hash{},
				LedgerSequence: 0,
				TxIdx:          0,
				OpIdx:          0,
				LedgerClosedAt: 0,
				EventIdx:       0,
				ContractEvent:  makeContractEvent(t, "Z"),
			},
		},
		{
			name: "max uint32 indices (sentinel -1 for op idx)",
			p: Payload{
				TxHash:         makeTxHash(0xff),
				LedgerSequence: math.MaxUint32,
				TxIdx:          math.MaxUint32,
				OpIdx:          math.MaxUint32,
				LedgerClosedAt: math.MaxInt64,
				EventIdx:       math.MaxUint32,
				ContractEvent:  makeContractEvent(t, "MAX"),
			},
		},
		{
			name: "negative ledger closed at",
			p: Payload{
				TxHash:         makeTxHash(0x01),
				LedgerSequence: 2,
				TxIdx:          1,
				OpIdx:          1,
				LedgerClosedAt: -1,
				EventIdx:       1,
				ContractEvent:  makeContractEvent(t, "NEG"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf, err := tt.p.Marshal()
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(buf), headerLen)
			assert.Equal(t, PayloadVersion, buf[0], "first byte must be version")

			var got Payload
			require.NoError(t, got.Unmarshal(buf))
			assert.Equal(t, tt.p, got)

			// Marshal again from the decoded form — bytes must be
			// byte-identical (deterministic, canonical encoding).
			buf2, err := got.Marshal()
			require.NoError(t, err)
			assert.Equal(t, buf, buf2)
		})
	}
}

func TestUnmarshalRejectsUnknownVersion(t *testing.T) {
	p := Payload{
		TxHash:        makeTxHash(0x01),
		ContractEvent: makeContractEvent(t, "X"),
	}
	buf, err := p.Marshal()
	require.NoError(t, err)

	// Flip the version byte to an unknown value.
	buf[0] = 0xff

	var got Payload
	err = got.Unmarshal(buf)
	require.ErrorIs(t, err, ErrUnknownPayloadVersion)

	// Also reject a zero version byte — only 0x01 is valid today.
	buf[0] = 0x00
	err = got.Unmarshal(buf)
	require.ErrorIs(t, err, ErrUnknownPayloadVersion)
}

func TestUnmarshalRejectsEmptyBuffer(t *testing.T) {
	var got Payload
	err := got.Unmarshal(nil)
	assert.ErrorIs(t, err, ErrShortPayloadBuffer)
}

func TestUnmarshalRejectsTruncatedHeader(t *testing.T) {
	p := Payload{ContractEvent: makeContractEvent(t, "X")}
	buf, err := p.Marshal()
	require.NoError(t, err)

	// Anything between 1 and headerLen-1 is a valid version byte but
	// an incomplete header.
	truncated := buf[:headerLen-1]
	var got Payload
	err = got.Unmarshal(truncated)
	assert.ErrorIs(t, err, ErrShortPayloadBuffer)
}

func TestUnmarshalRejectsTruncatedContractEvent(t *testing.T) {
	p := Payload{ContractEvent: makeContractEvent(t, "X")}
	buf, err := p.Marshal()
	require.NoError(t, err)

	// Drop the last byte of the ContractEvent XDR payload. The
	// declared length still says the full size, so this must fail.
	truncated := buf[:len(buf)-1]
	var got Payload
	err = got.Unmarshal(truncated)
	require.Error(t, err)
	assert.True(t,
		errors.Is(err, ErrShortPayloadBuffer) || err.Error() != "",
		"expected an error, got %v", err,
	)
}

func TestVersionByteIsAtOffsetZero(t *testing.T) {
	// Explicit guard: callers (e.g. cold-readers introspecting
	// unknown bytes) rely on the version byte being the very first
	// byte of the wire format. Lock this in.
	p := Payload{ContractEvent: makeContractEvent(t, "X")}
	buf, err := p.Marshal()
	require.NoError(t, err)
	require.NotEmpty(t, buf)
	assert.Equal(t, byte(0x01), buf[0])
}
