package events

import (
	"encoding/binary"
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

// eventBytesFor builds the canonical event fixture for sym and returns its
// marshaled XDR — the form a Payload carries (ContractEventBytes).
func eventBytesFor(t *testing.T, sym string) []byte {
	t.Helper()
	b, err := makeContractEvent(t, sym).MarshalBinary()
	require.NoError(t, err)
	return b
}

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		p    Payload
	}{
		{
			name: "typical event",
			p: Payload{
				TxHash:             makeTxHash(0xab),
				LedgerSequence:     50_002,
				TxIdx:              7,
				OpIdx:              2,
				LedgerClosedAt:     1_700_000_000,
				EventIdx:           3,
				ContractEventBytes: eventBytesFor(t, "TRANSFER"),
			},
		},
		{
			name: "zero values",
			p: Payload{
				TxHash:             xdr.Hash{},
				LedgerSequence:     0,
				TxIdx:              0,
				OpIdx:              0,
				LedgerClosedAt:     0,
				ContractEventBytes: eventBytesFor(t, "Z"),
			},
		},
		{
			name: "max uint32 indices (sentinel -1 for op idx)",
			p: Payload{
				TxHash:             makeTxHash(0xff),
				LedgerSequence:     math.MaxUint32,
				TxIdx:              math.MaxUint32,
				OpIdx:              math.MaxUint32,
				LedgerClosedAt:     math.MaxInt64,
				EventIdx:           math.MaxUint32,
				ContractEventBytes: eventBytesFor(t, "MAX"),
			},
		},
		{
			name: "negative ledger closed at",
			p: Payload{
				TxHash:             makeTxHash(0x01),
				LedgerSequence:     2,
				TxIdx:              1,
				OpIdx:              1,
				LedgerClosedAt:     -1,
				ContractEventBytes: eventBytesFor(t, "NEG"),
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
			// Unmarshal decodes the scalar fields and aliases the raw
			// event XDR into ContractEventBytes.
			assert.Equal(t, tt.p.TxHash, got.TxHash)
			assert.Equal(t, tt.p.LedgerSequence, got.LedgerSequence)
			assert.Equal(t, tt.p.TxIdx, got.TxIdx)
			assert.Equal(t, tt.p.OpIdx, got.OpIdx)
			assert.Equal(t, tt.p.LedgerClosedAt, got.LedgerClosedAt)
			assert.Equal(t, tt.p.EventIdx, got.EventIdx)
			assert.Equal(t, buf[headerLen:], got.ContractEventBytes)

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
		TxHash:             makeTxHash(0x01),
		ContractEventBytes: eventBytesFor(t, "X"),
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
	p := Payload{ContractEventBytes: eventBytesFor(t, "X")}
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
	p := Payload{ContractEventBytes: eventBytesFor(t, "X")}
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

// TestUnmarshalAliasesContractEventBytes pins the alias contract:
// Unmarshal parses the fixed-width header into scalar fields and points
// ContractEventBytes at the caller's input buffer (no copy). Marshal on
// the resulting Payload returns the original bytes verbatim.
func TestUnmarshalAliasesContractEventBytes(t *testing.T) {
	src := Payload{
		TxHash:             makeTxHash(0xab),
		LedgerSequence:     50_002,
		TxIdx:              7,
		OpIdx:              2,
		LedgerClosedAt:     1_700_000_000,
		ContractEventBytes: eventBytesFor(t, "TRANSFER"),
	}
	wire, err := src.Marshal()
	require.NoError(t, err)

	var got Payload
	require.NoError(t, got.Unmarshal(wire))

	// ContractEventBytes is the raw XDR sub-slice ALIASED into wire.
	require.NotNil(t, got.ContractEventBytes)
	assert.Equal(t, wire[headerLen:], got.ContractEventBytes)

	// Aliasing: mutating wire's ContractEvent region is observable via
	// got.ContractEventBytes (proving no copy was made).
	original := wire[headerLen]
	wire[headerLen] = original ^ 0xff
	assert.Equal(t, wire[headerLen], got.ContractEventBytes[0],
		"ContractEventBytes must alias the input buffer")
	wire[headerLen] = original

	// Marshal-after-Unmarshal returns the original bytes.
	roundTrip, err := got.Marshal()
	require.NoError(t, err)
	assert.Equal(t, wire, roundTrip)
}

func TestVersionByteIsAtOffsetZero(t *testing.T) {
	// Explicit guard: callers (e.g. cold-readers introspecting
	// unknown bytes) rely on the version byte being the very first
	// byte of the wire format. Lock this in.
	p := Payload{ContractEventBytes: eventBytesFor(t, "X")}
	buf, err := p.Marshal()
	require.NoError(t, err)
	require.NotEmpty(t, buf)
	assert.Equal(t, byte(0x01), buf[0])
}

// TestUnmarshalRejectsOldLayoutAndTrailingBytes locks in the loud-failure
// guarantee from the exact-length check: a record whose declared ContractEvent
// length does not account for EVERY remaining byte must fail, not silently
// alias a wrong slice. This catches records written in the eventIdx-less
// layout that briefly existed on the feature branch — a 57-byte header with no
// eventIdx slot, so this layout reads the event XDR's leading 4 bytes as
// contractEventLen and the declared length won't match what remains.
func TestUnmarshalRejectsOldLayoutAndTrailingBytes(t *testing.T) {
	// Build a record in the eventIdx-LESS 57-byte layout: header (no eventIdx)
	// + eventLen + event bytes.
	eventBytes := eventBytesFor(t, "OLDFMT")
	old := make([]byte, 0, 57+len(eventBytes))
	old = append(old, PayloadVersion)
	old = append(old, make([]byte, 32)...) // txHash
	old = binary.BigEndian.AppendUint32(old, 50_002)
	old = binary.BigEndian.AppendUint32(old, 7)
	old = binary.BigEndian.AppendUint32(old, 2)
	old = binary.BigEndian.AppendUint64(old, 1_700_000_000)
	// no eventIdx slot — this is the layout the check must reject
	old = binary.BigEndian.AppendUint32(old, uint32(len(eventBytes)))
	old = append(old, eventBytes...)

	var p Payload
	require.Error(t, p.Unmarshal(old),
		"eventIdx-less record must fail loudly, not return empty/garbage event bytes")

	// New-format record with trailing junk must also fail.
	good, err := (&Payload{
		TxHash: makeTxHash(0x01), LedgerSequence: 1, TxIdx: 1, OpIdx: 0,
		LedgerClosedAt: 1, ContractEventBytes: eventBytes,
	}).Marshal()
	require.NoError(t, err)
	require.NoError(t, p.Unmarshal(good), "sanity: exact-length record parses")
	require.Error(t, p.Unmarshal(append(good, 0x00)), "trailing byte must be rejected")
}
