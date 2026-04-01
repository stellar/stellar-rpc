package intpack

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGroupRoundTrip(t *testing.T) {
	values := []uint32{10, 20, 30, 40, 50}
	encoded := EncodeGroup(values)

	decoded, consumed, err := DecodeGroup(encoded, len(values), nil)
	require.NoError(t, err)
	require.Equal(t, len(encoded), consumed)
	require.Equal(t, values, decoded)
}

func TestWidth32(t *testing.T) {
	values := []uint32{0, math.MaxUint32}
	encoded := EncodeGroup(values)

	decoded, consumed, err := DecodeGroup(encoded, len(values), nil)
	require.NoError(t, err)
	require.Equal(t, len(encoded), consumed)
	require.Equal(t, values, decoded)
}

func TestDecodeGroupErrors(t *testing.T) {
	// Invalid width (> 32): set the 5th-from-last byte to 33.
	data := make([]byte, 12)
	data[len(data)-5] = 33 // width = 33

	_, _, err := DecodeGroup(data, 1, nil)
	require.Error(t, err)

	// Data too short (< 5 bytes).
	_, _, err = DecodeGroup([]byte{1, 2, 3, 4}, 1, nil)
	require.Error(t, err)

	// n <= 0.
	_, _, err = DecodeGroup(data, 0, nil)
	require.Error(t, err)

	_, _, err = DecodeGroup(data, -1, nil)
	require.Error(t, err)

	// Truncated payload: valid footer but packed residuals cut short.
	// Encode 128 values (width=7, packSize=112), then keep only last 6 bytes
	// (1 packed byte + 1-byte width + 4-byte min). DecodeGroup needs 112+5=117 bytes.
	vals := make([]uint32, 128)
	for i := range vals {
		vals[i] = uint32(i)
	}

	encoded := EncodeGroup(vals)
	truncated := encoded[len(encoded)-6:] // [1 packed byte][width][min]

	_, _, err = DecodeGroup(truncated, 128, nil)
	require.Error(t, err)
}

func TestRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		values []uint32
	}{
		{"uniform", []uint32{100, 100, 100, 100}},
		{"ascending", []uint32{10, 20, 30, 40, 50}},
		{"single", []uint32{42}},
		{"wide_range", []uint32{0, 1, 1000000}},
		{"max_group", func() []uint32 {
			v := make([]uint32, 128)
			for i := range v {
				v[i] = uint32(i * 7)
			}

			return v
		}()},
	}

	var dst []uint32

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeGroup(tt.values)

			var err error

			dst, _, err = DecodeGroup(encoded, len(tt.values), dst)
			require.NoError(t, err)
			require.Equal(t, tt.values, dst)
		})
	}
}

func TestDecodeGroupBufferReuse(t *testing.T) {
	sizes1 := []uint32{10, 20, 30}
	encoded1 := EncodeGroup(sizes1)

	dst, _, err := DecodeGroup(encoded1, len(sizes1), nil)
	require.NoError(t, err)

	sizes2 := []uint32{100, 200, 300, 400, 500}
	encoded2 := EncodeGroup(sizes2)

	dst, _, err = DecodeGroup(encoded2, len(sizes2), dst)
	require.NoError(t, err)
	require.Equal(t, sizes2, dst)
}

func TestEncodeGroupLayout(t *testing.T) {
	sizes := []uint32{100, 200, 300}
	encoded := EncodeGroup(sizes)

	// The last 5 bytes are [W][min(4)].
	// W = bits.Len32(300-100) = bits.Len32(200) = 8.
	require.Equal(t, uint8(8), encoded[len(encoded)-5])
	require.Equal(t, uint32(100), binary.LittleEndian.Uint32(encoded[len(encoded)-4:]))
}

func TestDecodeGroupWithPrefix(t *testing.T) {
	values := []uint32{5, 10, 15}
	encoded := EncodeGroup(values)

	buf := make([]byte, 20+len(encoded))
	for i := range buf[:20] {
		buf[i] = 0xAB
	}

	copy(buf[20:], encoded)

	decoded, consumed, err := DecodeGroup(buf, len(values), nil)
	require.NoError(t, err)
	require.Equal(t, len(encoded), consumed)
	require.Equal(t, values, decoded)
}

func TestDecodeGroupWidth0(t *testing.T) {
	// Craft a buffer with width=0: all values are equal to groupMin.
	// Layout: [0 packed bytes][width=0][min=42 LE]
	var buf [5]byte

	buf[0] = 0 // width = 0
	binary.LittleEndian.PutUint32(buf[1:], 42)

	decoded, consumed, err := DecodeGroup(buf[:], 3, nil)
	require.NoError(t, err)
	require.Equal(t, 5, consumed)
	require.Equal(t, []uint32{42, 42, 42}, decoded)
}
