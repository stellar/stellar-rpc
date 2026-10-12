package packfile

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		offsets []int64
	}{
		{"single record", []int64{0, 1000}},
		{"few records", []int64{0, 1000, 2500, 5000}},
		{"uniform sizes", func() []int64 {
			offsets := make([]int64, 101)
			for i := range offsets {
				offsets[i] = int64(i) * 4096
			}

			return offsets
		}()},
		{"variable sizes", []int64{0, 100, 5000, 5100, 50000, 50001}},
		{"exceeds one group", func() []int64 {
			offsets := make([]int64, 200+1) // 200 records = 2 groups
			for i := range offsets {
				offsets[i] = int64(i) * 6400
			}

			return offsets
		}()},
		{"exactly one group", func() []int64 {
			offsets := make([]int64, 128+1)
			for i := range offsets {
				offsets[i] = int64(i) * 1024
			}

			return offsets
		}()},
		{"partial last group", func() []int64 {
			offsets := make([]int64, 129+1) // 129 records = 1 full + 1 partial
			for i := range offsets {
				offsets[i] = int64(i) * 2048
			}

			return offsets
		}()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := encodeIndex(tt.offsets)
			require.NoError(t, err)

			recordCount := len(tt.offsets) - 1
			indexBase := tt.offsets[recordCount]

			decoded, err := decodeIndex(encoded, recordCount, len(encoded), indexBase)
			require.NoError(t, err)
			require.Equal(t, tt.offsets, decoded)
		})
	}
}

func TestIndexCRCCorruption(t *testing.T) {
	offsets := []int64{0, 1000, 2000, 3000}

	encoded, err := encodeIndex(offsets)
	require.NoError(t, err)

	// Corrupt a byte in the payload (before the CRC).
	encoded[0] ^= 0xFF

	_, err = decodeIndex(encoded, 3, len(encoded), 3000)
	require.ErrorIs(t, err, ErrChecksum)
}

func TestIndexCorruptCRCBytes(t *testing.T) {
	offsets := []int64{0, 1000, 2000, 3000}

	encoded, err := encodeIndex(offsets)
	require.NoError(t, err)

	// Corrupt the CRC itself.
	crcOffset := len(encoded) - 4
	binary.LittleEndian.PutUint32(encoded[crcOffset:], 0xDEADBEEF)

	_, err = decodeIndex(encoded, 3, len(encoded), 3000)
	require.ErrorIs(t, err, ErrChecksum)
}

func TestIndexTooSmall(t *testing.T) {
	_, err := decodeIndex([]byte{1, 2, 3}, 1, 3, 100)
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestIndexImplausibleRecordCount(t *testing.T) {
	// indexSize=10 means payload=6 bytes, max 1 group of 128 records.
	// Requesting 200 records should fail the OOM guard.
	buf := make([]byte, 10)

	_, err := decodeIndex(buf, 200, 10, 0)
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestIndexBaseMismatch(t *testing.T) {
	offsets := []int64{0, 1000, 2000, 3000}

	encoded, err := encodeIndex(offsets)
	require.NoError(t, err)

	// Pass wrong indexBase — should fail structural check.
	_, err = decodeIndex(encoded, 3, len(encoded), 9999)
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestIndexNegativeRecordCount(t *testing.T) {
	_, err := decodeIndex(make([]byte, 10), -1, 10, 0)
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestIndexBufferTooSmall(t *testing.T) {
	_, err := decodeIndex(make([]byte, 5), 1, 10, 0)
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestIndexEncodeEmptyOffsets(t *testing.T) {
	_, err := encodeIndex([]int64{})
	require.Error(t, err)
}

func TestIndexEncodeNonZeroStart(t *testing.T) {
	_, err := encodeIndex([]int64{100, 200})
	require.Error(t, err)
}

func TestIndexNonMonotonicOffsets(t *testing.T) {
	_, err := encodeIndex([]int64{0, 1000, 500})
	require.Error(t, err)
}

func TestIndexZeroRecords(t *testing.T) {
	offsets := []int64{0}

	encoded, err := encodeIndex(offsets)
	require.NoError(t, err)

	decoded, err := decodeIndex(encoded, 0, len(encoded), 0)
	require.NoError(t, err)
	require.Equal(t, []int64{0}, decoded)
}

func TestIndexDeltaExceedsUint32(t *testing.T) {
	_, err := encodeIndex([]int64{0, math.MaxUint32 + 1})
	require.Error(t, err)
}

func TestIndexLargeDelta(t *testing.T) {
	// Delta near MaxUint32 exercises width=32 in the FOR encoder.
	offsets := []int64{0, math.MaxUint32}

	encoded, err := encodeIndex(offsets)
	require.NoError(t, err)

	decoded, err := decodeIndex(encoded, 1, len(encoded), math.MaxUint32)
	require.NoError(t, err)
	require.Equal(t, offsets, decoded)
}
