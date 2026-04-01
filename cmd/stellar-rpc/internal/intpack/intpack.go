// Package intpack provides Frame-of-Reference (FOR) integer encoding for
// groups of uint32 values. Values are encoded as bit-packed residuals relative
// to the group minimum, requiring only ceil(log2(max-min)) bits per value.
//
// On-disk layout per group:
//
//	[packed residuals][1-byte width][4-byte minimum (little-endian)]
//
// Width and minimum are always the final 5 bytes, so callers can locate
// metadata from the tail of any buffer without knowing the packed size upfront.
package intpack

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"slices"
)

// EncodeGroup FOR-encodes values into one group.
// Panics if len(values) == 0.
func EncodeGroup(values []uint32) []byte {
	minVal, width := rangeWidth(values)
	packSize := (int(width)*len(values) + 7) / 8

	// 5 bytes for the footer (1-byte width + 4-byte minimum),
	// 7 bytes of overshoot for safe 8-byte writes during bit-packing.
	buf := make([]byte, packSize+5+7)
	packResiduals(buf, values, minVal, width)
	buf[packSize] = width
	binary.LittleEndian.PutUint32(buf[packSize+1:], minVal)

	return buf[:packSize+5]
}

// DecodeGroup FOR-decodes one group of n values from the tail of buf.
// The encoded group must be at the end of buf, but buf may contain
// additional data before it. Returns decoded values (written into dst[0:n],
// reallocating if cap(dst) < n), bytes consumed from the tail, and any error.
func DecodeGroup(buf []byte, n int, dst []uint32) ([]uint32, int, error) {
	if n <= 0 {
		return dst, 0, fmt.Errorf("intpack: FOR decode n must be > 0, got %d", n)
	}

	if len(buf) < 5 {
		return dst, 0, fmt.Errorf("intpack: FOR decode buf too short (%d bytes, need >= 5)", len(buf))
	}

	width := uint64(buf[len(buf)-5])
	if width > 32 {
		return dst, 0, fmt.Errorf("intpack: invalid FOR width %d (max 32)", width)
	}

	groupMin := binary.LittleEndian.Uint32(buf[len(buf)-4:])
	packSize := (int(width)*n + 7) / 8

	consumed := packSize + 5
	if len(buf) < consumed {
		return dst, 0, fmt.Errorf("intpack: FOR decode buf too short for payload (%d bytes, need >= %d)", len(buf), consumed)
	}

	dst = ensureCapU32(dst, n)
	unpackResiduals(buf[len(buf)-5-packSize:len(buf)-5], n, width, groupMin, dst)

	return dst, consumed, nil
}

// rangeWidth computes the minimum value and the bit width needed to
// FOR-encode values. Width is clamped to at least 1.
// bits.Len32 returns at most 32, which always fits in uint8.
func rangeWidth(values []uint32) (uint32, uint8) {
	minVal := slices.Min(values)
	maxVal := slices.Max(values)

	width := uint8(bits.Len32(maxVal - minVal)) //nolint:gosec // bits.Len32 returns [0,32], fits in uint8
	if width == 0 {
		width = 1
	}

	return minVal, width
}

// packResiduals bit-packs (values[i] - minVal) into buf using 8-byte
// read-modify-writes. buf must be at least 7 bytes longer than the packed
// payload to allow safe overshoot at the boundary.
func packResiduals(buf []byte, values []uint32, minVal uint32, width uint8) {
	for j, v := range values {
		residual := uint64(v - minVal)
		bitPos := uint64(j) * uint64(width)
		bytePos := bitPos / 8
		shift := bitPos % 8
		existing := binary.LittleEndian.Uint64(buf[bytePos:])
		binary.LittleEndian.PutUint64(buf[bytePos:], existing|(residual<<shift))
	}
}

// unpackResiduals unpacks n bit-packed residuals from packed and adds groupMin.
// Safe to call with any packed length — elements near the boundary where an
// 8-byte read would exceed len(packed) are decoded byte-by-byte.
//
// The uint64-to-int and uint64-to-uint32 conversions are safe:
//   - bytePos is bounded by len(packed) which fits in int
//   - (raw>>shift)&mask is bounded by width <= 32 bits, fits in uint32
func unpackResiduals(packed []byte, n int, w uint64, groupMin uint32, values []uint32) {
	if w == 0 {
		for i := range values {
			values[i] = groupMin
		}

		return
	}

	mask := uint64((1 << w) - 1)

	// When len(packed) < 7, safeLimit is negative — all reads use the
	// byte-by-byte fallback path since int(bytePos) is always >= 0.
	safeLimit := len(packed) - 7

	for j := range n {
		bitPos := uint64(j) * w
		bytePos := bitPos / 8
		shift := bitPos % 8

		var raw uint64
		if int(bytePos) < safeLimit { //nolint:gosec // bytePos bounded by packed length
			raw = binary.LittleEndian.Uint64(packed[bytePos:])
		} else {
			for k := 0; k < 8 && int(bytePos)+k < len(packed); k++ { //nolint:gosec // bytePos bounded by packed length
				raw |= uint64(packed[int(bytePos)+k]) << (k * 8) //nolint:gosec // bytePos bounded by packed length
			}
		}

		values[j] = groupMin + uint32((raw>>shift)&mask) //nolint:gosec // masked to width <= 32 bits
	}
}

func ensureCapU32(s []uint32, n int) []uint32 {
	if cap(s) < n {
		return make([]uint32, n)
	}

	return s[:n]
}
