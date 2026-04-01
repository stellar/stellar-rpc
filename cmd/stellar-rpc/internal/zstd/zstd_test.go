package zstd

import (
	"bytes"
	"crypto/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRoundTrip(t *testing.T) {
	data := make([]byte, 10000)
	rand.Read(data)

	compressed, err := Encode(data)
	require.NoError(t, err)

	decompressed, err := Decode(nil, compressed)
	require.NoError(t, err)
	require.Equal(t, data, decompressed)
}

func TestCorruptData(t *testing.T) {
	data := make([]byte, 1000)
	rand.Read(data)

	compressed, err := Encode(data)
	require.NoError(t, err)

	// Truncate to half — should always fail.
	truncated := compressed[:len(compressed)/2]

	_, err = Decode(nil, truncated)
	require.Error(t, err)

	// Zero out the frame header — should always fail.
	corrupted := make([]byte, len(compressed))
	copy(corrupted, compressed)

	for i := range min(8, len(corrupted)) {
		corrupted[i] = 0
	}

	_, err = Decode(nil, corrupted)
	require.Error(t, err)
}

// TestEmptyInput verifies that nil and zero-length inputs produce nil/empty
// output without touching C code (which would SIGSEGV on a nil pointer).
func TestEmptyInput(t *testing.T) {
	got, err := Encode(nil)
	require.NoError(t, err)
	require.Nil(t, got)

	got, err = Encode([]byte{})
	require.NoError(t, err)
	require.Nil(t, got)

	// Decode with nil dst.
	got, err = Decode(nil, nil)
	require.NoError(t, err)
	require.Empty(t, got)

	// Decode with pre-allocated dst: returned slice should reuse dst's backing array.
	dst := make([]byte, 0, 64)

	got, err = Decode(dst, nil)
	require.NoError(t, err)
	require.Empty(t, got)
	require.Equal(t, cap(dst), cap(got), "Decode(dst, nil) did not reuse dst backing array")
}

// TestOneByte exercises the minimum-size input boundary.
func TestOneByte(t *testing.T) {
	data := []byte{0x42}

	compressed, err := Encode(data)
	require.NoError(t, err)
	require.NotEmpty(t, compressed)

	got, err := Decode(nil, compressed)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

// TestContextReuse verifies that Compressor and Decompressor produce correct
// results across multiple calls with varying payload sizes and compressibility.
func TestContextReuse(t *testing.T) {
	payloads := [][]byte{
		bytes.Repeat([]byte{0}, 4096), // highly compressible
		make([]byte, 1),               // minimal
		make([]byte, 100000),          // large random
		{0xDE, 0xAD},                  // tiny
		make([]byte, 8192),            // medium random
	}
	// Fill random payloads.
	rand.Read(payloads[2])
	rand.Read(payloads[4])

	c := NewCompressor()
	defer c.Close()

	d := NewDecompressor()
	defer d.Close()

	var dst []byte

	for i, data := range payloads {
		compressed, err := c.Encode(data)
		require.NoError(t, err, "iteration %d", i)

		// Copy compressed data before next Encode overwrites scratch.
		saved := make([]byte, len(compressed))
		copy(saved, compressed)

		dst, err = d.Decode(dst, saved)
		require.NoError(t, err, "iteration %d", i)
		require.Equal(t, data, dst, "iteration %d", i)
	}
}

// TestScratchAliasing verifies the aliasing contract: the slice returned by
// Compressor.Encode shares the internal scratch buffer, so a saved copy must
// be used if the result is needed after the next Encode call.
func TestScratchAliasing(t *testing.T) {
	c := NewCompressor()
	defer c.Close()

	data1 := make([]byte, 4096)
	rand.Read(data1)

	out1, err := c.Encode(data1)
	require.NoError(t, err)

	saved := make([]byte, len(out1))
	copy(saved, out1)

	got, err := Decode(nil, saved)
	require.NoError(t, err)
	require.Equal(t, data1, got)

	// Second encode with different data reuses scratch.
	data2 := make([]byte, 8192)
	rand.Read(data2)

	out2, err := c.Encode(data2)
	require.NoError(t, err)

	saved2 := make([]byte, len(out2))
	copy(saved2, out2)

	got, err = Decode(nil, saved2)
	require.NoError(t, err)
	require.Equal(t, data2, got)
}

// TestDstBufferReuse verifies that Decode reuses a pre-allocated dst buffer
// when it is large enough, avoiding allocation.
func TestDstBufferReuse(t *testing.T) {
	data := make([]byte, 5000)
	rand.Read(data)

	compressed, err := Encode(data)
	require.NoError(t, err)

	// Pre-allocate dst larger than needed.
	dst := make([]byte, 0, 10000)
	ptrBefore := &dst[:1][0]

	got, err := Decode(dst, compressed)
	require.NoError(t, err)
	require.Equal(t, data, got)

	ptrAfter := &got[:1][0]
	require.Equal(t, ptrBefore, ptrAfter, "Decode allocated new buffer instead of reusing dst")
}

// TestCloseIdempotent verifies that calling Close twice does not panic or
// double-free the C context.
func TestCloseIdempotent(_ *testing.T) {
	c := NewCompressor()
	c.Close()
	c.Close() // must not panic or double-free

	d := NewDecompressor()
	d.Close()
	d.Close() // must not panic or double-free
}

// TestChecksumDetectsBitFlip verifies that the xxhash64 content checksum
// (enabled via ZSTD_c_checksumFlag) catches single-bit corruption in the
// compressed payload.
func TestChecksumDetectsBitFlip(t *testing.T) {
	data := make([]byte, 4096)
	rand.Read(data)

	compressed, err := Encode(data)
	require.NoError(t, err)

	// Flip a bit in the middle of the compressed data (block area),
	// leaving magic and frame header intact.
	corrupted := make([]byte, len(compressed))
	copy(corrupted, compressed)

	flipIdx := len(corrupted) / 2
	corrupted[flipIdx] ^= 0x01

	_, err = Decode(nil, corrupted)
	require.Error(t, err)
}

// TestWithoutChecksum verifies that WithoutChecksum produces valid frames
// and that bit flips in the compressed payload are NOT detected (no checksum).
func TestWithoutChecksum(t *testing.T) {
	data := make([]byte, 4096)
	rand.Read(data)

	c := NewCompressor(WithoutChecksum())
	defer c.Close()

	compressed, err := c.Encode(data)
	require.NoError(t, err)

	saved := make([]byte, len(compressed))
	copy(saved, compressed)

	got, err := Decode(nil, saved)
	require.NoError(t, err)
	require.Equal(t, data, got)

	// Without checksum, a bit flip in the block area may decode without error
	// (producing wrong data) rather than being caught.
	corrupted := make([]byte, len(saved))
	copy(corrupted, saved)
	corrupted[len(corrupted)/2] ^= 0x01

	result, err := Decode(nil, corrupted)
	if err == nil && bytes.Equal(result, data) {
		t.Fatal("bit-flipped data decoded to identical output — checksum may still be enabled")
	}
	// Either err != nil (zstd block structure broken) or result != data (silent corruption).
	// Both are acceptable without checksum; the key is we don't guarantee detection.
}

// TestConcurrentInstances verifies that independent Compressor/Decompressor
// instances work correctly from multiple goroutines simultaneously.
func TestConcurrentInstances(t *testing.T) {
	const (
		goroutines = 8
		iterations = 100
	)

	payloads := make([][]byte, goroutines)
	for i := range payloads {
		payloads[i] = make([]byte, 1000+i*500)
		rand.Read(payloads[i])
	}

	var wg sync.WaitGroup

	wg.Add(goroutines)

	for g := range goroutines {
		go func() {
			defer wg.Done()

			c := NewCompressor()
			defer c.Close()

			d := NewDecompressor()
			defer d.Close()

			var dst []byte

			for range iterations {
				compressed, err := c.Encode(payloads[g])
				if err != nil {
					t.Errorf("goroutine %d: Encode: %v", g, err)
					return
				}

				saved := make([]byte, len(compressed))
				copy(saved, compressed)

				dst, err = d.Decode(dst, saved)
				if err != nil {
					t.Errorf("goroutine %d: Decode: %v", g, err)
					return
				}

				if !bytes.Equal(dst, payloads[g]) {
					t.Errorf("goroutine %d: round-trip mismatch", g)
					return
				}
			}
		}()
	}

	wg.Wait()
}

// TestEncodeAfterClose verifies that calling Encode on a closed Compressor
// returns an error instead of passing a nil C pointer (which would SIGSEGV).
func TestEncodeAfterClose(t *testing.T) {
	c := NewCompressor()
	c.Close()

	_, err := c.Encode([]byte("should error"))
	require.Error(t, err)
}

// TestDecodeAfterClose verifies that calling Decode on a closed Decompressor
// returns an error instead of crashing.
func TestDecodeAfterClose(t *testing.T) {
	d := NewDecompressor()
	d.Close()

	_, err := d.Decode(nil, []byte("dummy"))
	require.Error(t, err)
}

// TestDecodeNonZstdData verifies that garbage bytes are rejected cleanly
// with an error, not a crash.
func TestDecodeNonZstdData(t *testing.T) {
	garbage := []byte("this is not zstd compressed data at all")

	_, err := Decode(nil, garbage)
	require.Error(t, err)
}
