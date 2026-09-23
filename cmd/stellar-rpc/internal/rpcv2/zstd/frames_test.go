package zstd

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// compressible returns n bytes that zstd shrinks but does not erase, so frame
// sizes in a test are neither degenerate nor random-incompressible.
func compressible(n int) []byte {
	out := make([]byte, n)
	seed := make([]byte, 64)
	rand.Read(seed)
	for i := range out {
		out[i] = seed[i%len(seed)] ^ byte(i/len(seed))
	}
	return out
}

// TestDecodeConcatenatedFrames pins the read contract the framed ledger value
// rests on: several independently compressed frames decode, in order, to the
// concatenation of their inputs.
func TestDecodeConcatenatedFrames(t *testing.T) {
	parts := [][]byte{compressible(5000), compressible(17), compressible(120000)}
	var joined, stream []byte
	for _, p := range parts {
		joined = append(joined, p...)
		frame, err := Encode(p)
		require.NoError(t, err)
		stream = append(stream, frame...)
	}

	got, err := Decode(nil, stream)
	require.NoError(t, err)
	assert.Equal(t, joined, got)
}

// TestDecodeSkipsALeadingSkippableFrame pins the cold record's shape: a
// skippable frame carrying a reader's own metadata contributes nothing to the
// decoded bytes.
func TestDecodeSkipsALeadingSkippableFrame(t *testing.T) {
	payload := []byte("a transaction span table would live here")
	body := compressible(9000)
	frame, err := Encode(body)
	require.NoError(t, err)
	record := append(SkippableFrame(nil, payload), frame...)

	got, err := Decode(nil, record)
	require.NoError(t, err)
	assert.Equal(t, body, got)

	require.True(t, IsSkippable(record))
	back, frameLen, err := SkippablePayload(record)
	require.NoError(t, err)
	assert.Equal(t, payload, back)
	assert.Equal(t, len(payload)+8, frameLen)
	assert.False(t, IsSkippable(record[frameLen:]))
}

// TestDecodeOfARecordSizedFromTheFirstFrameOnly pins the deliberate trade in
// leaving the cold pack format unbumped: a reader predating framed records
// sizes its destination from the FIRST frame's content size, which a leading
// skippable frame reports as zero — so such a reader produces nothing at all
// rather than a wrong ledger.
func TestDecodeOfARecordSizedFromTheFirstFrameOnly(t *testing.T) {
	body := compressible(9000)
	frame, err := Encode(body)
	require.NoError(t, err)
	record := append(SkippableFrame(nil, []byte("table")), frame...)

	first, err := FrameContentSize(record)
	require.NoError(t, err)
	assert.Zero(t, first, "a skippable frame reports no content")

	// The same sizing over a plain single-frame record still yields the whole
	// ledger, which is why untabled records stay readable by any reader.
	plain, err := FrameContentSize(frame)
	require.NoError(t, err)
	assert.Equal(t, len(body), plain)
}

// TestFrameHeaderValidAcceptsConcatenations pins the freeze-time guard: any
// number of checksummed, content-sized frames behind at most one leading
// skippable frame is valid, and nothing else is.
func TestFrameHeaderValidAcceptsConcatenations(t *testing.T) {
	one, err := Encode(compressible(4000))
	require.NoError(t, err)
	two, err := Encode(compressible(9000))
	require.NoError(t, err)
	skippable := SkippableFrame(nil, []byte("table"))

	require.NoError(t, FrameHeaderValid(one))
	require.NoError(t, FrameHeaderValid(append(bytes.Clone(one), two...)))
	require.NoError(t, FrameHeaderValid(append(bytes.Clone(skippable), append(bytes.Clone(one), two...)...)))

	require.Error(t, FrameHeaderValid(skippable), "a skippable frame alone carries no ledger")
	require.Error(t, FrameHeaderValid(append(bytes.Clone(one), skippable...)),
		"only a LEADING skippable frame is allowed")
	require.Error(t, FrameHeaderValid(append(bytes.Clone(one), 0x00)),
		"trailing bytes are a corruption, not padding")

	// A frame without a checksum is refused even inside an otherwise valid
	// sequence, since the checksum is what makes a corrupt read loud.
	unchecked, err := NewCompressor(WithoutChecksum()).Encode(nil, compressible(100))
	require.NoError(t, err)
	require.Error(t, FrameHeaderValid(append(bytes.Clone(one), unchecked...)))
}

// TestFramesWalksASequence pins the directory-building walk: one entry per
// frame with its on-wire and decompressed extents, skippable frames included.
func TestFramesWalksASequence(t *testing.T) {
	bodies := [][]byte{compressible(3000), compressible(11000)}
	record := SkippableFrame(nil, []byte("table"))
	for _, b := range bodies {
		frame, err := Encode(b)
		require.NoError(t, err)
		record = append(record, frame...)
	}

	frames, err := Frames(record)
	require.NoError(t, err)
	require.Len(t, frames, 3)
	assert.Equal(t, FrameSize{Compressed: len("table") + 8, Raw: 0}, frames[0])
	assert.Equal(t, len(bodies[0]), frames[1].Raw)
	assert.Equal(t, len(bodies[1]), frames[2].Raw)

	total := 0
	for _, f := range frames {
		total += f.Compressed
	}
	assert.Equal(t, len(record), total, "the frames must tile the record")
}

// TestEncodeFramesInsideTheWindowIsUnchanged pins the byte-identity floor: an
// input the window covers is exactly the bytes Encode produces for it, whatever
// the caller passes as the header cut.
func TestEncodeFramesInsideTheWindowIsUnchanged(t *testing.T) {
	src := compressible(50000)
	want, err := NewEncoderState().Encode(src)
	require.NoError(t, err)
	want = bytes.Clone(want)

	state := NewEncoderState()
	got, frames, err := state.EncodeFrames(src, 128, 1<<20)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	require.Len(t, frames, 1)
	assert.Equal(t, FrameSize{Compressed: len(want), Raw: len(src)}, frames[0])
}

// TestEncodeFramesCutsPastTheWindow pins the framed shape: the header cut
// first, then fixed window-sized cuts, each an independent frame whose sizes
// the caller can navigate by.
func TestEncodeFramesCutsPastTheWindow(t *testing.T) {
	const window, header = 4096, 300
	src := compressible(window*3 + 17)

	state := NewEncoderState()
	got, frames, err := state.EncodeFrames(src, header, window)
	require.NoError(t, err)
	require.Len(t, frames, 1+(len(src)-header+window-1)/window,
		"the header cut plus ceil((n-header)/window) cuts")
	assert.Equal(t, header, frames[0].Raw)

	// The sizes must tile both the input and the output, and each frame must
	// decode to exactly its own slice of the input.
	rawAt, compAt := 0, 0
	for i, f := range frames {
		assert.Equal(t, f.Compressed, mustFrameSize(t, got[compAt:]), "frame %d on-wire size", i)
		decoded, derr := Decode(nil, got[compAt:compAt+f.Compressed])
		require.NoError(t, derr)
		assert.Equal(t, src[rawAt:rawAt+f.Raw], decoded, "frame %d content", i)
		rawAt += f.Raw
		compAt += f.Compressed
	}
	assert.Equal(t, len(src), rawAt)
	assert.Equal(t, len(got), compAt)

	// And the whole concatenation still decodes to the whole input.
	whole, err := Decode(nil, got)
	require.NoError(t, err)
	assert.Equal(t, src, whole)

	require.NoError(t, FrameHeaderValid(got))
}

// TestEncodeFramesIsDeterministic pins what the two cold materializers rely
// on: the same input and options produce the same bytes, whichever contexts
// the concurrent frame encode happened to use.
func TestEncodeFramesIsDeterministic(t *testing.T) {
	const window = 2048
	src := compressible(window*9 + 5)
	first, sizes, err := NewEncoderState().EncodeFrames(src, 64, window)
	require.NoError(t, err)
	first = bytes.Clone(first)

	second, again, err := NewEncoderState().EncodeFrames(src, 64, window)
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.Equal(t, sizes, again)
}

// TestEncodeFramesRejectsAnImpossibleHeaderCut pins that a caller who cannot
// locate the header is told so rather than handed an arbitrary cut.
func TestEncodeFramesRejectsAnImpossibleHeaderCut(t *testing.T) {
	src := compressible(5000)
	_, _, err := NewEncoderState().EncodeFrames(src, 0, 1024)
	require.Error(t, err)
	_, _, err = NewEncoderState().EncodeFrames(src, len(src), 1024)
	require.Error(t, err)
	_, _, err = NewEncoderState().EncodeFrames(src, 100, 0)
	require.Error(t, err)
}

// TestSkippablePayloadRejectsTruncation pins that a payload claiming more than
// the buffer holds is an error, not a slice past the end.
func TestSkippablePayloadRejectsTruncation(t *testing.T) {
	frame := SkippableFrame(nil, []byte("0123456789"))
	_, _, err := SkippablePayload(frame[:len(frame)-1])
	require.Error(t, err)
	_, _, err = SkippablePayload([]byte{1, 2, 3})
	require.Error(t, err)

	body, err := Encode(compressible(100))
	require.NoError(t, err)
	_, _, err = SkippablePayload(body)
	require.Error(t, err, "a compressed frame is not a skippable one")
}

func mustFrameSize(t *testing.T, src []byte) int {
	t.Helper()
	n, err := FrameCompressedSize(src)
	require.NoError(t, err)
	return n
}
