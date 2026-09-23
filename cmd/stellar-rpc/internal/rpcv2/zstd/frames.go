package zstd

/*
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>
*/
import "C"

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"unsafe"
)

// This file is the frame-sequence half of the package: reading a concatenation
// of frames without decompressing it, the skippable-frame codec a caller hides
// its own metadata in, and the windowed encoder that cuts a large input into
// independently decodable frames.
//
// ZSTD_findDecompressedSize and ZSTD_decompressBound are declared under
// libzstd's ZSTD_STATIC_LINKING_ONLY guard, so the preamble defines it; both
// symbols are exported by the shared library this package links, and the
// runtime version gate (>= 1.5.7) is what pins their behavior.

// A skippable frame is 4 magic bytes, a 4-byte little-endian payload length,
// and the payload. skippableMagicStart is the first of the sixteen magic
// numbers RFC 8878 §3.1.2 reserves for them and skippableMagicMask isolates
// that range from every other frame magic.
const (
	skippableMagicStart = 0x184D2A50
	skippableMagicMask  = 0xFFFFFFF0
	skippableHeaderLen  = 8
)

// maxFrameEncodeInFlight bounds how many of one input's frames EncodeFrames
// compresses at a time. Each in-flight frame holds a compression context and,
// under WithWorkers, that context's own libzstd threads, so the cap is what
// keeps a single large input from claiming the box.
const maxFrameEncodeInFlight = 8

// FrameSize is one frame's place in a sequence: the bytes it occupies on the
// wire and the bytes it expands to. A skippable frame's Raw is zero — it
// decompresses to nothing.
type FrameSize struct {
	Compressed, Raw int
}

// FrameCompressedSize is the on-wire length of the first frame in src,
// skippable frames included. It is how a reader walks a concatenation: the
// next frame begins that many bytes in.
func FrameCompressedSize(src []byte) (int, error) {
	if len(src) == 0 {
		return 0, errors.New("zstd: frame size: empty input")
	}
	n := C.ZSTD_findFrameCompressedSize(unsafe.Pointer(&src[0]), C.size_t(len(src)))
	if C.ZSTD_isError(n) != 0 {
		return 0, fmt.Errorf("zstd: frame size: %s", C.GoString(C.ZSTD_getErrorName(n)))
	}
	if n == 0 || uint64(n) > uint64(len(src)) {
		return 0, fmt.Errorf("zstd: frame size: first frame claims %d bytes of a %d-byte input",
			uint64(n), len(src))
	}
	return int(n), nil
}

// FrameContentSize is the decompressed length recorded in the first frame's
// header. A frame without one — a streaming frame — is an error: every
// compressed frame this package writes records it. A skippable frame reports
// zero, which is what it expands to.
func FrameContentSize(src []byte) (int, error) {
	if len(src) == 0 {
		return 0, errors.New("zstd: frame content size: empty input")
	}
	fcs := C.ZSTD_getFrameContentSize(unsafe.Pointer(&src[0]), C.size_t(len(src)))
	switch fcs {
	case C.ZSTD_CONTENTSIZE_ERROR:
		return 0, errors.New("zstd: frame content size: frame header invalid")
	case C.ZSTD_CONTENTSIZE_UNKNOWN:
		return 0, errors.New("zstd: frame content size: frame carries none")
	}
	if uint64(fcs) > math.MaxInt {
		return 0, fmt.Errorf("zstd: frame claims content size %d (exceeds addressable memory)", uint64(fcs))
	}
	return int(fcs), nil
}

// Frames walks src and returns one FrameSize per frame, in order, so a reader
// can map a raw offset onto the frame that holds it and that frame onto its
// bytes. src must end exactly on a frame boundary.
func Frames(src []byte) ([]FrameSize, error) {
	var out []FrameSize
	for off := 0; off < len(src); {
		n, err := FrameCompressedSize(src[off:])
		if err != nil {
			return nil, fmt.Errorf("zstd: frame at offset %d: %w", off, err)
		}
		raw := 0
		if !IsSkippable(src[off:]) {
			raw, err = FrameContentSize(src[off : off+n])
			if err != nil {
				return nil, fmt.Errorf("zstd: frame at offset %d: %w", off, err)
			}
		}
		out = append(out, FrameSize{Compressed: n, Raw: raw})
		off += n
	}
	return out, nil
}

// IsSkippable reports whether src begins with a skippable frame — one libzstd's
// decompressor passes over, which is where a caller puts bytes of its own
// inside an otherwise ordinary zstd stream.
func IsSkippable(src []byte) bool {
	return len(src) >= 4 && binary.LittleEndian.Uint32(src)&skippableMagicMask == skippableMagicStart
}

// SkippableFrame appends a skippable frame carrying payload to dst and returns
// the extended buffer. A decompressor skips the frame, so the payload reaches
// only a reader that goes looking for it.
//
// payload must be under 4 GiB, the width of the frame's length field; a longer
// one is a programming error, not a runtime condition.
func SkippableFrame(dst, payload []byte) []byte {
	if uint64(len(payload)) > math.MaxUint32 {
		panic(fmt.Sprintf("zstd: skippable frame payload is %d bytes, past the 4 GiB frame limit", len(payload)))
	}
	dst = binary.LittleEndian.AppendUint32(dst, skippableMagicStart)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(payload)))
	return append(dst, payload...)
}

// SkippablePayload returns the payload of the skippable frame at the head of
// src, ALIASING src, along with the whole frame's on-wire length. It errors
// when src does not begin with a skippable frame, or when the frame runs past
// the end of src.
func SkippablePayload(src []byte) ([]byte, int, error) {
	if len(src) < skippableHeaderLen {
		return nil, 0, fmt.Errorf("zstd: skippable frame: %d bytes, want >= %d", len(src), skippableHeaderLen)
	}
	if !IsSkippable(src) {
		return nil, 0, errors.New("zstd: skippable frame: bad magic")
	}
	size := uint64(binary.LittleEndian.Uint32(src[4:]))
	if size > uint64(len(src)-skippableHeaderLen) {
		return nil, 0, fmt.Errorf("zstd: skippable frame: payload claims %d bytes, %d remain",
			size, len(src)-skippableHeaderLen)
	}
	return src[skippableHeaderLen : skippableHeaderLen+size], skippableHeaderLen + int(size), nil
}

// EncodeFrames compresses src into a sequence of independently decodable
// frames and returns the concatenated bytes plus one FrameSize per frame, in
// order. The bytes are valid until this state's next Encode or EncodeFrames,
// exactly as Encode's are.
//
// An input at or under window is ONE frame, byte-for-byte what Encode produces
// for it, and headerEnd is ignored — the property that keeps a deployment
// whose inputs all fit the window writing the bytes it wrote before frames
// existed. A larger input is cut at headerEnd first and then every window raw
// bytes: frame 0 is src[:headerEnd], so a reader wanting only that prefix
// decodes it alone, and the rest are fixed cuts aligned to nothing in the
// content, so a reader recovers each frame's raw extent from the returned
// sizes and nothing else.
//
// Frames compress concurrently, at most maxFrameEncodeInFlight at a time, each
// through a pooled context carrying this state's own options — so one input's
// frames are the same bytes however many goroutines ran.
func (s *EncoderState) EncodeFrames(src []byte, headerEnd, window int) ([]byte, []FrameSize, error) {
	if window <= 0 {
		return nil, nil, fmt.Errorf("zstd: frame window must be positive, got %d", window)
	}
	if len(src) == 0 {
		out, err := s.Encode(src)
		return out, nil, err
	}
	if len(src) <= window {
		out, err := s.Encode(src)
		if err != nil {
			return nil, nil, err
		}
		return out, []FrameSize{{Compressed: len(out), Raw: len(src)}}, nil
	}
	if headerEnd <= 0 || headerEnd >= len(src) {
		return nil, nil, fmt.Errorf("zstd: first frame ends at %d, outside a %d-byte input", headerEnd, len(src))
	}
	return s.encodeCut(src, cuts(len(src), headerEnd, window))
}

// cuts returns the raw length of each frame: the header prefix, then fixed
// window-sized pieces of what follows.
func cuts(total, headerEnd, window int) []int {
	rest := total - headerEnd
	out := make([]int, 0, 1+(rest+window-1)/window)
	out = append(out, headerEnd)
	for off := 0; off < rest; off += window {
		out = append(out, min(window, rest-off))
	}
	return out
}

// encodeCut is EncodeFrames' multi-frame half. Each frame compresses into its
// own worst-case slot of one retained buffer — slots are laid out from the
// raw lengths alone, so a worker addresses its own without waiting on the
// frames ahead of it — and the slots are then compacted forward into the
// final concatenation.
func (s *EncoderState) encodeCut(src []byte, raws []int) ([]byte, []FrameSize, error) {
	slots := make([]int, len(raws)+1)
	for k, n := range raws {
		bound := int(C.ZSTD_compressBound(C.size_t(n)))
		if bound <= 0 || slots[k] > math.MaxInt-bound {
			return nil, nil, fmt.Errorf("zstd: %d frames overflow the encode buffer", len(raws))
		}
		slots[k+1] = slots[k] + bound
	}
	need := slots[len(raws)]
	if cap(s.buf) < need {
		s.buf = make([]byte, need)
	}
	buf := s.buf[:need]

	sizes := make([]FrameSize, len(raws))
	errs := make([]error, len(raws))
	var wg sync.WaitGroup
	for w := range min(len(raws), maxFrameEncodeInFlight) {
		wg.Go(func() {
			c := s.borrowCompressor()
			defer s.returnCompressor(c)
			lo := 0
			for k := range raws {
				if k%maxFrameEncodeInFlight == w {
					out, err := c.Encode(buf[slots[k]:slots[k]:slots[k+1]], src[lo:lo+raws[k]])
					if err != nil {
						errs[k] = fmt.Errorf("zstd: frame %d: %w", k, err)
						return
					}
					sizes[k] = FrameSize{Compressed: len(out), Raw: raws[k]}
				}
				lo += raws[k]
			}
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, nil, err
	}

	// Compact: every frame moves DOWN to the end of its predecessor, so each
	// slot is read before anything is written over it.
	pos := 0
	for k := range raws {
		if pos != slots[k] {
			copy(buf[pos:], buf[slots[k]:slots[k]+sizes[k].Compressed])
		}
		pos += sizes[k].Compressed
	}
	s.buf = buf[:cap(buf)]
	return buf[:pos], sizes, nil
}

// borrowCompressor takes a frame-encode context from the state's pool,
// building one with the state's own options when the pool is empty — the
// options are what make a frame encode identically whichever context served
// it.
func (s *EncoderState) borrowCompressor() *Compressor {
	if c, ok := s.spare.Get().(*Compressor); ok {
		return c
	}
	return NewCompressor(s.opts...)
}

func (s *EncoderState) returnCompressor(c *Compressor) { s.spare.Put(c) }
