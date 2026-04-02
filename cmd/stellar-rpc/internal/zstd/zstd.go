// Package zstd provides compression and decompression using system libzstd
// via a thin CGO wrapper (~60 lines of C calls).
//
// We wrote this instead of using existing Go zstd bindings because:
//
//   - klauspost/compress (pure Go): ~92 MB/s compress on our 28KB blocks at
//     level 3. System C libzstd: ~365 MB/s. 4x slower.
//
//   - DataDog/zstd: by default compiles vendored C zstd without -O2, giving
//     only ~49 MB/s. With -tags external_libzstd it links system libzstd
//     (~236 MB/s) but the build tag is easy to forget. Worse, the vendored C
//     symbols cause ELF symbol interposition on Linux: when linked in the same
//     binary as RocksDB (which also uses libzstd), RocksDB's calls to
//     ZSTD_compress2 resolve to the unoptimized vendored copy, making RocksDB
//     5x slower. macOS is immune (Mach-O two-level namespace).
//
// This wrapper links system libzstd directly. On Linux it statically links
// libzstd.a; on macOS it dynamically links via pkg-config (homebrew). No build
// tags, no vendored code, no interposition risk. Requires libzstd >= 1.5.7
// (enforced at compile time and runtime). Compressor and Decompressor are not
// safe for concurrent use — each goroutine should own its own instance.
package zstd

/*
#cgo darwin pkg-config: libzstd
#cgo darwin CFLAGS: -I/opt/homebrew/include -I/usr/local/include
#cgo darwin LDFLAGS: -L/opt/homebrew/lib -L/usr/local/lib
#cgo linux LDFLAGS: -l:libzstd.a
#include <zstd.h>

#if ZSTD_VERSION_NUMBER < 10507
#error "libzstd >= 1.5.7 required"
#endif
*/
import "C"

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"unsafe"
)

var versionCheck sync.Once //nolint:gochecknoglobals // one-time runtime version check

func checkVersion() {
	versionCheck.Do(func() {
		const minVersion = 10507 // 1.5.7

		v := uint(C.ZSTD_versionNumber())
		if v < minVersion {
			panic(fmt.Sprintf("zstd: runtime library version %d.%d.%d < required 1.5.7",
				v/10000, (v/100)%100, v%100))
		}
	})
}

const zstdLevel = 3

// Compressor holds a reusable zstd compression context and output buffer.
// Not safe for concurrent use — each goroutine should own one.
type Compressor struct {
	ctx     *C.ZSTD_CCtx
	scratch []byte
}

// CompressorOption configures a Compressor.
type CompressorOption func(*compressorConfig)

type compressorConfig struct {
	checksum bool
}

// WithoutChecksum disables the zstd content checksum.
// Use when the caller provides its own integrity check (e.g., CRC32C).
func WithoutChecksum() CompressorOption {
	return func(cfg *compressorConfig) { cfg.checksum = false }
}

// NewCompressor creates a new Compressor. By default content checksums are enabled.
func NewCompressor(opts ...CompressorOption) *Compressor {
	checkVersion()
	cfg := compressorConfig{checksum: true}
	for _, o := range opts {
		o(&cfg)
	}
	ctx := C.ZSTD_createCCtx()
	if ctx == nil {
		panic("zstd: ZSTD_createCCtx returned NULL (out of memory)")
	}
	if rc := C.ZSTD_CCtx_setParameter(ctx, C.ZSTD_c_compressionLevel, zstdLevel); C.ZSTD_isError(rc) != 0 {
		C.ZSTD_freeCCtx(ctx)
		panic("zstd: set compression level: " + C.GoString(C.ZSTD_getErrorName(rc)))
	}
	var flag C.int
	if cfg.checksum {
		flag = 1
	}
	if rc := C.ZSTD_CCtx_setParameter(ctx, C.ZSTD_c_checksumFlag, flag); C.ZSTD_isError(rc) != 0 {
		C.ZSTD_freeCCtx(ctx)
		panic("zstd: set checksum flag: " + C.GoString(C.ZSTD_getErrorName(rc)))
	}
	c := &Compressor{ctx: ctx}
	// Safety net for C memory: when sync.Pool evicts items, there's no cleanup
	// callback, so the finalizer ensures the C context is eventually freed.
	runtime.SetFinalizer(c, (*Compressor).Close)

	return c
}

// Encode compresses data, reusing internal buffers.
// The returned slice is valid until the next call to Encode.
func (c *Compressor) Encode(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if c.ctx == nil {
		return nil, errors.New("zstd: Encode called on closed Compressor")
	}
	boundSize := C.ZSTD_compressBound(C.size_t(len(data)))
	if boundSize > C.size_t(math.MaxInt) {
		return nil, fmt.Errorf("zstd: input too large (compressBound=%d exceeds max int)", uint64(boundSize))
	}

	bound := int(boundSize)
	if cap(c.scratch) < bound {
		c.scratch = make([]byte, bound)
	} else {
		c.scratch = c.scratch[:bound]
	}

	n := C.ZSTD_compress2(c.ctx,
		unsafe.Pointer(&c.scratch[0]), C.size_t(bound),
		unsafe.Pointer(&data[0]), C.size_t(len(data)))
	if C.ZSTD_isError(n) != 0 {
		return nil, fmt.Errorf("zstd: compress: %s", C.GoString(C.ZSTD_getErrorName(n)))
	}
	return c.scratch[:int(n)], nil
}

// Close frees the compression context.
func (c *Compressor) Close() {
	if c.ctx != nil {
		C.ZSTD_freeCCtx(c.ctx)
		c.ctx = nil
	}
}

// Encode compresses data with zstd level 3 and content checksum.
// Allocates per call. Use Compressor for hot paths.
func Encode(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	c := NewCompressor()
	defer c.Close()
	out, err := c.Encode(data)
	if err != nil {
		return nil, err
	}
	// Copy since the slice is backed by c.scratch.
	result := make([]byte, len(out))
	copy(result, out)
	return result, nil
}

// Decompressor holds a reusable zstd decompression context.
// Not safe for concurrent use — each goroutine should own one.
type Decompressor struct {
	ctx *C.ZSTD_DCtx
}

// NewDecompressor creates a new Decompressor.
func NewDecompressor() *Decompressor {
	checkVersion()
	ctx := C.ZSTD_createDCtx()
	if ctx == nil {
		panic("zstd: ZSTD_createDCtx returned NULL (out of memory)")
	}
	d := &Decompressor{ctx: ctx}
	runtime.SetFinalizer(d, (*Decompressor).Close)

	return d
}

// Decode decompresses src into dst, returning the result.
// dst is reused if large enough. Frames must include the decompressed size
// in the header (standard for ZSTD_compress2). Streaming frames without a
// content size use a fixed 4x estimate and will fail if the actual ratio exceeds that.
func (d *Decompressor) Decode(dst, src []byte) ([]byte, error) {
	if len(src) == 0 {
		return dst[:0], nil
	}
	if d.ctx == nil {
		return nil, errors.New("zstd: Decode called on closed Decompressor")
	}

	// Get decompressed size from frame header.
	// ZSTD_getFrameContentSize returns an unsigned 64-bit int with two sentinel values:
	//   ZSTD_CONTENTSIZE_UNKNOWN (0xFFFFFFFFFFFFFFFF) — size not in header
	//   ZSTD_CONTENTSIZE_ERROR   (0xFFFFFFFFFFFFFFFE) — corrupt/invalid frame
	fcs := C.ZSTD_getFrameContentSize(
		unsafe.Pointer(&src[0]), C.size_t(len(src)))
	var size int
	switch fcs {
	case C.ZSTD_CONTENTSIZE_ERROR:
		return nil, errors.New("zstd: zstd frame header invalid")
	case C.ZSTD_CONTENTSIZE_UNKNOWN:
		// Our Compressor always writes content size (ZSTD_compress2 default).
		// This path only triggers for externally-produced streaming frames.
		size = len(src) * 4 // fallback estimate
	default:
		if fcs > math.MaxInt {
			return nil, fmt.Errorf("zstd: frame claims decompressed size %d (exceeds addressable memory)", uint64(fcs))
		}
		size = int(fcs)
		if size == 0 {
			return dst[:0], nil
		}
	}
	if cap(dst) < size {
		dst = make([]byte, size)
	} else {
		dst = dst[:size]
	}

	n := C.ZSTD_decompressDCtx(d.ctx,
		unsafe.Pointer(&dst[0]), C.size_t(len(dst)),
		unsafe.Pointer(&src[0]), C.size_t(len(src)))
	if C.ZSTD_isError(n) != 0 {
		return nil, fmt.Errorf("zstd: zstd decompress: %s",
			C.GoString(C.ZSTD_getErrorName(n)))
	}
	return dst[:int(n)], nil
}

// Close frees the decompression context.
func (d *Decompressor) Close() {
	if d.ctx != nil {
		C.ZSTD_freeDCtx(d.ctx)
		d.ctx = nil
	}
}

// Decode decompresses src into dst, returning the result.
// Allocates a context per call. Use Decompressor for hot paths.
func Decode(dst, src []byte) ([]byte, error) {
	d := NewDecompressor()
	defer d.Close()
	return d.Decode(dst, src)
}
