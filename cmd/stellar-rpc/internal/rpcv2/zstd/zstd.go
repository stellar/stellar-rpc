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
// This wrapper links system libzstd directly. On macOS it dynamically links
// via pkg-config (homebrew). No build tags, no vendored code, no interposition
// risk. Requires libzstd >= 1.5.7 (enforced at runtime via checkVersion()).
// Compressor is not safe for concurrent use — each goroutine should own
// its own instance. Decompressor is safe for concurrent use (it pools
// DCtxs internally) — instantiate once and share.
package zstd

/*
#cgo darwin pkg-config: libzstd
#cgo darwin CFLAGS: -I/opt/homebrew/include -I/usr/local/include
#cgo darwin LDFLAGS: -L/opt/homebrew/lib -L/usr/local/lib
#cgo linux LDFLAGS: -lzstd
#include <zstd.h>
*/
import "C"

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"unsafe"
)

//nolint:gochecknoglobals // one-time runtime version check
var checkVersion = sync.OnceFunc(func() {
	const minVersion = 10507 // 1.5.7

	v := uint(C.ZSTD_versionNumber())
	if v < minVersion {
		panic(fmt.Sprintf("zstd: runtime library version %d.%d.%d < required 1.5.7",
			v/10000, (v/100)%100, v%100))
	}
})

const zstdLevel = 3

// Compressor holds a reusable zstd compression context.
// Not safe for concurrent use — each goroutine should own one.
type Compressor struct {
	ctx *C.ZSTD_CCtx
}

// EncoderState pairs one compressor with its retained destination buffer —
// the reuse idiom for zstd encode state (a CGo context is expensive to
// create; a fresh worst-case dst per Encode was measured at 43% of hot
// ingestion's allocations before reuse). NOT safe for concurrent use: the
// owner serializes Encodes (the ledger hot store's single-flight
// compression). Deliberately NOT a sync.Pool: the state is one expensive,
// long-lived object, and sync.Pool's GC-emptied semantics were measured
// dropping it ~1-in-5 ledgers under per-ledger GC cadence — a ~15MB dst
// re-allocation plus a CGo context re-init each time. Callers' consumers
// must copy synchronously (e.g. rocksdb BatchWriter.Put, packfile
// AppendItem do).
type EncoderState struct {
	comp *Compressor
	buf  []byte
}

// NewEncoderState returns an encoder state with the given compressor options.
func NewEncoderState(opts ...CompressorOption) *EncoderState {
	return &EncoderState{comp: NewCompressor(opts...)}
}

// Encode compresses src into the retained buffer and returns the encoded
// bytes, which are valid until this state's next Encode.
func (s *EncoderState) Encode(src []byte) ([]byte, error) {
	out, err := s.comp.Encode(s.buf[:0], src)
	if err != nil {
		return nil, err
	}
	s.buf = out[:cap(out)]
	return out, nil
}

// CompressorOption configures a Compressor.
type CompressorOption func(*compressorConfig)

type compressorConfig struct {
	checksum bool
	workers  int
}

// WithWorkers enables zstd's internal multithreaded compression (n worker
// threads inside libzstd). The output is ONE standard frame — same header
// shape FrameHeaderValid gates, deterministic for fixed input+params — so
// the cold-inherits-hot frame contract is unaffected; the trade is a ≲1%
// ratio cost from job splitting. Requires a ZSTD_MULTITHREAD build of
// libzstd; NewCompressor panics loudly if the library refuses the
// parameter (a silent single-threaded fallback would fake the experiment).
func WithWorkers(n int) CompressorOption {
	return func(c *compressorConfig) { c.workers = n }
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
	if cfg.workers > 0 {
		if rc := C.ZSTD_CCtx_setParameter(ctx, C.ZSTD_c_nbWorkers, C.int(cfg.workers)); C.ZSTD_isError(rc) != 0 {
			panic("zstd: libzstd built without ZSTD_MULTITHREAD — WithWorkers unavailable")
		}
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

// Encode compresses src and writes the result into dst (growing dst if its
// capacity is insufficient). The returned slice is rooted in dst (or a fresh
// allocation) and is owned by the caller.
func (c *Compressor) Encode(dst, src []byte) ([]byte, error) {
	if len(src) == 0 {
		return dst[:0], nil
	}
	if c.ctx == nil {
		return nil, errors.New("zstd: Encode called on closed Compressor")
	}
	boundSize := C.ZSTD_compressBound(C.size_t(len(src)))
	if boundSize > C.size_t(math.MaxInt) {
		return nil, fmt.Errorf("zstd: input too large (compressBound=%d exceeds max int)", uint64(boundSize))
	}

	bound := int(boundSize)
	if cap(dst) < bound {
		dst = make([]byte, bound)
	} else {
		dst = dst[:bound]
	}

	n := C.ZSTD_compress2(c.ctx,
		unsafe.Pointer(&dst[0]), C.size_t(bound),
		unsafe.Pointer(&src[0]), C.size_t(len(src)))
	if C.ZSTD_isError(n) != 0 {
		return nil, fmt.Errorf("zstd: compress: %s", C.GoString(C.ZSTD_getErrorName(n)))
	}
	return dst[:int(n)], nil
}

// Close frees the compression context.
func (c *Compressor) Close() error {
	if c.ctx != nil {
		C.ZSTD_freeCCtx(c.ctx)
		c.ctx = nil
	}
	return nil
}

// Encode compresses data with zstd level 3 and content checksum.
// Allocates per call. Use Compressor for hot paths.
func Encode(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	c := NewCompressor()
	defer c.Close()
	return c.Encode(nil, data)
}

// Decompressor is a concurrent-safe pool of zstd decompression contexts.
// Decode may be called from any number of goroutines simultaneously; each
// call borrows a DCtx from an internal sync.Pool for the duration of the
// decompression. Idle DCtxs are reclaimed during GC; each carries a
// finalizer that frees its underlying C state.
//
// A single *Decompressor per process is the typical usage — instantiate
// once at app startup, share across all consumers.
type Decompressor struct {
	pool sync.Pool // holds *dctx
}

// dctx wraps a single ZSTD_DCtx so we can install a finalizer on it. The
// finalizer is the cleanup path when the pool drains an idle DCtx during
// GC; explicit Close on Decompressor is intentionally not provided — see
// the package doc.
type dctx struct {
	c *C.ZSTD_DCtx
}

func newDCtx() *dctx {
	c := C.ZSTD_createDCtx()
	if c == nil {
		panic("zstd: ZSTD_createDCtx returned NULL (out of memory)")
	}
	d := &dctx{c: c}
	runtime.SetFinalizer(d, (*dctx).free)
	return d
}

func (d *dctx) free() {
	if d.c != nil {
		C.ZSTD_freeDCtx(d.c)
		d.c = nil
	}
}

// NewDecompressor creates a new Decompressor. The returned value is
// concurrent-safe; multiple goroutines may share a single instance.
func NewDecompressor() *Decompressor {
	checkVersion()
	d := &Decompressor{}
	d.pool.New = func() any { return newDCtx() }
	return d
}

// Decode decompresses src into dst, returning the result.
// dst is reused if large enough. Frames must include the decompressed size
// in the header (standard for ZSTD_compress2). Streaming frames without a
// content size use a fixed 4x estimate and will fail if the actual ratio exceeds that.
//
// Safe to call from multiple goroutines concurrently.
func (d *Decompressor) Decode(dst, src []byte) ([]byte, error) {
	if len(src) == 0 {
		return dst[:0], nil
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

	ctx, _ := d.pool.Get().(*dctx)
	defer d.pool.Put(ctx)

	n := C.ZSTD_decompressDCtx(ctx.c,
		unsafe.Pointer(&dst[0]), C.size_t(len(dst)),
		unsafe.Pointer(&src[0]), C.size_t(len(src)))
	if C.ZSTD_isError(n) != 0 {
		return nil, fmt.Errorf("zstd: zstd decompress: %s",
			C.GoString(C.ZSTD_getErrorName(n)))
	}
	return dst[:int(n)], nil
}

// Decode decompresses src into dst, returning the result.
// Allocates a context per call. Use Decompressor for hot paths.
func Decode(dst, src []byte) ([]byte, error) {
	return NewDecompressor().Decode(dst, src)
}

// Frame_Header_Descriptor bit layout (RFC 8878 §3.1.1.1.1): bits 0-1 are the
// Dictionary_ID_flag, bit 2 the Content_Checksum_flag.
const (
	frameDescriptorDictIDMask   = 0x03
	frameDescriptorChecksumFlag = 0x04
)

// frameMagic is the zstd frame magic number 0xFD2FB528 as it appears on disk
// (little-endian).
var frameMagic = []byte{0x28, 0xB5, 0x2F, 0xFD} //nolint:gochecknoglobals // immutable format constant

// FrameHeaderValid checks — without decompressing — that src is a single
// zstd frame this package's other half can serve: magic number, a recorded
// frame content size (present and <= MaxUint32, the bound packfile item
// lengths live under), no dictionary ID (the shared Decompressor is
// dictionary-less), and the content checksum flag set (Compressor's default;
// the checksum is what makes a later corrupt read loud).
//
// This is the freeze-time guard for verbatim frame copies (hot ledgers CF →
// cold pack): it pins the invariant that hot ledger values remain plain,
// dictionary-less, content-sized, checksummed single frames. Any hot-side
// change that breaks one of these must revisit the cold ledger format in the
// same commit.
func FrameHeaderValid(src []byte) error {
	if len(src) < 5 {
		return fmt.Errorf("zstd: frame header: %d bytes, want >= 5", len(src))
	}
	if !bytes.Equal(src[:4], frameMagic) {
		return errors.New("zstd: frame header: bad magic")
	}
	descriptor := src[4]
	if descriptor&frameDescriptorDictIDMask != 0 {
		return errors.New("zstd: frame header: dictionary ID present; the shared decompressor is dictionary-less")
	}
	if descriptor&frameDescriptorChecksumFlag == 0 {
		return errors.New("zstd: frame header: content checksum flag unset")
	}
	fcs := C.ZSTD_getFrameContentSize(unsafe.Pointer(&src[0]), C.size_t(len(src)))
	switch fcs {
	case C.ZSTD_CONTENTSIZE_ERROR:
		return errors.New("zstd: frame header invalid")
	case C.ZSTD_CONTENTSIZE_UNKNOWN:
		return errors.New("zstd: frame header carries no content size")
	}
	if uint64(fcs) > math.MaxUint32 {
		return fmt.Errorf("zstd: frame claims content size %d > MaxUint32", uint64(fcs))
	}
	return nil
}
