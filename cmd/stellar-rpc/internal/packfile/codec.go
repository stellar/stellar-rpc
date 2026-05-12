// Codec extension points for packfile records.
//
// A packfile record's bytes can optionally be transformed by a caller-supplied
// codec on the way to disk and back. Format identifies which codec was used;
// RecordEncoder is the write-side transformation; RecordDecoder is the read
// side. Implementations and Format values are agreed on out of band — the
// packfile library does not interpret Format.

package packfile

import "io"

// Format is a caller-assigned identifier stored in the trailer. The packfile
// library does not interpret Format values; readers use them to dispatch to
// the matching decoder.
type Format uint32

// RecordEncoder transforms one record (the concatenation of items in a record)
// before it's written to disk. Typical implementations compress (e.g. zstd,
// which carries its own per-frame checksum) or add integrity (e.g. raw + a
// trailing CRC32C, for codecs that don't include a checksum). The library
// doesn't interpret the output bytes — readers dispatch on the trailer's
// Format field to apply the matching decoder.
//
// Encode writes the encoded bytes into dst (growing the slice if dst's
// capacity is insufficient) and returns the result. Pass nil or an empty
// slice for dst to get a freshly-allocated result. The returned slice is
// rooted in dst (or a new allocation if dst was too small) and is owned by
// the caller.
//
// Encode is called once per record before write. Close is called when the
// writer no longer needs the encoder (Finish or Close), so stateful encoders
// that hold non-Go resources (e.g. CGo zstd contexts) can release them
// deterministically rather than waiting on GC finalizers.
//
// A RecordEncoder is not safe for concurrent use — the writer creates one
// per worker goroutine via WriterOptions.NewRecordEncoder.
type RecordEncoder interface {
	Encode(dst, src []byte) ([]byte, error)
	io.Closer
}

// RecordDecoder is the read-side counterpart of RecordEncoder. It transforms
// one record's on-disk bytes back into the original record payload (the
// concatenation of items in the record). Typical implementations decompress
// (e.g. zstd) or strip a trailing CRC32C wrapper. Passthrough mode (nil
// decoder) reads bytes verbatim — symmetric to the writer's nil
// NewRecordEncoder.
//
// Decode writes the decoded bytes into dst (growing the slice if dst's
// capacity is insufficient) and returns the result. The returned slice is
// rooted in dst (or a new allocation) and is owned by the caller.
//
// A RecordDecoder MUST be safe for concurrent use — the reader shares a
// single instance across all workers in a ReadItems call and across all
// concurrent Read* calls on a Reader. The caller is responsible for any
// resource cleanup (typically via the decoder's own GC finalizer, the
// pattern *zstd.Decompressor uses); the library never calls anything on
// the decoder beyond Decode.
type RecordDecoder interface {
	Decode(dst, src []byte) ([]byte, error)
}
