package eventstore

// cold_format.go defines the on-disk format for cold-Chunk artifacts
// and the building blocks shared by the cold writer and reader. Four
// concerns live here:
//
//   1. Filenames and packfile format identifiers for the three cold
//      artifacts (events.pack, index.pack, index.hash).
//
//   2. events.pack record codec: ItemsPerRecord, the zstd encoder
//      constructor, and the shared zstd decoder.
//
//   3. events.LedgerOffsets app-data encoding (encodeLedgerOffsets /
//      DecodeLedgerOffsets). The writer embeds the encoded form in
//      events.pack's app-data slot; the reader decodes it on open.
//
//   4. MPHF wrapper around github.com/stellar/streamhash —
//      buildMPHF + openMPHF + Lookup. The writer builds the
//      index.hash file via buildMPHF; the reader opens it via
//      openMPHF and routes term-key queries through Lookup.
//
// Writer-side code (ColdWriter, WriteColdIndex) lives in
// cold_writer.go + cold_index.go; the reader lives in cold_reader.go.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/zstd"
)

// ──────────────────────────────────────────────────────────────────
// Filenames + packfile format identifiers.
// ──────────────────────────────────────────────────────────────────

// Cold artifact filenames are chunk-ID-prefixed and live as flat
// siblings inside a bucket directory, per the backfill design doc
// (full-history/design-docs/03-backfill-workflow.md §"Directory
// Structure"). Layout: {events_root}/{bucketID:05d}/{chunkID:08d}-events.pack
// and analogous for index.pack / index.hash.
//
// Bucket path composition is the orchestrator's job — this package
// takes a bucket directory and composes the per-chunk filename via
// these helpers.

// EventsPackName returns the events.pack filename for chunkID.
func EventsPackName(chunkID chunk.ID) string {
	return chunkID.String() + "-events.pack"
}

// IndexPackName returns the index.pack filename for chunkID.
func IndexPackName(chunkID chunk.ID) string {
	return chunkID.String() + "-index.pack"
}

// IndexHashName returns the index.hash filename for chunkID.
func IndexHashName(chunkID chunk.ID) string {
	return chunkID.String() + "-index.hash"
}

// Packfile format identifiers — caller-assigned, arbitrary but
// stable. Distinct values catch mis-pointed reads at open time.
const (
	// Bumped from 0xFE1E000A when events.pack switched to zstd-compressed
	// records. The Format value identifies the on-disk codec; readers
	// dispatch on it to select a matching RecordDecoder.
	eventsPackFormat packfile.Format = 0xFE1E000C // "Fellow Events 0xC" (zstd)
	indexPackFormat  packfile.Format = 0xFE1E000B // "Fellow Events 0xB"
)

// IndexRecordFingerprintLen is the byte width of the leading
// fingerprint in every index.pack record. The cold reader checks
// this against the queried term's first four bytes to filter MPHF
// false positives before deserializing the bitmap.
const IndexRecordFingerprintLen = 4

// ──────────────────────────────────────────────────────────────────
// events.pack record codec.
// ──────────────────────────────────────────────────────────────────

// eventsPackItemsPerRecord is the number of payloads packed into one
// events.pack record. Records are the unit the zstd encoder sees, so
// this also sets the compression frame size.
const eventsPackItemsPerRecord = 128

// indexPackItemsPerRecord is the number of bitmaps packed into one
// index.pack record. Chosen to keep the on-disk offset array small
// — one offset entry per record, not per bitmap. At ~600K unique
// terms per production chunk, batch=1 produces a ~2.4 MB resident
// offset array per ColdReader; batch=128 reduces that to ~19 KB
// (~130× smaller), and that cost scales linearly with concurrent
// reader count.
//
// Lookup latency is measured in noise between the two settings;
// per-record I/O reads 128 bitmaps' worth of bytes but only decodes
// one, and the bitmaps themselves are small enough that the wasted
// read is dominated by the bitmap deserialization the caller does
// anyway.
const indexPackItemsPerRecord = 128

// newEventsPackEncoder constructs a fresh zstd encoder for one
// packfile writer goroutine. RecordEncoder is not safe for concurrent
// use, so the packfile writer invokes this per worker.
func newEventsPackEncoder() packfile.RecordEncoder { return zstd.NewCompressor() }

// eventsPackDecoder is the process-wide zstd decoder for events.pack
// records. packfile.RecordDecoder is required to be concurrent-safe,
// and zstd.Decompressor satisfies that, so a single shared instance
// serves every ColdReader.
//
//nolint:gochecknoglobals // shared by design; the decoder is stateless + concurrent-safe
var eventsPackDecoder = zstd.NewDecompressor()

// ──────────────────────────────────────────────────────────────────
// events.LedgerOffsets app-data wire format.
//
// Embedded in events.pack's app-data slot:
//
//	offset  size       field
//	0       1          version (0x01)
//	1       4          startLedger        (uint32 BE)
//	5       4          ledgerCount N      (uint32 BE)
//	9       N × 4      cumulative event count per position
//	                   (uint32 BE; entry i = events through ledger
//	                    startLedger + i)
//
// Cumulative counts (rather than per-ledger counts) match the
// in-memory representation of events.LedgerOffsets and let the cold reader
// resolve ledger range → eventID range in two array lookups.
//
// The version byte makes future format additions safe across
// already-frozen Chunks; readers reject unknown versions at decode
// time so older binaries fail loudly.
// ──────────────────────────────────────────────────────────────────

// LedgerOffsetsFormatVersion is the current on-disk version for the
// events.LedgerOffsets app-data block.
const LedgerOffsetsFormatVersion byte = 0x01

const ledgerOffsetsHeaderLen = 1 + 4 + 4

// ErrUnknownLedgerOffsetsVersion is returned when decoding app data
// whose leading version byte isn't recognized by this binary.
var ErrUnknownLedgerOffsetsVersion = errors.New("events: unknown events.LedgerOffsets format version")

// ErrShortLedgerOffsets is returned when the app data buffer is
// shorter than the declared header or trailing cumulative array.
var ErrShortLedgerOffsets = errors.New("events: events.LedgerOffsets app data too short")

// encodeLedgerOffsets serializes o for packfile app-data embedding.
func encodeLedgerOffsets(o *events.LedgerOffsets) ([]byte, error) {
	if o == nil {
		return nil, errors.New("events: nil events.LedgerOffsets")
	}
	cumulative := o.Offsets()
	n := uint32(len(cumulative)) //nolint:gosec // bounded by chunk's ledger count

	buf := make([]byte, ledgerOffsetsHeaderLen+int(n)*4)
	buf[0] = LedgerOffsetsFormatVersion
	binary.BigEndian.PutUint32(buf[1:5], o.StartLedger())
	binary.BigEndian.PutUint32(buf[5:9], n)
	for i, c := range cumulative {
		binary.BigEndian.PutUint32(buf[ledgerOffsetsHeaderLen+i*4:], c)
	}
	return buf, nil
}

// DecodeLedgerOffsets parses the packfile app-data block written by
// encodeLedgerOffsets back into a *events.LedgerOffsets. Used by the cold
// reader (PR-3a).
func DecodeLedgerOffsets(data []byte) (*events.LedgerOffsets, error) {
	if len(data) < ledgerOffsetsHeaderLen {
		return nil, ErrShortLedgerOffsets
	}
	if data[0] != LedgerOffsetsFormatVersion {
		return nil, fmt.Errorf("%w: 0x%02x", ErrUnknownLedgerOffsetsVersion, data[0])
	}
	startLedger := binary.BigEndian.Uint32(data[1:5])
	n := binary.BigEndian.Uint32(data[5:9])
	expected := ledgerOffsetsHeaderLen + int(n)*4
	if len(data) != expected {
		return nil, fmt.Errorf("%w: want %d bytes, got %d", ErrShortLedgerOffsets, expected, len(data))
	}

	offsets := events.NewLedgerOffsets(startLedger)
	var prev uint32
	for i := range n {
		cumulative := binary.BigEndian.Uint32(data[ledgerOffsetsHeaderLen+int(i)*4:])
		if cumulative < prev {
			return nil, fmt.Errorf("events: non-monotonic cumulative count at ledger %d", startLedger+i)
		}
		if err := offsets.Append(startLedger+i, cumulative-prev); err != nil {
			return nil, fmt.Errorf("events: decode events.LedgerOffsets: %w", err)
		}
		prev = cumulative
	}
	return offsets, nil
}

// ──────────────────────────────────────────────────────────────────
// MPHF wrapper around github.com/stellar/streamhash.
//
// The MPHF maps each events.TermKey (16 bytes of xxh3-128 over
// `field || value`) to a unique slot in [0, N), where N is the
// number of unique terms in a Chunk. index.pack is laid out as one
// roaring-bitmap record per slot. The cold reader looks up a
// events.TermKey via Lookup, reads the bitmap record at that slot, and
// MUST verify a 4-byte fingerprint stored alongside the bitmap
// before trusting it: an MPHF returns a slot for every input,
// including keys never added at build time. False positives are
// screened by the fingerprint check; the bitmap intersection /
// post-filter logic downstream handles the residual single-event
// false-positive rate.
//
// Hash compatibility: streamhash's AddKey/Query do not re-hash the
// supplied key — they take the first 16 bytes as the routing
// identity. events.TermKey is already a uniformly distributed 16-byte
// xxh3-128 value (produced by events.ComputeTermKey via
// streamhash.PreHashInPlace), so the wrapper passes events.TermKey[:]
// through. No double-hashing.
// ──────────────────────────────────────────────────────────────────

// ErrEmptyBuildSet is returned by buildMPHF when len(keys) == 0. An
// MPHF over zero keys has no slots and isn't useful to the cold
// index pipeline.
var ErrEmptyBuildSet = errors.New("events: cannot build an MPHF with zero keys")

// ErrKeyNotFound is returned by Lookup when streamhash decides the
// supplied key was not in the build set. Vanilla MPHF semantics
// return a slot for any input (the design doc assumes this and uses
// a 4-byte fingerprint in index.pack to screen false positives).
// streamhash adds a partial fingerprint of its own: routing-stage
// detection catches some unseen keys outright and reports
// ErrKeyNotFound — a free fast-path no-match the caller should
// check before reading from index.pack. Unseen keys that slip past
// streamhash's check still need the 4-byte fingerprint downstream.
var ErrKeyNotFound = errors.New("events: key not in build set")

// mphf wraps a streamhash MPHF index, suitable for repeated Lookup
// against term keys.
type mphf struct {
	idx *streamhash.Index
}

// buildMPHF constructs an MPHF over every events.TermKey in bitmaps,
// writes the serialized form to outputPath, and returns an opened
// handle ready for immediate Lookup. The freeze path needs slot
// assignments before closing so it can populate index.pack at the
// correct offsets.
//
// len(bitmaps) supplies streamhash's required total-keys count;
// the map is iterated once to feed keys to the builder. The bitmap
// values are not consumed — only the events.TermKey participates in
// MPHF construction.
//
// Memory usage is bounded by streamhash's internal partition buffers,
// not by the chunk's unique-term count.
//
// len(bitmaps) == 0 returns ErrEmptyBuildSet. Duplicate keys are
// rejected by streamhash.
//
// ctx is propagated to streamhash.NewBuilder so a long index build
// honors caller cancellation. The AddKey/Finish loop also checks
// ctx between keys so cancellation surfaces promptly on large
// inputs.
//
//nolint:nonamedreturns // named err carries through to the deferred builder.Close
func buildMPHF(ctx context.Context, bitmaps events.Bitmaps, outputPath string) (m *mphf, err error) {
	total := len(bitmaps)
	if total <= 0 {
		return nil, ErrEmptyBuildSet
	}

	tmpDir, terr := os.MkdirTemp("", "eventstore-unsorted-")
	if terr != nil {
		return nil, fmt.Errorf("events: create tmp dir for streamhash builder: %w", terr)
	}
	defer os.RemoveAll(tmpDir)

	builder, builderErr := streamhash.NewUnsortedBuilder(ctx, outputPath, uint64(total), tmpDir)
	if builderErr != nil {
		return nil, fmt.Errorf("events: create streamhash builder: %w", builderErr)
	}
	// streamhash.Builder owns temp partition files and fds. On any
	// error path below, builder.Close must run to release them.
	// builder.Finish takes ownership and Close becomes a no-op
	// on the success path.
	defer func() {
		if err != nil {
			_ = builder.Close()
		}
	}()

	var i int
	for key := range bitmaps {
		if err = ctx.Err(); err != nil {
			return nil, fmt.Errorf("events: build MPHF canceled after %d keys: %w", i, err)
		}
		if err = builder.AddKey(key[:], 0); err != nil {
			return nil, fmt.Errorf("events: add key %d: %w", i, err)
		}
		i++
	}
	if err = builder.Finish(); err != nil {
		return nil, fmt.Errorf("events: finalize build at %s: %w", outputPath, err)
	}

	return openMPHF(outputPath)
}

// openMPHF loads a previously-built MPHF file (typically
// <chunkDir>/index.hash produced by an earlier buildMPHF) for
// query-time lookups.
//
// The file is read into memory up-front via os.ReadFile +
// streamhash.OpenBytes rather than mmapped. Rationale: a typical
// MPHF for a single Chunk is small (~hundreds of KB at production
// term counts), and on storage with expensive random IOPS (e.g.
// EBS, ~1 ms each) mmap page-faults on cold Lookups cost more than
// a single sequential read amortized across the index's lifetime.
//
// Close on the returned handle is a no-op for the OpenBytes path
// (streamhash holds no fd / mmap), but callers should still call it
// for symmetry with other open variants.
func openMPHF(path string) (*mphf, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("events: read %s: %w", path, err)
	}
	idx, err := streamhash.OpenBytes(data)
	if err != nil {
		return nil, fmt.Errorf("events: parse %s: %w", path, err)
	}
	return &mphf{idx: idx}, nil
}

// Lookup returns the dense slot in [0, N) that key maps to.
//
// streamhash returns ErrKeyNotFound for keys its routing-stage check
// can prove were never in the build set; callers should treat this
// as a fast no-match and skip the index.pack read. For keys that DO
// produce a slot, callers MUST still validate the result via the
// 4-byte fingerprint stored alongside the bitmap at that slot in
// index.pack — an MPHF can map an unseen key to a valid build-set
// slot, and only the fingerprint catches that residual collision.
func (m *mphf) Lookup(key events.TermKey) (uint32, error) {
	slot, err := m.idx.QueryRank(key[:])
	if err != nil {
		if errors.Is(err, streamhash.ErrNotFound) {
			return 0, ErrKeyNotFound
		}
		return 0, fmt.Errorf("events: query: %w", err)
	}
	if slot > math.MaxUint32 {
		// streamhash returns uint64 but slot count is bounded by the
		// chunk's unique-term count (≪ 2^32). An overflow here would
		// signal a build-time invariant violation, not a query error.
		return 0, fmt.Errorf("events: slot %d overflows uint32", slot)
	}
	return uint32(slot), nil
}

// Close releases the underlying mmap. Idempotent.
func (m *mphf) Close() error {
	return m.idx.Close()
}
