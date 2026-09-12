package event

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
//   3. LedgerOffsets app-data encoding (encodeLedgerOffsets /
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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/zstd"
)

// ──────────────────────────────────────────────────────────────────
// Filenames + packfile format identifiers.
// ──────────────────────────────────────────────────────────────────

// Cold artifact filenames are chunk-ID-prefixed and live as flat
// siblings inside a bucket directory, per the backfill design doc
// (design-docs/full-history-streaming-workflow.md).
// Layout: {events_root}/{bucketID:05d}/{chunkID:08d}-events.pack
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
	// Bumped from 0xFE1E000B when index.pack grew dense-term parts: a bucket
	// record no longer means "one term per item" and the app data no longer
	// means "just a build stamp", so an older binary must refuse the artifact
	// rather than read a part record as 128 terms. 0xC..0xF are spent on
	// unreleased experiments, so the next free id is 0x10.
	indexPackFormat packfile.Format = 0xFE1E0010 // "Fellow Events 0x10" (dense-term parts)
)

// indexPackChecksum belongs to index.pack's on-disk identity, so it lives here
// with the format ID rather than at the writer's call site: every builder of
// this artifact has to agree on it, and the cold reader rejects an index.pack
// that lacks it.
//
// index.pack is the artifact that needs one. It stores records uncompressed,
// so no frame checksum covers them, and a flipped bit inside a serialized
// bitmap unmarshals cleanly into a DIFFERENT posting set — a wrong query
// answer rather than a failure. events.pack gets the same protection from its
// zstd frames.
const indexPackChecksum = packfile.ChecksumCRC32C

// IndexRecordFingerprintLen is the byte width of the leading
// fingerprint in every index.pack record. The cold reader checks
// this against the queried term's first four bytes to filter MPHF
// false positives before deserializing the bitmap.
const IndexRecordFingerprintLen = 4

// ──────────────────────────────────────────────────────────────────
// index.pack layout and app data.
//
// index.pack is a packfile of 128-item records (indexPackItemsPerRecord)
// with a CRC32C per record. There are two kinds of record.
//
// Bucket records come first, one per 128 MPHF slots, and hold the terms
// the chunk indexed in slot order: bucket record r holds slots 128r ..
// 128r+127, item = fp[4] ‖ roaring portable bitmap of the term's ids. The
// last bucket record is padded out to 128 items with empty ones, so the
// part records that follow start on a record boundary and their addresses
// are arithmetic.
//
// A term whose bitmap would make its bucket too big to read in one I/O
// unit is demoted (see cold_index.go): its slot item keeps the
// fingerprint and drops the bitmap, and the term is written again as part
// records. A part record covers 2^k slabs of 65,536 ids — item 0 =
// fp[4] ‖ roaring bitmap of that span's ids, items 1..127 empty — and a
// term's parts are contiguous, in span order, after every bucket record.
// Every span in [0, partCount) gets a record, empty ones included, so
// part p of a term is item 128·(firstRecord+p) and no search is needed.
//
// The directory that names the demoted terms rides in index.pack's
// app-data slot, behind the build stamp:
//
//	offset  size   field
//	0       1      version (0x02)
//	1       2      term schema version (uint16 BE)
//	3       8      indexed-field bitmask (uint64 BE)
//	11      8      numKeys        (uint64 BE)
//	19      4      bucketCount    (uint32 BE)
//	23      4      totalParts     (uint32 BE)
//	27      4      entryCount     (uint32 BE)
//	31      23×N   entries, sorted by TermKey:
//	                 key[16] ‖ firstRecord uint32 BE ‖ partCount uint16 BE ‖ k uint8
//
// The stamp records which term-derivation scheme and field set the index
// was built under, making the artifact self-describing: an index missing a
// term family becomes distinguishable from one that simply matched
// nothing. The four header counts are what the reader pairs index.hash and
// index.pack on. The entries stay bytes on the reader and are binary-searched
// in place — never parsed into a map, since the whole point is that a
// chunk has a few hundred of them and a query touches a handful.
//
// Freeze and walk write identical app data (the schema and mask are
// compile-time constants and demotion is deterministic), so freeze-vs-walk
// byte identity is unaffected. Decoding ignores bytes past the last entry so
// a future version can extend the blob without moving these fields.
// ──────────────────────────────────────────────────────────────────

const (
	indexStampVersion byte = 0x02
	indexStampLen          = 1 + 2 + 8
	// indexDirHeaderLen is the four counts that follow the stamp.
	indexDirHeaderLen = 8 + 4 + 4 + 4
	// indexDirEntryLen is the fixed stride of one directory entry.
	indexDirEntryLen = 16 + 4 + 2 + 1
)

// indexSlabShift is the slab width the part layout is cut on, as a power of
// two: one roaring container, 65,536 ids. It is the format's own constant,
// not the engine's slabShift (a test seam) — the bytes on disk cannot move
// because a test shrank a walk.
const indexSlabShift = 16

// partEntry is one directory row: where a demoted term's parts start, how
// many there are, and how many slabs each covers.
type partEntry struct {
	firstRecord uint32
	partCount   uint16
	k           uint8
}

// window is the range of part indices that can hold an id in w. Parts tile
// the chunk on spans of 2^k slabs, so this is a shift, not a search. It
// reports false when the window is empty or starts past the term's last
// part — both mean "this term has nothing here", which is a non-nil empty
// result and no read at all.
func (e partEntry) window(w IDRange) (uint32, uint32, bool) {
	if w.isEmpty() || e.partCount == 0 {
		return 0, 0, false
	}
	shift := uint(e.k) + indexSlabShift
	first := uint64(w.Start) >> shift
	last := uint64(w.End-1) >> shift
	top := uint64(e.partCount) - 1
	if first > top {
		return 0, 0, false
	}
	if last > top {
		last = top
	}
	return uint32(first), uint32(last), true //nolint:gosec // both <= partCount-1 (uint16)
}

// indexDirectory is index.pack's app data as the reader keeps it: the four
// pairing counts, plus the entry table left as bytes for binary search.
type indexDirectory struct {
	numKeys     uint64
	bucketCount uint32
	totalParts  uint32
	entries     []byte // entryCount × indexDirEntryLen, sorted by key
}

// entryCount is how many demoted terms the directory names.
func (d indexDirectory) entryCount() int { return len(d.entries) / indexDirEntryLen }

// lookup finds key's parts, reporting false for a term that was not demoted
// (which is almost every term). Binary search over the fixed stride; the
// table is sorted by TermKey, the same bytes the caller holds.
func (d indexDirectory) lookup(key TermKey) (partEntry, bool) {
	n := d.entryCount()
	i := sort.Search(n, func(i int) bool {
		return bytes.Compare(d.entries[i*indexDirEntryLen:i*indexDirEntryLen+len(key)], key[:]) >= 0
	})
	if i >= n {
		return partEntry{}, false
	}
	row := d.entries[i*indexDirEntryLen : (i+1)*indexDirEntryLen]
	if !bytes.Equal(row[:len(key)], key[:]) {
		return partEntry{}, false
	}
	return partEntry{
		firstRecord: binary.BigEndian.Uint32(row[16:20]),
		partCount:   binary.BigEndian.Uint16(row[20:22]),
		k:           row[22],
	}, true
}

// encodeIndexAppData serializes the build stamp followed by dir.
func encodeIndexAppData(dir indexDirectory) []byte {
	buf := make([]byte, indexStampLen+indexDirHeaderLen, indexStampLen+indexDirHeaderLen+len(dir.entries))
	buf[0] = indexStampVersion
	binary.BigEndian.PutUint16(buf[1:3], TermSchemaVersion)
	binary.BigEndian.PutUint64(buf[3:11], IndexedFieldMask())
	binary.BigEndian.PutUint64(buf[11:19], dir.numKeys)
	binary.BigEndian.PutUint32(buf[19:23], dir.bucketCount)
	binary.BigEndian.PutUint32(buf[23:27], dir.totalParts)
	//nolint:gosec // entry count is bounded by the chunk's term count
	binary.BigEndian.PutUint32(buf[27:31], uint32(len(dir.entries)/indexDirEntryLen))
	return append(buf, dir.entries...)
}

// decodeIndexAppData recovers (termSchema, fieldMask, directory) from an
// index.pack app-data blob, rejecting a short blob or an unknown stamp
// version. Bytes past the last entry are ignored (future extension room).
func decodeIndexAppData(data []byte) (uint16, uint64, indexDirectory, error) {
	var dir indexDirectory
	if err := stores.CheckBlobVersion(data, indexStampVersion); err != nil {
		return 0, 0, dir, fmt.Errorf("events: index.pack build stamp: %w", err)
	}
	if len(data) < indexStampLen+indexDirHeaderLen {
		return 0, 0, dir, fmt.Errorf("events: index.pack app data is %d bytes, want at least %d",
			len(data), indexStampLen+indexDirHeaderLen)
	}
	dir.numKeys = binary.BigEndian.Uint64(data[11:19])
	dir.bucketCount = binary.BigEndian.Uint32(data[19:23])
	dir.totalParts = binary.BigEndian.Uint32(data[23:27])
	count := int(binary.BigEndian.Uint32(data[27:31]))
	end := indexStampLen + indexDirHeaderLen + count*indexDirEntryLen
	if count < 0 || end < 0 || len(data) < end {
		return 0, 0, dir, fmt.Errorf("%w: events: index.pack directory claims %d entries, app data holds %d bytes",
			stores.ErrCorrupt, count, len(data))
	}
	dir.entries = data[indexStampLen+indexDirHeaderLen : end]
	return binary.BigEndian.Uint16(data[1:3]), binary.BigEndian.Uint64(data[3:11]), dir, nil
}

// loadIndexAppData reads index.pack's app data, refuses a stamp that names a
// term schema or field set other than this binary's own, and hands back the
// directory behind it.
func loadIndexAppData(indexPackPath string, r *stores.PackReader) (indexDirectory, error) {
	ad, err := r.AppData()
	if err != nil {
		return indexDirectory{}, fmt.Errorf("events: read build stamp of %s: %w", indexPackPath, err)
	}
	schema, mask, dir, err := decodeIndexAppData(ad)
	if err != nil {
		return indexDirectory{}, fmt.Errorf("events: %s: %w", indexPackPath, err)
	}
	if schema != TermSchemaVersion || mask != IndexedFieldMask() {
		return indexDirectory{}, fmt.Errorf(
			"events: %s was built under term schema %d with field mask %#x; this binary expects "+
				"schema %d with mask %#x (rebuilt index required, or a binary matching the artifact)",
			indexPackPath, schema, mask, TermSchemaVersion, IndexedFieldMask())
	}
	return dir, nil
}

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
// LedgerOffsets app-data wire format.
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
// in-memory representation of LedgerOffsets and let the cold reader
// resolve ledger range → eventID range in two array lookups.
//
// The version byte makes future format additions safe across
// already-frozen Chunks; readers reject unknown versions at decode
// time so older binaries fail loudly.
// ──────────────────────────────────────────────────────────────────

// LedgerOffsetsFormatVersion is the current on-disk version for the
// LedgerOffsets app-data block.
const LedgerOffsetsFormatVersion byte = 0x01

const ledgerOffsetsHeaderLen = 1 + 4 + 4

// ErrShortLedgerOffsets is returned when the app data buffer is empty,
// carries an unknown version byte, or is shorter than the declared header
// or trailing cumulative array.
var ErrShortLedgerOffsets = errors.New("events: LedgerOffsets app data too short")

// encodeLedgerOffsets serializes o for packfile app-data embedding.
func encodeLedgerOffsets(o *LedgerOffsets) ([]byte, error) {
	if o == nil {
		return nil, errors.New("events: nil LedgerOffsets")
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
// encodeLedgerOffsets back into a *LedgerOffsets. Used by the cold
// reader (PR-3a).
func DecodeLedgerOffsets(data []byte) (*LedgerOffsets, error) {
	if err := stores.CheckBlobVersion(data, LedgerOffsetsFormatVersion); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrShortLedgerOffsets, err)
	}
	if len(data) < ledgerOffsetsHeaderLen {
		return nil, ErrShortLedgerOffsets
	}
	startLedger := binary.BigEndian.Uint32(data[1:5])
	n := binary.BigEndian.Uint32(data[5:9])
	expected := ledgerOffsetsHeaderLen + int(n)*4
	if len(data) != expected {
		return nil, fmt.Errorf("%w: want %d bytes, got %d", ErrShortLedgerOffsets, expected, len(data))
	}

	offsets := NewLedgerOffsets(startLedger)
	var prev uint32
	for i := range n {
		cumulative := binary.BigEndian.Uint32(data[ledgerOffsetsHeaderLen+int(i)*4:])
		if cumulative < prev {
			return nil, fmt.Errorf("events: non-monotonic cumulative count at ledger %d", startLedger+i)
		}
		if err := offsets.Append(startLedger+i, cumulative-prev); err != nil {
			return nil, fmt.Errorf("events: decode LedgerOffsets: %w", err)
		}
		prev = cumulative
	}
	return offsets, nil
}

// ──────────────────────────────────────────────────────────────────
// MPHF wrapper around github.com/stellar/streamhash.
//
// The MPHF maps each TermKey (16 bytes of xxh3-128 over
// `field || value`) to a unique slot in [0, N), where N is the
// number of unique terms in a Chunk. index.pack is laid out as one
// roaring-bitmap record per slot. The cold reader looks up a
// TermKey via Lookup, reads the bitmap record at that slot, and
// MUST verify a 4-byte fingerprint stored alongside the bitmap
// before trusting it: an MPHF returns a slot for every input,
// including keys never added at build time. False positives are
// screened by the fingerprint check; the bitmap intersection /
// post-filter logic downstream handles the residual single-event
// false-positive rate.
//
// Hash compatibility: streamhash's AddKey/Query do not re-hash the
// supplied key — they take the first 16 bytes as the routing
// identity. TermKey is already a uniformly distributed 16-byte
// xxh3-128 value (produced by ComputeTermKey via
// streamhash.PreHashInPlace), but xxh3 is unkeyed, so an attacker
// could grind terms into one streamhash block and abort the build
// (see stores/blind.go). The wrapper therefore feeds
// streamhash stores.BlindKey(secret, TermKey) at both build and
// query, with the deterministic per-chunk secret (ColdIndexSecret)
// stored in index.hash's user metadata. The 4-byte app fingerprint in index.pack and the
// downstream post-filter stay on the ORIGINAL TermKey bytes.
// ──────────────────────────────────────────────────────────────────

// index.hash user-metadata wire format (streamhash WithMetadata):
//
//	offset  size  field
//	0       1     version (0x01)
//	1       16    routing secret (SipHash-2-4-128 key)
const (
	eventsMetaVersion byte = 0x01
	eventsMetaLen          = 1 + stores.SecretLen
)

// errBadIndexMetadata is returned when index.hash carries user
// metadata this binary cannot parse.
var errBadIndexMetadata = errors.New("malformed index.hash metadata")

func encodeEventsMeta(secret [stores.SecretLen]byte) []byte {
	buf := make([]byte, eventsMetaLen)
	buf[0] = eventsMetaVersion
	copy(buf[1:], secret[:])
	return buf
}

func decodeEventsMeta(data []byte) ([stores.SecretLen]byte, error) {
	var secret [stores.SecretLen]byte
	if err := stores.CheckBlobVersion(data, eventsMetaVersion); err != nil {
		return secret, fmt.Errorf("%w: %w", errBadIndexMetadata, err)
	}
	if len(data) != eventsMetaLen {
		return secret, fmt.Errorf("%w: %d bytes, want %d", errBadIndexMetadata, len(data), eventsMetaLen)
	}
	copy(secret[:], data[1:])
	return secret, nil
}

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
	idx    *streamhash.Index
	secret [stores.SecretLen]byte
}

// buildMPHF constructs an MPHF over every TermKey in bitmaps,
// writes the serialized form to outputPath, and returns an opened
// handle ready for immediate Lookup. The freeze path needs slot
// assignments before closing so it can populate index.pack at the
// correct offsets.
//
// len(bitmaps) supplies streamhash's required total-keys count;
// the map is iterated once to feed keys to the builder. The bitmap
// values are not consumed — only the TermKey participates in
// MPHF construction.
//
// Memory usage is bounded by streamhash's internal partition buffers,
// not by the chunk's unique-term count.
//
// Duplicate keys are rejected by streamhash.
//
// ctx is propagated to streamhash.NewBuilder so a long index build
// honors caller cancellation. The AddKey/Finish loop also checks
// ctx between keys so cancellation surfaces promptly on large
// inputs.
//
// secret is the chunk's deterministic routing secret (ColdIndexSecret),
// stored in metadata so readers route identically. Because it is fixed
// per chunk, an ErrBlockOverflow is non-retryable: a rebuild routes the
// same keys to the same blocks.
//
//nolint:nonamedreturns // named err carries through to the deferred builder.Close
func buildMPHF(
	ctx context.Context, bitmaps Bitmaps, outputPath string, secret [stores.SecretLen]byte,
) (m *mphf, err error) {
	total := len(bitmaps)

	tmpDir, terr := os.MkdirTemp("", "eventstore-unsorted-")
	if terr != nil {
		return nil, fmt.Errorf("events: create tmp dir for streamhash builder: %w", terr)
	}
	defer os.RemoveAll(tmpDir)

	builder, builderErr := streamhash.NewUnsortedBuilder(ctx, outputPath, uint64(total), tmpDir,
		streamhash.WithMetadata(encodeEventsMeta(secret)))
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
		rk := stores.BlindKey(secret, key[:])
		if err = builder.AddKey(rk[:], 0); err != nil {
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
// The file is mmapped rather than read whole. Pages fault in from the kernel
// page cache, which every reader of the same chunk shares regardless of its
// own lifetime, so a per-request open costs a map and unmap plus the pages
// its lookups touch, not a copy of a file whose size scales with the chunk's
// term count.
//
// Close unmaps; callers must call it.
func openMPHF(path string) (*mphf, error) {
	idx, err := streamhash.Open(path)
	if err != nil {
		return nil, fmt.Errorf("events: open %s: %w", path, err)
	}
	secret, merr := decodeEventsMeta(idx.UserMetadata())
	if merr != nil {
		_ = idx.Close()
		return nil, fmt.Errorf("events: parse %s: %w", path, merr)
	}
	return &mphf{idx: idx, secret: secret}, nil
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
func (m *mphf) Lookup(key TermKey) (uint32, error) {
	rk := stores.BlindKey(m.secret, key[:])
	slot, err := m.idx.QueryRank(rk[:])
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

// Close unmaps the index file; callers must call it (see openMPHF).
func (m *mphf) Close() error {
	return m.idx.Close()
}

// isEmpty reports whether the index holds zero terms (an eventless chunk).
func (m *mphf) isEmpty() bool { return m.numKeys() == 0 }

// numKeys returns the number of keys the MPHF was built over (0 for an
// eventless chunk); the cold reader cross-checks it against index.pack's
// record count.
func (m *mphf) numKeys() uint64 { return m.idx.NumKeys() }
