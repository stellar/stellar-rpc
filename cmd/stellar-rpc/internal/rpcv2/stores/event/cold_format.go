package event

// cold_format.go defines the on-disk format for cold-Chunk artifacts
// and the building blocks shared by the cold writer and reader. Four
// concerns live here:
//
//   1. Filenames and packfile format identifiers for the three cold
//      artifacts (events.pack, index.pack, index.hash).
//
//   2. events.pack record codec: the record limits, the zstd encoder
//      constructor, and the shared zstd decoder.
//
//   3. LedgerOffsets app-data encoding (encodeLedgerOffsets /
//      decodeLedgerOffsets). The writer embeds the encoded form in
//      events.pack's app-data slot; the reader decodes it on open.
//
//   4. MPHF wrapper around github.com/stellar/streamhash —
//      buildMPHF + openMPHF + Lookup. The writer builds the
//      index.hash file via buildMPHF; the reader opens it via
//      openMPHF and routes term-key queries through Lookup.
//
// Writer-side code (ColdWriter, ColdIndexBuilder) lives in
// cold_writer.go + cold_index.go; the reader lives in cold_reader.go.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"math"
	"sort"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/intpack"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/zstd"
)

// ──────────────────────────────────────────────────────────────────
// Filenames + packfile format identifiers.
// ──────────────────────────────────────────────────────────────────

// Cold artifact filenames are chunk-ID-prefixed and live directly in a
// bucket directory, per the backfill design doc
// (design-docs/full-history-streaming-workflow.md).
// Layout: {events_root}/{bucketID:05d}/{chunkID:08d}-events.pack and
// {events_index_root}/{bucketID:05d}/{chunkID:08d}-index.pack (and
// -index.hash). The two roots are distinct, so a chunk's three files
// are not in one directory. ColdDirs carries the pair.
//
// Bucket path composition is the orchestrator's job — this package
// takes bucket directories and composes the per-chunk filename via
// these helpers.

// EventsPackName returns the events.pack filename for chunkID.
func EventsPackName(chunkID chunk.ID) string {
	return chunkID.String() + "-events.pack"
}

// IndexPackName returns the index.pack filename for chunkID.
func IndexPackName(chunkID chunk.ID) string {
	return chunkID.String() + "-index.pack"
}

// IndexRunsDirName returns the name of the directory ColdIndexBuilder spills
// chunkID's runs into while it builds the index.
func IndexRunsDirName(chunkID chunk.ID) string {
	return chunkID.String() + "-index.runs"
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
	// Bumped from 0xFE1E000B when the record fingerprint moved to the routed
	// key, so an older index.pack is rejected instead of missing every lookup.
	indexPackFormat packfile.Format = 0xFE1E000D // "Fellow Events 0xD"
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

// IndexRecordFingerprintLen is the byte width of a term's index.pack fingerprint.
const IndexRecordFingerprintLen = 4

// ──────────────────────────────────────────────────────────────────
// index.pack layout and app data.
//
// Entries are in MPHF slot order. A term whose serialized bitmap is at
// most indexSplitBytes is one entry: fingerprint[4] ‖ roaring portable
// bitmap. A larger term is split into C entries, one per slab of the
// chunk: its ids in that slab with no fingerprint, zero-length where
// it has none. With r split terms below slot s, slot s's entry, or its
// slab-0 entry, is at s + r×(C−1).
//
// The app data is the build stamp, then what locates the entries:
//
//	offset  size  field
//	0       1     version (0x02)
//	1       2     term schema version (uint16 BE)
//	3       8     indexed-field bitmask (uint64 BE)
//	11      4     slab count C (uint32 BE)
//	15      8×D   one row per split term, ascending by slot:
//	              slot (uint32 BE) ‖ fingerprint[4]
//
// The stamp records which term-derivation scheme and field set the
// index was built under, making the artifact self-describing: an
// index missing a term family becomes distinguishable from one that
// simply matched nothing. Freeze and walk write identical app data: the
// schema and mask are compile-time constants, and whether a term splits
// depends on its bitmap alone. The blob has an exact length, so any
// growth is a version bump.
// ──────────────────────────────────────────────────────────────────

const (
	indexStampVersion byte = 0x02
	indexStampLen          = 1 + 2 + 8
	indexAppDataLen        = indexStampLen + 4
	indexRowLen            = 4 + IndexRecordFingerprintLen
)

// indexSlabShift cuts a split term into slabs of one roaring container. It is
// fixed by the format, unlike slabShift, which tests shrink.
const indexSlabShift = 16

// indexSplitBytes is the serialized size past which a term is split.
const indexSplitBytes = 64 << 10

// indexPackMaxRecordBytes closes index.pack's records. With no item limit,
// their count, and so the tail Open reads, follows the file size.
const indexPackMaxRecordBytes = 16 << 10

// indexPackTailBytesPerRecord bounds the tail a record adds, app data and
// trailer included (3.3 to 3.5 bytes measured).
const indexPackTailBytesPerRecord = 4

// indexPackFirstRead sizes Open's first read to take in the tail of a pack of full records.
func indexPackFirstRead(fileSize int64) int {
	n := max(64<<10, indexPackTailBytesPerRecord*fileSize/indexPackMaxRecordBytes)
	return int((n + 4095) &^ 4095)
}

// indexPackWriterOptions has no codec: compressing roaring's encoding again
// made lookups 3.6× slower.
func indexPackWriterOptions() packfile.WriterOptions {
	return packfile.WriterOptions{
		Format:         indexPackFormat,
		MaxRecordBytes: indexPackMaxRecordBytes,
		RecordChecksum: indexPackChecksum,
		ContentHash:    true,
		Overwrite:      true,
	}
}

// indexLayout is what index.pack's app data says about where entries are.
type indexLayout struct {
	slabs uint32 // C
	rows  []byte // D rows of indexRowLen, ascending by slot
}

// appendIndexRow appends slot's row; slot must be above every slot in rows.
func appendIndexRow(rows []byte, slot uint32, fp [IndexRecordFingerprintLen]byte) []byte {
	rows = binary.BigEndian.AppendUint32(rows, slot)
	return append(rows, fp[:]...)
}

// locate returns the position of slot's entry, or of its slab-0 entry when
// slot is split, and for a split slot the fingerprint its row carries.
func (l indexLayout) locate(slot uint32) (int, [IndexRecordFingerprintLen]byte, bool) {
	n := len(l.rows) / indexRowLen
	below := sort.Search(n, func(i int) bool {
		return binary.BigEndian.Uint32(l.rows[i*indexRowLen:]) >= slot
	})
	pos := int(slot) + below*(int(l.slabs)-1)
	var fp [IndexRecordFingerprintLen]byte
	if below == n || binary.BigEndian.Uint32(l.rows[below*indexRowLen:]) != slot {
		return pos, fp, false
	}
	copy(fp[:], l.rows[below*indexRowLen+4:])
	return pos, fp, true
}

// check refuses a layout that does not address the pack it rides in. locate's
// search relies on the rows being strictly ascending.
func (l indexLayout) check(path string, keys uint64, count uint32) error {
	rows := uint64(len(l.rows) / indexRowLen)
	if rows > 0 && (l.slabs == 0 || l.slabs > 1<<(32-indexSlabShift)) {
		return fmt.Errorf("%w: events: %s splits %d terms over %d slabs", stores.ErrCorrupt, path, rows, l.slabs)
	}
	if want := keys + rows*(uint64(l.slabs)-1); uint64(count) != want {
		return fmt.Errorf(
			"%w: events: index pair mismatch at %s: index.pack holds %d entries, but index.hash holds %d keys "+
				"and the app data splits %d terms over %d slabs (mispaired artifacts)",
			stores.ErrCorrupt, path, count, keys, rows, l.slabs)
	}
	next := uint64(0)
	for i := range rows {
		slot := uint64(binary.BigEndian.Uint32(l.rows[i*indexRowLen:]))
		if slot < next || slot >= keys {
			return fmt.Errorf("%w: events: %s split row %d names slot %d, want one in [%d, %d)",
				stores.ErrCorrupt, path, i, slot, next, keys)
		}
		next = slot + 1
	}
	return nil
}

func encodeIndexAppData(l indexLayout) []byte {
	buf := make([]byte, indexAppDataLen, indexAppDataLen+len(l.rows))
	buf[0] = indexStampVersion
	binary.BigEndian.PutUint16(buf[1:3], TermSchemaVersion)
	binary.BigEndian.PutUint64(buf[3:11], IndexedFieldMask())
	binary.BigEndian.PutUint32(buf[11:15], l.slabs)
	return append(buf, l.rows...)
}

// decodeIndexAppData returns the term schema, field mask and layout in data.
// The layout's rows alias data.
func decodeIndexAppData(data []byte) (uint16, uint64, indexLayout, error) {
	if err := stores.CheckBlobVersion(data, indexStampVersion); err != nil {
		return 0, 0, indexLayout{}, fmt.Errorf("events: index.pack build stamp: %w", err)
	}
	if len(data) < indexAppDataLen || (len(data)-indexAppDataLen)%indexRowLen != 0 {
		return 0, 0, indexLayout{}, fmt.Errorf("%w: events: index.pack app data is %d bytes, want %d and %d per split term",
			stores.ErrCorrupt, len(data), indexAppDataLen, indexRowLen)
	}
	layout := indexLayout{slabs: binary.BigEndian.Uint32(data[11:15]), rows: data[indexAppDataLen:]}
	return binary.BigEndian.Uint16(data[1:3]), binary.BigEndian.Uint64(data[3:11]), layout, nil
}

// loadIndexLayout returns index.pack's layout, refusing a stamp that names a
// term schema or field set other than this binary's own.
func loadIndexLayout(indexPackPath string, r *stores.PackReader) (indexLayout, error) {
	ad, err := r.AppData()
	if err != nil {
		return indexLayout{}, fmt.Errorf("events: read build stamp of %s: %w", indexPackPath, err)
	}
	schema, mask, layout, err := decodeIndexAppData(ad)
	if err != nil {
		return indexLayout{}, fmt.Errorf("events: %s: %w", indexPackPath, err)
	}
	if schema != TermSchemaVersion || mask != IndexedFieldMask() {
		return indexLayout{}, fmt.Errorf(
			"events: %s was built under term schema %d with field mask %#x; this binary expects "+
				"schema %d with mask %#x (rebuilt index required, or a binary matching the artifact)",
			indexPackPath, schema, mask, TermSchemaVersion, IndexedFieldMask())
	}
	return layout, nil
}

// ──────────────────────────────────────────────────────────────────
// events.pack record codec.
// ──────────────────────────────────────────────────────────────────

// eventsPackItemsPerRecord is the most payloads packed into one
// events.pack record. Records are the unit the zstd encoder sees, so
// this also sets the compression frame size.
const eventsPackItemsPerRecord = 128

// eventsPackMaxRecordBytes bounds what other contracts' large events add
// to a read. Payloads average about 250 bytes, so records still close at
// eventsPackItemsPerRecord and the index stores no item counts.
const eventsPackMaxRecordBytes = 128 << 10

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
//	offset  size  field
//	0       1     version (0x02)
//	1       4     startLedger    (uint32 BE)
//	5       4     ledgerCount N  (uint32 BE), at most chunk.LedgersPerChunk
//	9       ...   each ledger's event count, in intpack groups of 128
//	              ledgers in ledger order; only the last group can be short
//
// Groups carry no length: intpack ends each group with its width and
// minimum, so the decoder walks the groups back from the end of the blob.
//
// The version byte makes future format additions safe across
// already-frozen Chunks; readers reject unknown versions at decode
// time so older binaries fail loudly.
// ──────────────────────────────────────────────────────────────────

const (
	ledgerOffsetsFormatVersion byte = 0x02
	ledgerOffsetsHeaderLen          = 1 + 4 + 4
	ledgerOffsetsGroupSize          = 128
)

// errBadLedgerOffsets is returned when events.pack's app data does not
// decode as LedgerOffsets.
var errBadLedgerOffsets = errors.New("malformed LedgerOffsets app data")

// encodeLedgerOffsets serializes o for packfile app-data embedding.
func encodeLedgerOffsets(o *LedgerOffsets) ([]byte, error) {
	if o == nil {
		return nil, errors.New("events: nil LedgerOffsets")
	}
	cumulative := o.Offsets()
	if len(cumulative) > int(chunk.LedgersPerChunk) {
		return nil, fmt.Errorf("events: LedgerOffsets holds %d ledgers, a chunk holds %d",
			len(cumulative), chunk.LedgersPerChunk)
	}

	buf := []byte{ledgerOffsetsFormatVersion}
	buf = binary.BigEndian.AppendUint32(buf, o.StartLedger())
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(cumulative))) //nolint:gosec // bounded above
	var counts [ledgerOffsetsGroupSize]uint32
	var prev uint32
	for base := 0; base < len(cumulative); base += ledgerOffsetsGroupSize {
		group := cumulative[base:min(base+ledgerOffsetsGroupSize, len(cumulative))]
		for i, c := range group {
			counts[i] = c - prev
			prev = c
		}
		buf = append(buf, intpack.EncodeGroup(counts[:len(group)])...)
	}
	return buf, nil
}

// decodeLedgerOffsets parses the app data written by encodeLedgerOffsets.
func decodeLedgerOffsets(data []byte) (*LedgerOffsets, error) {
	if err := stores.CheckBlobVersion(data, ledgerOffsetsFormatVersion); err != nil {
		return nil, fmt.Errorf("%w: %w", errBadLedgerOffsets, err)
	}
	if len(data) < ledgerOffsetsHeaderLen {
		return nil, fmt.Errorf("%w: %d bytes, want at least %d", errBadLedgerOffsets, len(data), ledgerOffsetsHeaderLen)
	}
	startLedger := binary.BigEndian.Uint32(data[1:5])
	n := binary.BigEndian.Uint32(data[5:9])
	if n > chunk.LedgersPerChunk {
		return nil, fmt.Errorf("%w: %d ledgers, a chunk holds %d", errBadLedgerOffsets, n, chunk.LedgersPerChunk)
	}

	offsets := make([]uint32, n)
	payload := data[ledgerOffsetsHeaderLen:]
	groupCount := (len(offsets) + ledgerOffsetsGroupSize - 1) / ledgerOffsetsGroupSize
	for g := groupCount - 1; g >= 0; g-- {
		base := g * ledgerOffsetsGroupSize
		group := offsets[base:min(base+ledgerOffsetsGroupSize, len(offsets))]
		_, consumed, err := intpack.DecodeGroup(payload, len(group), group)
		if err != nil {
			return nil, fmt.Errorf("%w: group %d: %w", errBadLedgerOffsets, g, err)
		}
		payload = payload[:len(payload)-consumed]
	}
	if len(payload) != 0 {
		return nil, fmt.Errorf("%w: %d bytes before the first group", errBadLedgerOffsets, len(payload))
	}

	var total uint32
	for i, count := range offsets {
		if count > math.MaxUint32-total {
			return nil, fmt.Errorf("%w: event counts overflow uint32", errBadLedgerOffsets)
		}
		total += count
		offsets[i] = total
	}
	return &LedgerOffsets{offsets: offsets, startLedger: startLedger}, nil
}

// ──────────────────────────────────────────────────────────────────
// MPHF wrapper around github.com/stellar/streamhash.
//
// The MPHF maps each TermKey (16 bytes of xxh3-128 over
// `field || value`) to a unique slot in [0, N), where N is the
// number of unique terms in a Chunk. index.pack holds each slot's
// bitmap, in slot order. The cold reader looks up a TermKey via
// Lookup, reads the slot's bitmap, and MUST verify the 4-byte
// fingerprint index.pack stores for the slot before trusting it: an
// MPHF returns a slot for every input, including keys never added
// at build time. False positives are screened by the fingerprint
// check; the bitmap intersection / post-filter logic downstream
// handles the residual single-event false-positive rate.
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
// stored in index.hash's user metadata. The 4-byte app fingerprint in index.pack
// comes from the routed key too; the downstream post-filter stays on the
// ORIGINAL TermKey bytes.
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

// ErrKeyNotFound is what Lookup reports when streamhash decides
// the supplied key was not in the build set. Vanilla MPHF semantics
// return a slot for any input (the design doc assumes this and uses
// a 4-byte fingerprint in index.pack to screen false positives).
// streamhash adds a partial fingerprint of its own: routing-stage
// detection catches some unseen keys outright and reports
// ErrKeyNotFound — a free fast-path no-match the caller should
// check before reading from index.pack. Unseen keys that slip past
// streamhash's check still need the 4-byte fingerprint downstream.
var ErrKeyNotFound = errors.New("events: key not in build set")

// mphf wraps a streamhash MPHF index, suitable for repeated lookups
// against term keys.
type mphf struct {
	idx    *streamhash.Index
	secret [stores.SecretLen]byte
}

// buildMPHF writes the MPHF over keys, which are routed and ascend, to
// outputPath and opens it. total is how many keys there are.
func buildMPHF(
	ctx context.Context, keys iter.Seq2[TermKey, error], total uint64, outputPath string, secret [stores.SecretLen]byte,
) (m *mphf, err error) {
	builder, builderErr := streamhash.NewSortedBuilder(ctx, outputPath, total,
		streamhash.WithMetadata(encodeEventsMeta(secret)))
	if builderErr != nil {
		return nil, fmt.Errorf("events: create streamhash builder: %w", builderErr)
	}
	// builder.Finish takes ownership of the builder's files; Close releases
	// them on every error path before it and is a no-op after it.
	defer func() {
		if err != nil {
			_ = builder.Close()
		}
	}()

	var i uint64
	for key, kerr := range keys {
		if kerr != nil {
			return nil, kerr
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

// routedKey is term blinded under the chunk secret, the key the MPHF uses.
func routedKey(secret [stores.SecretLen]byte, term TermKey) TermKey {
	return TermKey(stores.BlindKey(secret, term[:]))
}

// slotLookup is one key's answer from Lookup.
type slotLookup struct {
	slot uint32
	fp   [IndexRecordFingerprintLen]byte
	err  error
}

// Lookup returns, for each key, the dense slot in [0, N) it maps to
// and the fingerprint that index.pack must store for that slot.
//
// streamhash returns ErrKeyNotFound for keys its routing-stage check
// can prove were never in the build set; callers should treat this
// as a fast no-match and skip the index.pack read. For keys that DO
// produce a slot, callers MUST still validate the result via the
// 4-byte fingerprint index.pack stores for that slot: an MPHF can
// map an unseen key to a valid build-set slot, and only the
// fingerprint catches that residual collision.
//
// streamhash requests the keys' index pages together rather than one
// lookup after another, so on a cold page cache the lookups cost about
// one round of reads instead of one per key. results[i] answers keys[i].
func (m *mphf) Lookup(keys []TermKey) []slotLookup {
	routed := make([]TermKey, len(keys))
	queries := make([][]byte, len(keys))
	results := make([]slotLookup, len(keys))
	for i, key := range keys {
		routed[i] = routedKey(m.secret, key)
		queries[i] = routed[i][:]
		results[i].fp = fingerprintOf(routed[i])
	}
	for i, r := range m.idx.QueryBatch(queries) {
		results[i].slot, results[i].err = slotOf(r.Rank, r.Err)
	}
	return results
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

// lookupRouted is Lookup's answer for one key already routed.
func (m *mphf) lookupRouted(rk TermKey) (uint32, [IndexRecordFingerprintLen]byte, error) {
	slot, err := slotOf(m.idx.QueryRank(rk[:]))
	return slot, fingerprintOf(rk), err
}

// fingerprintOf is the fingerprint index.pack stores for the term routed to rk.
func fingerprintOf(rk TermKey) [IndexRecordFingerprintLen]byte {
	var fp [IndexRecordFingerprintLen]byte
	v, _ := streamhash.Fingerprint(rk[:]) // rk is 16 bytes, so this cannot fail
	binary.LittleEndian.PutUint32(fp[:], v)
	return fp
}

// slotOf turns a streamhash rank and error into Lookup's slot and error.
func slotOf(rank uint64, err error) (uint32, error) {
	if err != nil {
		if errors.Is(err, streamhash.ErrNotFound) {
			return 0, ErrKeyNotFound
		}
		return 0, fmt.Errorf("events: query: %w", err)
	}
	if rank > math.MaxUint32 {
		// streamhash returns uint64 but slot count is bounded by the
		// chunk's unique-term count (≪ 2^32). An overflow here would
		// signal a build-time invariant violation, not a query error.
		return 0, fmt.Errorf("events: slot %d overflows uint32", rank)
	}
	return uint32(rank), nil
}
