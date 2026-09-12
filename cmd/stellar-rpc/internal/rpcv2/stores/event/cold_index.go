package event

// cold_index.go is the index half of the cold-Chunk pipeline. It
// produces index.pack (per-slot bitmap records) + index.hash (the
// serialized MPHF) inside a Chunk's cold directory.
//
// The events.pack writer half lives in cold_writer.go. Shared format
// constants, the LedgerOffsets app-data wire format, and the
// MPHF wrapper live in cold_format.go.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// coldRoutingDomain is the DeriveIndexSecret domain for events cold indexes
// (distinct from txhash's "txhash").
const coldRoutingDomain = "events"

// ColdIndexSecret derives chunkID's routing secret from the build-side master
// key. The single derivation both ingest and the index build use, so rebuilds
// stay byte-identical.
func ColdIndexSecret(catalogSecret []byte, chunkID chunk.ID) [stores.SecretLen]byte {
	return stores.DeriveIndexSecret(catalogSecret, coldRoutingDomain, uint32(chunkID))
}

// WriteColdIndex produces index.pack + index.hash for chunkID inside
// bucketDir. Both files are fsync'd before the function returns.
// bucketDir is the chunk's bucket directory; filenames are composed
// from chunkID via IndexPackName / IndexHashName so the two halves
// of the cold artifact always live together.
//
// A zero-term bitmaps (an eventless chunk, e.g. a pre-Soroban
// backfill range) produces a real (empty) index.hash over zero terms
// plus a zero-record index.pack. The cold reader resolves every
// LookupKeys entry against it to a nil-bitmap miss through the
// ordinary path, so neither readers nor orchestrators need a
// pack-without-index special case.
//
// bitmaps is the complete term index for the Chunk, uniquely owned by
// the caller (no concurrent reader holds a pointer to any of its
// bitmaps). WriteColdIndex mutates each bitmap in place via
// RunOptimize before MarshalBinary — RunOptimize re-encodes long runs
// of set bits as RUN containers, which MarshalBinary then serializes
// more compactly. For chunk 5999 the on-disk shrink is ~14% (108MB →
// 93MB), concentrated in dense, clustered terms (popular contracts,
// common topic[0] verbs). This pairs with the fastaggregation.go fix
// in RoaringBitmap/roaring#81 — without that fix, the RUN containers
// hit a slow (*Bitmap).lazyOR path at query time and K≥12 regresses
// catastrophically.
//
// Both cold backfill and the live-chunk freeze build a Bitmaps single-threaded by
// re-deriving terms from raw LCMs (per-event TermsForBytes + Bitmaps.AddTo) and
// hand it directly here.
//
// index.hash is the MPHF serialized via buildMPHF.
//
// index.pack format. One packfile record per MPHF slot, in slot
// order. Each record is:
//
//	offset  size  field
//	0       4     fingerprint (first 4 bytes of the TermKey hash)
//	4       N     serialized roaring bitmap (Bitmap.MarshalBinary)
//
// The cold reader uses mphf.Lookup(term) → slot to find the record
// position, packfile.Reader.ReadItem(slot, ...) to read the bytes,
// verifies the 4-byte fingerprint against term[:4], and then
// deserializes the bitmap on match. Unseen terms still produce a
// slot (vanilla MPHF semantics) but their fingerprint mismatches —
// the cold reader rejects them at that point.
//
// streamhash's MPHF is a *minimal* perfect hash: slots are dense in
// [0, len(bitmaps)), so packfile record positions exactly equal
// slots. An assertion guards this invariant in case streamhash
// semantics ever shift.
//
// Failure semantics: on error, WriteColdIndex removes any index.hash
// or index.pack it produced so the bucket dir is left clean for retry.
// (index.pack cleanup is handled by packfile.Writer.Close; index.hash
// is removed here via a deferred best-effort os.Remove.)
//
// ctx cancels the MPHF build phase (the expensive part for large
// chunks); the subsequent index.pack write is a tight in-memory
// loop that doesn't poll ctx.
//
// secret is the chunk's deterministic routing secret (ColdIndexSecret).
// A streamhash.ErrBlockOverflow is non-retryable: rebuilding with the
// same secret routes the same keys to the same blocks.
func WriteColdIndex(
	ctx context.Context, chunkID chunk.ID, bitmaps Bitmaps, bucketDir string, secret [stores.SecretLen]byte,
) (err error) {
	indexPackPath := filepath.Join(bucketDir, IndexPackName(chunkID))
	indexHashPath := filepath.Join(bucketDir, IndexHashName(chunkID))

	// On any error path past this point (including a partial write
	// from buildMPHF itself), remove the orphaned index.hash. Joined
	// into the returned error so cleanup failures surface to callers.
	defer func() {
		if err == nil {
			return
		}
		if rmErr := os.Remove(indexHashPath); rmErr != nil && !errors.Is(rmErr, fs.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("events: remove orphan %s: %w", indexHashPath, rmErr))
		}
	}()

	m, err := buildMPHF(ctx, bitmaps, indexHashPath, secret)
	if err != nil {
		return fmt.Errorf("events: build MPHF: %w", err)
	}
	defer m.Close()

	entries := make([]indexEntry, 0, len(bitmaps))
	for term, bitmap := range bitmaps {
		slot, lerr := m.Lookup(term)
		if lerr != nil {
			return fmt.Errorf("events: MPHF lookup during index.pack build: %w", lerr)
		}
		// App fingerprint stays on the ORIGINAL term key, not the routed key.
		var fp [IndexRecordFingerprintLen]byte
		copy(fp[:], term[:IndexRecordFingerprintLen])
		// Mutate in place — bitmaps is uniquely owned by the caller, built
		// single-threaded either way: cold backfill from the .pack, or the freeze
		// from the read-only hot DB.
		bitmap.RunOptimize()
		entries = append(entries, indexEntry{slot: slot, key: term, fp: fp, bitmap: bitmap})
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].slot < entries[j].slot })

	// Sanity: streamhash's MPHF is minimal, so slots must be dense
	// [0, n). A gap here would corrupt the slot→record correspondence
	// the cold reader relies on.
	for i, e := range entries {
		if e.slot != uint32(i) {
			return fmt.Errorf("events: non-dense MPHF slots: expected %d, got %d at position %d", i, e.slot, i)
		}
	}

	// indexPackItemsPerRecord bitmaps per record (see cold_format.go
	// for the rationale: offset-array size is per-record, so larger
	// records shrink the resident array proportionally).
	//
	// No record codec is used: roaring's MarshalBinary already
	// container-encodes (array / bitmap / RLE) the underlying data,
	// and a second compression pass slows the query hot path
	// measurably (~3.6× lookup latency in measurement) for marginal
	// byte savings. Contrast events.pack, where XDR payloads grouped
	// at 128/record offer plenty of compression headroom.
	// Skipping compression is also why indexPackChecksum exists.
	pw, err := packfile.Create(indexPackPath, packfile.WriterOptions{
		Format:         indexPackFormat,
		ItemsPerRecord: indexPackItemsPerRecord,
		ContentHash:    true,
		Overwrite:      true,
		RecordChecksum: indexPackChecksum,
	})
	if err != nil {
		return fmt.Errorf("events: create index.pack at %s: %w", indexPackPath, err)
	}

	writerErr := writeIndexPackEntries(pw, entries)
	if writerErr != nil {
		// pw.Close removes the partial index.pack. Join its error so a
		// cleanup failure surfaces alongside the original write error,
		// matching the index.hash cleanup defer above.
		if closeErr := pw.Close(); closeErr != nil {
			writerErr = errors.Join(writerErr, fmt.Errorf("events: close partial index.pack: %w", closeErr))
		}
		return writerErr
	}
	return nil
}

// indexEntry is one term's place in index.pack: the slot it lands at, its
// key (which the directory is sorted by), the 4-byte fingerprint, and the
// bitmap to serialize.
type indexEntry struct {
	slot   uint32
	key    TermKey
	fp     [IndexRecordFingerprintLen]byte
	bitmap *roaring.Bitmap
}

// Budgets that decide which terms become parts, and how big a part is.
//
// indexBucketBudget is the read unit the bucket layout is sized for: a read
// of 256 KiB or less costs one I/O unit on the storage this serves, so a
// bucket that fits it is one unit however many of its terms a query wants.
//
// indexDemoteFloor is the per-term floor: a term smaller than this is not
// worth a directory entry and a record of its own, and demoting it would
// trade a bucket read for a part read of the same size. A bucket made only
// of sub-floor terms is left whole — its size is bounded by 128 × the floor.
//
// indexPartTarget is what one part is meant to weigh. It sets how many parts
// a demoted term is cut into, before the slab extent rounds that to a power
// of two (see partLayout).
const (
	indexBucketBudget = 256 << 10
	indexDemoteFloor  = 16 << 10
	indexPartTarget   = 64 << 10
)

// chunkSlabCount is how many slabs of 65,536 ids the chunk spans, derived
// from the largest id any term holds. It is the extent the part layout cuts:
// a term's parts tile [0, chunkSlabCount) whatever the term's own extent, so
// part addressing is arithmetic and the reader needs no per-term extent.
func chunkSlabCount(entries []indexEntry) uint64 {
	var maxID uint64
	var any bool
	for i := range entries {
		if entries[i].bitmap.IsEmpty() {
			continue
		}
		if m := uint64(entries[i].bitmap.Maximum()); !any || m > maxID {
			maxID, any = m, true
		}
	}
	if !any {
		return 0
	}
	return maxID>>indexSlabShift + 1
}

// partLayout resolves a demoted term's part geometry from its serialized
// size and the chunk's slab count: partCount = ceil(S / indexPartTarget)
// target parts, cut on spans of 2^k slabs with k = floor(log2(C / target))
// clamped at zero. Parts come out at or under the target size in
// expectation, and never bigger than the whole term. The span follows the
// chunk's slab extent, not the term's byte size, so a term whose ids sit in
// a corner of the chunk still answers a window there in one part.
func partLayout(size, chunkSlabs uint64) (k uint8, records uint32, err error) {
	if chunkSlabs == 0 {
		return 0, 0, errors.New("events: a demoted term in a chunk with no ids")
	}
	target := (size + indexPartTarget - 1) / indexPartTarget
	if target == 0 {
		target = 1
	}
	var shift uint8
	for target<<(shift+1) <= chunkSlabs {
		shift++
	}
	span := uint64(1) << shift
	n := (chunkSlabs + span - 1) / span
	if n > math.MaxUint16 {
		return 0, 0, fmt.Errorf("events: %d parts overflows the directory's uint16", n)
	}
	return shift, uint32(n), nil //nolint:gosec // bounded by MaxUint16 just above
}

// demoteBucket picks the terms in one bucket that become parts: while the
// bucket's serialized size is over the budget and it still holds a term at
// or above the floor, the largest such term is demoted. Ties go to the lower
// slot, so freeze and walk demote identically. Returns the demoted positions
// within bucket, ascending.
func demoteBucket(bucket []indexEntry, sizes []uint64, demoted []bool, out []int) []int {
	total := 0
	for i := range bucket {
		sizes[i] = bucket[i].bitmap.GetSerializedSizeInBytes()
		demoted[i] = false
		total += IndexRecordFingerprintLen + int(sizes[i]) //nolint:gosec // chunk-bounded
	}
	for total > indexBucketBudget {
		best := -1
		for i := range bucket {
			if demoted[i] || sizes[i] < indexDemoteFloor {
				continue
			}
			if best < 0 || sizes[i] > sizes[best] {
				best = i
			}
		}
		if best < 0 {
			// Nothing left worth demoting: this bucket is all small terms and
			// stays whole, over budget or not.
			break
		}
		demoted[best] = true
		total -= int(sizes[best]) //nolint:gosec // chunk-bounded
	}
	for i := range bucket {
		if demoted[i] {
			out = append(out, i)
		}
	}
	return out
}

// writeIndexPackEntries writes index.pack: every term's bucket item in slot
// order, the last bucket padded to a full record, then the demoted terms'
// part records, and finally the app data carrying the directory that names
// them.
//
// One reused buffer serializes every bitmap rather than a fresh
// MarshalBinary slice per item. AppendItem copies its input, so the buffer
// is safe to reuse across iterations; roaring's WriteTo emits the same bytes
// MarshalBinary would, so the pack is byte-identical.
func writeIndexPackEntries(pw *packfile.Writer, entries []indexEntry) error {
	chunkSlabs := chunkSlabCount(entries)
	var (
		buf     bytes.Buffer
		sizes   [indexPackItemsPerRecord]uint64
		flags   [indexPackItemsPerRecord]bool
		demoted []indexEntry
		picks   []int
	)
	items := 0
	for lo := 0; lo < len(entries); lo += indexPackItemsPerRecord {
		bucket := entries[lo:min(lo+indexPackItemsPerRecord, len(entries))]
		picks = demoteBucket(bucket, sizes[:len(bucket)], flags[:len(bucket)], picks[:0])
		next := 0
		for i := range bucket {
			e := &bucket[i]
			if next < len(picks) && picks[next] == i {
				next++
				demoted = append(demoted, *e)
				// A demoted slot keeps only its fingerprint. Its body is
				// zero-length, which roaring cannot decode — a reader that
				// ever lands here reports corruption rather than a miss,
				// which is right: reaching it means the directory and the
				// buckets disagree.
				if err := pw.AppendItem(e.fp[:]); err != nil {
					return fmt.Errorf("events: write demoted slot %d to index.pack: %w", e.slot, err)
				}
				items++
				continue
			}
			buf.Reset()
			if _, werr := e.bitmap.WriteTo(&buf); werr != nil {
				return fmt.Errorf("events: serialize bitmap at slot %d: %w", e.slot, werr)
			}
			if err := pw.AppendItem(e.fp[:], buf.Bytes()); err != nil {
				return fmt.Errorf("events: write slot %d to index.pack: %w", e.slot, err)
			}
			items++
		}
	}
	// Pad the last bucket record out so the part records start on a record
	// boundary and part p of a term is item 128·(firstRecord+p). A bare
	// AppendItem() is a no-op, so the empty item has to be spelled out.
	bucketCount := (len(entries) + indexPackItemsPerRecord - 1) / indexPackItemsPerRecord
	for i := len(entries); i < bucketCount*indexPackItemsPerRecord; i++ {
		if err := pw.AppendItem([]byte{}); err != nil {
			return fmt.Errorf("events: pad index.pack bucket %d: %w", bucketCount-1, err)
		}
		items++
	}

	dir := indexDirectory{
		numKeys:     uint64(len(entries)),
		bucketCount: uint32(bucketCount), //nolint:gosec // chunk term count / 128
	}
	dir.entries = make([]byte, 0, len(demoted)*indexDirEntryLen)
	record := uint32(bucketCount) //nolint:gosec // chunk term count / 128
	for i := range demoted {
		e := &demoted[i]
		k, records, err := partLayout(e.bitmap.GetSerializedSizeInBytes(), chunkSlabs)
		if err != nil {
			return fmt.Errorf("events: part layout for slot %d: %w", e.slot, err)
		}
		written, err := writeTermParts(pw, e, k, records, record, items)
		if err != nil {
			return err
		}
		items += written
		dir.entries = appendDirEntry(dir.entries, e.key, record, records, k)
		record += records
		dir.totalParts += records
	}
	// The directory has to be sorted by key for the reader's binary search;
	// slot order is the MPHF's, which is not key order.
	sortDirEntries(dir.entries)
	return pw.Finish(encodeIndexAppData(dir))
}

// writeTermParts writes one demoted term's part records: every span in
// [0, records) gets a record whose item 0 is fp[4] ‖ the span's bitmap and
// whose other 127 items are empty, empty spans included. firstRecord is
// where the term's parts are supposed to land, which the item count is
// checked against — the whole addressing scheme is that arithmetic.
func writeTermParts(
	pw *packfile.Writer, e *indexEntry, k uint8, records, firstRecord uint32, items int,
) (int, error) {
	var buf bytes.Buffer
	width := uint64(1) << (uint64(k) + indexSlabShift)
	written := 0
	for p := range uint64(records) {
		if got := (items + written) / indexPackItemsPerRecord; got != int(firstRecord)+int(p) {
			return written, fmt.Errorf(
				"events: part %d of slot %d lands at record %d, not %d (parts must be contiguous after the buckets)",
				p, e.slot, got, uint64(firstRecord)+p)
		}
		span := roaring.New()
		span.AddRange(p*width, min((p+1)*width, uint64(math.MaxUint32)+1))
		part := roaring.And(e.bitmap, span)
		// The same form policy whole terms get: runs pay for themselves on
		// the query side, and the reader decodes parts through roaring's own
		// reader either way.
		part.RunOptimize()
		buf.Reset()
		if _, err := part.WriteTo(&buf); err != nil {
			return written, fmt.Errorf("events: serialize part %d of slot %d: %w", p, e.slot, err)
		}
		if err := pw.AppendItem(e.fp[:], buf.Bytes()); err != nil {
			return written, fmt.Errorf("events: write part %d of slot %d: %w", p, e.slot, err)
		}
		written++
		for range indexPackItemsPerRecord - 1 {
			if err := pw.AppendItem([]byte{}); err != nil {
				return written, fmt.Errorf("events: pad part %d of slot %d: %w", p, e.slot, err)
			}
			written++
		}
	}
	return written, nil
}

// appendDirEntry appends one fixed-stride directory row.
func appendDirEntry(dst []byte, key TermKey, firstRecord uint32, partCount uint32, k uint8) []byte {
	var row [indexDirEntryLen]byte
	copy(row[:16], key[:])
	binary.BigEndian.PutUint32(row[16:20], firstRecord)
	binary.BigEndian.PutUint16(row[20:22], uint16(partCount)) //nolint:gosec // partLayout bounds it
	row[22] = k
	return append(dst, row[:]...)
}

// sortDirEntries sorts the fixed-stride rows by key in place. Rows are built
// in slot order, because that is the order the parts are written in, and the
// reader binary-searches them by key.
func sortDirEntries(rows []byte) {
	n := len(rows) / indexDirEntryLen
	row := func(i int) []byte { return rows[i*indexDirEntryLen : (i+1)*indexDirEntryLen] }
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool {
		return bytes.Compare(row(order[a])[:16], row(order[b])[:16]) < 0
	})
	sorted := make([]byte, len(rows))
	for i, j := range order {
		copy(sorted[i*indexDirEntryLen:], row(j))
	}
	copy(rows, sorted)
}
