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
	"errors"
	"fmt"
	"io/fs"
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
// bucketDir is the chunk's INDEX bucket directory (ColdDirs.Index), not
// the one holding events.pack. It must already exist; filenames are
// composed from chunkID via IndexPackName / IndexHashName.
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
// index.pack holds every term's bitmap in MPHF slot order, a big term
// as one entry per slab (layout in cold_format.go). Unseen terms still
// produce a slot (vanilla MPHF semantics) but their fingerprint
// mismatches, and the cold reader rejects them there.
//
// streamhash's MPHF is a *minimal* perfect hash: slots are dense in
// [0, len(bitmaps)), which index.pack's entry positions are computed
// from. An assertion guards this invariant in case streamhash
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
	var slabs uint32
	for term, bitmap := range bitmaps {
		slot, fp, lerr := m.Lookup(term)
		if lerr != nil {
			return fmt.Errorf("events: MPHF lookup during index.pack build: %w", lerr)
		}
		// Mutate in place — bitmaps is uniquely owned by the caller, built
		// single-threaded either way: cold backfill from the .pack, or the freeze
		// from the read-only hot DB.
		bitmap.RunOptimize()
		if !bitmap.IsEmpty() {
			slabs = max(slabs, bitmap.Maximum()>>indexSlabShift+1)
		}
		entries = append(entries, indexEntry{slot: slot, fp: fp, bitmap: bitmap})
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

	pw, err := packfile.Create(indexPackPath, indexPackWriterOptions())
	if err != nil {
		return fmt.Errorf("events: create index.pack at %s: %w", indexPackPath, err)
	}

	writerErr := writeIndexPackEntries(pw, entries, slabs)
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

type indexEntry struct {
	slot   uint32
	fp     [IndexRecordFingerprintLen]byte
	bitmap *roaring.Bitmap
}

// writeIndexPackEntries writes entries, which must be in slot order, and finishes the pack.
func writeIndexPackEntries(pw *packfile.Writer, entries []indexEntry, slabs uint32) error {
	// Serialize each bitmap into one reused buffer rather than a fresh
	// MarshalBinary slice per entry. AppendItem copies its input, so the
	// buffer is safe to reuse across iterations; roaring's WriteTo emits
	// the same bytes MarshalBinary would, so the pack is byte-identical.
	var buf bytes.Buffer
	layout := indexLayout{slabs: slabs}
	for _, e := range entries {
		if e.bitmap.GetSerializedSizeInBytes() > indexSplitBytes {
			if err := appendSplitTerm(pw, e.bitmap, slabs, &buf); err != nil {
				return fmt.Errorf("events: write split slot %d to index.pack: %w", e.slot, err)
			}
			layout.rows = appendIndexRow(layout.rows, e.slot, e.fp)
			continue
		}
		buf.Reset()
		if _, werr := e.bitmap.WriteTo(&buf); werr != nil {
			return fmt.Errorf("events: serialize bitmap at slot %d: %w", e.slot, werr)
		}
		if err := pw.AppendItem(e.fp[:], buf.Bytes()); err != nil {
			return fmt.Errorf("events: write slot %d to index.pack: %w", e.slot, err)
		}
	}
	return pw.Finish(encodeIndexAppData(layout))
}

func appendSplitTerm(pw *packfile.Writer, bm *roaring.Bitmap, slabs uint32, buf *bytes.Buffer) error {
	for x := range uint64(slabs) {
		mask := roaring.New()
		mask.AddRange(x<<indexSlabShift, (x+1)<<indexSlabShift)
		slab := roaring.And(mask, bm)
		buf.Reset()
		if !slab.IsEmpty() {
			slab.RunOptimize()
			if _, err := slab.WriteTo(buf); err != nil {
				return err
			}
		}
		if err := pw.AppendItem(buf.Bytes()); err != nil {
			return err
		}
	}
	return nil
}
