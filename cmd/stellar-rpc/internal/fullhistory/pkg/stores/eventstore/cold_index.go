package eventstore

// cold_index.go is the index half of the cold-Chunk pipeline. It
// produces index.pack (per-slot bitmap records) + index.hash (the
// serialized MPHF) inside a Chunk's cold directory.
//
// The events.pack writer half lives in cold_writer.go. Shared format
// constants, the events.LedgerOffsets app-data wire format, and the
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

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/packfile"
)

// WriteColdIndex produces index.pack + index.hash for chunkID inside
// bucketDir. Both files are fsync'd before the function returns.
// bucketDir is the chunk's bucket directory; filenames are composed
// from chunkID via IndexPackName / IndexHashName so the two halves
// of the cold artifact always live together.
//
// A zero-term bitmaps (an eventless chunk, e.g. a pre-Soroban
// backfill range) produces the EMPTY index: a zero-length index.hash
// plus a zero-record index.pack. The cold reader resolves every
// lookup against it to ErrTermNotFound through the ordinary path, so
// neither readers nor orchestrators need a pack-without-index special
// case.
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
// Two callers produce bitmaps:
//
//   - Cold backfill builds a Bitmaps single-threaded via per-event
//     events.TermsFor + Bitmaps.AddTo, hands it directly to this
//     function.
//   - The live-chunk freeze path calls hotStore.Index().Snapshot() to
//     materialize a uniquely-owned Bitmaps from the concurrent live
//     mirror; that Snapshot Clones each bitmap so this function may
//     mutate them freely.
//
// index.hash is the MPHF serialized via buildMPHF.
//
// index.pack format. One packfile record per MPHF slot, in slot
// order. Each record is:
//
//	offset  size  field
//	0       4     fingerprint (first 4 bytes of the events.TermKey hash)
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
func WriteColdIndex(ctx context.Context, chunkID chunk.ID, bitmaps events.Bitmaps, bucketDir string) (err error) {
	n := len(bitmaps)

	indexPackPath := filepath.Join(bucketDir, IndexPackName(chunkID))
	indexHashPath := filepath.Join(bucketDir, IndexHashName(chunkID))

	// Zero terms (an eventless chunk, e.g. an all-pre-Soroban backfill
	// range — the COMMON case for early history): streamhash cannot
	// build an MPHF over zero keys, so write the empty index instead —
	// a zero-length index.hash (the sentinel openMPHF recognizes) plus
	// a zero-record index.pack. Readers then need no missing-file
	// special case: every Lookup resolves to ErrTermNotFound through
	// the ordinary path, and all three cold artifacts always exist for
	// a finalized chunk.
	if n == 0 {
		return writeEmptyColdIndex(indexPackPath, indexHashPath)
	}

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

	m, err := buildMPHF(ctx, bitmaps, indexHashPath)
	if err != nil {
		return fmt.Errorf("events: build MPHF: %w", err)
	}
	defer m.Close()

	entries := make([]indexEntry, 0, n)
	for term, bitmap := range bitmaps {
		slot, lerr := m.Lookup(term)
		if lerr != nil {
			return fmt.Errorf("events: MPHF lookup during index.pack build: %w", lerr)
		}
		var fp [IndexRecordFingerprintLen]byte
		copy(fp[:], term[:IndexRecordFingerprintLen])
		// Mutate in place — bitmaps is uniquely owned by the caller
		// (built single-threaded for cold backfill, or Cloned via
		// ConcurrentBitmaps.Snapshot for the live-chunk freeze path).
		bitmap.RunOptimize()
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
	pw, err := packfile.Create(indexPackPath, packfile.WriterOptions{
		Format:         indexPackFormat,
		ItemsPerRecord: indexPackItemsPerRecord,
		Overwrite:      true,
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

// indexEntry is one assembled index.pack record: the slot it lands at,
// the 4-byte fingerprint, and the bitmap to serialize.
type indexEntry struct {
	slot   uint32
	fp     [IndexRecordFingerprintLen]byte
	bitmap *roaring.Bitmap
}

// writeIndexPackEntries appends every assembled record to the index.pack
// writer in slot order and finishes the pack.
func writeIndexPackEntries(pw *packfile.Writer, entries []indexEntry) error {
	// Serialize each bitmap into one reused buffer rather than a fresh
	// MarshalBinary slice per record. AppendItem copies its input, so the
	// buffer is safe to reuse across iterations; roaring's WriteTo emits
	// the same bytes MarshalBinary would, so the pack is byte-identical.
	var buf bytes.Buffer
	for _, e := range entries {
		buf.Reset()
		if _, werr := e.bitmap.WriteTo(&buf); werr != nil {
			return fmt.Errorf("events: serialize bitmap at slot %d: %w", e.slot, werr)
		}
		if err := pw.AppendItem(e.fp[:], buf.Bytes()); err != nil {
			return fmt.Errorf("events: write slot %d to index.pack: %w", e.slot, err)
		}
	}
	return pw.Finish(nil)
}

// writeEmptyColdIndex publishes the empty cold index for an eventless
// chunk: a zero-length index.hash (the sentinel openMPHF recognizes as
// "zero terms") and a zero-record index.pack. Both are fsync'd, matching
// WriteColdIndex's durability contract. On error any partial artifact is
// removed so the bucket dir stays clean for retry.
func writeEmptyColdIndex(indexPackPath, indexHashPath string) (err error) {
	defer func() {
		if err == nil {
			return
		}
		if rmErr := os.Remove(indexHashPath); rmErr != nil && !errors.Is(rmErr, fs.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("events: remove orphan %s: %w", indexHashPath, rmErr))
		}
	}()

	f, err := os.Create(indexHashPath)
	if err != nil {
		return fmt.Errorf("events: create empty index.hash at %s: %w", indexHashPath, err)
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("events: sync empty index.hash at %s: %w", indexHashPath, err)
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("events: close empty index.hash at %s: %w", indexHashPath, err)
	}

	pw, err := packfile.Create(indexPackPath, packfile.WriterOptions{
		Format:         indexPackFormat,
		ItemsPerRecord: indexPackItemsPerRecord,
		Overwrite:      true,
	})
	if err != nil {
		return fmt.Errorf("events: create empty index.pack at %s: %w", indexPackPath, err)
	}
	if ferr := pw.Finish(nil); ferr != nil {
		// pw.Close removes the partial index.pack.
		if closeErr := pw.Close(); closeErr != nil {
			ferr = errors.Join(ferr, fmt.Errorf("events: close partial index.pack: %w", closeErr))
		}
		return fmt.Errorf("events: finish empty index.pack at %s: %w", indexPackPath, ferr)
	}
	return nil
}
