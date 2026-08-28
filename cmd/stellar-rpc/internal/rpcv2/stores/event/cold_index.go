package event

// cold_index.go is the index half of the cold-Chunk pipeline. It
// produces index.pack (per-slot bitmap records) + index.hash (the
// serialized MPHF) inside a Chunk's cold directory.
//
// The events.pack writer half lives in cold_writer.go. Shared format
// constants, the LedgerOffsets app-data wire format, and the
// MPHF wrapper live in cold_format.go.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
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
// LookupKeys entry against it to a clean miss through the ordinary
// path, so neither readers nor orchestrators need a
// pack-without-index special case.
//
// bitmaps is the complete term index for the Chunk. It is read, not
// modified: encodeIndexBody builds its own bitmap for the terms that
// need one.
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
//	4       1     codec (itemCodecRoaring | itemCodecDelta)
//	5       N     postings in that codec
//
// The cold reader uses mphf.Lookup(term) → slot to find the record
// position, packfile.Reader.ReadItem(slot, ...) to read the bytes,
// verifies the 4-byte fingerprint against term[:4], and then
// decodes the postings on match. Unseen terms still produce a
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
		body, berr := encodeIndexBody(nil, bitmap.ToArray())
		if berr != nil {
			return berr
		}
		entries = append(entries, indexEntry{slot: slot, fp: fp, body: body})
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
	// No record codec is used: an item body is already
	// container-encoded (roaring's array / bitmap / RLE) or
	// delta-varint, and a second compression pass slows the query hot
	// path measurably (~3.6× lookup latency in measurement) for
	// marginal byte savings. Contrast events.pack, where XDR payloads
	// grouped at 128/record offer plenty of compression headroom.
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
// the 4-byte fingerprint, and the encoded item body (codec ‖ postings).
type indexEntry struct {
	slot uint32
	fp   [IndexRecordFingerprintLen]byte
	body []byte
}

// encodeIndexBody appends the index.pack item body for ids — the codec byte,
// then the postings in that codec — to dst. ids must be ascending and deduped.
//
// It is the single encoder every index.pack builder goes through, so no two
// of them can disagree about a term's encoding. It takes ids rather than a
// bitmap because the delta codec needs a sorted slice, while a bitmap is
// cheap to build from ids for the terms that need one. The one term's array
// that costs on the way in is transient and bounded by that term's
// cardinality — noise in an offline build that already holds the whole
// Bitmaps in memory.
func encodeIndexBody(dst []byte, ids []uint32) ([]byte, error) {
	if len(ids) == 0 {
		// A zero-posting term has no reason to exist, and the delta codec
		// cannot represent one: DecodePostings rejects a zero count, so
		// writing it would produce a record no reader can decode.
		return nil, errors.New("events: index body for a term with no postings")
	}
	if len(ids) <= deltaPostingMaxCardinality {
		return events.AppendPostings(append(dst, itemCodecDelta), ids), nil
	}
	// RunOptimize re-encodes long runs of set bits as RUN containers, which
	// the serialization then writes more compactly: ~14% on chunk 5999,
	// concentrated in dense clustered terms (popular contracts, common
	// topic[0] verbs). It is also what makes the serialization canonical.
	// This pairs with the fastaggregation.go fix in RoaringBitmap/roaring#81
	// — without that fix, the RUN containers hit a slow (*Bitmap).lazyOR
	// path at query time and K≥12 regresses catastrophically.
	bm := roaring.BitmapOf(ids...)
	bm.RunOptimize()
	b, err := bm.ToBytes()
	if err != nil {
		return nil, fmt.Errorf("events: serialize bitmap: %w", err)
	}
	return append(append(dst, itemCodecRoaring), b...), nil
}

// writeIndexPackEntries appends every assembled record to the index.pack
// writer in slot order and finishes the pack.
func writeIndexPackEntries(pw *packfile.Writer, entries []indexEntry) error {
	for _, e := range entries {
		if err := pw.AppendItem(e.fp[:], e.body); err != nil {
			return fmt.Errorf("events: write slot %d to index.pack: %w", e.slot, err)
		}
	}
	return pw.Finish(nil)
}
