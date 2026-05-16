package eventstore

// cold_index.go is the index half of the cold-Chunk pipeline. It
// produces index.pack (per-slot bitmap records) + index.hash (the
// serialized MPHF) inside a Chunk's cold directory.
//
// The events.pack writer half lives in cold_writer.go. Shared format
// constants, the events.LedgerOffsets app-data wire format, and the
// MPHF wrapper live in cold_format.go.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

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
// idx is the complete term index for the Chunk. The freezer hands
// HotStore.Index() directly — no rebuild. Backfill maintains an
// in-memory events.BitmapIndex (events.NewMemBitmaps + per-event
// events.TermsFor) as it processes LCMs and hands the same shape in
// at chunk completion.
//
// idx.All takes a read lock for the duration of each iteration body;
// this implementation does all per-term work (MPHF lookup, fingerprint
// extraction, MarshalBinary) inside the body so the yielded live
// pointer stays valid. Concurrent AddTo against idx would be blocked
// by that read lock — the orchestrator is expected to have stopped
// ingest before invoking WriteColdIndex, so contention isn't expected
// in practice.
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
// [0, idx.Len()), so packfile record positions exactly equal slots.
// An assertion guards this invariant in case streamhash semantics
// ever shift.
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
//nolint:cyclop // linear pipeline: build MPHF -> assemble entries -> sanity-check -> write pack
func WriteColdIndex(ctx context.Context, chunkID chunk.ID, idx events.BitmapIndex, bucketDir string) (err error) {
	n := idx.Len()
	if n <= 0 {
		return ErrEmptyBuildSet
	}

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

	m, err := buildMPHF(ctx, idx, indexHashPath)
	if err != nil {
		return fmt.Errorf("events: build MPHF: %w", err)
	}
	defer m.Close()

	type entry struct {
		slot        uint32
		fp          [IndexRecordFingerprintLen]byte
		bitmapBytes []byte
	}
	entries := make([]entry, 0, n)
	var iterErr error
	for term, bitmap := range idx.All() {
		slot, err := m.Lookup(term)
		if err != nil {
			iterErr = fmt.Errorf("events: MPHF lookup during index.pack build: %w", err)
			break
		}
		var fp [IndexRecordFingerprintLen]byte
		copy(fp[:], term[:IndexRecordFingerprintLen])
		// Marshal inside the All body: the bitmap pointer is only
		// valid while idx.All holds its read lock, which it does
		// for the iteration body. The returned []byte is owned by
		// us and safe to retain past the iteration.
		bitmapBytes, err := bitmap.MarshalBinary()
		if err != nil {
			iterErr = fmt.Errorf("events: marshal bitmap at slot %d: %w", slot, err)
			break
		}
		entries = append(entries, entry{slot: slot, fp: fp, bitmapBytes: bitmapBytes})
	}
	if iterErr != nil {
		return iterErr
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

	writerErr := func() error {
		for _, e := range entries {
			if err := pw.AppendItem(e.fp[:], e.bitmapBytes); err != nil {
				return fmt.Errorf("events: write slot %d to index.pack: %w", e.slot, err)
			}
		}
		return pw.Finish(nil)
	}()
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
