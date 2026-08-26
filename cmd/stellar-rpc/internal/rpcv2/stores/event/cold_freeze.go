package event

// cold_freeze.go — freeze-by-merge: build a completed hot chunk's cold events
// artifacts DIRECTLY from its durable hot state instead of re-deriving them
// from raw ledgers. The hot tier already holds exactly what cold needs:
//
//   - events_data values ARE the canonical marshaled payloads (the same
//     MarshalInto bytes ColdWriter.Append would produce) → stream them into
//     events.pack verbatim, in eventID order (the CF's BE-key order).
//   - events_offsets IS the per-ledger count sequence → LedgerOffsets.
//   - the term index IS the hot engine's published run set: the
//     manifest-listed sealed runs (union-merged, CRC-framed EVR2 files beside
//     the chunk DB, read in place) plus the un-sealed events_index tail past
//     the sealed frontier, windowed into tail-only scratch runs. Both feed
//     WriteColdIndexFromRuns' one MergeRuns — the same trust rule as warmup
//     (hot_store.go): sealed rows' derived runs are the CRC-verified
//     authority, packed rows replay from lastSealed+1. This is the txhash
//     freeze's shape (txhash/cold_freeze.go): merge the engine's durable runs
//     with its un-sealed tail.
//
// Byte identity with the walk-derived build is structural, not incidental:
// per-term event IDs are strictly ascending and globally duplicate-free, so
// MergeRuns' per-term union is invariant across ANY partition of the ledger
// rows into runs, and [sealed runs + tail rows] hold exactly the fold of all
// events_index rows. Crash states change the partition, never the content:
// an empty manifest makes the whole chunk tail (the degenerate full scan —
// this same path with zero manifest runs, no separate fallback), and an
// orphan run (a crash between run write and manifest Put, or a discarded
// merge) is unlisted, therefore ignored, and re-covered by the tail. A
// corrupt or missing listed run fails the freeze loudly (the merge drains
// every run to its CRC trailer — runspill.ErrCorruptRun). Live runs are
// strictly read-only inputs — only scratchDir, outside the chunk DB, is ever
// wiped — so the freeze is idempotent. All pinned by cold_freeze_test.go's
// identity gates and the composition gate (ingest/freeze_test.go).

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event/runspill"
)

// replayOffsets rebuilds the chunk's ledger offsets from OffsetsCF through
// the shared scanner (one shape/decode trust boundary with warmup); range,
// overflow, and sequencing checks stay beside the accumulator they guard.
func replayOffsets(store *rocksdb.Store, chunkID chunk.ID) (*LedgerOffsets, error) {
	offsets := NewLedgerOffsets(chunkID.FirstLedger())
	if err := scanOffsetsCF(store, func(ledger, count uint32) error {
		if ledger > chunkID.LastLedger() {
			return fmt.Errorf("events: freeze offsets: ledger %d past chunk %s", ledger, chunkID)
		}
		if uint64(offsets.TotalEvents())+uint64(count) > math.MaxUint32 {
			return fmt.Errorf("events: freeze offsets: cumulative overflow at ledger %d", ledger)
		}
		// Append validates the in-sequence invariant itself (untrusted rows).
		if aerr := offsets.Append(ledger, count); aerr != nil {
			return fmt.Errorf("events: freeze offsets: %w", aerr)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return offsets, nil
}

// copyDataCF streams the DataCF verbatim into the cold writer in eventID
// order, enforcing key shape and ID density, and returns the copied count.
func copyDataCF(ctx context.Context, store *rocksdb.Store, w *ColdWriter) (uint64, error) {
	var copied uint64
	next := uint32(0)
	for entry, ierr := range store.Iterate(DataCF, nil) {
		if ierr != nil {
			return copied, fmt.Errorf("events: freeze scan %s: %w", DataCF, ierr)
		}
		// ctx.Err takes a mutex; polling per event costs real time over
		// millions of event. Same cadence as the sibling freeze scans.
		if next%256 == 0 {
			if cerr := ctx.Err(); cerr != nil {
				return copied, cerr
			}
		}
		if len(entry.Key) != dataKeyLen {
			return copied, fmt.Errorf("events: freeze %s key length %d (want %d)", DataCF, len(entry.Key), dataKeyLen)
		}
		// The hot writer assigns dense chunk-relative IDs; a gap here is real
		// corruption, and events.pack positions must equal IDs.
		if id := binary.BigEndian.Uint32(entry.Key); id != next {
			return copied, fmt.Errorf("events: freeze %s: event id %d, expected %d", DataCF, id, next)
		}
		next++
		copied++
		if aerr := w.AppendMarshaled(entry.Value); aerr != nil {
			return copied, aerr
		}
	}
	return copied, nil
}

// freezeIndexWindowBytes caps how many packed-row bytes accumulate in the
// window map before it flushes as one spill run. The tail is normally under
// one seal window (≤windowLedgers event-bearing rows → a single small run);
// the cap bounds every freeze's respill and tail windows —
// largest for a crash-inherited backlog — up to the whole chunk when
// the manifest is empty — where ~32MB windows keep freeze memory bounded.
const freezeIndexWindowBytes = 32 << 20

// FreezeColdFromStore builds all three cold events artifacts for chunkID in
// bucketDir from the chunk's (read-only) hot store. scratchDir hosts the
// blinded re-spill of the sealed runs, the tail spill runs, and terms.run;
// it is wiped on entry and removed on
// success, and must lie OUTSIDE the chunk DB (production: the cold events
// bucket) — the manifest-listed runs beside the DB's CFs are consumed in
// place, strictly read-only. opts tunes the events.pack writer exactly as
// the walk-driven build does.
// secret is the chunk's routing secret (ColdIndexSecret): the walk path
// blinds term keys before its spiller, so this path blinds at ITS run
// boundaries — the sealed runs are re-spilled under blinded keys (they serve
// raw-key hot lookups and cannot be blinded in place) and the tail's window
// map blinds at insert — keeping both builds keyed by one blinded identity.
func FreezeColdFromStore(
	ctx context.Context,
	chunkID chunk.ID,
	store *rocksdb.Store,
	scratchDir, bucketDir string,
	secret [stores.SecretLen]byte,
	opts ColdWriterOptions,
) (err error) {
	if err := os.MkdirAll(bucketDir, 0o755); err != nil {
		return fmt.Errorf("events: mkdir freeze bucket %s: %w", bucketDir, err)
	}
	if err := os.RemoveAll(scratchDir); err != nil {
		return fmt.Errorf("events: wipe freeze scratch %s: %w", scratchDir, err)
	}
	if err := os.MkdirAll(scratchDir, 0o755); err != nil {
		return fmt.Errorf("events: mkdir freeze scratch %s: %w", scratchDir, err)
	}

	// ── events.pack: verbatim DataCF copy in eventID order. ──
	w, err := NewColdWriter(chunkID, bucketDir, opts)
	if err != nil {
		return err
	}
	// Close after a successful Commit is a documented no-op returning nil, so
	// the unconditional defer needs no lifecycle flag of its own.
	defer func() {
		if cerr := w.Close(); err == nil {
			err = cerr
		}
	}()

	copied, err := copyDataCF(ctx, store, w)
	if err != nil {
		return err
	}

	// ── offsets: OffsetsCF replay through the shared scanner (one shape/
	// decode trust boundary with warmup); range, overflow, and sequencing
	// checks stay here with the accumulator they guard. ──
	offsets, err := replayOffsets(store, chunkID)
	if err != nil {
		return err
	}
	if total := uint64(offsets.TotalEvents()); total != copied {
		return fmt.Errorf("events: freeze: offsets count %d events, data CF holds %d", total, copied)
	}
	if err := w.Commit(offsets); err != nil {
		return fmt.Errorf("events: freeze Commit: %w", err)
	}

	// ── index: the engine's durable sealed runs + the un-sealed tail. ──
	runs, err := freezeIndexInputs(ctx, store, chunkID, scratchDir, secret)
	if err != nil {
		return err
	}
	if err := WriteColdIndexFromRuns(ctx, chunkID, runs, scratchDir, bucketDir, secret); err != nil {
		return err
	}
	return os.RemoveAll(scratchDir)
}

// freezeIndexInputs assembles the index merge's run list: the manifest-listed
// sealed runs re-spilled under BLINDED keys into scratch (respillBlinded —
// the originals under the chunk DB's run dir stay read-only, never moved or
// deleted, because they serve raw-key hot lookups) followed by the tail
// scratch runs from freezeIndexRuns. Only manifest-named runs qualify — an orphan a crash
// left beside them holds nothing the tail past the frontier doesn't
// re-cover, while a missing or corrupt LISTED run fails the merge loudly
// rather than silently thinning the index.
func freezeIndexInputs(
	ctx context.Context, store *rocksdb.Store, chunkID chunk.ID, scratchDir string,
	secret [stores.SecretLen]byte,
) ([]string, error) {
	names, lastSealed, err := rocksdbManifest{store: store}.GetRuns()
	if err != nil {
		return nil, fmt.Errorf("events: freeze manifest: %w", err)
	}
	runDir := filepath.Join(store.Path(), hotIndexRunDir)
	sealed := make([]string, 0, len(names))
	for _, name := range names {
		sealed = append(sealed, filepath.Join(runDir, name))
	}
	runs, err := respillBlinded(ctx, sealed, scratchDir, secret)
	if err != nil {
		return nil, err
	}
	tail, err := freezeIndexRuns(ctx, store, chunkID, scratchDir, lastSealed, secret)
	if err != nil {
		return nil, err
	}
	return append(runs, tail...), nil
}

// respillBlinded merges the manifest-listed sealed runs (raw term keys, read
// in place) and re-spills them as BLINDED scratch runs — the freeze's half of
// the ingest rule: cold artifacts are keyed by the blinded routing identity,
// while the hot tier's sealed runs serve raw-key lookups and cannot be
// blinded in place. One MergeRuns pass dedups terms across the sealed runs
// (per-term union, ascending — the merge's own contract), so each blinded
// term lands in the window map exactly once; the byte-bounded flush mirrors
// the tail scan's.
func respillBlinded(
	ctx context.Context, sealed []string, scratchDir string, secret [stores.SecretLen]byte,
) ([]string, error) {
	if len(sealed) == 0 {
		return nil, nil
	}
	var (
		window      = make(map[TermKey][]uint32, 1<<16)
		windowBytes int
		runs        []string
	)
	flush := func() error {
		if len(window) == 0 {
			return nil
		}
		path := filepath.Join(scratchDir, fmt.Sprintf("blind-%06d.run", len(runs)))
		if _, werr := writeSortedRun(window, path, nil); werr != nil {
			return werr
		}
		runs = append(runs, path)
		clear(window)
		windowBytes = 0
		return nil
	}
	emitted := 0
	if err := runspill.MergeRuns(sealed, func(term [16]byte, ids []uint32) error {
		if emitted%256 == 0 {
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
		}
		emitted++
		bk := TermKey(stores.BlindKey(secret, term[:]))
		// MergeRuns emits each term exactly once, so an occupied slot means
		// two DISTINCT terms blinded to one key (~2^-128). Silently keeping
		// either posting set would serve wrong results — reject loudly, the
		// same call the index build makes on a duplicate key.
		if _, dup := window[bk]; dup {
			return fmt.Errorf("events: blinded key collision on %x", bk[:])
		}
		// MergeRuns reuses the ids buffer across emits — copy before keeping.
		window[bk] = slices.Clone(ids)
		windowBytes += len(term) + 4*len(ids)
		if windowBytes >= freezeIndexWindowBytes {
			return flush()
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("events: freeze re-key sealed runs: %w", err)
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return runs, nil
}

// freezeIndexRuns scans the UN-SEALED IndexCF tail — per-ledger packed rows
// past the sealed frontier, the same lastSealed+1 replay rule warmup applies
// — unions the rows in a byte-bounded window map, and flushes each full
// window as one scratch spill run. Every row is term-sorted with per-term
// ascending IDs, and ledger rows arrive in ledger order, so per-term appends
// stay ascending within a window; cross-window (and sealed-run/tail)
// duplicates union in the merge. An empty manifest reads as lastSealed 0 —
// the whole chunk is tail and this is the fresh-crash full scan, not a
// separate path.
func freezeIndexRuns(
	ctx context.Context, store *rocksdb.Store, chunkID chunk.ID, scratchDir string, lastSealed uint32,
	secret [stores.SecretLen]byte,
) ([]string, error) {
	var (
		window      = make(map[TermKey][]uint32, 1<<16)
		windowBytes int
		runs        []string
	)
	flush := func() error {
		if len(window) == 0 {
			return nil
		}
		// Stream the window in sorted-term order through the seal path's
		// shared writer — no whole-payload buffer (the same trim the
		// seal/merge path got in the streaming rework).
		path := filepath.Join(scratchDir, fmt.Sprintf("freeze-%06d.run", len(runs)))
		if _, werr := writeSortedRun(window, path, nil); werr != nil {
			return werr
		}
		runs = append(runs, path)
		clear(window)
		windowBytes = 0
		return nil
	}
	for entry, ierr := range store.IterateRange(IndexCF, rocksdb.EncodeUint32(lastSealed+1), nil) {
		if ierr != nil {
			return nil, fmt.Errorf("events: freeze scan %s: %w", IndexCF, ierr)
		}
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
		if len(entry.Key) != packedIndexKeyLen {
			return nil, fmt.Errorf("events: freeze %s key length %d (want %d)", IndexCF, len(entry.Key), packedIndexKeyLen)
		}
		// Range-check the row's ledger against the chunk, mirroring the txhash
		// arm: a stray row past the chunk bound would otherwise fold into the
		// index silently.
		if seq := binary.BigEndian.Uint32(entry.Key); seq > chunkID.LastLedger() {
			return nil, fmt.Errorf("events: freeze %s row seq %d outside chunk %s", IndexCF, seq, chunkID)
		}
		if derr := DecodePackedRow(entry.Value, func(term TermKey, ids []uint32) {
			// Blind at the run boundary — the ingest rule (see the freeze doc).
			bk := TermKey(stores.BlindKey(secret, term[:]))
			window[bk] = append(window[bk], ids...)
			// Account the DECODED cost the window actually holds (key + id
			// slice), not the varint-encoded row length — encoded bytes
			// undercount RAM several-fold on dense rows.
			windowBytes += 16 + 4*len(ids)
		}); derr != nil {
			return nil, fmt.Errorf("events: freeze ledger %d row: %w", binary.BigEndian.Uint32(entry.Key), derr)
		}
		if windowBytes >= freezeIndexWindowBytes {
			if ferr := flush(); ferr != nil {
				return nil, ferr
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return runs, nil
}
