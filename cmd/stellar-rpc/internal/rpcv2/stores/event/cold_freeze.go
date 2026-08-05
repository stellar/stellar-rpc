package event

// cold_freeze.go — freeze-by-merge: build a completed hot chunk's cold events
// artifacts DIRECTLY from its hot DB's column families instead of re-deriving
// them from raw ledgers. The hot tier already holds exactly what cold needs:
//
//   - events_data values ARE the canonical marshaled payloads (the same
//     MarshalInto bytes ColdWriter.Append would produce) → stream them into
//     event.pack verbatim, in eventID order (the CF's BE-key order).
//   - events_offsets IS the per-ledger count sequence → LedgerOffsets.
//   - events_index packed rows are term-sorted runs → window-merge them into
//     spill runs and finalize through WriteColdIndexFromRuns.
//
// This deletes the freeze's ExtractLedgerEvents shaping, TermsForBytes
// hashing, and every per-term allocation from the window where the freeze
// runs BESIDE live ingestion — the design's freeze-synergy lens
// (~/bench-artifacts/cold-ingest-design.md). Artifacts remain byte-identical
// to the walk-derived build by construction of each input.

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
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
		// corruption, and event.pack positions must equal IDs.
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
// window map before it flushes as one spill run. ~32MB keeps freeze memory
// bounded (window + map overhead) while producing ~40-80 runs for a
// worst-case chunk — well inside the merge's comfort zone.
const freezeIndexWindowBytes = 32 << 20

// FreezeColdFromStore builds all three cold events artifacts for chunkID in
// bucketDir from the chunk's (read-only) hot store. scratchDir hosts the
// intermediate spill runs and terms.run; it is wiped on entry and removed on
// success. opts tunes the event.pack writer exactly as the walk-driven
// build does.
// secret is the chunk's routing secret (ColdIndexSecret): the walk path
// blinds term keys before its spiller, so this path blinds them at its own
// run boundary — the IndexCF window map — keeping both builds keyed by the
// same blinded identity.
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

	// ── event.pack: verbatim DataCF copy in eventID order. ──
	w, err := NewColdWriter(chunkID, bucketDir, opts)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			err2 := w.Close()
			if err == nil {
				err = err2
			}
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
	if err := w.Finish(offsets); err != nil {
		return fmt.Errorf("events: freeze Finish: %w", err)
	}
	closed = true

	// ── index: window-merge IndexCF packed rows into spill runs. ──
	runs, err := freezeIndexRuns(ctx, store, scratchDir, secret)
	if err != nil {
		return err
	}
	if err := WriteColdIndexFromRuns(ctx, chunkID, runs, scratchDir, bucketDir, secret); err != nil {
		return err
	}
	return os.RemoveAll(scratchDir)
}

// freezeIndexRuns scans the IndexCF's per-ledger packed rows, unions them in
// a byte-bounded window map, and flushes each full window as one spill run.
// Every row is term-sorted with per-term ascending IDs, and ledger rows
// arrive in ledger order, so per-term appends stay ascending within a
// window; cross-window duplicates union in the merge.
func freezeIndexRuns(
	ctx context.Context, store *rocksdb.Store, scratchDir string, secret [stores.SecretLen]byte,
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
	for entry, ierr := range store.Iterate(IndexCF, nil) {
		if ierr != nil {
			return nil, fmt.Errorf("events: freeze scan %s: %w", IndexCF, ierr)
		}
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
		if len(entry.Key) != packedIndexKeyLen {
			return nil, fmt.Errorf("events: freeze %s key length %d (want %d)", IndexCF, len(entry.Key), packedIndexKeyLen)
		}
		if derr := DecodePackedRow(entry.Value, func(term TermKey, ids []uint32) {
			// The run key is the BLINDED routing identity — the same rule the
			// walk applies before its spiller. Blinding is injective, so the
			// per-term grouping and ascending-ID invariants are unchanged.
			bk := TermKey(stores.BlindKey(secret, term[:]))
			window[bk] = append(window[bk], ids...)
		}); derr != nil {
			return nil, fmt.Errorf("events: freeze ledger %d row: %w", binary.BigEndian.Uint32(entry.Key), derr)
		}
		windowBytes += len(entry.Value)
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
