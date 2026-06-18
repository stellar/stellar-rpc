package streaming

import (
	"fmt"
	"os"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// Progress derivation. There is NO stored watermark (see the data model's
// "Progress is derived, never stored"): both consumers recompute their bound
// from durable catalog keys on every call. Two derivations at two granularities:
//
//   - deriveCompleteThrough — chunk granularity, for the lifecycle tick (which
//     chunks are complete + where the retention floor anchors). Pure read of the
//     catalog; opens no hot DB.
//   - deriveWatermark — deriveCompleteThrough refined by exactly ONE read of the
//     highest ready hot DB, for ingestion's resume point (sub-chunk precision +
//     boundary-crash recovery). Runs once before ingestion starts.
//
// SIGNED-DOMAIN arithmetic (the sentinel-underflow guard): chunk.ID is uint32
// and CANNOT hold the pre-genesis sentinel -1, nor survive a `maxChunk-1` /
// `earliest-1` underflow when the live chunk is chunk 0 or the floor pin is
// absent. Every "highest complete chunk" computation below therefore happens in
// int64, with -1 meaning "nothing below is complete"; completeThrough maps the
// signed chunk index to its last ledger, returning the pre-genesis sentinel for
// any negative input. A raw chunk.ID is never fed an underflowed value, and
// ID(^uint32(0)) is never passed to LastLedger() (which would overflow — see
// chunk.go's LastLedger note).

// preGenesisLedger is the watermark when NOTHING below the floor is complete:
// FirstLedgerSeq-1, i.e. "ingest from genesis". It is the value completeThrough
// returns for the pre-genesis sentinel (a negative signed chunk index).
const preGenesisLedger uint32 = chunk.FirstLedgerSeq - 1

// completeThrough maps a SIGNED chunk index to the last ledger that chunk index
// represents as a "complete through" bound:
//
//   - c < 0 (the pre-genesis sentinel): no chunk below is complete, so the bound
//     is FirstLedgerSeq-1 — the design's chunkLastLedger(-1) = 1, computed here
//     without uint32 wraparound.
//   - c >= 0: chunk.ID(c).LastLedger().
//
// This is the single chokepoint that keeps the cold/positional/floor terms out
// of the uint32 underflow trap the design pseudocode's signed math hid.
func completeThrough(c int64) uint32 {
	if c < 0 {
		return preGenesisLedger
	}
	return chunk.ID(c).LastLedger() //nolint:gosec // c >= 0 and bounded by real chunk ids
}

// deriveCompleteThrough is the highest ledger the lifecycle may treat as durably
// ingested. It maxes three terms, each computed in the signed domain and mapped
// through completeThrough so a fresh/young store can never underflow to MaxUint32:
//
//   - COLD term — the highest chunk whose artifacts are ALL durable
//     (highestDurableChunk; -1 on a fresh start). Leads at startup, before
//     ingestion has created any hot key.
//   - POSITIONAL term — everything below the live chunk, by the key-creation
//     invariant: counts only "ready" hot keys (max ready chunk - 1). A
//     "transient" key never advances the bound, which is what lets recovery
//     demote any hot key without inflating it. -1 when no ready key exists, and
//     when the live chunk is chunk 0 (max ready = 0, so 0-1 = -1: nothing below
//     chunk 0 is complete). Leads in steady state.
//   - FLOOR term — EarliestLedger()-1, computed as int64(earliest)-1 so an
//     absent/zero pin yields the pre-genesis sentinel rather than underflowing.
func deriveCompleteThrough(cat *Catalog) (uint32, error) {
	cold, err := highestDurableChunk(cat)
	if err != nil {
		return 0, err
	}
	through := completeThrough(cold)

	pos, err := highestReadyChunkSigned(cat)
	if err != nil {
		return 0, err
	}
	if pos >= 0 {
		// Positional term: everything BELOW the live (highest ready) chunk.
		through = max(through, completeThrough(pos-1))
	}

	earliest, ok, err := cat.EarliestLedger()
	if err != nil {
		return 0, err
	}
	if ok {
		// int64 before the -1 so a zero/genesis pin does not underflow.
		floor := int64(earliest) - 1
		if floor < 0 {
			floor = 0
		}
		through = max(through, uint32(floor)) //nolint:gosec // floor >= 0, fits uint32
	}

	return through, nil
}

// deriveWatermark is deriveCompleteThrough refined by exactly ONE read of the
// highest ready hot DB. That read does two jobs: (1) sub-chunk precision inside
// the live chunk, and (2) recovering the chunk-level frontier when the
// positional term under-counts — a boundary crash can leave the live chunk
// "transient", so the highest *ready* chunk is the just-completed predecessor
// whose completion no key now advertises; reading its MaxCommittedSeq supplies
// that frontier.
//
// Before that one read, it asserts the dir-existence invariant for EVERY ready
// hot key (not just the one opened): derivation runs before any other open
// site, so a lost hot volume must surface here as the curated recovery
// instruction (ErrHotVolumeLost / case 4), never be silently healed by a later
// discard. probe opens the highest ready chunk read-only; it is safe to open
// here only because derivation runs before ingestion takes the live DB's
// exclusive lock.
func deriveWatermark(cat *Catalog, probe HotProbe) (uint32, error) {
	ready, err := cat.ReadyHotChunkKeys()
	if err != nil {
		return 0, err
	}

	// Dir-existence fatal loop over EVERY ready key.
	for _, c := range ready {
		dir := cat.layout.HotChunkPath(c)
		if _, statErr := os.Stat(dir); statErr != nil {
			if os.IsNotExist(statErr) {
				return 0, fmt.Errorf(
					"%w: chunk %s is %q but its hot dir %s is missing",
					ErrHotVolumeLost, c, HotReady, dir)
			}
			return 0, fmt.Errorf(
				"%w: chunk %s: stat hot dir %s: %w",
				ErrHotVolumeLost, c, dir, statErr)
		}
	}

	w, err := deriveCompleteThrough(cat)
	if err != nil {
		return 0, err
	}

	// One refinement read of the highest ready hot DB (if any). ready is sorted
	// ascending, so the last element is the highest.
	if len(ready) == 0 {
		return w, nil
	}
	live := ready[len(ready)-1]

	hot, ok, openErr := probe.OpenHotChunk(live)
	if openErr != nil {
		// The dir existed at the stat above; an open failure now is loss.
		return 0, fmt.Errorf("%w: chunk %s: open hot DB: %w", ErrHotVolumeLost, live, openErr)
	}
	if !ok {
		// Raced away between the stat and the open — same loss verdict.
		return 0, fmt.Errorf("%w: chunk %s: hot directory absent", ErrHotVolumeLost, live)
	}
	defer func() { _ = hot.Close() }()

	maxSeq, present, seqErr := hot.MaxCommittedSeq()
	if seqErr != nil {
		return 0, fmt.Errorf("%w: chunk %s: max committed seq: %w", ErrHotVolumeLost, live, seqErr)
	}
	if present {
		w = max(w, maxSeq)
	}
	return w, nil
}

// highestDurableChunk returns the highest chunk id whose artifacts are ALL
// durable, or -1 when no chunk is fully durable (a fresh start). "All durable"
// is the pendingArtifacts-empty test: ledgers frozen AND events frozen AND (txhash
// frozen OR the chunk is covered by a frozen index coverage). It is NOT merely
// "ledgers frozen": a crash mid-freeze can leave ledgers frozen while events is still
// "freezing", and counting that chunk would let reads open over a partial
// artifact — so an incompletely frozen tip chunk DEGRADES the bound and backfill
// repairs it.
//
// Returns int64 so the -1 sentinel is representable; deriveCompleteThrough feeds
// it through completeThrough.
func highestDurableChunk(cat *Catalog) (int64, error) {
	refs, err := cat.ChunkArtifactKeys()
	if err != nil {
		return 0, err
	}

	// Collect frozen per-kind state per chunk.
	type kinds struct{ ledgers, events, txhash bool }
	frozen := map[chunk.ID]*kinds{}
	for _, ref := range refs {
		if ref.State != StateFrozen {
			continue
		}
		k := frozen[ref.Chunk]
		if k == nil {
			k = &kinds{}
			frozen[ref.Chunk] = k
		}
		switch ref.Kind {
		case KindLedgers:
			k.ledgers = true
		case KindEvents:
			k.events = true
		case KindTxHash:
			k.txhash = true
		}
	}

	// Frozen index coverages let a chunk's txhash requirement be satisfied even
	// after the per-chunk .bin was demoted at window finalization.
	covered, err := frozenCoverageContains(cat)
	if err != nil {
		return 0, err
	}

	highest := int64(-1)
	for c, k := range frozen {
		if !k.ledgers || !k.events {
			continue
		}
		if !k.txhash && !covered(c) {
			continue
		}
		if id := int64(c); id > highest {
			highest = id
		}
	}
	return highest, nil
}

// frozenCoverageContains returns a predicate reporting whether a chunk falls
// inside SOME frozen index coverage [Lo, Hi]. It reads every window's coverages
// once (AllIndexKeys) and keeps only the frozen ones; the per-chunk artifact
// scan then asks "is this chunk's txhash satisfied by a covering index" without
// re-scanning.
func frozenCoverageContains(cat *Catalog) (func(chunk.ID) bool, error) {
	covs, err := cat.AllIndexKeys()
	if err != nil {
		return nil, err
	}
	var frozen []IndexCoverage
	for _, cov := range covs {
		if cov.State == StateFrozen {
			frozen = append(frozen, cov)
		}
	}
	return func(c chunk.ID) bool {
		for _, cov := range frozen {
			if cov.Lo <= c && c <= cov.Hi {
				return true
			}
		}
		return false
	}, nil
}

// highestReadyChunkSigned returns the highest "ready" hot chunk id as int64, or
// -1 when there is no ready hot key. The signed return lets deriveCompleteThrough
// compute the positional term (max ready - 1) without a uint32 underflow when the
// live chunk is chunk 0.
func highestReadyChunkSigned(cat *Catalog) (int64, error) {
	ready, err := cat.ReadyHotChunkKeys()
	if err != nil {
		return 0, err
	}
	if len(ready) == 0 {
		return -1, nil
	}
	// ReadyHotChunkKeys is sorted ascending; the last is the highest.
	return int64(ready[len(ready)-1]), nil
}
