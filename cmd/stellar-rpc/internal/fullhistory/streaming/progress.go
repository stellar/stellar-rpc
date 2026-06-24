package streaming

import (
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// Progress derivation. There is NO stored watermark (see the data model's
// "Progress is derived, never stored"): every consumer recomputes its bound
// from durable catalog keys on every call. ONE derivation, lastCommittedLedger,
// a pure catalog read over the cold tier (the cold-only Phase-1 daemon has no
// hot tier to refine against): the highest fully-durable chunk's last ledger,
// clamped up by the earliest-1 floor.
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

// lastCommittedLedger is the single highest-durably-committed-ledger derivation
// for the cold-only daemon — a pure catalog read over the cold tier. It maxes
// the cold term and the earliest-1 floor, each computed in the signed domain and
// mapped through completeThrough so a fresh/young store can never underflow to
// MaxUint32:
//
//   - COLD term — the highest chunk whose artifacts are ALL durable
//     (highestDurableChunk; -1 on a fresh start).
//   - FLOOR term — EarliestLedger()-1, computed as int64(earliest)-1 so an
//     absent/zero pin yields the pre-genesis sentinel rather than underflowing.
func lastCommittedLedger(cat *Catalog) (uint32, error) {
	cold, err := highestDurableChunk(cat)
	if err != nil {
		return 0, err
	}
	through := completeThrough(cold)

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

// highestDurableChunk returns the highest chunk id whose artifacts are ALL
// durable, or -1 when no chunk is fully durable (a fresh start). "All durable"
// is the pendingArtifacts-empty test: ledgers frozen AND events frozen AND (txhash
// frozen OR the chunk is covered by a frozen index coverage). It is NOT merely
// "ledgers frozen": a crash mid-freeze can leave ledgers frozen while events is still
// "freezing", and counting that chunk would let reads open over a partial
// artifact — so an incompletely frozen tip chunk DEGRADES the bound and backfill
// repairs it.
//
// Returns int64 so the -1 sentinel is representable; lastCommittedLedger feeds
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

// ---------------------------------------------------------------------------
// Cold retention/chunk arithmetic. These signed-domain helpers map between
// ledgers and chunk indices for the catch-up loop and the retention floor; they
// keep every "highest complete chunk" / floor computation out of the uint32
// underflow trap (a young store or large retentionChunks driving an index
// negative).
// ---------------------------------------------------------------------------

// chunkIDOfLedger maps a ledger to its chunk, signed so the watermark sentinel
// (below genesis) yields a negative index instead of panicking like
// chunk.IDFromLedger. The caller only ever feeds it completeThrough, which is >=
// FirstLedgerSeq-1; a sentinel maps to chunk -1 ("before the first chunk").
func chunkIDOfLedger(ledger uint32) int64 {
	if ledger < chunk.FirstLedgerSeq {
		return -1
	}
	return int64(chunk.IDFromLedger(ledger))
}
