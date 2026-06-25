package streaming

import (
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// Progress derivation. There is NO stored watermark ("Progress is derived, never
// stored"): every consumer recomputes from durable keys. ONE derivation,
// lastCommittedLedger — a pure catalog read over the cold tier (Phase 1 has no
// hot tier to refine against): the highest fully-durable chunk's last ledger,
// clamped up by the earliest-1 floor.
//
// SIGNED-DOMAIN arithmetic guards the sentinel underflow: chunk.ID is uint32 and
// cannot hold the pre-genesis sentinel -1 nor survive a `maxChunk-1`/`earliest-1`
// underflow (live chunk 0, or an absent floor pin). So every "highest complete
// chunk" computation happens in int64 (-1 = "nothing below is complete") and
// completeThrough maps it to a last ledger, returning the sentinel for any
// negative input — never feeding ID(^uint32(0)) to LastLedger() (which overflows).

// preGenesisLedger is the watermark when NOTHING below the floor is complete:
// FirstLedgerSeq-1, i.e. "ingest from genesis". It is the value completeThrough
// returns for the pre-genesis sentinel (a negative signed chunk index).
const preGenesisLedger uint32 = chunk.FirstLedgerSeq - 1

// completeThrough maps a SIGNED chunk index to its "complete through" last ledger:
// c < 0 (pre-genesis sentinel) ⇒ FirstLedgerSeq-1 (the design's chunkLastLedger(-1)
// = 1, without uint32 wraparound); c >= 0 ⇒ chunk.ID(c).LastLedger(). It is the
// single chokepoint keeping the cold/floor terms out of the underflow trap.
func completeThrough(c int64) uint32 {
	if c < 0 {
		return preGenesisLedger
	}
	return chunk.ID(c).LastLedger() //nolint:gosec // c >= 0 and bounded by real chunk ids
}

// lastCommittedLedger is the cold-only daemon's highest-durably-committed-ledger
// derivation — a pure catalog read maxing two signed-domain terms (so a fresh
// store never underflows to MaxUint32): the COLD term (highestDurableChunk; -1 on
// a fresh start) and the FLOOR term (EarliestLedger()-1).
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
// durable, or -1 on a fresh start. "All durable": ledgers frozen AND events
// frozen AND (txhash frozen OR covered by a frozen index coverage). NOT merely
// "ledgers frozen" — a mid-freeze crash can leave ledgers frozen while events is
// "freezing", and counting it would open reads over a partial artifact; such a
// tip chunk DEGRADES the bound and backfill repairs it. Returns int64 for the -1
// sentinel, which lastCommittedLedger feeds through completeThrough.
func highestDurableChunk(cat *Catalog) (int64, error) {
	refs, err := cat.ChunkArtifactKeys()
	if err != nil {
		return 0, err
	}

	// Frozen per-kind state per chunk.
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

	// A frozen index coverage satisfies a chunk's txhash even after its .bin was
	// demoted at window finalization.
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

// frozenCoverageContains returns a predicate reporting whether a chunk falls in
// SOME frozen index coverage [Lo, Hi]. It reads all coverages once (AllIndexKeys)
// and keeps the frozen ones, so the per-chunk scan needn't re-scan.
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
// Cold retention/chunk arithmetic: signed-domain helpers mapping ledgers ↔ chunk
// indices for the catch-up loop and retention floor, keeping every computation
// out of the uint32 underflow trap.
// ---------------------------------------------------------------------------

// chunkIDOfLedger maps a ledger to its chunk, signed so a sub-genesis sentinel
// yields -1 ("before the first chunk") instead of panicking like
// chunk.IDFromLedger. Callers only feed it completeThrough (>= FirstLedgerSeq-1).
func chunkIDOfLedger(ledger uint32) int64 {
	if ledger < chunk.FirstLedgerSeq {
		return -1
	}
	return int64(chunk.IDFromLedger(ledger))
}
