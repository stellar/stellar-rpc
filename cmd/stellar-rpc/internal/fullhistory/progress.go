package fullhistory

import (
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// Progress is derived, never stored: every consumer recomputes from durable keys.
// "Highest complete chunk" arithmetic runs in int64 (-1 = "nothing complete") to
// avoid uint32 wraparound on the pre-genesis sentinel; completeThrough is the chokepoint.

// preGenesisLedger is the watermark when nothing below the floor is complete
// (FirstLedgerSeq-1, "ingest from genesis"); completeThrough returns it for the sentinel.
const preGenesisLedger uint32 = chunk.FirstLedgerSeq - 1

// completeThrough maps a signed chunk index to its "complete through" last ledger:
// c < 0 ⇒ preGenesisLedger (no uint32 wraparound); c >= 0 ⇒ chunk.ID(c).LastLedger().
func completeThrough(c int64) uint32 {
	if c < 0 {
		return preGenesisLedger
	}
	return chunk.ID(c).LastLedger() //nolint:gosec // c >= 0 and bounded by real chunk ids
}

// lastCommittedLedger derives the highest durably committed ledger: max of the cold
// term (highestDurableChunk) and the floor term (EarliestLedger()-1), in signed
// domain so a fresh store never underflows.
func lastCommittedLedger(cat *catalog.Catalog) (uint32, error) {
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

// highestDurableChunk returns the highest chunk id with all artifacts durable
// (ledgers frozen AND events frozen AND (txhash frozen OR covered by a frozen
// index)), or -1 on a fresh start. A partially-frozen tip chunk is excluded —
// counting it would open reads over a partial artifact; backfill repairs it.
func highestDurableChunk(cat *catalog.Catalog) (int64, error) {
	refs, err := cat.ChunkArtifactKeys()
	if err != nil {
		return 0, err
	}

	// Frozen per-kind state per chunk.
	type kinds struct{ ledgers, events, txhash bool }
	frozen := map[chunk.ID]*kinds{}
	for _, ref := range refs {
		if ref.State != geometry.StateFrozen {
			continue
		}
		k := frozen[ref.Chunk]
		if k == nil {
			k = &kinds{}
			frozen[ref.Chunk] = k
		}
		switch ref.Kind {
		case geometry.KindLedgers:
			k.ledgers = true
		case geometry.KindEvents:
			k.events = true
		case geometry.KindTxHash:
			k.txhash = true
		}
	}

	// A frozen index coverage satisfies a chunk's txhash even after its .bin was demoted.
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
// some frozen index coverage [Lo, Hi]; coverages are read once up front.
func frozenCoverageContains(cat *catalog.Catalog) (func(chunk.ID) bool, error) {
	covs, err := cat.AllTxHashIndexKeys()
	if err != nil {
		return nil, err
	}
	var frozen []geometry.TxHashIndexCoverage
	for _, cov := range covs {
		if cov.State == geometry.StateFrozen {
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

// chunkIDOfLedger maps a ledger to its chunk, signed so a sub-genesis ledger
// yields -1 instead of panicking like chunk.IDFromLedger.
func chunkIDOfLedger(ledger uint32) int64 {
	if ledger < chunk.FirstLedgerSeq {
		return -1
	}
	return int64(chunk.IDFromLedger(ledger))
}
