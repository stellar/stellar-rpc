package fullhistory

import (
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// Progress is derived, never stored: every consumer recomputes from durable keys.
// "Highest complete chunk" arithmetic runs in int64 (-1 = "nothing complete") to
// avoid uint32 wraparound on the pre-genesis sentinel.

// lastCommittedLedger derives the highest durably committed ledger: the max of the
// floor term (EarliestLedger()-1) and the cold term (the highest fully-durable
// chunk's last ledger). Computed signed so a fresh/unpinned store doesn't underflow,
// then floored at the pre-genesis watermark (FirstLedgerSeq-1) — the "ingest from
// genesis, nothing committed" base.
func lastCommittedLedger(cat *catalog.Catalog) (uint32, error) {
	cold, err := highestDurableChunk(cat)
	if err != nil {
		return 0, err
	}
	earliest, ok, err := cat.EarliestLedger()
	if err != nil {
		return 0, err
	}

	through := int64(chunk.FirstLedgerSeq) - 1 // pre-genesis base
	if ok {
		through = max(through, int64(earliest)-1)
	}
	if cold >= 0 {
		through = max(through, int64(chunk.ID(cold).LastLedger())) //nolint:gosec // cold >= 0, a real chunk id
	}
	return uint32(through), nil // through >= FirstLedgerSeq-1 >= 0
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
