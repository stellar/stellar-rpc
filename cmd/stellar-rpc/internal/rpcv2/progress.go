package rpcv2

import (
	"fmt"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

// Progress is derived, never stored. "Highest complete chunk" arithmetic runs in
// int64 (-1 = "nothing complete") to avoid uint32 wraparound on the pre-genesis
// sentinel; chunk.LastLedgerOf is the chokepoint (the signed chunk↔ledger
// maps live in geometry so there is one -1 convention across the daemon).

// lastCommittedLedger is the single highest-durably-committed-ledger derivation.
// It maxes three terms, each in the signed domain so a fresh/young store never
// underflows to MaxUint32:
//
//   - COLD — highest chunk with all artifacts durable (highestDurableChunk; -1 on
//     a fresh start). Leads at startup before any hot key exists.
//   - HOT — only when hot > cold, over "ready" keys: one read-only MaxCommittedSeq
//     read of the highest ready hot DB (empty DB ⇒ positional LastLedgerOf(hot-1)).
//     The read-only open takes no RocksDB LOCK, so it never contends with a writer;
//     in practice it runs before ingestion opens the live chunk anyway.
//   - FLOOR — EarliestLedger()-1 as int64(earliest)-1, so an absent/zero pin
//     yields the pre-genesis sentinel rather than underflowing.
//
// The refinement's read-only open reuses the catalog's own logger
// (hotchunk.OpenReadyView needs one), landing the design signature
// lastCommittedLedger(cat).
func lastCommittedLedger(cat *catalog.Catalog) (uint32, error) {
	cold, err := highestDurableChunk(cat)
	if err != nil {
		return 0, err
	}
	lastCommitted := chunk.LastLedgerOf(cold)

	hot, err := highestReadyChunkSigned(cat)
	if err != nil {
		return 0, err
	}
	if hot > cold {
		// One refinement read of the highest ready hot DB; loss detected lazily on
		// this open (no eager scan over every ready key).
		refined, rerr := refineWithHotDB(cat, hot)
		if rerr != nil {
			return 0, rerr
		}
		lastCommitted = max(lastCommitted, refined)
	}

	earliest, ok, err := cat.EarliestLedger()
	if err != nil {
		return 0, err
	}
	if ok {
		// int64 before the -1 so a zero/genesis pin does not underflow.
		floor := max(int64(earliest)-1, 0)
		lastCommitted = max(lastCommitted, uint32(floor)) //nolint:gosec // floor in [0, MaxUint32), fits uint32
	}

	return lastCommitted, nil
}

// refineWithHotDB opens the highest ready hot chunk read-only straight from its
// Layout path and returns its MaxCommittedSeq, or LastLedgerOf(live-1) on an
// empty DB. A "ready" key whose dir/DB is gone surfaces as an ordinary
// (restartable) error — the read-only open never auto-heals it into a fresh empty
// DB. A read-only open replays any crash-left synced WAL into memtables, so
// MaxCommittedSeq is correct even after an ungraceful crash.
func refineWithHotDB(cat *catalog.Catalog, live int64) (uint32, error) {
	id := chunk.ID(live) //nolint:gosec // live > cold >= -1, so live >= 0
	// live is the highest "ready" hot chunk (from ReadyHotChunkKeys), so route the
	// read-only open through the single ready-open enforcement site: must-exist,
	// never auto-healed, uniform won't-open error.
	hot, openErr := hotchunk.OpenReadyView(geometry.HotReady, cat.Layout().HotChunkPath(id), id, cat.Logger())
	if openErr != nil {
		return 0, openErr
	}
	defer func() { _ = hot.Close() }()

	maxSeq, present, seqErr := hot.MaxCommittedSeq()
	if seqErr != nil {
		return 0, fmt.Errorf("chunk %s: read hot max committed seq: %w", id, seqErr)
	}
	if present {
		return maxSeq, nil
	}
	// Empty live DB: positional fallback (everything below it).
	return chunk.LastLedgerOf(live - 1), nil
}

// highestReadyChunkSigned returns the highest "ready" hot chunk id as int64, or -1
// when none. The signed return lets LastLedgerOf compute the positional term
// without a uint32 underflow when the live chunk is chunk 0.
func highestReadyChunkSigned(cat *catalog.Catalog) (int64, error) {
	ready, err := cat.ReadyHotChunkKeys()
	if err != nil {
		return 0, err
	}
	if len(ready) == 0 {
		return -1, nil
	}
	// Sorted ascending; the last is the highest.
	return int64(ready[len(ready)-1]), nil
}

// highestDurableChunk returns the highest chunk id with all artifacts durable
// (ledgers AND events frozen AND (txhash frozen OR covered by a frozen index)),
// or -1 on a fresh start. A partially-frozen tip chunk is excluded; backfill
// repairs it.
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

	highest := int64(-1)
	for c, k := range frozen {
		if !k.ledgers || !k.events {
			continue
		}
		// A frozen index coverage satisfies txhash even after the .bin was demoted.
		// The shared catalog predicate asserts INV-2 (one frozen coverage per window)
		// on every read, so last-committed-ledger derivation, discard eligibility, and
		// resolve can never disagree about the same snapshot.
		if !k.txhash {
			covered, err := cat.FrozenIndexCovers(c)
			if err != nil {
				return 0, err
			}
			if !covered {
				continue
			}
		}
		if id := int64(c); id > highest {
			highest = id
		}
	}
	return highest, nil
}
