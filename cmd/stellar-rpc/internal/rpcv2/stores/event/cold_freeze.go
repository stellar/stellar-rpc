package event

// cold_freeze.go builds a complete hot chunk's cold events artifacts from
// its hot DB, without the chunk's ledgers. The hot DB holds every input in
// the form the cold build takes it:
//
//   - DataCF holds each event in the wire format events.pack stores, in
//     event id order, so the pack is a copy;
//   - OffsetsCF holds each ledger's event count, so the ledger offsets
//     are a replay;
//   - IndexCF holds each sealed slab's terms in key order with their
//     serialized bitmaps, the form the cold build spills a slab in, so a
//     sealed slab is a run as it is. The events after the sealed slabs
//     are indexed again from their DataCF rows, as warmup indexes them.
//
// The artifacts are the walk's bytes: the pack appends the same items, the
// builder merges the same runs in the same order.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// freezeCtxEvery is how many events the DataCF scan copies between polls
// of the context.
const freezeCtxEvery = 1024

// FreezeColdFromStore builds chunkID's three cold events artifacts in dirs
// from store, the chunk's complete hot DB, and returns how many events it
// wrote. store may be open read-only. secret is the chunk's cold index
// secret, ColdIndexSecret(catalogSecret, chunkID), which the hot index
// files its terms under; a secret the hot index was not built under is
// refused before anything is written. opts tunes the pack writers as the
// walk does. The scans bypass the block cache, so the freeze does not evict
// what the chunk is still serving.
func FreezeColdFromStore(
	ctx context.Context,
	chunkID chunk.ID,
	store *rocksdb.Store,
	dirs ColdDirs,
	secret [stores.SecretLen]byte,
	opts ColdWriterOptions,
) (_ uint32, err error) {
	for _, dir := range []string{dirs.Data, dirs.Index} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return 0, fmt.Errorf("events: freeze chunk %s: mkdir %s: %w", chunkID, dir, err)
		}
	}
	offsets, sealed, err := freezeInputs(store, chunkID, secret)
	if err != nil {
		return 0, fmt.Errorf("events: freeze chunk %s: %w", chunkID, err)
	}
	total := offsets.TotalEvents()

	w, err := NewColdWriter(chunkID, dirs.Data, opts)
	if err != nil {
		return 0, err
	}
	// Close after Finish is a no-op; before it, Close removes the partial pack.
	defer func() {
		if cerr := w.Close(); err == nil {
			err = cerr
		}
	}()
	b := NewColdIndexBuilder(chunkID, dirs, secret, opts.BytesPerSync)
	defer func() {
		if cerr := b.Close(); err == nil {
			err = cerr
		}
	}()

	for slab := range sealed {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if err := addSealedSlab(store, b, slab); err != nil {
			return 0, fmt.Errorf("events: freeze chunk %s: %w", chunkID, err)
		}
	}
	copied, err := copyEvents(ctx, store, w, b, sealed<<indexSlabShift)
	if err != nil {
		return 0, fmt.Errorf("events: freeze chunk %s: %w", chunkID, err)
	}
	if copied != total {
		return 0, fmt.Errorf("events: freeze chunk %s: %s holds %d events, offsets count %d",
			chunkID, DataCF, copied, total)
	}
	if err := w.Finish(offsets.View()); err != nil {
		return 0, fmt.Errorf("events: freeze chunk %s: finish events.pack: %w", chunkID, err)
	}
	if err := b.Write(ctx); err != nil {
		return 0, fmt.Errorf("events: freeze chunk %s: %w", chunkID, err)
	}
	return total, nil
}

// freezeInputs reads what the freeze builds from: the chunk's ledger
// offsets, which must cover the whole chunk, and how many index slabs are
// sealed. It refuses an inconsistent chunk and a secret the sealed slabs
// are not keyed under.
func freezeInputs(
	store *rocksdb.Store, chunkID chunk.ID, secret [stores.SecretLen]byte,
) (*ConcurrentLedgerOffsets, uint32, error) {
	offsets, err := warmupOffsets(store, chunkID)
	if err != nil {
		return nil, 0, err
	}
	if end := offsets.EndLedger(); end != chunkID.LastLedger()+1 {
		return nil, 0, fmt.Errorf("offsets end at ledger %d, want %d", end, chunkID.LastLedger()+1)
	}
	sealed, err := sealedHotSlabs(store)
	if err != nil {
		return nil, 0, err
	}
	if err := verifyChunkConsistency(store, offsets.TotalEvents(), sealed); err != nil {
		return nil, 0, err
	}
	if err := verifySealedSecret(store, sealed, secret); err != nil {
		return nil, 0, err
	}
	return offsets, sealed, nil
}

// verifySealedSecret checks that the sealed slabs file their terms under
// secret: event 0's terms, routed under it, must all be in slab 0. Two
// secrets would otherwise merge into one index, the sealed slabs under
// the hot index's and the tail under the freeze's, and no lookup would
// find the sealed half.
func verifySealedSecret(store *rocksdb.Store, sealed uint32, secret [stores.SecretLen]byte) error {
	if sealed == 0 {
		return nil
	}
	raw, found, err := store.Get(DataCF, encodeDataKey(0))
	if err != nil {
		return fmt.Errorf("read event 0: %w", err)
	}
	if !found {
		return fmt.Errorf("corrupt chunk: %d index slabs sealed but event 0 is missing", sealed)
	}
	var p Payload
	if err := p.Unmarshal(raw); err != nil {
		return fmt.Errorf("decode event 0: %w", err)
	}
	keys, err := TermsForBytes(p.ContractEventBytes)
	if err != nil {
		return fmt.Errorf("derive terms for event 0: %w", err)
	}
	for _, k := range keys {
		_, found, err := store.Get(IndexCF, hotIndexKey(0, routedKey(secret, k)))
		if err != nil {
			return fmt.Errorf("probe index slab 0: %w", err)
		}
		if !found {
			return errors.New("the hot index is not keyed under the chunk's cold index secret")
		}
	}
	return nil
}

// addSealedSlab adds one sealed slab of IndexCF to the builder as a run.
func addSealedSlab(store *rocksdb.Store, b *ColdIndexBuilder, slab uint32) error {
	var scanErr error
	terms := func(yield func(TermKey, []byte) bool) {
		prefix := hotIndexKey(slab, TermKey{})[:2]
		for entry, err := range store.IterateUncached(IndexCF, prefix) {
			if err != nil {
				scanErr = fmt.Errorf("scan %s slab %d: %w", IndexCF, slab, err)
				return
			}
			if len(entry.Key) != hotIndexKeyLen {
				scanErr = fmt.Errorf("corrupt chunk: %s key of %d bytes in slab %d", IndexCF, len(entry.Key), slab)
				return
			}
			if !yield(TermKey(entry.Key[2:]), entry.Value) {
				return
			}
		}
	}
	if err := b.AddSlab(slab, terms); err != nil {
		return errors.Join(err, scanErr)
	}
	return scanErr
}

// copyEvents appends every DataCF row to the pack in id order and indexes
// the rows from unsealed on. It returns how many rows it copied.
func copyEvents(
	ctx context.Context, store *rocksdb.Store, w *ColdWriter, b *ColdIndexBuilder, unsealed uint32,
) (uint32, error) {
	var next uint32
	for entry, err := range store.IterateUncached(DataCF, nil) {
		if err != nil {
			return next, fmt.Errorf("scan %s: %w", DataCF, err)
		}
		if next%freezeCtxEvery == 0 {
			if err := ctx.Err(); err != nil {
				return next, err
			}
		}
		if len(entry.Key) != dataKeyLen || binary.BigEndian.Uint32(entry.Key) != next {
			return next, fmt.Errorf("corrupt chunk: %s key %x where event %d belongs", DataCF, entry.Key, next)
		}
		if next >= unsealed {
			var p Payload
			if err := p.Unmarshal(entry.Value); err != nil {
				return next, fmt.Errorf("decode event %d: %w", next, err)
			}
			keys, err := TermsForBytes(p.ContractEventBytes)
			if err != nil {
				return next, fmt.Errorf("derive terms for event %d: %w", next, err)
			}
			if err := b.Add(next, keys); err != nil {
				return next, err
			}
		}
		if err := w.AppendMarshaled(entry.Value); err != nil {
			return next, fmt.Errorf("append event %d: %w", next, err)
		}
		next++
	}
	return next, nil
}
