package event

// cold_freeze.go builds a complete hot chunk's cold events artifacts from
// its hot DB:
//
//   - DataCF holds the sealed events in the wire format events.pack stores,
//     in event id order, so they are copied. The events after them come
//     from the caller, which derives them from their ledgers again;
//   - OffsetsCF holds each ledger's event count, so the ledger offsets
//     are a replay;
//   - IndexCF holds each sealed slab's terms in key order with their
//     serialized bitmaps, the form the cold build spills a slab in, so a
//     sealed slab is a run as it is. The events after the sealed slabs
//     are indexed again.
//
// The artifacts are the walk's bytes: the pack appends the same items, the
// builder merges the same runs in the same order.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"os"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// freezeCtxEvery is how many events the freeze copies between polls of the
// context.
const freezeCtxEvery = 1024

// FreezeColdFromStore builds chunkID's three cold events artifacts in dirs
// from store, the chunk's complete hot DB, and returns how many events it
// wrote. store may be open read-only. unsealed returns the events of the
// ledgers from a ledger on, which no sealed window holds. secret is
// the chunk's cold index secret, ColdIndexSecret(catalogSecret, chunkID),
// which the hot index files its terms under; a secret the hot index was not
// built under is refused before anything is written. opts tunes the pack
// writers as the walk does. The scans bypass the block cache, so the freeze
// does not evict what the chunk is still serving.
func FreezeColdFromStore(
	ctx context.Context,
	chunkID chunk.ID,
	store *rocksdb.Store,
	unsealed func(from uint32) ([]LedgerEvents, error),
	dirs ColdDirs,
	secret [stores.SecretLen]byte,
	opts ColdWriterOptions,
) (_ uint32, err error) {
	for _, dir := range []string{dirs.Data, dirs.Index} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return 0, fmt.Errorf("events: freeze chunk %s: mkdir %s: %w", chunkID, dir, err)
		}
	}
	in, err := readFreezeInputs(store, chunkID, unsealed, secret)
	if err != nil {
		return 0, fmt.Errorf("events: freeze chunk %s: %w", chunkID, err)
	}
	total := in.offsets.TotalEvents()

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

	for slab := range in.sealedSlabs {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if err := addSealedSlab(store, b, slab); err != nil {
			return 0, fmt.Errorf("events: freeze chunk %s: %w", chunkID, err)
		}
	}
	if err := copyEvents(ctx, in.events(store), w, b, in.sealedSlabs<<indexSlabShift); err != nil {
		return 0, fmt.Errorf("events: freeze chunk %s: %w", chunkID, err)
	}
	if err := w.Finish(in.offsets); err != nil {
		return 0, fmt.Errorf("events: freeze chunk %s: finish events.pack: %w", chunkID, err)
	}
	if err := b.Write(ctx); err != nil {
		return 0, fmt.Errorf("events: freeze chunk %s: %w", chunkID, err)
	}
	return total, nil
}

// freezeInputs is what the freeze builds from: the chunk's ledger offsets,
// how many index slabs and events are sealed, and the events after the
// sealed ones.
type freezeInputs struct {
	offsets     *LedgerOffsets
	sealedSlabs uint32
	sealed      uint32
	tail        []LedgerEvents
}

// readFreezeInputs refuses an incomplete or inconsistent chunk and a secret
// the sealed slabs are not keyed under.
func readFreezeInputs(
	store *rocksdb.Store, chunkID chunk.ID, unsealed func(from uint32) ([]LedgerEvents, error),
	secret [stores.SecretLen]byte,
) (freezeInputs, error) {
	var in freezeInputs
	offsets, err := warmupOffsets(store, chunkID)
	if err != nil {
		return in, err
	}
	if end := offsets.EndLedger(); end != chunkID.LastLedger()+1 {
		return in, fmt.Errorf("offsets end at ledger %d, want %d", end, chunkID.LastLedger()+1)
	}
	in.offsets = offsets.View()
	if in.sealedSlabs, in.sealed, err = sealedCounts(store, in.offsets.TotalEvents()); err != nil {
		return in, err
	}
	from, err := unsealedLedger(in.offsets, in.sealed)
	if err != nil {
		return in, err
	}
	if from < in.offsets.EndLedger() {
		if in.tail, err = unsealed(from); err != nil {
			return in, fmt.Errorf("events from ledger %d: %w", from, err)
		}
	}
	next := in.sealed
	for i := range in.tail {
		if err := checkCommitted(in.offsets, &in.tail[i]); err != nil {
			return in, err
		}
		in.tail[i].first = next
		next += uint32(len(in.tail[i].ends)) //nolint:gosec // an event count
	}
	if next != in.offsets.TotalEvents() {
		return in, fmt.Errorf("corrupt chunk: %d events derived from ledger %d on, %d committed",
			next-in.sealed, from, in.offsets.TotalEvents()-in.sealed)
	}
	return in, in.verifySealedSecret(store, secret)
}

// events yields every event of the chunk in id order: DataCF's up to the
// sealed count, then the rebuilt ones. A seal that lands meanwhile only adds
// ids from the sealed count on, which the scan stops before.
func (in *freezeInputs) events(store *rocksdb.Store) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		var next uint32
		if in.sealed > 0 {
			for entry, err := range store.IterateUncached(DataCF, nil) {
				if err == nil && (len(entry.Key) != dataKeyLen || binary.BigEndian.Uint32(entry.Key) != next) {
					err = fmt.Errorf("corrupt chunk: %s key %x where event %d belongs", DataCF, entry.Key, next)
				}
				if err != nil {
					yield(nil, fmt.Errorf("scan %s: %w", DataCF, err))
					return
				}
				if !yield(entry.Value, nil) {
					return
				}
				if next++; next == in.sealed {
					break
				}
			}
			if next != in.sealed {
				yield(nil, fmt.Errorf("corrupt chunk: %s holds %d of its %d sealed events", DataCF, next, in.sealed))
				return
			}
		}
		for i := range in.tail {
			row := &in.tail[i]
			for j := range row.ends {
				if !yield(row.event(row.first+uint32(j)), nil) { //nolint:gosec // an event count
					return
				}
			}
		}
	}
}

// verifySealedSecret checks that the sealed slabs file their terms under
// secret: event 0's terms, keyed under it, must all be in slab 0. Two
// secrets would otherwise merge into one index, the sealed slabs under
// the hot index's and the tail under the freeze's, and no lookup would
// find the sealed half.
func (in *freezeInputs) verifySealedSecret(store *rocksdb.Store, secret [stores.SecretLen]byte) error {
	if in.sealedSlabs == 0 {
		return nil
	}
	var p Payload
	for raw, err := range in.events(store) {
		if err != nil {
			return err
		}
		if err := p.Unmarshal(raw); err != nil {
			return fmt.Errorf("decode event 0: %w", err)
		}
		break
	}
	terms, err := TermsForBytes(p.ContractEventBytes)
	if err != nil {
		return fmt.Errorf("derive terms for event 0: %w", err)
	}
	for _, term := range terms {
		_, found, err := store.Get(IndexCF, hotIndexKey(0, term.Key(secret)))
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

// copyEvents appends every event to the pack in id order and indexes the
// ones from indexFrom on.
func copyEvents(
	ctx context.Context, events iter.Seq2[[]byte, error], w *ColdWriter, b *ColdIndexBuilder, indexFrom uint32,
) error {
	var next uint32
	var terms Terms
	for raw, err := range events {
		if err != nil {
			return err
		}
		if next%freezeCtxEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if next >= indexFrom {
			var p Payload
			if err := p.Unmarshal(raw); err != nil {
				return fmt.Errorf("decode event %d: %w", next, err)
			}
			terms.Reset()
			if err := terms.AppendEvent(p.ContractEventBytes); err != nil {
				return fmt.Errorf("derive terms for event %d: %w", next, err)
			}
			if err := b.Add(next, &terms); err != nil {
				return err
			}
		}
		if err := w.AppendMarshaled(raw); err != nil {
			return fmt.Errorf("append event %d: %w", next, err)
		}
		next++
	}
	return nil
}
