package query

import (
	"context"
	"fmt"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/entrychange"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
)

// EntryChanges resolves chunk c's ledger entry change index. A cold reader is
// view-owned (Release closes it); the hot facade is registry-owned.
func (a *ReadView) EntryChanges(c chunk.ID) (entrychange.Reader, error) {
	t, db, err := a.resolveTier(c, geometry.KindEntryChanges)
	if err != nil {
		return nil, err
	}
	switch t {
	case tierCold:
		cr, err := entrychange.OpenColdReader(c, a.catalog.Layout().EntryChangesBucketDir(c))
		if err != nil {
			return nil, err
		}
		a.closers = append(a.closers, cr.Close)
		return cr, nil
	case tierHot:
		return db.EntryChanges(), nil
	default:
		return nil, a.unavailable(c, geometry.KindEntryChanges)
	}
}

// LastEntryChange is a key's last change at or before a ledger.
type LastEntryChange struct {
	// Found is set when a ledger in the view's range, at or before the target,
	// changed the key. The other fields are meaningful only then.
	Found bool
	// Ledger is the last such ledger.
	Ledger uint32
	// Entry is the key's entry XDR after Ledger, nil when it was removed. For
	// an evicted key it is the entry's value before the eviction, nil when no
	// value is in the view's range.
	Entry []byte
	// Evicted is set when Ledger evicted the key.
	Evicted bool
}

// LastEntryChanges returns, aligned with keys (ledger key XDR), each key's
// last change at or before ledger, from the entry change index. It walks
// chunks down from ledger's to the floor, reading each candidate ledger the
// index names and resolving the key's state in apply order. The walk stops for
// a key once it finds a ledger that set a value or removed it; past an
// eviction it keeps walking for the value the eviction archived.
//
// The walk is bounded only by the floor, so a key that last changed long ago
// probes every chunk in between. ctx bounds it.
func (a *ReadView) LastEntryChanges(
	ctx context.Context, passphrase string, keys [][]byte, ledger uint32,
) ([]LastEntryChange, error) {
	out := make([]LastEntryChange, len(keys))
	if ledger < a.OldestLedger() || ledger > a.LatestLedger() {
		return nil, &RangeError{Requested: ledger, Oldest: a.OldestLedger(), Latest: a.LatestLedger()}
	}
	terms := make([]event.TermKey, len(keys))
	for i, k := range keys {
		terms[i] = entrychange.TermKey(k)
	}
	pending := make([]int, len(keys))
	for i := range pending {
		pending[i] = i
	}
	decoded := map[uint32]*xdr.LedgerCloseMeta{}

	for c := chunk.IDFromLedger(ledger); len(pending) > 0; c-- {
		bitmaps, err := a.changeLedgers(ctx, c, terms, pending)
		if err != nil {
			return nil, err
		}
		var still []int
		for j, i := range pending {
			done, err := a.resolveInChunk(ctx, passphrase, keys[i], bitmaps[j], ledger, decoded, &out[i])
			if err != nil {
				return nil, err
			}
			if !done {
				still = append(still, i)
			}
		}
		pending = still
		if c == a.FloorChunk() {
			break
		}
	}
	return out, nil
}

// changeLedgers looks up the pending keys' terms in chunk c's index.
func (a *ReadView) changeLedgers(
	ctx context.Context, c chunk.ID, terms []event.TermKey, pending []int,
) ([]*roaring.Bitmap, error) {
	r, err := a.EntryChanges(c)
	if err != nil {
		return nil, fmt.Errorf("entry change index for chunk %s: %w", c, err)
	}
	ts := make([]event.TermKey, len(pending))
	for j, i := range pending {
		ts[j] = terms[i]
	}
	return r.ChangeLedgers(ctx, ts)
}

// resolveInChunk walks one chunk's candidate ledgers for a key, newest first,
// at or before ledger, updating res. done reports the key is resolved.
func (a *ReadView) resolveInChunk(
	ctx context.Context, passphrase string, key []byte, candidates *roaring.Bitmap, ledger uint32,
	decoded map[uint32]*xdr.LedgerCloseMeta, res *LastEntryChange,
) (bool, error) {
	if candidates == nil {
		return false, nil
	}
	for it := candidates.ReverseIterator(); it.HasNext(); {
		seq := it.Next()
		if seq > ledger {
			continue
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		lcm, err := a.decodedLedger(seq, decoded)
		if err != nil {
			return false, err
		}
		state, changed, err := entrychange.StateAfter(passphrase, *lcm, key)
		if err != nil {
			return false, fmt.Errorf("resolve entry change at ledger %d: %w", seq, err)
		}
		if !changed {
			continue
		}
		if !res.Found {
			res.Found = true
			res.Ledger = seq
		}
		if state.Evicted {
			res.Evicted = true
			continue
		}
		res.Entry = state.Entry
		return true, nil
	}
	return false, nil
}

func (a *ReadView) decodedLedger(seq uint32, decoded map[uint32]*xdr.LedgerCloseMeta) (*xdr.LedgerCloseMeta, error) {
	if lcm, ok := decoded[seq]; ok {
		return lcm, nil
	}
	lcm := &xdr.LedgerCloseMeta{}
	if err := a.WithLedger(seq, lcm.UnmarshalBinary); err != nil {
		return nil, fmt.Errorf("read ledger %d: %w", seq, err)
	}
	decoded[seq] = lcm
	return lcm, nil
}

// EntriesChangedAfter reports, aligned with keys, whether any ledger after
// ledger, up to the view's latest, changed each key, from the index alone. A
// key the index names but the ledger did not change after all reads as
// changed, which only sends the caller down its slower path.
func (a *ReadView) EntriesChangedAfter(ctx context.Context, keys [][]byte, ledger uint32) ([]bool, error) {
	out := make([]bool, len(keys))
	latest := a.LatestLedger()
	if ledger >= latest {
		return out, nil
	}
	terms := make([]event.TermKey, len(keys))
	pending := make([]int, len(keys))
	for i, k := range keys {
		terms[i] = entrychange.TermKey(k)
		pending[i] = i
	}
	for c := chunk.IDFromLedger(ledger + 1); len(pending) > 0 && c <= chunk.IDFromLedger(latest); c++ {
		bitmaps, err := a.changeLedgers(ctx, c, terms, pending)
		if err != nil {
			return nil, err
		}
		var still []int
		for j, i := range pending {
			bm := bitmaps[j]
			if bm != nil {
				bm.RemoveRange(0, uint64(ledger)+1)
				bm.RemoveRange(uint64(latest)+1, uint64(1)<<32)
			}
			if bm != nil && !bm.IsEmpty() {
				out[i] = true
				continue
			}
			still = append(still, i)
		}
		pending = still
	}
	return out, nil
}
