package adapters

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerentries"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
)

// NewLedgerEntryGetterAt returns getLedgerEntries' getter for a request's
// ledger: entries as they were after that ledger, from the entry change index
// over the request's read view. latest reads the current state from
// stellar-core; a key no ledger changed after the requested one is read there,
// since its state then is its state now.
func NewLedgerEntryGetterAt(
	passphrase string, latest ledgerentries.LedgerEntryGetter,
) func(ledger uint32) ledgerentries.LedgerEntryGetter {
	return func(ledger uint32) ledgerentries.LedgerEntryGetter {
		if ledger == 0 {
			return latest
		}
		return &historicalEntryGetter{passphrase: passphrase, latest: latest, ledger: ledger}
	}
}

type historicalEntryGetter struct {
	passphrase string
	latest     ledgerentries.LedgerEntryGetter
	ledger     uint32
}

// keyState is what the index walk settled for one key at the ledger.
type keyState int

const (
	stateUnknown  keyState = iota // nothing in range changed it at or before the ledger
	stateAbsent                   // removed, or evicted while temporary
	statePresent                  // entry holds its value
	stateArchived                 // evicted while persistent; entry holds the archived value
	stateCurrent                  // unchanged since the ledger: read the current state
)

// GetLedgerEntries resolves each key at g.ledger. A contract data or code
// entry also resolves its TTL key, for liveUntilLedgerSeq. A key that changed
// after the ledger but not in the stored range at or before it is unknowable
// unless the node holds history from genesis, where it did not exist yet.
//
//nolint:cyclop,funlen // one pass over the keys' cases
func (g *historicalEntryGetter) GetLedgerEntries(
	ctx context.Context, keys []xdr.LedgerKeyView,
) ([]ledgerentries.LedgerKeyAndEntry, uint32, error) {
	view, err := query.ViewFrom(ctx)
	if err != nil {
		return nil, 0, err
	}
	if g.ledger < view.OldestLedger() {
		return nil, 0, fmt.Errorf("%w: ledger %d is below the oldest stored ledger %d",
			ledgerentries.ErrUnavailable, g.ledger, view.OldestLedger())
	}

	// all is the data keys, then the TTL keys of the soroban ones.
	all := make([][]byte, len(keys))
	ttlOf := make([]int, len(keys)) // index in all of key i's TTL key, -1 for none
	temporary := make([]bool, len(keys))
	for i, k := range keys {
		all[i] = []byte(k)
		ttlOf[i] = -1
	}
	for i, k := range keys {
		var key xdr.LedgerKey
		if err := key.UnmarshalBinary(k); err != nil {
			return nil, 0, err
		}
		switch key.Type {
		case xdr.LedgerEntryTypeContractData, xdr.LedgerEntryTypeContractCode:
			ttlOf[i] = len(all)
			all = append(all, ttlKey(k))
			temporary[i] = key.Type == xdr.LedgerEntryTypeContractData &&
				key.ContractData.Durability == xdr.ContractDataDurabilityTemporary
		default:
		}
	}

	last, err := view.LastEntryChanges(ctx, g.passphrase, all, g.ledger)
	if err != nil {
		return nil, 0, err
	}
	var unresolved [][]byte
	var unresolvedAt []int
	for i, l := range last {
		if !l.Found {
			unresolved = append(unresolved, all[i])
			unresolvedAt = append(unresolvedAt, i)
		}
	}
	changedAfter := make([]bool, len(all))
	if len(unresolved) > 0 {
		after, err := view.EntriesChangedAfter(ctx, unresolved, g.ledger)
		if err != nil {
			return nil, 0, err
		}
		for j, i := range unresolvedAt {
			changedAfter[i] = after[j]
		}
	}
	fullHistory := view.OldestLedger() == chunk.FirstLedgerSeq

	settle := func(i int) keyState {
		l := last[i]
		switch {
		case !l.Found && !changedAfter[i]:
			return stateCurrent
		case !l.Found && fullHistory:
			return stateAbsent
		case !l.Found:
			return stateUnknown
		case l.Evicted && l.Entry != nil:
			return stateArchived
		case l.Entry == nil && !l.Evicted:
			return stateAbsent
		case l.Entry == nil:
			// Evicted with its value below the floor (or temporary and gone).
			if fullHistory {
				return stateAbsent
			}
			return stateUnknown
		default:
			return statePresent
		}
	}

	// Keys that read the current state, data keys or their TTLs, share one
	// stellar-core read keyed by the data key (core reports the TTL with it).
	var coreKeys []xdr.LedgerKeyView
	needsCore := map[int]bool{}
	for i := range keys {
		ds := settle(i)
		if ds == stateCurrent || (ttlOf[i] >= 0 && ds == statePresent && settle(ttlOf[i]) == stateCurrent) {
			needsCore[i] = true
			coreKeys = append(coreKeys, keys[i])
		}
	}
	current := map[string]ledgerentries.LedgerKeyAndEntry{}
	if len(coreKeys) > 0 {
		entries, _, err := g.latest.GetLedgerEntries(ctx, coreKeys)
		if err != nil {
			return nil, 0, err
		}
		for _, e := range entries {
			current[string(e.Key)] = e
		}
	}

	var out []ledgerentries.LedgerKeyAndEntry
	for i, k := range keys {
		ds := settle(i)
		if ds == stateUnknown {
			return nil, 0, fmt.Errorf("%w: the state of a key at ledger %d predates the oldest stored ledger %d",
				ledgerentries.ErrUnavailable, g.ledger, view.OldestLedger())
		}
		if ds == stateCurrent {
			if e, ok := current[string(k)]; ok {
				out = append(out, e)
			}
			continue
		}
		if ds == stateAbsent || (ds == stateArchived && temporary[i]) {
			continue
		}
		res := ledgerentries.LedgerKeyAndEntry{Key: k, Entry: last[i].Entry}
		if ttlOf[i] >= 0 {
			liveUntil, ok, err := g.liveUntil(ttlOf[i], ds, settle, last, current, k)
			if err != nil {
				return nil, 0, err
			}
			if !ok {
				continue
			}
			res.LiveUntilLedgerSeq = &liveUntil
		}
		out = append(out, res)
	}
	return out, g.ledger, nil
}

// liveUntil returns a soroban entry's live-until ledger at g.ledger. ok is
// false for a temporary entry that had expired, which is gone.
func (g *historicalEntryGetter) liveUntil(
	ttlIdx int, ds keyState, settle func(int) keyState, last []query.LastEntryChange,
	current map[string]ledgerentries.LedgerKeyAndEntry, k xdr.LedgerKeyView,
) (uint32, bool, error) {
	if ds == stateArchived {
		// stellar-core reports no TTL for an archived entry; 0 is the placeholder.
		return 0, true, nil
	}
	var liveUntil uint32
	switch settle(ttlIdx) {
	case statePresent:
		var ttl xdr.LedgerEntry
		if err := ttl.UnmarshalBinary(last[ttlIdx].Entry); err != nil {
			return 0, false, err
		}
		liveUntil = uint32(ttl.Data.MustTtl().LiveUntilLedgerSeq)
	case stateCurrent:
		e, ok := current[string(k)]
		if !ok || e.LiveUntilLedgerSeq == nil {
			return 0, false, fmt.Errorf("%w: no TTL for a contract entry at ledger %d",
				ledgerentries.ErrUnavailable, g.ledger)
		}
		liveUntil = *e.LiveUntilLedgerSeq
	default:
		return 0, false, fmt.Errorf("%w: the TTL of a contract entry at ledger %d is not in the stored history",
			ledgerentries.ErrUnavailable, g.ledger)
	}
	return liveUntil, true, nil
}

// ttlKey is the XDR of the TTL key for a contract data or code key's XDR.
func ttlKey(keyXDR []byte) []byte {
	key := xdr.LedgerKey{Type: xdr.LedgerEntryTypeTtl, Ttl: &xdr.LedgerKeyTtl{KeyHash: xdr.Hash(sha256.Sum256(keyXDR))}}
	b, err := key.MarshalBinary()
	if err != nil {
		panic(err) // a fixed-size key always marshals
	}
	return b
}
