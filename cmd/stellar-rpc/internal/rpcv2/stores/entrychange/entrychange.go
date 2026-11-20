// Package entrychange is the ledger entry change index: for each chunk, which
// ledgers changed each ledger key. It answers getLedgerEntries at any stored
// ledger: the state of a key after ledger L is its state after the last ledger
// at or before L that changed it, read from that ledger's meta.
//
// The index records keys only, so it carries no ordering or state. A ledger it
// names is a candidate: StateAfter decodes the ledger and resolves the key's
// final state in apply order, and reports when the ledger did not change the
// key after all.
//
// Hot chunks hold the index in the entrychanges column family of the chunk's
// hot DB, keyed by term then ledger. Frozen chunks hold it as an index.pack +
// index.hash pair in the events index format (event.WriteColdIndexFiles),
// mapping each term to a bitmap of ledger offsets within the chunk.
package entrychange

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
)

// termField separates ledger key terms from other term families that share
// event.ComputeTermKey. The index holds nothing else, so it is only a domain tag.
const termField event.Field = 0xEC

// TermKey is the index term for a ledger key's XDR.
func TermKey(keyXDR []byte) event.TermKey { return event.ComputeTermKey(keyXDR, termField) }

// ChangedKeys returns the XDR of every ledger key lcm changes, each once: fee
// processing, transaction apply, post-apply fee processing, upgrades and
// evictions. It is the set the index records for the ledger. It may include a
// key whose change StateAfter does not count (operations of a pre-protocol-13
// transaction that failed with an internal error); StateAfter reports those as
// not changed.
func ChangedKeys(lcm xdr.LedgerCloseMeta) ([][]byte, error) {
	var all []xdr.LedgerEntryChanges
	for i := range lcm.CountTransactions() {
		all = append(all, lcm.FeeProcessing(i))
		meta, err := metaChanges(lcm.TxApplyProcessing(i))
		if err != nil {
			return nil, fmt.Errorf("transaction %d: %w", i, err)
		}
		all = append(all, meta...)
	}
	if v2, ok := lcm.GetV2(); ok {
		for _, tx := range v2.TxProcessing {
			all = append(all, tx.PostTxApplyFeeProcessing)
		}
	}
	for _, u := range lcm.UpgradesProcessing() {
		all = append(all, u.Changes)
	}

	seen := map[string]struct{}{}
	var keys [][]byte
	add := func(key xdr.LedgerKey) error {
		b, err := key.MarshalBinary()
		if err != nil {
			return err
		}
		if _, ok := seen[string(b)]; !ok {
			seen[string(b)] = struct{}{}
			keys = append(keys, b)
		}
		return nil
	}
	for _, changes := range all {
		for _, c := range changes {
			key, err := changeKey(c)
			if err != nil {
				return nil, err
			}
			if err := add(key); err != nil {
				return nil, err
			}
		}
	}
	evicted, err := lcm.EvictedLedgerKeys()
	if err != nil {
		return nil, err
	}
	for _, key := range evicted {
		if err := add(key); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

// metaChanges returns every change list in a transaction's meta.
func metaChanges(m xdr.TransactionMeta) ([]xdr.LedgerEntryChanges, error) {
	var out []xdr.LedgerEntryChanges
	switch m.V {
	case 0:
		for _, op := range *m.Operations {
			out = append(out, op.Changes)
		}
	case 1:
		v := m.MustV1()
		out = append(out, v.TxChanges)
		for _, op := range v.Operations {
			out = append(out, op.Changes)
		}
	case 2:
		v := m.MustV2()
		out = append(out, v.TxChangesBefore, v.TxChangesAfter)
		for _, op := range v.Operations {
			out = append(out, op.Changes)
		}
	case 3:
		v := m.MustV3()
		out = append(out, v.TxChangesBefore, v.TxChangesAfter)
		for _, op := range v.Operations {
			out = append(out, op.Changes)
		}
	case 4:
		v := m.MustV4()
		out = append(out, v.TxChangesBefore, v.TxChangesAfter)
		for _, op := range v.Operations {
			out = append(out, op.Changes)
		}
	default:
		return nil, fmt.Errorf("unsupported TransactionMeta.V %d", m.V)
	}
	return out, nil
}

func changeKey(c xdr.LedgerEntryChange) (xdr.LedgerKey, error) {
	switch c.Type {
	case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
		return c.Created.LedgerKey()
	case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
		return c.Updated.LedgerKey()
	case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
		return c.MustRemoved(), nil
	case xdr.LedgerEntryChangeTypeLedgerEntryState:
		return c.State.LedgerKey()
	case xdr.LedgerEntryChangeTypeLedgerEntryRestored:
		return c.Restored.LedgerKey()
	default:
		return xdr.LedgerKey{}, fmt.Errorf("unknown ledger entry change type %d", c.Type)
	}
}

// State is a ledger key's state after a ledger that changed it.
type State struct {
	// Entry is the entry's XDR after the ledger, nil when the ledger removed or
	// evicted it.
	Entry []byte
	// Evicted is set when the ledger evicted the key, after any other change
	// to it. A persistent entry is archived, so its last value is the one
	// before the eviction; a temporary entry is gone.
	Evicted bool
}

// StateAfter returns keyXDR's state after lcm, resolving its changes in apply
// order. changed is false when lcm did not change the key. passphrase is the
// network passphrase, which the SDK change reader needs to pair transaction
// envelopes with their meta.
func StateAfter(passphrase string, lcm xdr.LedgerCloseMeta, keyXDR []byte) (State, bool, error) {
	reader, err := sdkingest.NewLedgerChangeReaderFromLedgerCloseMeta(passphrase, lcm)
	if err != nil {
		return State{}, false, err
	}
	defer reader.Close()

	var state State
	changed := false
	for {
		c, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return State{}, false, err
		}
		key, err := c.LedgerKey()
		if err != nil {
			return State{}, false, err
		}
		b, err := key.MarshalBinary()
		if err != nil {
			return State{}, false, err
		}
		if !bytes.Equal(b, keyXDR) {
			continue
		}
		changed = true
		state.Entry = nil
		if c.Post != nil {
			if state.Entry, err = c.Post.MarshalBinary(); err != nil {
				return State{}, false, err
			}
		}
	}

	evicted, err := lcm.EvictedLedgerKeys()
	if err != nil {
		return State{}, false, err
	}
	for _, key := range evicted {
		b, err := key.MarshalBinary()
		if err != nil {
			return State{}, false, err
		}
		if bytes.Equal(b, keyXDR) {
			return State{Evicted: true}, true, nil
		}
	}
	return state, changed, nil
}
