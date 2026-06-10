package views

import (
	"fmt"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// envPart is one envelope (raw bytes + type) gathered while enumerating a
// TxSet. raw aliases the view buffer (zero-copy). hash is the transaction
// hash computed from the envelope under the network passphrase — the key the
// TxProcessing entries reference. isSoroban mirrors
// ingest.LedgerTransaction.IsSorobanTx (computed from the SAME transient decode
// used for hashing), used by the read path to gate V3 contract events the way
// the struct path does.
type envPart struct {
	raw       []byte
	typ       xdr.EnvelopeType
	hash      [32]byte
	isSoroban bool
}

// envPartFromView decodes the envelope view into an envPart: the RETURNED raw
// bytes stay the zero-copy .Raw() slice; the binary decode feeds the
// transaction hash (the map key the TxProcessing entries reference), the
// envelope type, and the soroban flag. Mirrors ingest.LedgerTransactionReader.storeTransactions,
// which hashes every TxSet envelope (via network.HashTransactionInEnvelope,
// which handles the fee-bump case) because the TxSet is in agreed-set /
// hash-sorted order, NOT TxProcessing apply order.
func envPartFromView(env xdr.TransactionEnvelopeView, passphrase string) (envPart, error) {
	raw, err := env.Raw()
	if err != nil {
		return envPart{}, fmt.Errorf("views: envelope.Raw: %w", err)
	}
	// Decode once and read everything we need off it: the transaction hash
	// (network.HashTransactionInEnvelope needs a decoded envelope — it builds
	// the tx-hash preimage, not a hash of the raw bytes), the envelope type,
	// and the soroban flag (which must inspect Tx.Ext). The decode is required
	// regardless for isSoroban, so hashing piggybacks on it rather than being
	// extra work. The RETURNED raw slice stays the zero-copy .Raw() view
	// buffer, not this decoded value.
	var decoded xdr.TransactionEnvelope
	if uerr := decoded.UnmarshalBinary(raw); uerr != nil {
		return envPart{}, fmt.Errorf("views: envelope decode for hashing: %w", uerr)
	}
	hash, err := network.HashTransactionInEnvelope(decoded, passphrase)
	if err != nil {
		return envPart{}, fmt.Errorf("views: hash envelope: %w", err)
	}
	return envPart{raw: raw, typ: decoded.Type, hash: hash, isSoroban: envelopeIsSoroban(decoded)}, nil
}

// envelopeIsSoroban mirrors ingest.LedgerTransaction.IsSorobanTx /
// GetSorobanData: a tx is Soroban iff its (inner, for fee-bump) tx envelope
// carries SorobanTransactionData in its Ext. Only the v1 Tx and fee-bump inner
// v1 Tx shapes can carry Soroban data; every other envelope type is classic.
//
// NB: the Tx -> V1.Tx.Ext.GetSorobanData (and fee-bump -> inner) access path
// here mirrors the SDK's ingest.LedgerTransaction.IsSorobanTx/GetSorobanData;
// if the SDK ever relocates where soroban-data lives, this is a known drift
// point to update in lockstep.
func envelopeIsSoroban(env xdr.TransactionEnvelope) bool {
	switch env.Type {
	case xdr.EnvelopeTypeEnvelopeTypeTx:
		_, ok := env.V1.Tx.Ext.GetSorobanData()
		return ok
	case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
		_, ok := env.FeeBump.Tx.InnerTx.V1.Tx.Ext.GetSorobanData()
		return ok
	default:
		return false
	}
}

// envYield is the per-envelope sink the TxSet enumerators drive. It is
// called once per envelope (in agreed-set order, NOT TxProcessing apply
// order); returning stop==true ends enumeration early (used by the
// by-hash lookup once it finds its target), returning an error aborts it.
type envYield func(envPart) (stop bool, err error)

// enumerateEnvelopesFromV0TxSet walks a V0 TransactionSet, invoking yield for
// every envelope. The TxSet is in agreed-set order (NOT TxProcessing apply
// order); callers pair envelopes to transactions by hash, so enumeration order
// is irrelevant here. Returns stopped==true if yield asked to stop early.
func enumerateEnvelopesFromV0TxSet(txSet xdr.TransactionSetView, passphrase string, yield envYield) (stopped bool, err error) {
	txs, err := txSet.Txs()
	if err != nil {
		return false, fmt.Errorf("views: V0 TxSet.Txs: %w", err)
	}
	i := 0
	for env, eerr := range txs.Iter() {
		if eerr != nil {
			return false, fmt.Errorf("views: V0 envelope at %d: %w", i, eerr)
		}
		p, perr := envPartFromView(env, passphrase)
		if perr != nil {
			return false, perr
		}
		stop, yerr := yield(p)
		if yerr != nil {
			return false, yerr
		}
		if stop {
			return true, nil
		}
		i++
	}
	return false, nil
}

// enumerateEnvelopesFromGeneralized walks a V1/V2 GeneralizedTransactionSet
// (phases -> components/clusters -> txs), invoking yield for every envelope.
// Like the V0 walker, the order is the agreed-set order (NOT TxProcessing apply
// order); pairing is by hash so order does not matter. Returns stopped==true if
// yield asked to stop early.
func enumerateEnvelopesFromGeneralized(txSet xdr.GeneralizedTransactionSetView, passphrase string, yield envYield) (stopped bool, err error) {
	v1Set, err := txSet.V1TxSet()
	if err != nil {
		return false, fmt.Errorf("views: V1TxSet: %w", err)
	}
	phases, err := v1Set.Phases()
	if err != nil {
		return false, fmt.Errorf("views: Phases: %w", err)
	}
	for phase, perr := range phases.Iter() {
		if perr != nil {
			return false, fmt.Errorf("views: phase iter: %w", perr)
		}
		pv, err := phase.V()
		if err != nil {
			return false, fmt.Errorf("views: phase.V: %w", err)
		}
		pDisc, err := pv.Value()
		if err != nil {
			return false, fmt.Errorf("views: phase.V value: %w", err)
		}
		switch pDisc {
		case 0:
			comps, err := phase.V0Components()
			if err != nil {
				return false, fmt.Errorf("views: V0Components: %w", err)
			}
			stop, err := enumerateV0Components(comps, passphrase, yield)
			if err != nil {
				return false, err
			}
			if stop {
				return true, nil
			}
		case 1:
			ptx, err := phase.ParallelTxsComponent()
			if err != nil {
				return false, fmt.Errorf("views: ParallelTxsComponent: %w", err)
			}
			stop, err := enumerateParallelTxs(ptx, passphrase, yield)
			if err != nil {
				return false, err
			}
			if stop {
				return true, nil
			}
		default:
			return false, fmt.Errorf("views: unknown TransactionPhase V=%d", pDisc)
		}
	}
	return false, nil
}

// enumerateV0Components invokes yield for every envelope in V0-style phase
// components (one component per fee group). Returns stopped==true if yield
// asked to stop early.
func enumerateV0Components(comps xdr.TransactionPhaseV0ComponentsView, passphrase string, yield envYield) (stopped bool, err error) {
	for comp, cerr := range comps.Iter() {
		if cerr != nil {
			return false, fmt.Errorf("views: component iter: %w", cerr)
		}
		tdf, err := comp.TxsMaybeDiscountedFee()
		if err != nil {
			return false, fmt.Errorf("views: TxsMaybeDiscountedFee: %w", err)
		}
		txs, err := tdf.Txs()
		if err != nil {
			return false, fmt.Errorf("views: component Txs: %w", err)
		}
		for env, eerr := range txs.Iter() {
			if eerr != nil {
				return false, fmt.Errorf("views: component envelope iter: %w", eerr)
			}
			p, perr := envPartFromView(env, passphrase)
			if perr != nil {
				return false, perr
			}
			stop, yerr := yield(p)
			if yerr != nil {
				return false, yerr
			}
			if stop {
				return true, nil
			}
		}
	}
	return false, nil
}

// enumerateParallelTxs invokes yield for every envelope in V1-style parallel-txs
// (stages -> clusters -> txs). Returns stopped==true if yield asked to stop
// early.
func enumerateParallelTxs(ptx xdr.ParallelTxsComponentView, passphrase string, yield envYield) (stopped bool, err error) {
	stages, err := ptx.ExecutionStages()
	if err != nil {
		return false, fmt.Errorf("views: ExecutionStages: %w", err)
	}
	for stage, serr := range stages.Iter() {
		if serr != nil {
			return false, fmt.Errorf("views: stage iter: %w", serr)
		}
		for cluster, cerr := range stage.Iter() {
			if cerr != nil {
				return false, fmt.Errorf("views: cluster iter: %w", cerr)
			}
			for env, eerr := range cluster.Iter() {
				if eerr != nil {
					return false, fmt.Errorf("views: cluster envelope iter: %w", eerr)
				}
				p, perr := envPartFromView(env, passphrase)
				if perr != nil {
					return false, perr
				}
				stop, yerr := yield(p)
				if yerr != nil {
					return false, yerr
				}
				if stop {
					return true, nil
				}
			}
		}
	}
	return false, nil
}
