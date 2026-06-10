package views

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"strings"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// envPart is one envelope (raw bytes + type) gathered while enumerating a
// TxSet. raw aliases the view buffer (zero-copy). hash is the transaction
// hash computed from the envelope wire bytes under the network passphrase —
// the key the TxProcessing entries reference. isSoroban mirrors
// ingest.LedgerTransaction.IsSorobanTx (read from the inner Tx.Ext view
// discriminant), used by the read path to gate V3 contract events the way
// the struct path does (see the trusted-input invariant in doc.go).
type envPart struct {
	raw       []byte
	typ       xdr.EnvelopeType
	hash      [32]byte
	isSoroban bool
}

// txHasher computes transaction hashes straight from envelope view bytes.
// The tx-hash preimage is
//
//	SHA256(networkID ‖ envelope-type tag (4 bytes) ‖ Transaction XDR)
//
// — exactly what network.HashTransactionInEnvelope produces — and every
// piece is available as a wire-byte slice off the view, so no envelope
// decode is needed. The network ID is computed once per extraction and the
// SHA-256 state is reused, so hashing allocates nothing per envelope.
type txHasher struct {
	networkID [32]byte
	h         hash.Hash
}

// newTxHasher derives the network ID from passphrase. The empty-passphrase
// guard matches network.hashTx's, surfaced once per extraction instead of
// once per envelope.
func newTxHasher(passphrase string) (*txHasher, error) {
	if strings.TrimSpace(passphrase) == "" {
		return nil, errors.New("views: empty network passphrase")
	}
	return &txHasher{networkID: network.ID(passphrase), h: sha256.New()}, nil
}

// sum computes SHA256(networkID ‖ tag ‖ parts...).
func (th *txHasher) sum(tag xdr.EnvelopeType, parts ...[]byte) [32]byte {
	th.h.Reset()
	th.h.Write(th.networkID[:])
	var tagBuf [4]byte
	binary.BigEndian.PutUint32(tagBuf[:], uint32(tag)) //nolint:gosec // enum tag is small and non-negative
	th.h.Write(tagBuf[:])
	for _, p := range parts {
		th.h.Write(p)
	}
	var out [32]byte
	th.h.Sum(out[:0])
	return out
}

// ed25519KeyTypePrefix is the 4-byte XDR discriminant
// CryptoKeyTypeKeyTypeEd25519 (0) that converting a TransactionV0 to a
// Transaction prepends to the source account key on the wire.
var ed25519KeyTypePrefix = [4]byte{0, 0, 0, 0}

// envPartFromView reads one envelope view into an envPart without decoding
// the envelope: the type comes from the union discriminant view, the
// transaction hash is computed by th straight from the inner transaction's
// wire bytes, and the soroban flag is read from the inner Tx.Ext view. The
// returned raw slice stays the zero-copy .Raw() view buffer.
//
// A TX_V0 envelope hashes as its V1 conversion (network.HashTransactionV0):
// on the wire that conversion is exactly the 4-byte ed25519 key-type prefix
// followed by the unchanged TransactionV0 bytes — the V0 optional-TimeBounds
// and V1 Preconditions encodings are identical (absent ⇒ 0 ⇒ PRECOND_NONE,
// present ⇒ 1 + TimeBounds ⇒ PRECOND_TIME), as are all fields after them
// (both Ext arms are void).
func envPartFromView(env xdr.TransactionEnvelopeView, th *txHasher) (envPart, error) {
	typ, err := xdr.Try(func() xdr.EnvelopeType { return env.MustType().MustValue() })
	if err != nil {
		return envPart{}, fmt.Errorf("views: envelope type: %w", err)
	}
	switch typ {
	case xdr.EnvelopeTypeEnvelopeTypeTx,
		xdr.EnvelopeTypeEnvelopeTypeTxV0,
		xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
	default:
		return envPart{}, fmt.Errorf("views: invalid transaction envelope type %v", typ)
	}
	p := envPart{typ: typ}
	err = xdr.TryVoid(func() {
		p.raw = env.MustRaw()
		switch typ {
		case xdr.EnvelopeTypeEnvelopeTypeTx:
			tx := env.MustV1().MustTx()
			p.hash = th.sum(typ, tx.MustRaw())
			p.isSoroban = txExtIsSoroban(tx)
		case xdr.EnvelopeTypeEnvelopeTypeTxV0:
			p.hash = th.sum(xdr.EnvelopeTypeEnvelopeTypeTx,
				ed25519KeyTypePrefix[:], env.MustV0().MustTx().MustRaw())
		case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
			fbTx := env.MustFeeBump().MustTx()
			p.hash = th.sum(typ, fbTx.MustRaw())
			// IsSorobanTx inspects the fee-bump INNER transaction.
			p.isSoroban = txExtIsSoroban(fbTx.MustInnerTx().MustV1().MustTx())
		}
	})
	if err != nil {
		return envPart{}, fmt.Errorf("views: envelope (%v): %w", typ, err)
	}
	return p, nil
}

// txExtIsSoroban reads Tx.Ext's union discriminant (1 ⟺
// SorobanTransactionData present), mirroring
// ingest.LedgerTransaction.IsSorobanTx / GetSorobanData. Must-style: panics
// with *xdr.ViewError on malformed input, recovered by the caller's TryVoid.
func txExtIsSoroban(tx xdr.TransactionView) bool {
	return tx.MustExt().MustV().MustValue() == 1
}

// envYield is the per-envelope sink the TxSet enumerators drive. It is
// called once per envelope (in agreed-set order, NOT TxProcessing apply
// order); returning stop==true ends enumeration early (used by the
// by-hash lookups once every wanted hash is resolved), returning an error
// aborts it.
type envYield func(envPart) (stop bool, err error)

// enumerateEnvelopesFromV0TxSet walks a V0 TransactionSet, invoking yield for
// every envelope. The TxSet is in agreed-set order (NOT TxProcessing apply
// order); callers pair envelopes to transactions by hash, so enumeration order
// is irrelevant here. Returns stopped==true if yield asked to stop early.
func enumerateEnvelopesFromV0TxSet(txSet xdr.TransactionSetView, th *txHasher, yield envYield) (stopped bool, err error) {
	txs, err := txSet.Txs()
	if err != nil {
		return false, fmt.Errorf("views: V0 TxSet.Txs: %w", err)
	}
	i := 0
	for env, eerr := range txs.Iter() {
		if eerr != nil {
			return false, fmt.Errorf("views: V0 envelope at %d: %w", i, eerr)
		}
		p, perr := envPartFromView(env, th)
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
func enumerateEnvelopesFromGeneralized(txSet xdr.GeneralizedTransactionSetView, th *txHasher, yield envYield) (stopped bool, err error) {
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
			stop, err := enumerateV0Components(comps, th, yield)
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
			stop, err := enumerateParallelTxs(ptx, th, yield)
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
func enumerateV0Components(comps xdr.TransactionPhaseV0ComponentsView, th *txHasher, yield envYield) (stopped bool, err error) {
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
			p, perr := envPartFromView(env, th)
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
func enumerateParallelTxs(ptx xdr.ParallelTxsComponentView, th *txHasher, yield envYield) (stopped bool, err error) {
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
				p, perr := envPartFromView(env, th)
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
