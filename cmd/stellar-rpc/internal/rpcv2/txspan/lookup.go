package txspan

import (
	"bytes"
	"fmt"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// PieceReader hands back the exact bytes of one row's two spans: env is the
// whole TransactionEnvelope out of the TxSet and elem the whole txProcessing
// element. Both ALIAS storage the reader owns and are valid only until the
// call that produced them is done with — a tier that decodes frames on demand
// reuses its scratch — so a caller retaining either copies it out.
//
// Both slices of ONE call are valid together; a later call may invalidate
// both.
type PieceReader func(r Row) (env, elem []byte, err error)

// RawPieces is the PieceReader over a whole decoded ledger: it slices raw
// directly, after proving the row's spans lie inside it.
func RawPieces(raw []byte) PieceReader {
	return func(r Row) ([]byte, []byte, error) {
		if err := inBounds(len(raw), r); err != nil {
			return nil, nil, err
		}
		return raw[r.EnvStart:r.EnvEnd], raw[r.ElemStart:r.ElemEnd], nil
	}
}

// Lookup finds the transaction with the given hash in raw through t and
// materializes it, without walking the ledger: the table names the two byte
// spans and ingest.LedgerTransactionViewFromParts reads them.
//
// It is LookupPieces over the whole-ledger PieceReader — the shape a caller
// holding the decoded bytes already has — and it reads the ledger header from
// raw itself. A caller serving one transaction out of a framed value reads
// that header from the value's first frame (ReadLedgerHeader) and calls
// LookupPieces.
func Lookup(raw []byte, t Table, hash [32]byte, passphrase string) (ingest.LedgerTransactionView, bool, error) {
	header, err := ReadLedgerHeader(raw)
	if err != nil {
		return ingest.LedgerTransactionView{}, false, err
	}
	return LookupPieces(t, RawPieces(raw), hash, header, passphrase)
}

// LookupPieces finds the transaction with the given hash through t and
// materializes it from the two spans pieces reads, touching no more of the
// ledger than those spans. A fee-bump transaction matches either of its
// hashes; the view's own Hash is always the outer one, as the decode path's
// is.
//
// header is the LEDGER's own, read from its bytes by the caller — never the
// table's copy. Its sequence and close time are what the served view carries,
// and its union discriminant is what the element is read under; the table's
// two stamps are checked against it and serve nothing.
//
// found is false with a nil error for exactly one reason: no row the index
// routed this hash to carries it. A four-byte prefix collides, so a routed
// row whose element holds another transaction is that row declining, not a
// failure — and a table that simply does not hold the hash is a NEGATIVE the
// caller acts on, not an error.
//
// Everything else is an ERROR (ErrTable), and deliberately not something a
// caller works around: a table that disagrees with its ledger is a bad
// artifact, and a read that quietly answered from somewhere else would leave
// the operator with no sign of it.
//
// The table names TWO spans, and all three of them are checked before any is
// served. The element is confirmed against hash as the index entry claims; the
// ENVELOPE is then re-hashed with network.TransactionViewHasher (hence
// passphrase) and required to equal the element's own outer transaction hash,
// which is the only thing that ties the two halves of a row together; and the
// assembled view's own Hash must be that same hash, which is what ties the
// answer to the confirmation. A row whose spans have drifted apart, or whose
// element assembles into a different transaction — a table crafted or
// corrupted in a way its CRC still covers — fails here rather than serving
// someone else's transaction.
//
// t must be the table built for exactly the ledger pieces reads from. Every
// byte field of the returned view ALIASES what pieces returned — the SDK's own
// zero-copy contract, since the assembly IS the SDK's — so a caller retaining
// it copies out first.
func LookupPieces(
	t Table, pieces PieceReader, hash [32]byte, header LedgerHeader, passphrase string,
) (ingest.LedgerTransactionView, bool, error) {
	// The table is only meaningful for one ledger of one shape, and both are
	// the LEDGER's to state: a stamp that does not match the header the caller
	// read means this table was paired with another ledger, and reading its
	// offsets against these bytes would answer from a shape they do not have.
	if t.LedgerSeq() != header.LedgerSeq {
		return ingest.LedgerTransactionView{}, false, fmt.Errorf(
			"%w: table stamped for ledger %d, read against ledger %d",
			ErrTable, t.LedgerSeq(), header.LedgerSeq)
	}
	if int32(t.LCMVersion()) != header.LCMVersion {
		return ingest.LedgerTransactionView{}, false, fmt.Errorf(
			"%w: ledger %d: table built for LedgerCloseMeta V%d, the ledger is V%d",
			ErrTable, header.LedgerSeq, t.LCMVersion(), header.LCMVersion)
	}
	ledgerSeq := header.LedgerSeq
	ext := t.ExtBytes()
	// Built at most once per lookup, and only for a row that got as far as
	// needing it: deriving the network ID costs a SHA-256 of the passphrase,
	// which a miss must not pay.
	var hasher *network.TransactionViewHasher
	for m := range t.Find(hash) {
		env, elem, err := pieces(m.Row)
		if err != nil {
			return ingest.LedgerTransactionView{}, false, err
		}
		if len(elem) < ext+hashLen {
			return ingest.LedgerTransactionView{}, false, fmt.Errorf(
				"%w: ledger %d row %d: element span [%d, %d) does not hold %d extension bytes and a hash",
				ErrTable, ledgerSeq, m.ApplyIdx, m.ElemStart, m.ElemEnd, ext)
		}
		confirmed, err := elementCarries(elem, ext, hash)
		if err != nil {
			return ingest.LedgerTransactionView{}, false, fmt.Errorf(
				"%w: ledger %d row %d: %w", ErrTable, ledgerSeq, m.ApplyIdx, err)
		}
		// The one negative this package reports: a four-byte prefix routed
		// the hash here and the element holds another transaction, which is a
		// collision and not a disagreement.
		if !confirmed {
			continue
		}
		if hasher == nil {
			if hasher, err = network.NewTransactionViewHasher(passphrase); err != nil {
				return ingest.LedgerTransactionView{}, false,
					fmt.Errorf("%w: transaction hasher: %w", ErrTable, err)
			}
		}
		// The element vouched for itself; nothing so far vouches for the
		// envelope the SAME row names. Hashing it is that check — one SHA-256
		// over bytes already in hand — and a disagreement is the table naming
		// two halves of different transactions, which no other check can see.
		if !envelopeCarries(hasher, env, [32]byte(elem[ext:ext+hashLen])) {
			return ingest.LedgerTransactionView{}, false, fmt.Errorf(
				"%w: ledger %d row %d: its envelope does not hash to the transaction its element carries",
				ErrTable, ledgerSeq, m.ApplyIdx)
		}
		// The pairing and the confirmation are this package's; the assembly is
		// the per-element half of the SDK's own read path, reached by span
		// arithmetic instead of a traversal, so the two cannot drift.
		view, err := ingest.LedgerTransactionViewFromParts(
			env, elem, header.LCMVersion, m.ApplyIdx, header.LedgerSeq, header.CloseTime)
		if err != nil {
			return ingest.LedgerTransactionView{}, false, fmt.Errorf(
				"%w: ledger %d row %d: assembling the transaction: %w", ErrTable, ledgerSeq, m.ApplyIdx, err)
		}
		// The confirmation read the hash at the offset the table's extension
		// width names; the assembly read its own at the offset the table's
		// LCM version implies. Nothing before this requires those to be the
		// same place, so a view is served only if it came back carrying the
		// hash that was confirmed — for a fee bump, the outer one, which is
		// what both sides read whichever hash the lookup arrived by.
		if view.Hash != [32]byte(elem[ext:ext+hashLen]) {
			return ingest.LedgerTransactionView{}, false, fmt.Errorf(
				"%w: ledger %d row %d: assembled transaction %x under a row confirmed for %x",
				ErrTable, ledgerSeq, m.ApplyIdx, view.Hash, elem[ext:ext+hashLen])
		}
		return view, true, nil
	}
	return ingest.LedgerTransactionView{}, false, nil
}

// inBounds proves a row's two spans lie inside a ledger of the given size. A
// table paired with the wrong or a truncated ledger fails here instead of
// panicking on a slice; whether the element is long enough for its extension
// point and a hash is the caller's check, since a reader that slices frames
// learns that only from the bytes it got back.
func inBounds(size int, r Row) error {
	//nolint:gosec // size is a slice length; it is never negative
	n := uint64(size)
	if uint64(r.EnvStart) > uint64(r.EnvEnd) || uint64(r.EnvEnd) > n {
		return fmt.Errorf("%w: envelope span [%d, %d) is not inside a %d-byte ledger",
			ErrTable, r.EnvStart, r.EnvEnd, size)
	}
	if uint64(r.ElemStart) > uint64(r.ElemEnd) || uint64(r.ElemEnd) > n {
		return fmt.Errorf("%w: element span [%d, %d) is not inside a %d-byte ledger",
			ErrTable, r.ElemStart, r.ElemEnd, size)
	}
	return nil
}

// elementCarries reports whether elem's transaction is the one hash names. The
// outer hash sits at a fixed offset, so the common case is a 32-byte compare;
// only when that differs is the result union read for a fee-bump's inner hash,
// which is the other hash the same transaction answers to.
func elementCarries(elem []byte, extBytes int, hash [32]byte) (bool, error) {
	if bytes.Equal(elem[extBytes:extBytes+hashLen], hash[:]) {
		return true, nil
	}
	inner, feeBump, err := innerHash(elem, extBytes)
	if err != nil {
		return false, err
	}
	return feeBump && inner == hash, nil
}

// envelopeCarries reports whether env hashes to outer — the transaction hash
// the element it was paired with carries. It is the row's two spans checked
// against each other: the table says "this envelope belongs to that element",
// and the network hash is the only thing that can confirm it.
//
// A fee-bump row is covered by the same equality: the span holds the OUTER
// envelope and the element's outer hash is the fee-bump transaction's own, so
// a lookup that arrived by the inner hash is verified against the outer pair
// exactly as a plain one is.
//
// Malformed envelope bytes are a mismatch, not an error: the only way the
// hasher can refuse them is that the span does not point at an envelope, which
// is the same disagreement by a different name.
func envelopeCarries(hasher *network.TransactionViewHasher, env []byte, outer [32]byte) bool {
	got, err := hasher.Hash(xdr.TransactionEnvelopeView(env))
	return err == nil && got == outer
}

// innerHash reads the inner transaction's hash out of a fee-bump element's
// result, reporting feeBump false for any other result code — only a fee-bump
// result carries an inner pair.
func innerHash(elem []byte, extBytes int) ([32]byte, bool, error) {
	var (
		inner   [32]byte
		feeBump bool
	)
	pair := xdr.TransactionResultPairView(elem[extBytes:])
	if err := xdr.TryVoid(func() {
		res := pair.MustResult().MustResult()
		switch res.MustCode() {
		case xdr.TransactionResultCodeTxFeeBumpInnerSuccess,
			xdr.TransactionResultCodeTxFeeBumpInnerFailed:
			inner = [32]byte(res.MustInnerResultPair().MustTransactionHash().MustValue())
			feeBump = true
		default:
			// Every other code belongs to a transaction with no inner pair.
		}
	}); err != nil {
		return [32]byte{}, false, fmt.Errorf("its txProcessing result will not read: %w", err)
	}
	return inner, feeBump, nil
}
