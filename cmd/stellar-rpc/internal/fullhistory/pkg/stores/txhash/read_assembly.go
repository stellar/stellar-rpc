package txhash

// read_assembly.go turns a tx hash into the full transaction — the read half
// of the cold txhash store, though it is not cold-specific. It federates any
// mix of CandidateSources (hot + cold) and, for each candidate ledger, reads
// that ledger's close meta and extracts the transaction via the SDK's
// tx-details-by-hash view. That view also performs the final verification: it
// reports the hash as not present when the candidate ledger does not actually
// contain it, which is how residual MPHF false positives are rejected.

import (
	"errors"
	"fmt"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// ErrInconsistent is returned when an exact candidate source maps a tx hash to
// a ledger that does not actually contain it (or that the ledger source cannot
// produce). The hot store is exact, so this means its (txhash → ledger) index
// disagrees with the ledger store — data corruption or a bug, never a normal
// miss. Callers should surface it as an internal error, not a not-found.
var ErrInconsistent = errors.New("txhash: exact source disagrees with the ledger store")

// LedgerSource fetches a ledger's raw LedgerCloseMeta wire bytes by sequence.
//
// GetLedgerRaw must return bytes that remain valid after it returns (owned,
// not a reused scratch buffer): the LedgerTransactionView the assembly returns
// aliases them zero-copy. The hot and cold ledger stores' GetLedgerRaw both
// satisfy this; the serving layer supplies an implementation that routes a seq
// to the right tier.
//
// A candidate the source cannot serve — outside its coverage, or a sparse
// miss — should come back as stores.ErrOutOfRange or stores.ErrNotFound. The
// assembly treats those as "this candidate did not verify" and moves on,
// rather than failing the whole lookup, since an unserviceable candidate is
// usually just a fingerprint false positive pointing nowhere.
type LedgerSource interface {
	GetLedgerRaw(seq uint32) ([]byte, error)
}

// TxReader resolves a tx hash to its transaction by federating candidate
// sources and verifying each candidate against the real ledger. Sources are
// split by exactness at construction so the exact (authoritative) ones are
// always consulted before the fingerprinted ones, independent of the order
// they were passed in.
type TxReader struct {
	exact      []CandidateSource
	inexact    []CandidateSource
	ledgers    LedgerSource
	passphrase string
}

// NewTxReader builds a TxReader over the given candidate sources, ledger
// source, and network passphrase. It partitions the sources by Exact() so an
// exact hit (the hot store) short-circuits before any fingerprinted index is
// probed — callers need not order the sources themselves.
func NewTxReader(sources []CandidateSource, ledgers LedgerSource, passphrase string) *TxReader {
	r := &TxReader{ledgers: ledgers, passphrase: passphrase}
	for _, s := range sources {
		if s.Exact() {
			r.exact = append(r.exact, s)
		} else {
			r.inexact = append(r.inexact, s)
		}
	}
	return r
}

// GetTransaction resolves hash to its transaction detail. found is false (with
// a nil error) when the hash is in no ledger — either a genuine miss, or every
// inexact candidate was an MPHF false positive that verification rejected.
//
// It returns ErrInconsistent if an exact source maps the hash to a ledger that
// does not contain it (or that the ledger source cannot produce): an exact
// source is authoritative, so that disagreement is corruption, not a miss.
//
// The returned view aliases the bytes the LedgerSource produced for the
// matching ledger (zero-copy); callers copy what they retain.
func (r *TxReader) GetTransaction(hash [32]byte) (ingest.LedgerTransactionView, bool, error) {
	// Exact sources first: a hit short-circuits, and a non-match is an error
	// rather than a skipped false positive.
	if txv, found, err := r.scan(hash, r.exact, true); found || err != nil {
		return txv, found, err
	}
	return r.scan(hash, r.inexact, false)
}

// scan walks one tier of sources. For an exact tier, a candidate that fails to
// verify (or whose ledger is unavailable) yields ErrInconsistent; for an
// inexact tier it is a fingerprint false positive that is skipped. No
// cross-candidate dedup is needed: the sources cover disjoint ledger ranges,
// and an exact hit short-circuits before the inexact tier runs.
func (r *TxReader) scan(
	hash [32]byte, sources []CandidateSource, exact bool,
) (ingest.LedgerTransactionView, bool, error) {
	for _, src := range sources {
		seqs, err := src.Candidates(hash)
		if err != nil {
			return ingest.LedgerTransactionView{}, false, fmt.Errorf("txhash: candidate lookup: %w", err)
		}
		for _, seq := range seqs {
			raw, err := r.ledgers.GetLedgerRaw(seq)
			if err != nil {
				if errors.Is(err, stores.ErrNotFound) || errors.Is(err, stores.ErrOutOfRange) {
					if exact {
						return ingest.LedgerTransactionView{}, false,
							fmt.Errorf("txhash: exact source mapped tx to unavailable ledger %d: %w", seq, ErrInconsistent)
					}
					// Inexact candidate pointing nowhere — a false positive; skip.
					continue
				}
				return ingest.LedgerTransactionView{}, false, fmt.Errorf("txhash: read ledger %d: %w", seq, err)
			}

			txv, found, err := ingest.LedgerTransactionViewByHash(xdr.LedgerCloseMetaView(raw), hash, r.passphrase)
			if err != nil {
				return ingest.LedgerTransactionView{}, false, fmt.Errorf("txhash: extract tx from ledger %d: %w", seq, err)
			}
			if found {
				return txv, true, nil
			}
			if exact {
				return ingest.LedgerTransactionView{}, false,
					fmt.Errorf("txhash: exact source mapped tx to ledger %d that does not contain it: %w", seq, ErrInconsistent)
			}
			// Inexact: a fingerprint false positive. Keep scanning.
		}
	}
	return ingest.LedgerTransactionView{}, false, nil
}
