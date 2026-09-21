package store

import (
	"context"
	"errors"
	"iter"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// ErrEmptyDB is returned when the storage backend holds no ledgers yet.
var ErrEmptyDB = errors.New("DB is empty")

type StreamLedgerFn func(xdr.LedgerCloseMeta) error

// LedgerInfo identifies one ledger: its sequence number and close time.
type LedgerInfo struct {
	Sequence  uint32
	CloseTime int64
}

// LedgerRange is the span a backend can serve: its oldest and newest ledgers.
type LedgerRange struct {
	FirstLedger LedgerInfo
	LastLedger  LedgerInfo
}

func (lr LedgerRange) ToLedgerSeqRange() protocol.LedgerSeqRange {
	return protocol.LedgerSeqRange{
		FirstLedger: lr.FirstLedger.Sequence,
		LastLedger:  lr.LastLedger.Sequence,
	}
}

// LedgerReader is the serving-side read contract every storage backend
// implements. Handlers depend on this interface, never on a concrete backend.
type LedgerReader interface {
	// ScanLedgers reads whatever the request already sees (the live store on
	// v1 and the request's read view on v2), without its own snapshot.
	ScanLedgers(ctx context.Context, start, end uint32) iter.Seq2[RawLedger, error]
	GetLedgerRange(ctx context.Context) (LedgerRange, error)
	StreamLedgerRange(ctx context.Context, startLedger uint32, endLedger uint32, f StreamLedgerFn) error
	NewTx(ctx context.Context) (LedgerReaderTx, error)
	GetLatestLedgerSequence(ctx context.Context) (uint32, error)
}

// WithLedgerRawFn receives one ledger's marshaled LCM on loan and the bytes
// are valid only inside the call, read-only. Copy whatever outlives fn.
type WithLedgerRawFn func(raw []byte) error

// LedgerScanner is the contract's one read idiom; LedgerReader and LedgerReaderTx both satisfy it.
type LedgerScanner interface {
	ScanLedgers(ctx context.Context, start, end uint32) iter.Seq2[RawLedger, error]
}

// WithLedgerRaw lends one ledger's bytes under RawLedger's loan: a scan of one.
// found is false when the ledger is absent and fn never ran; fn's own error
// comes back verbatim with found true.
func WithLedgerRaw(ctx context.Context, s LedgerScanner, seq uint32, fn WithLedgerRawFn) (bool, error) {
	for l, err := range s.ScanLedgers(ctx, seq, seq) {
		if err != nil {
			return false, err
		}
		return true, fn(l.Raw)
	}
	return false, nil
}

// GetLedger decodes one ledger, or reports it absent.
func GetLedger(ctx context.Context, s LedgerScanner, seq uint32) (xdr.LedgerCloseMeta, bool, error) {
	var lcm xdr.LedgerCloseMeta
	found, err := WithLedgerRaw(ctx, s, seq, lcm.UnmarshalBinary)
	if err != nil {
		return xdr.LedgerCloseMeta{}, false, err
	}
	return lcm, found, nil
}

// ScanLedgersFrom is the inverse adapter: a scan over a per-sequence lookup, for
// a source that only has point reads. get reports an absent ledger as false.
func ScanLedgersFrom(
	start, end uint32, get func(seq uint32) (xdr.LedgerCloseMeta, bool, error),
) iter.Seq2[RawLedger, error] {
	return func(yield func(RawLedger, error) bool) {
		for seq := start; seq <= end; seq++ {
			lcm, found, err := get(seq)
			if err != nil {
				yield(RawLedger{}, err)
				return
			}
			if found {
				raw, err := lcm.MarshalBinary()
				if err != nil {
					yield(RawLedger{}, err)
					return
				}
				if !yield(RawLedger{Sequence: seq, Raw: raw}, nil) {
					return
				}
			}
			if seq == end { // seq++ would wrap at MaxUint32
				return
			}
		}
	}
}

// RawLedger is one ledger as ScanLedgers yields it. Raw is the read-only LCM
// bytes on loan, valid only inside the loop body that received it, overwritten
// by the next step. Copy whatever outlives the body.
type RawLedger struct {
	Sequence uint32
	Raw      []byte
}

// LedgerReaderTx is a read-only snapshot of the ledger store: GetLedgerRange
// and every ScanLedgers on it answer from the same committed state. Call Done
// to release it, however the loops ended.
type LedgerReaderTx interface {
	// ScanLedgers yields the stored ledgers in [start, end], ascending, as one
	// forward read that ends when the loop body breaks. Ledgers the snapshot
	// does not hold, below its oldest, above its latest, or missing inside the
	// range, are not yielded, so callers should check the sequences they receive.
	// A non-nil error ends the stream and the RawLedger beside it is zero.
	ScanLedgers(ctx context.Context, start, end uint32) iter.Seq2[RawLedger, error]
	// GetLedgerRange is the snapshot's oldest and latest ledger.
	GetLedgerRange(ctx context.Context) (LedgerRange, error)
	Done() error
}
