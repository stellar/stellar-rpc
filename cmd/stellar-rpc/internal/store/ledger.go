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
	GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error)
	WithLedgerRaw(ctx context.Context, sequence uint32, fn WithLedgerRawFn) (found bool, err error)
	GetLedgerRange(ctx context.Context) (LedgerRange, error)
	StreamLedgerRange(ctx context.Context, startLedger uint32, endLedger uint32, f StreamLedgerFn) error
	NewTx(ctx context.Context) (LedgerReaderTx, error)
	GetLatestLedgerSequence(ctx context.Context) (uint32, error)
}

// WithLedgerRawFn receives one ledger's marshaled LCM on loan and the bytes
// are valid only inside the call, read-only. Copy whatever outlives fn.
type WithLedgerRawFn func(raw []byte) error

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

// LedgerMetadataChunk is one ledger as getLedgers serves it: the marshaled
// LedgerCloseMeta plus the marshaled LedgerHeaderHistoryEntry sliced out of
// it. Both stay raw bytes because the XDR wire format base64s them as-is.
type LedgerMetadataChunk struct {
	HeaderRaw []byte
	Lcm       []byte
}
