package store

import (
	"context"
	"errors"

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
	GetLedgerRange(ctx context.Context) (LedgerRange, error)
	StreamLedgerRange(ctx context.Context, startLedger uint32, endLedger uint32, f StreamLedgerFn) error
	NewTx(ctx context.Context) (LedgerReaderTx, error)
	GetLatestLedgerSequence(ctx context.Context) (uint32, error)
}

// LedgerReaderTx is a read-only snapshot of the ledger store. Call Done to
// release it.
//
// GetLedger and WithLedgerRaw are one walk, not free-form point reads. Call
// them with ascending, contiguous sequences, starting from the first call's
// sequence, and use only one of the two per Tx: they share a single cursor, so
// interleaving them consumes positions from each other. Read at most
// methods.LedgerScanLimit ledgers per Tx. This is getTransactions' access
// pattern. The v1 (SQL) backend accepts any pattern; the v2 backend serves
// the walk from a forward iterator primed on the first call and fails loudly
// on anything else.
//
// WithLedgerRaw is GetLedger without the decode: it LENDS the ledger's
// marshaled LedgerCloseMeta to fn instead of returning a decoded
// xdr.LedgerCloseMeta, so a caller that only needs some fields never pays to
// unmarshal the whole multi-MB value. found reports presence exactly as
// GetLedger does; when it is false fn does not run. fn's own error is
// returned verbatim (found is true), so a caller can tell "the ledger was not
// there" from "your callback failed".
//
// The loan: raw is valid only for the duration of the call to fn, is
// read-only, and must not be retained. Neither backend promises the bytes
// survive the call — the v2 backend lends its chunk reader's scratch buffer,
// which the next step of the walk overwrites. Copy whatever outlives fn.
type LedgerReaderTx interface {
	GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error)
	WithLedgerRaw(ctx context.Context, sequence uint32, fn WithLedgerRawFn) (bool, error)
	GetLedgerRange(ctx context.Context) (LedgerRange, error)
	BatchGetLedgers(ctx context.Context, start uint32, end uint32) ([]LedgerMetadataChunk, error)
	Done() error
}

// WithLedgerRawFn receives one ledger's marshaled LedgerCloseMeta on loan; see
// LedgerReaderTx.WithLedgerRaw for the loan's terms.
type WithLedgerRawFn func(raw []byte) error

// LedgerMetadataChunk is one ledger as getLedgers serves it: the marshaled
// LedgerCloseMeta plus the marshaled LedgerHeaderHistoryEntry sliced out of
// it. Both stay raw bytes because the XDR wire format base64s them as-is.
type LedgerMetadataChunk struct {
	HeaderRaw []byte
	Lcm       []byte
}
