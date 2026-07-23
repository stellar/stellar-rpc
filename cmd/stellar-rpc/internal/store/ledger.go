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
type LedgerReaderTx interface {
	GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error)
	GetLedgerRange(ctx context.Context) (LedgerRange, error)
	BatchGetLedgers(ctx context.Context, start uint32, end uint32) ([]LedgerMetadataChunk, error)
	Done() error
}

type LedgerMetadataChunk struct {
	Header xdr.LedgerHeaderHistoryEntry
	Lcm    []byte
}
