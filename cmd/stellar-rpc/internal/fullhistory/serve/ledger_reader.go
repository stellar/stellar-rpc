package serve

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerbucketwindow"
)

// errPOCUnsupported is returned by the read-path methods none of the four query
// handlers reach in the POC (the daemon serves only getLedgers / getTransaction
// / getTransactions).
var errPOCUnsupported = errors.New("not supported by full-history POC read path")

// ledgerReader adapts the serve Registry to the db.LedgerReader interface the v1
// query handlers consume, so methods.NewGetLedgersHandler and
// methods.NewGetTransactionsHandler run verbatim over full-history storage.
type ledgerReader struct {
	reg    *Registry
	layout geometry.Layout
}

// NewLedgerReader returns a db.LedgerReader backed by the serve Registry.
func NewLedgerReader(reg *Registry, layout geometry.Layout) db.LedgerReader {
	return ledgerReader{reg: reg, layout: layout}
}

func (r ledgerReader) NewTx(_ context.Context) (db.LedgerReaderTx, error) {
	// Admit exactly once and pin (latest, view) for the whole tx (design
	// §Admission) — every method below reads that one immutable snapshot.
	latest, v := r.reg.Admit()
	return &readTx{
		view:   v,
		layout: r.layout,
		latest: latest,
		first:  max(v.Floor.FirstLedger(), v.Earliest),
	}, nil
}

// GetLedger and GetLedgerRange are cheap one-shot transactions (the single-
// transaction getTransaction path calls GetLedgerRange on the reader directly).
func (r ledgerReader) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	tx, err := r.NewTx(ctx)
	if err != nil {
		return xdr.LedgerCloseMeta{}, false, err
	}
	defer func() { _ = tx.Done() }()
	return tx.GetLedger(ctx, sequence)
}

func (r ledgerReader) GetLedgerRange(ctx context.Context) (ledgerbucketwindow.LedgerRange, error) {
	tx, err := r.NewTx(ctx)
	if err != nil {
		return ledgerbucketwindow.LedgerRange{}, err
	}
	defer func() { _ = tx.Done() }()
	return tx.GetLedgerRange(ctx)
}

func (r ledgerReader) GetLatestLedgerSequence(_ context.Context) (uint32, error) {
	latest, _ := r.reg.Admit()
	if latest == 0 {
		return 0, db.ErrEmptyDB
	}
	return latest, nil
}

// POC: the daemon's read path never streams or counts ledger ranges.
func (r ledgerReader) StreamAllLedgers(_ context.Context, _ db.StreamLedgerFn) error {
	return errPOCUnsupported
}

// POC: unused by the four query handlers.
func (r ledgerReader) GetLedgerCountInRange(_ context.Context, _, _ uint32) (uint32, uint32, uint32, error) {
	return 0, 0, 0, errPOCUnsupported
}

// POC: unused by the four query handlers.
func (r ledgerReader) StreamLedgerRange(_ context.Context, _, _ uint32, _ db.StreamLedgerFn) error {
	return errPOCUnsupported
}

// readTx is one query's pinned snapshot: a fixed (latest, view) pair and the
// derived servable floor. Done() is a no-op release — nothing is held open
// between calls (per-chunk cold readers are opened and closed within each call).
type readTx struct {
	view   *View
	layout geometry.Layout
	latest uint32
	first  uint32
}

func (tx *readTx) Done() error { return nil }

func (tx *readTx) GetLedger(_ context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	if sequence < tx.first || sequence > tx.latest {
		return xdr.LedgerCloseMeta{}, false, nil
	}
	raw, err := tx.getRaw(sequence)
	switch {
	case errors.Is(err, stores.ErrNotFound), errors.Is(err, stores.ErrOutOfRange):
		return xdr.LedgerCloseMeta{}, false, nil
	case err != nil:
		return xdr.LedgerCloseMeta{}, false, err
	}
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(raw); err != nil {
		return xdr.LedgerCloseMeta{}, false, err
	}
	return lcm, true, nil
}

func (tx *readTx) GetLedgerRange(_ context.Context) (ledgerbucketwindow.LedgerRange, error) {
	// Not-yet-servable watermark: 0 before the first View is built, and the
	// FirstLedgerSeq-1 sentinel that startup seeds ("nothing committed yet")
	// until the first ledger commits. Guard both — a sub-genesis latest would
	// panic chunk.IDFromLedger in closeTime below.
	if tx.latest < chunk.FirstLedgerSeq {
		return ledgerbucketwindow.LedgerRange{}, db.ErrEmptyDB
	}
	firstCloseTime, err := tx.closeTime(tx.first)
	if err != nil {
		return ledgerbucketwindow.LedgerRange{}, err
	}
	lastCloseTime, err := tx.closeTime(tx.latest)
	if err != nil {
		return ledgerbucketwindow.LedgerRange{}, err
	}
	return ledgerbucketwindow.LedgerRange{
		FirstLedger: ledgerbucketwindow.LedgerInfo{Sequence: tx.first, CloseTime: firstCloseTime},
		LastLedger:  ledgerbucketwindow.LedgerInfo{Sequence: tx.latest, CloseTime: lastCloseTime},
	}, nil
}

func (tx *readTx) BatchGetLedgers(_ context.Context, start, end uint32) ([]db.LedgerMetadataChunk, error) {
	if end > tx.latest {
		end = tx.latest
	}
	if start < tx.first {
		return nil, fmt.Errorf("%w: batch start %d below retention floor %d", stores.ErrOutOfRange, start, tx.first)
	}
	if start > end {
		return nil, nil
	}

	out := make([]db.LedgerMetadataChunk, 0, end-start+1)
	for c := chunk.IDFromLedger(start); c <= chunk.IDFromLedger(end); c++ {
		lo := max(start, c.FirstLedger())
		hi := min(end, c.LastLedger())
		if err := tx.appendChunk(c, lo, hi, &out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// appendChunk resolves chunk c, appends its ledgers in [lo, hi], and closes the
// handle before returning. Each borrowed iterator buffer is copied so the
// retained Lcm outlives the iteration step.
func (tx *readTx) appendChunk(c chunk.ID, lo, hi uint32, out *[]db.LedgerMetadataChunk) error {
	lc, err := tx.view.ResolveLedgers(c, tx.layout)
	if err != nil {
		return err
	}
	defer func() { _ = lc.Close() }()

	for e, err := range lc.Iterate(lo, hi) {
		if err != nil {
			return err
		}
		raw := bytes.Clone(e.Bytes)
		header, err := decodeLedgerHeader(raw)
		if err != nil {
			return err
		}
		*out = append(*out, db.LedgerMetadataChunk{Header: header, Lcm: raw})
	}
	return nil
}

// getRaw resolves seq's chunk, reads its raw ledger bytes, and closes the handle.
func (tx *readTx) getRaw(seq uint32) ([]byte, error) {
	lc, err := tx.view.ResolveLedgers(chunk.IDFromLedger(seq), tx.layout)
	if err != nil {
		return nil, err
	}
	defer func() { _ = lc.Close() }()
	return lc.Get(seq)
}

// closeTime decodes just the close time of the ledger at seq via the zero-copy
// LedgerCloseMetaView (no full-body XDR decode).
func (tx *readTx) closeTime(seq uint32) (int64, error) {
	raw, err := tx.getRaw(seq)
	if err != nil {
		return 0, err
	}
	return xdr.LedgerCloseMetaView(raw).LedgerCloseTime()
}

// decodeLedgerHeader extracts the concrete LedgerHeaderHistoryEntry from raw
// LedgerCloseMeta bytes without decoding the (potentially large) transaction
// body — the same header-only skip v1's db.BatchGetLedgers uses.
func decodeLedgerHeader(raw []byte) (xdr.LedgerHeaderHistoryEntry, error) {
	var header xdr.LedgerHeaderHistoryEntry
	var version xdr.Int32
	rd := bytes.NewReader(raw)
	if _, err := xdr.Unmarshal(rd, &version); err != nil {
		return header, err
	}
	if version > 0 { // V0 has no extension
		var ext xdr.LedgerCloseMetaExt
		if _, err := xdr.Unmarshal(rd, &ext); err != nil {
			return header, err
		}
	}
	if _, err := xdr.Unmarshal(rd, &header); err != nil {
		return header, err
	}
	return header, nil
}
