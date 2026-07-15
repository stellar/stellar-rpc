// Package serve implements the v1 db reader interfaces (db.LedgerReader,
// db.TransactionReader) as thin veneers over the full-history registry, so
// the existing v1 JSON-RPC handlers (getLedgers, getTransactions,
// getTransaction, getLatestLedger, getNetwork, getVersionInfo, getHealth) run
// unchanged against the v2 chunked stores.
//
// Every request runs against exactly one admitted registry Snapshot
// {latest, View}: db.LedgerReader.NewTx pins one admission for a whole
// getLedgers/getTransactions request (the v1 handlers' read-transaction
// pattern maps directly onto the spec's per-request admission), and the
// point-read methods admit per call. Reads gate against the admitted
// [View.FloorLedger(), latest]: sequences below the floor read as not-found
// (R2), and sequences past latest read as not-found too, so a response never
// exposes a ledger its own watermark has not reached.
package serve

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/registry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerbucketwindow"
)

// LedgerReader serves the db.LedgerReader interface from the registry. The
// registry is per-run (it dies with the daemon run that built it), so a
// LedgerReader must not outlive the ServeReads scope that received the
// registry.
type LedgerReader struct {
	reg *registry.Registry
}

var _ db.LedgerReader = (*LedgerReader)(nil)

func NewLedgerReader(reg *registry.Registry) *LedgerReader {
	return &LedgerReader{reg: reg}
}

// admission is one admitted Snapshot plus the registry that resolves chunk
// stores under it. Every read helper below runs against the one Snapshot it
// was built with, which is what gives a multi-read request (getLedgers,
// getTransactions) a consistent view of history.
type admission struct {
	reg  *registry.Registry
	snap registry.Snapshot
}

func (l *LedgerReader) admit() admission {
	return admission{reg: l.reg, snap: l.reg.Admit()}
}

func (l *LedgerReader) GetLedger(_ context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	return l.admit().getLedger(sequence)
}

func (l *LedgerReader) GetLedgerRange(_ context.Context) (ledgerbucketwindow.LedgerRange, error) {
	return l.admit().ledgerRange()
}

func (l *LedgerReader) GetLatestLedgerSequence(_ context.Context) (uint32, error) {
	snap := l.reg.Admit()
	if snap.Latest == 0 {
		return 0, db.ErrEmptyDB
	}
	return snap.Latest, nil
}

// NewTx is the admission point for range requests: the returned
// db.LedgerReaderTx runs every read against the one Snapshot admitted here.
// There is no underlying transaction to release — Done is a no-op — but
// callers keep the v1 contract (always call Done) so the two backends stay
// interchangeable.
func (l *LedgerReader) NewTx(_ context.Context) (db.LedgerReaderTx, error) {
	return ledgerReaderTx{a: l.admit()}, nil
}

// StreamAllLedgers is a v1 ingest/migration helper; no served endpoint calls
// it.
func (l *LedgerReader) StreamAllLedgers(_ context.Context, _ db.StreamLedgerFn) error {
	return errors.New("StreamAllLedgers is not supported by the full-history backend")
}

// StreamLedgerRange is a v1 ingest/migration helper; no served endpoint
// calls it.
func (l *LedgerReader) StreamLedgerRange(_ context.Context, _, _ uint32, _ db.StreamLedgerFn) error {
	return errors.New("StreamLedgerRange is not supported by the full-history backend")
}

// GetLedgerCountInRange is a v1 migration helper; no served endpoint calls
// it.
func (l *LedgerReader) GetLedgerCountInRange(_ context.Context, _, _ uint32) (uint32, uint32, uint32, error) {
	return 0, 0, 0, errors.New("GetLedgerCountInRange is not supported by the full-history backend")
}

type ledgerReaderTx struct {
	a admission
}

var _ db.LedgerReaderTx = ledgerReaderTx{}

func (t ledgerReaderTx) GetLedger(_ context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	return t.a.getLedger(sequence)
}

func (t ledgerReaderTx) GetLedgerRange(_ context.Context) (ledgerbucketwindow.LedgerRange, error) {
	return t.a.ledgerRange()
}

// BatchGetLedgers walks [start, end] chunk by chunk in ascending order and
// concatenates the results. Like v1's SQL BETWEEN, the range is clamped
// silently: the portion below the admitted floor or above latest simply
// returns no rows (the out-of-range rejection is the handler's job, driven by
// GetLedgerRange).
func (t ledgerReaderTx) BatchGetLedgers(ctx context.Context, start, end uint32) ([]db.LedgerMetadataChunk, error) {
	if start > end {
		return nil, errors.New("batch size must be greater than zero")
	}
	lo := max(start, t.a.snap.View.FloorLedger())
	hi := min(end, t.a.snap.Latest)
	if lo > hi {
		return []db.LedgerMetadataChunk{}, nil
	}

	batch := make([]db.LedgerMetadataChunk, 0, hi-lo+1)
	for c := chunk.IDFromLedger(lo); ; c++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		chunkLo := max(lo, c.FirstLedger())
		chunkHi := min(hi, c.LastLedger())
		h, err := t.a.reg.LedgerReaderFor(t.a.snap.View, c)
		if err != nil {
			return nil, fmt.Errorf("resolve ledger store for chunk %s: %w", c, err)
		}
		for e, ierr := range h.IterateLedgers(chunkLo, chunkHi) {
			if ierr != nil {
				return nil, fmt.Errorf("iterate ledgers [%d, %d] of chunk %s: %w", chunkLo, chunkHi, c, ierr)
			}
			// e.Bytes is borrowed from the store iterator (valid only until
			// the next iteration step) — clone before retaining.
			item := db.LedgerMetadataChunk{Lcm: slices.Clone(e.Bytes)}
			if derr := decodeLedgerHeader(item.Lcm, &item.Header); derr != nil {
				return nil, fmt.Errorf("decode header of ledger %d: %w", e.Seq, derr)
			}
			batch = append(batch, item)
		}
		if chunkHi == hi {
			return batch, nil
		}
	}
}

func (t ledgerReaderTx) Done() error {
	return nil
}

// getLedgerRaw is the one routing primitive every ledger read uses: gate seq
// against the admitted [floor, latest], resolve its chunk's ledger store
// (cold wins over hot), and read the raw LedgerCloseMeta XDR. Both store
// tiers decompress internally and return fresh caller-owned buffers, so the
// bytes are directly servable.
//
// found=false only for sequences outside the admitted bounds. Inside them
// every ledger has a serving home by the View invariant, so a store-level
// miss there surfaces as an error (possible data loss), never as a silent
// not-found.
func (a admission) getLedgerRaw(seq uint32) ([]byte, bool, error) {
	if seq < a.snap.View.FloorLedger() || seq > a.snap.Latest {
		return nil, false, nil
	}
	h, err := a.reg.LedgerReaderFor(a.snap.View, chunk.IDFromLedger(seq))
	if err != nil {
		return nil, false, fmt.Errorf("resolve ledger store for seq %d: %w", seq, err)
	}
	raw, err := h.GetLedgerRaw(seq)
	if err != nil {
		return nil, false, fmt.Errorf("read ledger %d: %w", seq, err)
	}
	return raw, true, nil
}

func (a admission) getLedger(seq uint32) (xdr.LedgerCloseMeta, bool, error) {
	raw, found, err := a.getLedgerRaw(seq)
	if err != nil || !found {
		return xdr.LedgerCloseMeta{}, false, err
	}
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(raw); err != nil {
		return xdr.LedgerCloseMeta{}, false, fmt.Errorf("decode ledger %d: %w", seq, err)
	}
	return lcm, true, nil
}

// ledgerRange reports [floor, latest] with each end's close time fetched
// through the router (two point reads; the cold one rides the LRU cache).
func (a admission) ledgerRange() (ledgerbucketwindow.LedgerRange, error) {
	first := a.snap.View.FloorLedger()
	if a.snap.Latest == 0 || first > a.snap.Latest {
		return ledgerbucketwindow.LedgerRange{}, db.ErrEmptyDB
	}
	firstInfo, err := a.ledgerInfo(first)
	if err != nil {
		return ledgerbucketwindow.LedgerRange{}, err
	}
	lastInfo, err := a.ledgerInfo(a.snap.Latest)
	if err != nil {
		return ledgerbucketwindow.LedgerRange{}, err
	}
	return ledgerbucketwindow.LedgerRange{FirstLedger: firstInfo, LastLedger: lastInfo}, nil
}

func (a admission) ledgerInfo(seq uint32) (ledgerbucketwindow.LedgerInfo, error) {
	raw, found, err := a.getLedgerRaw(seq)
	if err != nil {
		return ledgerbucketwindow.LedgerInfo{}, err
	}
	if !found {
		return ledgerbucketwindow.LedgerInfo{}, fmt.Errorf(
			"ledger %d inside the admitted range [%d, %d] has no serving store",
			seq, a.snap.View.FloorLedger(), a.snap.Latest)
	}
	closeTime, err := xdr.LedgerCloseMetaView(raw).LedgerCloseTime()
	if err != nil {
		return ledgerbucketwindow.LedgerInfo{}, fmt.Errorf("decode close time of ledger %d: %w", seq, err)
	}
	return ledgerbucketwindow.LedgerInfo{Sequence: seq, CloseTime: closeTime}, nil
}

// decodeLedgerHeader extracts the LedgerHeaderHistoryEntry from raw
// LedgerCloseMeta bytes the way v1's BatchGetLedgers does: read the version
// discriminant, skip the V1+ ext field, then decode the header — a partial
// decode instead of unmarshaling the whole (potentially large) meta.
func decodeLedgerHeader(raw []byte, out *xdr.LedgerHeaderHistoryEntry) error {
	var v xdr.Int32
	rd := bytes.NewReader(raw)
	if _, err := xdr.Unmarshal(rd, &v); err != nil {
		return err
	}
	if v > 0 { // V0 has no extension
		var ext xdr.LedgerCloseMetaExt
		if _, err := xdr.Unmarshal(rd, &ext); err != nil { // skipped
			return err
		}
	}
	_, err := xdr.Unmarshal(rd, out)
	return err
}
