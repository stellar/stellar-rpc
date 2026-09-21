package methods

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcdatastore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

type ledgersHandler struct {
	ledgerReader          store.LedgerReader
	maxLimit              uint
	defaultLimit          uint
	datastoreLedgerReader rpcdatastore.LedgerReader
	logger                *log.Entry
}

// NewGetLedgersHandler returns a jrpc2.Handler for the getLedgers method.
func NewGetLedgersHandler(ledgerReader store.LedgerReader, maxLimit, defaultLimit uint,
	datastoreLedgerReader rpcdatastore.LedgerReader, logger *log.Entry,
) jrpc2.Handler {
	return NewHandler((&ledgersHandler{
		ledgerReader:          ledgerReader,
		maxLimit:              maxLimit,
		defaultLimit:          defaultLimit,
		datastoreLedgerReader: datastoreLedgerReader,
		logger:                logger,
	}).getLedgers)
}

// getLedgers fetch ledgers and relevant metadata from DB and falling back to
// the remote rpcdatastore if necessary.
func (h ledgersHandler) getLedgers(
	ctx context.Context, request protocol.GetLedgersRequest,
) (protocol.GetLedgersResponse, error) {
	readTx, err := h.ledgerReader.NewTx(ctx)
	if err != nil {
		return protocol.GetLedgersResponse{}, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: err.Error(),
		}
	}
	defer func() {
		_ = readTx.Done()
	}()

	ledgerRange, err := readTx.GetLedgerRange(ctx)
	switch {
	case errors.Is(err, store.ErrEmptyDB):
		// TODO: Support datastore-only mode (no local DB).
		fallthrough
	case err != nil:
		return protocol.GetLedgersResponse{}, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: err.Error(),
		}
	}
	availableLedgerRange := ledgerRange.ToLedgerSeqRange()

	if h.datastoreLedgerReader != nil {
		dsRange, err := h.datastoreLedgerReader.GetAvailableLedgerRange(ctx)
		if err != nil {
			// log error but continue using local ledger range
			h.logger.WithError(err).Error("failed to get available ledger range from datastore")
		} else {
			// extend available range to include datastore
			availableLedgerRange.FirstLedger = min(dsRange.FirstLedger, availableLedgerRange.FirstLedger)
		}
	}

	if err := request.Validate(h.maxLimit, availableLedgerRange); err != nil {
		return protocol.GetLedgersResponse{}, &jrpc2.Error{
			Code:    jrpc2.InvalidRequest,
			Message: err.Error(),
		}
	}

	start, limit, err := h.initializePagination(request, availableLedgerRange)
	if err != nil {
		return protocol.GetLedgersResponse{}, &jrpc2.Error{
			Code:    jrpc2.InvalidParams,
			Message: err.Error(),
		}
	}

	// A caught-up poller's cursor points at or past the tip. Echo it back on
	// an empty page instead of rejecting the server's own token. An explicit
	// startLedger above the tip stays an error (Validate above rejects it).
	if request.Pagination != nil && request.Pagination.Cursor != "" &&
		start > availableLedgerRange.LastLedger {
		return protocol.GetLedgersResponse{
			Ledgers:               []protocol.LedgerInfo{},
			LatestLedger:          ledgerRange.LastLedger.Sequence,
			LatestLedgerCloseTime: ledgerRange.LastLedger.CloseTime,
			OldestLedger:          ledgerRange.FirstLedger.Sequence,
			OldestLedgerCloseTime: ledgerRange.FirstLedger.CloseTime,
			Cursor:                request.Pagination.Cursor,
		}, nil
	}

	end := start + uint32(limit) - 1 //nolint:gosec
	ledgers, err := h.fetchLedgers(ctx, start, end, request.Format, readTx, ledgerRange.ToLedgerSeqRange())
	if err != nil {
		return protocol.GetLedgersResponse{}, err
	}

	var cursor string
	if len(ledgers) > 0 {
		cursor = strconv.FormatUint(uint64(ledgers[len(ledgers)-1].Sequence), 10)
	} else {
		if request.Pagination != nil && request.Pagination.Cursor != "" {
			cursor = request.Pagination.Cursor
		} else { // start > 0 by validation
			cursor = strconv.FormatUint(uint64(start-1), 10)
		}
	}

	return protocol.GetLedgersResponse{
		Ledgers: ledgers,
		//	TODO: update these fields using ledger range from datastore
		LatestLedger:          ledgerRange.LastLedger.Sequence,
		LatestLedgerCloseTime: ledgerRange.LastLedger.CloseTime,
		OldestLedger:          ledgerRange.FirstLedger.Sequence,
		OldestLedgerCloseTime: ledgerRange.FirstLedger.CloseTime,
		Cursor:                cursor,
	}, nil
}

// initializePagination parses the request pagination details and initializes the cursor.
func (h ledgersHandler) initializePagination(request protocol.GetLedgersRequest,
	ledgerRange protocol.LedgerSeqRange,
) (uint32, uint, error) {
	if request.Pagination == nil {
		return request.StartLedger, h.defaultLimit, nil
	}

	start := request.StartLedger
	var err error
	if request.Pagination.Cursor != "" {
		start, err = h.parseCursor(request.Pagination.Cursor, ledgerRange)
		if err != nil {
			return 0, 0, err
		}
	}

	limit := request.Pagination.Limit
	if limit <= 0 {
		limit = h.defaultLimit
	}
	return start, limit, nil
}

func (h ledgersHandler) parseCursor(cursor string, ledgerRange protocol.LedgerSeqRange) (uint32, error) {
	cursorInt, err := strconv.ParseUint(cursor, 10, 32)
	if err != nil {
		return 0, err
	}

	// Only the lower bound is an error: below the oldest ledger is data the
	// node no longer has. At or past the tip is a caught-up poller, answered
	// with an empty page by getLedgers. The +1 wraps a max-uint32 cursor to
	// start 0, which this check also catches.
	start := uint32(cursorInt) + 1
	if start < ledgerRange.FirstLedger {
		return 0, fmt.Errorf(
			"cursor ('%s') must be at or above the oldest ledger: %d for this rpc instance",
			cursor,
			ledgerRange.FirstLedger,
		)
	}

	return start, nil
}

// fetchLedgers retrieves a batch of ledgers in the range [start, start+limit-1]
// using the local DB when available, and falling back to the remote datastore
// for any portion of the range that lies outside the locally available range.
//
// It handles three cases:
//  1. Entire range is available in local db.
//  2. Entire range is unavailable in local db so fetch fully from datastore.
//  3. Range partially available in the local db with the rest fetched from the datastore.
func (h ledgersHandler) fetchLedgers(
	ctx context.Context,
	start, end uint32, format string,
	readTx store.LedgerReaderTx,
	localLedgerRange protocol.LedgerSeqRange,
) ([]protocol.LedgerInfo, error) {
	limit := end - start + 1
	result := make([]protocol.LedgerInfo, 0, limit)

	// appendLedger renders one ledger's raw LedgerCloseMeta into the page
	appendLedger := func(seq uint32, raw []byte) error {
		info, err := parseLedgerInfo(raw, format)
		if err != nil {
			return &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: fmt.Sprintf("error processing ledger %d: %v", seq, err),
			}
		}
		result = append(result, info)
		return nil
	}

	fetchFromLocalDB := func(start, end uint32) error {
		page, err := collectLedgerPage(ctx, readTx, start, end, int(limit)-len(result))
		if err != nil {
			return err
		}
		for _, l := range page {
			if aerr := appendLedger(l.Sequence, l.Raw); aerr != nil {
				return aerr
			}
		}
		return nil
	}

	fetchFromDatastore := func(start, end uint32) error {
		if h.datastoreLedgerReader == nil {
			return &jrpc2.Error{
				Code:    jrpc2.InvalidParams,
				Message: "datastore ledger reader not configured",
			}
		}
		ledgers, err := h.datastoreLedgerReader.GetLedgers(ctx, start, end)
		if err != nil {
			return &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: fmt.Sprintf("error fetching ledgers from datastore: %v", err),
			}
		}
		// Serialize lazily so a short page never marshals the ledgers past it.
		for i := range ledgers {
			if len(result) >= int(limit) {
				break
			}
			raw, merr := ledgers[i].MarshalBinary()
			if merr != nil {
				return &jrpc2.Error{
					Code:    jrpc2.InternalError,
					Message: fmt.Sprintf("error serializing ledgers: %v", merr),
				}
			}
			if aerr := appendLedger(ledgers[i].LedgerSequence(), raw); aerr != nil {
				return aerr
			}
		}
		return nil
	}

	var err error
	switch {
	// entire range is available in local DB
	case start >= localLedgerRange.FirstLedger:
		err = fetchFromLocalDB(start, end)

	// entire range is unavailable locally so fetch everything from datastore
	case end < localLedgerRange.FirstLedger:
		err = fetchFromDatastore(start, end)

	// part of the ledger range is available locally so fetch local ledgers from
	// db, and the rest from the datastore.
	default:
		err = errors.Join(
			fetchFromDatastore(start, localLedgerRange.FirstLedger-1),
			fetchFromLocalDB(localLedgerRange.FirstLedger, end))
	}

	return result, err
}

// collectLedgerPage copies up to room ledgers of [start, end] out of the scan, so
// rendering runs after the scan's readers close (encoding a page under an open
// hot-store iterator measurably slows it).
func collectLedgerPage(
	ctx context.Context, readTx store.LedgerReaderTx, start, end uint32, room int,
) ([]store.RawLedger, error) {
	page := make([]store.RawLedger, 0, max(room, 0))
	for ledger, err := range readTx.ScanLedgers(ctx, start, end) {
		if err != nil {
			return nil, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: fmt.Sprintf("error fetching ledgers from db: %v", err),
			}
		}
		if len(page) >= room {
			break
		}
		page = append(page, store.RawLedger{Sequence: ledger.Sequence, Raw: bytes.Clone(ledger.Raw)})
	}
	return page, nil
}

// parseLedgerInfo extracts and formats the ledger metadata and header
// information. In the error case, it returns a jrcp2.Error.
func parseLedgerInfo(raw []byte, format string) (protocol.LedgerInfo, error) {
	view := xdr.LedgerCloseMetaView(raw)
	sequence, err := view.LedgerSequence()
	if err != nil {
		return protocol.LedgerInfo{}, err
	}
	closeTime, err := view.LedgerCloseTime()
	if err != nil {
		return protocol.LedgerInfo{}, err
	}
	hash, err := view.LedgerHash()
	if err != nil {
		return protocol.LedgerInfo{}, err
	}
	// The header is a slice of raw, not a decode: the wire format base64s it as-is.
	headerView, err := view.LedgerHeader()
	if err != nil {
		return protocol.LedgerInfo{}, err
	}
	headerRaw, err := headerView.Raw()
	if err != nil {
		return protocol.LedgerInfo{}, err
	}
	var hashXdr xdr.Hash
	copy(hashXdr[:], hash)
	ledgerInfo := protocol.LedgerInfo{
		Hash:            hashXdr.HexString(),
		Sequence:        sequence,
		LedgerCloseTime: closeTime,
	}

	// Format the data according to the requested format (JSON or XDR)
	switch format {
	case protocol.FormatJSON:
		var convErr error
		ledgerInfo.LedgerMetadataJSON, ledgerInfo.LedgerHeaderJSON, convErr = ledgerToJSON(raw, headerRaw)
		if convErr != nil {
			return ledgerInfo, convErr
		}

	default:
		ledgerInfo.LedgerMetadata = base64.StdEncoding.EncodeToString(raw)
		ledgerInfo.LedgerHeader = base64.StdEncoding.EncodeToString(headerRaw)
	}
	return ledgerInfo, nil
}
