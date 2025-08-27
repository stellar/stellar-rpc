package methods

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"

	"github.com/creachadair/jrpc2"
	"github.com/stellar/go/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcdatastore"
	"github.com/stellar/stellar-rpc/protocol"
)

type ledgersHandler struct {
	ledgerReader          db.LedgerReader
	maxLimit              uint
	defaultLimit          uint
	datastoreLedgerReader rpcdatastore.LedgerReader
	logger                *log.Entry
}

// NewGetLedgersHandler returns a jrpc2.Handler for the getLedgers method.
func NewGetLedgersHandler(ledgerReader db.LedgerReader, maxLimit, defaultLimit uint,
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
	case errors.Is(err, db.ErrEmptyDB):
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

	end := start + uint32(limit) - 1 //nolint:gosec
	ledgers, err := h.fetchLedgers(ctx, start, end, request.Format, readTx, ledgerRange.ToLedgerSeqRange())
	if err != nil {
		return protocol.GetLedgersResponse{}, err
	}
	cursor := strconv.Itoa(int(ledgers[len(ledgers)-1].Sequence))

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

	start := uint32(cursorInt) + 1
	if !protocol.IsLedgerWithinRange(start, ledgerRange) {
		return 0, fmt.Errorf(
			"cursor ('%s') must be between the oldest ledger: %d and the latest ledger: %d for this rpc instance",
			cursor,
			ledgerRange.FirstLedger,
			ledgerRange.LastLedger,
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
	readTx db.LedgerReaderTx,
	localLedgerRange protocol.LedgerSeqRange,
) ([]protocol.LedgerInfo, error) {
	limit := end - start + 1
	result := make([]protocol.LedgerInfo, 0, limit)

	addToResult := func(ledgers []db.LedgerMetadataChunk) error {
		for _, chunk := range ledgers {
			if len(result) >= int(limit) {
				break
			}

			if info, err := parseLedgerInfo(chunk, format); err != nil {
				return &jrpc2.Error{
					Code: jrpc2.InternalError,
					Message: fmt.Sprintf("error processing ledger %d: %v",
						chunk.Header.Header.LedgerSeq, err),
				}
			} else {
				result = append(result, info)
			}
		}
		return nil
	}

	fetchFromLocalDB := func(start, end uint32) error {
		ledgers, err := readTx.BatchGetLedgers(ctx, start, end)
		if err != nil {
			return &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: fmt.Sprintf("error fetching ledgers from db: %v", err),
			}
		}

		return addToResult(ledgers)
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

		for _, ledger := range ledgers {
			raw, err := ledger.MarshalBinary()
			if err != nil {
				return &jrpc2.Error{
					Code:    jrpc2.InternalError,
					Message: fmt.Sprintf("error fetching ledgers from datastore: %v", err),
				}
			}

			if err := addToResult([]db.LedgerMetadataChunk{
				{Lcm: raw, Header: ledger.LedgerHeaderHistoryEntry()},
			}); err != nil {
				return err
			}
		}
		return nil
	}

	switch {
	case start >= localLedgerRange.FirstLedger:
		// entire range is available in local DB
		if err := fetchFromLocalDB(start, end); err != nil {
			return nil, err
		}

	case end < localLedgerRange.FirstLedger:
		// entire range is unavailable locally so fetch everything from datastore
		if err := fetchFromDatastore(start, end); err != nil {
			return nil, err
		}

	default:
		// part of the ledger range is available locally so fetch local ledgers from db,
		// and the rest from the datastore.
		if err := fetchFromDatastore(start, localLedgerRange.FirstLedger-1); err != nil {
			return nil, err
		}
		if err := fetchFromLocalDB(localLedgerRange.FirstLedger, end); err != nil {
			return nil, err
		}
	}

	return result, nil
}

// parseLedgerInfo extracts and formats the ledger metadata and header information.
func parseLedgerInfo(ledger db.LedgerMetadataChunk, format string) (protocol.LedgerInfo, error) {
	header := ledger.Header
	ledgerInfo := protocol.LedgerInfo{
		Hash:            header.Hash.HexString(),
		Sequence:        uint32(header.Header.LedgerSeq),
		LedgerCloseTime: int64(header.Header.ScpValue.CloseTime),
	}

	// Format the data according to the requested format (JSON or XDR)
	switch format {
	case protocol.FormatJSON:
		var convErr error
		ledgerInfo.LedgerMetadataJSON, ledgerInfo.LedgerHeaderJSON, convErr = ledgerToJSON(&ledger)
		if convErr != nil {
			return ledgerInfo, convErr
		}

	default:
		headerB, err := ledger.Header.MarshalBinary()
		if err != nil {
			return protocol.LedgerInfo{}, fmt.Errorf("error marshaling ledger header: %w", err)
		}

		ledgerInfo.LedgerMetadata = base64.StdEncoding.EncodeToString(ledger.Lcm)
		ledgerInfo.LedgerHeader = base64.StdEncoding.EncodeToString(headerB)
	}
	return ledgerInfo, nil
}
