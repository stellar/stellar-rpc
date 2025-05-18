package methods

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/creachadair/jrpc2"

	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/datastore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/protocol"
)

type ledgersHandler struct {
	ledgerReader          db.LedgerReader
	datastoreLedgerReader datastore.LedgerReader
	maxLimit              uint
	defaultLimit          uint
}

// NewGetLedgersHandler returns a jrpc2.Handler for the getLedgers method.
func NewGetLedgersHandler(ledgerReader db.LedgerReader, maxLimit, defaultLimit uint,
	datastoreLedgerReader datastore.LedgerReader,
) jrpc2.Handler {
	return NewHandler((&ledgersHandler{
		ledgerReader:          ledgerReader,
		maxLimit:              maxLimit,
		defaultLimit:          defaultLimit,
		datastoreLedgerReader: datastoreLedgerReader,
	}).getLedgers)
}

// getLedgers fetch ledgers and relevant metadata from DB and falling back to the remote datastore if necessary.
func (h ledgersHandler) getLedgers(ctx context.Context, request protocol.GetLedgersRequest,
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
	if err != nil {
		return protocol.GetLedgersResponse{}, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: err.Error(),
		}
	}

	availableLedgerRange := ledgerRange.ToLedgerSeqRange()

	if h.datastoreLedgerReader != nil {
		// Since we can't query the actual ledger range in the datastore, we assume it contains all
		// ledgers from genesis (seq 2) to latest. This avoids failing ledger in local range checks since
		// we're falling back to the datastore.
		availableLedgerRange.FirstLedger = 2 // genesis ledger
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
		Ledgers:               ledgers,
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
			"cursor must be between the oldest ledger: %d and the latest ledger: %d for this rpc instance",
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
func (h ledgersHandler) fetchLedgers(ctx context.Context, start uint32,
	end uint32, format string, readTx db.LedgerReaderTx, localLedgerRange protocol.LedgerSeqRange,
) ([]protocol.LedgerInfo, error) {
	fetchFromLocalDB := func(start uint32, end uint32) ([]xdr.LedgerCloseMeta, error) {
		ledgers, err := readTx.BatchGetLedgers(ctx, start, end)
		if err != nil {
			return nil, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: fmt.Sprintf("error fetching ledgers from db: %v", err),
			}
		}
		return ledgers, nil
	}

	fetchFromDatastore := func(start uint32, end uint32) ([]xdr.LedgerCloseMeta, error) {
		if h.datastoreLedgerReader == nil {
			return nil, &jrpc2.Error{
				Code:    jrpc2.InvalidParams,
				Message: "datastore ledger reader not configured",
			}
		}
		ledgers, err := h.datastoreLedgerReader.GetLedgers(ctx, start, end)
		if err != nil {
			return nil, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: fmt.Sprintf("error fetching ledgers from datastore: %v", err),
			}
		}
		return ledgers, nil
	}

	var ledgers []xdr.LedgerCloseMeta

	switch {
	case start >= localLedgerRange.FirstLedger:
		// entire range is available in local DB
		localLedgers, err := fetchFromLocalDB(start, end)
		if err != nil {
			return nil, err
		}
		ledgers = append(ledgers, localLedgers...)

	case end < localLedgerRange.FirstLedger:
		// entire range is unavailable locally so fetch everything from datastore
		dataStoreLedgers, err := fetchFromDatastore(start, end)
		if err != nil {
			return nil, err
		}
		ledgers = append(ledgers, dataStoreLedgers...)

	default:
		// part of the ledger range is available locally so fetch local ledgers from db,
		// and the rest from the datastore.

		dataStoreLedgers, err := fetchFromDatastore(start, localLedgerRange.FirstLedger-1)
		if err != nil {
			return nil, err
		}
		ledgers = append(ledgers, dataStoreLedgers...)

		localLedgers, err := fetchFromLocalDB(localLedgerRange.FirstLedger, end)
		if err != nil {
			return nil, err
		}
		ledgers = append(ledgers, localLedgers...)
	}

	limit := end - start + 1
	// convert raw lcm to protocol.LedgerInfo
	result := make([]protocol.LedgerInfo, 0, limit)
	for _, ledger := range ledgers {
		if uint32(len(result)) >= limit {
			break
		}

		ledgerInfo, err := h.parseLedgerInfo(ledger, format)
		if err != nil {
			return nil, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: fmt.Sprintf("error processing ledger %d: %v", ledger.LedgerSequence(), err),
			}
		}
		result = append(result, ledgerInfo)
	}

	return result, nil
}

// parseLedgerInfo extracts and formats the ledger metadata and header information.
func (h ledgersHandler) parseLedgerInfo(ledger xdr.LedgerCloseMeta, format string) (protocol.LedgerInfo, error) {
	ledgerInfo := protocol.LedgerInfo{
		Hash:            ledger.LedgerHash().HexString(),
		Sequence:        ledger.LedgerSequence(),
		LedgerCloseTime: ledger.LedgerCloseTime(),
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
		closeMetaB, err := ledger.MarshalBinary()
		if err != nil {
			return protocol.LedgerInfo{}, fmt.Errorf("error marshaling ledger close meta: %w", err)
		}

		headerB, err := ledger.LedgerHeaderHistoryEntry().MarshalBinary()
		if err != nil {
			return protocol.LedgerInfo{}, fmt.Errorf("error marshaling ledger header: %w", err)
		}

		ledgerInfo.LedgerMetadata = base64.StdEncoding.EncodeToString(closeMetaB)
		ledgerInfo.LedgerHeader = base64.StdEncoding.EncodeToString(headerB)
	}
	return ledgerInfo, nil
}
