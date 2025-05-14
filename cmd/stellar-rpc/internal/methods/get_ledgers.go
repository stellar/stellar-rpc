package methods

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/creachadair/jrpc2"

	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerbucketwindow"
	"github.com/stellar/stellar-rpc/protocol"
)

type ledgersHandler struct {
	ledgerReader db.LedgerReader
	maxLimit     uint
	defaultLimit uint
}

// NewGetLedgersHandler returns a jrpc2.Handler for the getLedgers method.
func NewGetLedgersHandler(ledgerReader db.LedgerReader, maxLimit, defaultLimit uint) jrpc2.Handler {
	return NewHandler((&ledgersHandler{
		ledgerReader: ledgerReader,
		maxLimit:     maxLimit,
		defaultLimit: defaultLimit,
	}).getLedgers)
}

// getLedgers fetch ledgers and relevant metadata from DB.
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

	if err := request.Validate(h.maxLimit, ledgerRange.ToLedgerSeqRange()); err != nil {
		return protocol.GetLedgersResponse{}, &jrpc2.Error{
			Code:    jrpc2.InvalidRequest,
			Message: err.Error(),
		}
	}

	start, limit, err := h.initializePagination(request, ledgerRange)
	if err != nil {
		return protocol.GetLedgersResponse{}, &jrpc2.Error{
			Code:    jrpc2.InvalidParams,
			Message: err.Error(),
		}
	}

	ledgers, err := h.fetchLedgers(ctx, start, limit, request.Format, readTx)
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
	ledgerRange ledgerbucketwindow.LedgerRange,
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

func (h ledgersHandler) parseCursor(cursor string, ledgerRange ledgerbucketwindow.LedgerRange) (uint32, error) {
	cursorInt, err := strconv.ParseUint(cursor, 10, 32)
	if err != nil {
		return 0, err
	}

	start := uint32(cursorInt) + 1
	if !protocol.IsLedgerWithinRange(start, ledgerRange.ToLedgerSeqRange()) {
		return 0, fmt.Errorf(
			"cursor must be between the oldest ledger: %d and the latest ledger: %d for this rpc instance",
			ledgerRange.FirstLedger.Sequence,
			ledgerRange.LastLedger.Sequence,
		)
	}

	return start, nil
}

// fetchLedgers fetches ledgers from the DB for the range [start, start+limit-1]
func (h ledgersHandler) fetchLedgers(ctx context.Context, start uint32,
	limit uint, format string, readTx db.LedgerReaderTx,
) ([]protocol.LedgerInfo, error) {
	ledgers, err := readTx.BatchGetLedgers(ctx, start, limit)
	if err != nil {
		return nil, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: fmt.Sprintf("error fetching ledgers from db: %v", err),
		}
	}

	result := make([]protocol.LedgerInfo, 0, limit)
	for _, ledger := range ledgers {
		if uint(len(result)) >= limit {
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
