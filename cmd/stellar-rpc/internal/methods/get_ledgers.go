package methods

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/creachadair/jrpc2"

	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerbucketwindow"
	"github.com/stellar/stellar-rpc/protocol"
)

type LedgerPaginationOptions struct {
	Cursor string `json:"cursor,omitempty"`
	Limit  uint   `json:"limit,omitempty"`
}

// isStartLedgerWithinBounds checks whether the request start ledger/cursor is within the max/min ledger
// for the current RPC instance.
func isStartLedgerWithinBounds(startLedger uint32, ledgerRange ledgerbucketwindow.LedgerRange) bool {
	return startLedger >= ledgerRange.FirstLedger.Sequence && startLedger <= ledgerRange.LastLedger.Sequence
}

// GetLedgersRequest represents the request parameters for fetching ledgers.
type GetLedgersRequest struct {
	StartLedger uint32                   `json:"startLedger"`
	Pagination  *LedgerPaginationOptions `json:"pagination,omitempty"`
	Format      string                   `json:"xdrFormat,omitempty"`
}

// validate checks the validity of the request parameters.
func (req *GetLedgersRequest) validate(maxLimit uint, ledgerRange ledgerbucketwindow.LedgerRange) error {
	switch {
	case req.Pagination != nil:
		switch {
		case req.Pagination.Cursor != "" && req.StartLedger != 0:
			return errors.New("startLedger and cursor cannot both be set")
		case req.Pagination.Limit > maxLimit:
			return fmt.Errorf("limit must not exceed %d", maxLimit)
		}
	case req.StartLedger != 0 && !isStartLedgerWithinBounds(req.StartLedger, ledgerRange):
		return fmt.Errorf(
			"start ledger must be between the oldest ledger: %d and the latest ledger: %d for this rpc instance",
			ledgerRange.FirstLedger.Sequence,
			ledgerRange.LastLedger.Sequence,
		)
	}

	return protocol.IsValidFormat(req.Format)
}

// LedgerInfo represents a single ledger in the response.
type LedgerInfo struct {
	Hash            string `json:"hash"`
	Sequence        uint32 `json:"sequence"`
	LedgerCloseTime int64  `json:"ledgerCloseTime,string"`

	LedgerHeader     string          `json:"headerXdr"`
	LedgerHeaderJSON json.RawMessage `json:"headerJson,omitempty"`

	LedgerMetadata     string          `json:"metadataXdr"`
	LedgerMetadataJSON json.RawMessage `json:"metadataJson,omitempty"`
}

// GetLedgersResponse encapsulates the response structure for getLedgers queries.
type GetLedgersResponse struct {
	Ledgers               []LedgerInfo `json:"ledgers"`
	LatestLedger          uint32       `json:"latestLedger"`
	LatestLedgerCloseTime int64        `json:"latestLedgerCloseTime"`
	OldestLedger          uint32       `json:"oldestLedger"`
	OldestLedgerCloseTime int64        `json:"oldestLedgerCloseTime"`
	Cursor                string       `json:"cursor"`
}

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
func (h ledgersHandler) getLedgers(ctx context.Context, request GetLedgersRequest) (GetLedgersResponse, error) {
	readTx, err := h.ledgerReader.NewTx(ctx)
	if err != nil {
		return GetLedgersResponse{}, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: err.Error(),
		}
	}
	defer func() {
		_ = readTx.Done()
	}()

	ledgerRange, err := readTx.GetLedgerRange(ctx)
	if err != nil {
		return GetLedgersResponse{}, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: err.Error(),
		}
	}

	if err := request.validate(h.maxLimit, ledgerRange); err != nil {
		return GetLedgersResponse{}, &jrpc2.Error{
			Code:    jrpc2.InvalidRequest,
			Message: err.Error(),
		}
	}

	start, limit, err := h.initializePagination(request, ledgerRange)
	if err != nil {
		return GetLedgersResponse{}, &jrpc2.Error{
			Code:    jrpc2.InvalidParams,
			Message: err.Error(),
		}
	}

	ledgers, err := h.fetchLedgers(ctx, start, limit, request.Format, readTx)
	if err != nil {
		return GetLedgersResponse{}, err
	}
	cursor := strconv.Itoa(int(ledgers[len(ledgers)-1].Sequence))

	return GetLedgersResponse{
		Ledgers:               ledgers,
		LatestLedger:          ledgerRange.LastLedger.Sequence,
		LatestLedgerCloseTime: ledgerRange.LastLedger.CloseTime,
		OldestLedger:          ledgerRange.FirstLedger.Sequence,
		OldestLedgerCloseTime: ledgerRange.FirstLedger.CloseTime,
		Cursor:                cursor,
	}, nil
}

// initializePagination parses the request pagination details and initializes the cursor.
func (h ledgersHandler) initializePagination(request GetLedgersRequest,
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
	if !isStartLedgerWithinBounds(start, ledgerRange) {
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
) ([]LedgerInfo, error) {
	ledgers, err := readTx.BatchGetLedgers(ctx, start, limit)
	if err != nil {
		return nil, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: fmt.Sprintf("error fetching ledgers from db: %v", err),
		}
	}

	result := make([]LedgerInfo, 0, limit)
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
func (h ledgersHandler) parseLedgerInfo(ledger xdr.LedgerCloseMeta, format string) (LedgerInfo, error) {
	ledgerInfo := LedgerInfo{
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
			return LedgerInfo{}, fmt.Errorf("error marshaling ledger close meta: %w", err)
		}

		headerB, err := ledger.LedgerHeaderHistoryEntry().MarshalBinary()
		if err != nil {
			return LedgerInfo{}, fmt.Errorf("error marshaling ledger header: %w", err)
		}

		ledgerInfo.LedgerMetadata = base64.StdEncoding.EncodeToString(closeMetaB)
		ledgerInfo.LedgerHeader = base64.StdEncoding.EncodeToString(headerB)
	}
	return ledgerInfo, nil
}
