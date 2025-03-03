package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
)

const GetLedgersMethodName = "getLedgers"

type LedgerPaginationOptions struct {
	Cursor string `json:"cursor,omitempty"`
	Limit  uint   `json:"limit,omitempty"`
}

type LedgerSeqRange struct {
	FirstLedger uint32
	LastLedger  uint32
}

// IsStartLedgerWithinBounds checks whether the request start ledger/cursor is within the max/min ledger
// for the current RPC instance.
func IsStartLedgerWithinBounds(startLedger uint32, ledgerRange LedgerSeqRange) bool {
	return startLedger >= ledgerRange.FirstLedger && startLedger <= ledgerRange.LastLedger
}

// GetLedgersRequest represents the request parameters for fetching ledgers.
type GetLedgersRequest struct {
	StartLedger uint32                   `json:"startLedger"`
	Pagination  *LedgerPaginationOptions `json:"pagination,omitempty"`
	Format      string                   `json:"xdrFormat,omitempty"`
}

// validate checks the validity of the request parameters.
func (req *GetLedgersRequest) Validate(maxLimit uint, ledgerRange LedgerSeqRange) error {
	switch {
	case req.Pagination != nil:
		switch {
		case req.Pagination.Cursor != "" && req.StartLedger != 0:
			return errors.New("startLedger and cursor cannot both be set")
		case req.Pagination.Limit > maxLimit:
			return fmt.Errorf("limit must not exceed %d", maxLimit)
		}
	case req.StartLedger != 0 && !IsStartLedgerWithinBounds(req.StartLedger, ledgerRange):
		return fmt.Errorf(
			"start ledger must be between the oldest ledger: %d and the latest ledger: %d for this rpc instance",
			ledgerRange.FirstLedger,
			ledgerRange.LastLedger,
		)
	}

	return IsValidFormat(req.Format)
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
