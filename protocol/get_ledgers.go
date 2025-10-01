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

// GetLedgersRequest represents the request parameters for fetching ledgers.
type GetLedgersRequest struct {
	StartLedger uint32                   `json:"startLedger"`
	Pagination  *LedgerPaginationOptions `json:"pagination,omitempty"`
	Format      string                   `json:"xdrFormat,omitempty"`
}

// validate checks the validity of the request parameters.
func (req *GetLedgersRequest) Validate(maxLimit uint, ledgerRange LedgerSeqRange) error {
	return errors.Join(
		ValidatePagination(req.StartLedger, req.Pagination, maxLimit, ledgerRange),
		IsValidFormat(req.Format),
	) // nils will coalesce
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

// IsLedgerWithinRange checks whether the request start ledger/cursor is within
// the max/min ledger for the current RPC instance.
func IsLedgerWithinRange(startLedger uint32, ledgerRange LedgerSeqRange) bool {
	return startLedger >= ledgerRange.FirstLedger && startLedger <= ledgerRange.LastLedger
}

// ValidatePagination ensures that pagination parameters across supported
// endpoints conform to the given requirements:
//
// * If pagination is set:
//   - If the cursor is set, the startLedger is not set.
//   - If the limit is set, it does not exceed the maxLimit.
//   - If the cursor is NOT set, the startLedger is in the RPC's range of known ledgers.
//
// * Otherwise,
//   - The startLedger is set and is in the RPC's range of known ledgers.
func ValidatePagination(
	startLedger uint32,
	pagination *LedgerPaginationOptions,
	maxLimit uint,
	ledgerRange LedgerSeqRange,
) error {
	errBadPage := fmt.Errorf(
		"start ledger (%d) must be between the oldest ledger: %d and the latest ledger: %d for this rpc instance",
		startLedger,
		ledgerRange.FirstLedger,
		ledgerRange.LastLedger,
	)

	if pagination != nil { //nolint:nestif // this is too hard to get right otherwise
		if pagination.Cursor != "" { // either cursor
			if startLedger != 0 {
				return fmt.Errorf("startLedger (%d) and cursor (%s) cannot both be set",
					startLedger, pagination.Cursor)
			}
		} else if !IsLedgerWithinRange(startLedger, ledgerRange) { // xor startLedger
			return errBadPage
		}
		if pagination.Limit > maxLimit {
			return fmt.Errorf("limit must not exceed %d", maxLimit)
		}
	} else if !IsLedgerWithinRange(startLedger, ledgerRange) {
		return errBadPage
	}

	return nil
}
