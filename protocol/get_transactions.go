package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
)

const GetTransactionsMethodName = "getTransactions"

// TransactionsPaginationOptions defines the available options for paginating through transactions.
type TransactionsPaginationOptions struct {
	Cursor string `json:"cursor,omitempty"`
	Limit  uint   `json:"limit,omitempty"`
}

// GetTransactionsRequest represents the request parameters for fetching transactions within a range of ledgers.
type GetTransactionsRequest struct {
	StartLedger uint32                         `json:"startLedger"`
	Pagination  *TransactionsPaginationOptions `json:"pagination,omitempty"`
	Format      string                         `json:"xdrFormat,omitempty"`
}

// IsValid checks the validity of the request parameters.
func (req GetTransactionsRequest) IsValid(maxLimit uint, ledgerRange LedgerSeqRange) error {
	if req.Pagination != nil && req.Pagination.Cursor != "" {
		if req.StartLedger != 0 {
			return errors.New("startLedger and cursor cannot both be set")
		}
	} else if req.StartLedger < ledgerRange.FirstLedger || req.StartLedger > ledgerRange.LastLedger {
		return fmt.Errorf(
			"start ledger must be between the oldest ledger: %d and the latest ledger: %d for this rpc instance",
			ledgerRange.FirstLedger,
			ledgerRange.LastLedger,
		)
	}

	if req.Pagination != nil && req.Pagination.Limit > maxLimit {
		return fmt.Errorf("limit must not exceed %d", maxLimit)
	}

	return IsValidFormat(req.Format)
}

type TransactionDetails struct {
	// Status is one of: TransactionSuccess, TransactionFailed, TransactionNotFound.
	Status string `json:"status"`
	// TransactionHash is the hex encoded hash of the transaction. Note that for
	// fee-bump transaction this will be the hash of the fee-bump transaction
	// instead of the inner transaction hash.
	TransactionHash string `json:"txHash"`
	// ApplicationOrder is the index of the transaction among all the
	// transactions for that ledger.
	ApplicationOrder int32 `json:"applicationOrder"`
	// FeeBump indicates whether the transaction is a feebump transaction
	FeeBump bool `json:"feeBump"`
	// EnvelopeXDR is the TransactionEnvelope XDR value.
	EnvelopeXDR  string          `json:"envelopeXdr,omitempty"`
	EnvelopeJSON json.RawMessage `json:"envelopeJson,omitempty"`
	// ResultXDR is the TransactionResult XDR value.
	ResultXDR  string          `json:"resultXdr,omitempty"`
	ResultJSON json.RawMessage `json:"resultJson,omitempty"`
	// ResultMetaXDR is the TransactionMeta XDR value.
	ResultMetaXDR  string          `json:"resultMetaXdr,omitempty"`
	ResultMetaJSON json.RawMessage `json:"resultMetaJson,omitempty"`
	// DiagnosticEventsXDR is present only if transaction was not successful.
	// DiagnosticEventsXDR is a base64-encoded slice of xdr.DiagnosticEvent
	DiagnosticEventsXDR  []string          `json:"diagnosticEventsXdr,omitempty"`
	DiagnosticEventsJSON []json.RawMessage `json:"diagnosticEventsJson,omitempty"`
	// Ledger is the sequence of the ledger which included the transaction.
	Ledger uint32 `json:"ledger"`
}

type TransactionInfo struct {
	TransactionDetails

	// LedgerCloseTime is the unix timestamp of when the transaction was
	// included in the ledger.
	LedgerCloseTime int64 `json:"createdAt"`
}

// GetTransactionsResponse encapsulates the response structure for getTransactions queries.
type GetTransactionsResponse struct {
	Transactions          []TransactionInfo `json:"transactions"`
	LatestLedger          uint32            `json:"latestLedger"`
	LatestLedgerCloseTime int64             `json:"latestLedgerCloseTimestamp"`
	OldestLedger          uint32            `json:"oldestLedger"`
	OldestLedgerCloseTime int64             `json:"oldestLedgerCloseTimestamp"`
	Cursor                string            `json:"cursor"`
}
