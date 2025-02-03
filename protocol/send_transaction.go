package protocol

import "encoding/json"

const SendTransactionMethodName = "sendTransaction"

// SendTransactionResponse represents the transaction submission response returned Stellar-RPC
type SendTransactionResponse struct {
	// ErrorResultXDR is present only if Status is equal to proto.TXStatusError.
	// ErrorResultXDR is a TransactionResult xdr string which contains details on why
	// the transaction could not be accepted by stellar-core.
	ErrorResultXDR  string          `json:"errorResultXdr,omitempty"`
	ErrorResultJSON json.RawMessage `json:"errorResultJson,omitempty"`

	// DiagnosticEventsXDR is present only if Status is equal to proto.TXStatusError.
	// DiagnosticEventsXDR is a base64-encoded slice of xdr.DiagnosticEvent
	DiagnosticEventsXDR  []string          `json:"diagnosticEventsXdr,omitempty"`
	DiagnosticEventsJSON []json.RawMessage `json:"diagnosticEventsJson,omitempty"`

	// Status represents the status of the transaction submission returned by stellar-core.
	// Status can be one of: proto.TXStatusPending, proto.TXStatusDuplicate,
	// proto.TXStatusTryAgainLater, or proto.TXStatusError.
	Status string `json:"status"`
	// Hash is a hash of the transaction which can be used to look up whether
	// the transaction was included in the ledger.
	Hash string `json:"hash"`
	// LatestLedger is the latest ledger known to Stellar-RPC at the time it handled
	// the transaction submission request.
	LatestLedger uint32 `json:"latestLedger"`
	// LatestLedgerCloseTime is the unix timestamp of the close time of the latest ledger known to
	// Stellar-RPC at the time it handled the transaction submission request.
	LatestLedgerCloseTime int64 `json:"latestLedgerCloseTime,string"`
}

// SendTransactionRequest is the Stellar-RPC request to submit a transaction.
type SendTransactionRequest struct {
	// Transaction is the base64 encoded transaction envelope.
	Transaction string `json:"transaction"`
	Format      string `json:"xdrFormat,omitempty"`
}
