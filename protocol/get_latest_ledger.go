package protocol

import "encoding/json"

const GetLatestLedgerMethodName = "getLatestLedger"

type GetLatestLedgerResponse struct {
	// Hash of the latest ledger as a hex-encoded string
	Hash string `json:"id"`
	// Stellar Core protocol version associated with the ledger
	ProtocolVersion uint32 `json:"protocolVersion"`
	// Sequence number of the latest ledger
	Sequence uint32 `json:"sequence"`
	// Time the ledger closed at as an int64
	LedgerCloseTime int64 `json:"closeTime,string"`
	// LedgerHeader of the latest ledger (base64-encoded XDR)
	LedgerHeader string `json:"header"`
	// JSON representation of the latest ledger header
	LedgerHeaderJSON json.RawMessage `json:"headerJson,omitempty"`
	// LedgerMetadata of the latest ledger (base64-encoded XDR)
	LedgerMetadata string `json:"metadata"`
	// JSON representation of the latest ledger metadata
	LedgerMetadataJSON json.RawMessage `json:"metadataJson,omitempty"`
}
