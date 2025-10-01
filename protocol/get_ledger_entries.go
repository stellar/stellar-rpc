package protocol

import "encoding/json"

const GetLedgerEntriesMethodName = "getLedgerEntries"

type GetLedgerEntriesRequest struct {
	Keys   []string `json:"keys"`
	Format string   `json:"xdrFormat,omitempty"`
}

type LedgerEntryResult struct {
	// Original request key matching this LedgerEntryResult.
	KeyXDR  string          `json:"key,omitempty"`
	KeyJSON json.RawMessage `json:"keyJson,omitempty"`
	// Ledger entry data encoded in base 64.
	DataXDR  string          `json:"xdr,omitempty"`
	DataJSON json.RawMessage `json:"dataJson,omitempty"`
	// Last modified ledger for this entry.
	LastModifiedLedger uint32 `json:"lastModifiedLedgerSeq"`
	// The ledger sequence until the entry is live, available for entries that have associated ttl ledger entries.
	LiveUntilLedgerSeq *uint32 `json:"liveUntilLedgerSeq,omitempty"`
	// Extension field for this entry, if any
	ExtensionXDR  string          `json:"extXdr,omitempty"`
	ExtensionJSON json.RawMessage `json:"extJson,omitempty"`
}

type GetLedgerEntriesResponse struct {
	// All found ledger entries.
	Entries []LedgerEntryResult `json:"entries"`
	// Sequence number of the latest ledger at time of request.
	LatestLedger uint32 `json:"latestLedger"`
}
