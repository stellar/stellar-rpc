package protocol

const GetLatestLedgerMethodName = "getLatestLedger"

type GetLatestLedgerResponse struct {
	// Hash of the latest ledger as a hex-encoded string
	Hash string `json:"id"`
	// Stellar Core protocol version associated with the ledger.
	ProtocolVersion uint32 `json:"protocolVersion"`
	// Sequence number of the latest ledger.
	Sequence uint32 `json:"sequence"`
}
