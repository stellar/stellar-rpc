package event

import "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/eventquery"

// Payload and its codec live in the shared eventquery package; these names
// keep the store's callers unchanged.
type Payload = eventquery.Payload

const PayloadVersion = eventquery.PayloadVersion

var (
	ErrUnknownPayloadVersion = eventquery.ErrUnknownPayloadVersion
	ErrShortPayloadBuffer    = eventquery.ErrShortPayloadBuffer
	ErrPayloadLengthMismatch = eventquery.ErrPayloadLengthMismatch
)
