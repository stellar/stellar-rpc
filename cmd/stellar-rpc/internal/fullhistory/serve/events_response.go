package serve

import (
	"context"
	"encoding/json"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
)

// The v2 getEvents response deliberately drops inSuccessfulContractCall. The
// field is deprecated in the public API (the SDK marks it "remove in v24"),
// and the v2 event store does not record it — the veneer would only ever hand
// back a meaningless false. Rather than fork the whole v1 handler, the v1
// handler runs unchanged and its response is re-mapped into these local
// structs, which mirror the SDK's protocol.GetEventsResponse/EventInfo JSON
// field-for-field EXCEPT for that one key.

type eventInfo struct {
	EventType       string `json:"type"`
	Ledger          int32  `json:"ledger"`
	LedgerClosedAt  string `json:"ledgerClosedAt"`
	ContractID      string `json:"contractId"`
	ID              string `json:"id"`
	OpIndex         uint32 `json:"operationIndex"`
	TxIndex         uint32 `json:"transactionIndex"`
	TransactionHash string `json:"txHash"`

	TopicXDR  []string          `json:"topic,omitempty"`
	TopicJSON []json.RawMessage `json:"topicJson,omitempty"`

	ValueXDR  string          `json:"value,omitempty"`
	ValueJSON json.RawMessage `json:"valueJson,omitempty"`
}

type getEventsResponse struct {
	Events []eventInfo `json:"events"`
	Cursor string      `json:"cursor"`

	LatestLedger          uint32 `json:"latestLedger"`
	OldestLedger          uint32 `json:"oldestLedger"`
	LatestLedgerCloseTime int64  `json:"latestLedgerCloseTime,string"`
	OldestLedgerCloseTime int64  `json:"oldestLedgerCloseTime,string"`
}

// newGetEventsHandler is the v2 getEvents method: the v1 handler verbatim,
// with its successful responses re-mapped to the deprecated-field-free shape.
func newGetEventsHandler(
	logger *supportlog.Entry,
	events db.EventReader,
	maxLimit, defaultLimit uint,
	ledgers db.LedgerReader,
) jrpc2.Handler {
	inner := methods.NewGetEventsHandler(logger, events, maxLimit, defaultLimit, ledgers)
	return func(ctx context.Context, req *jrpc2.Request) (any, error) {
		res, err := inner(ctx, req)
		if err != nil {
			return res, err
		}
		full, ok := res.(protocol.GetEventsResponse)
		if !ok {
			return nil, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "getEvents returned an unexpected response type",
			}
		}
		return stripDeprecatedEventFields(full), nil
	}
}

func stripDeprecatedEventFields(full protocol.GetEventsResponse) getEventsResponse {
	events := make([]eventInfo, 0, len(full.Events))
	for _, ev := range full.Events {
		events = append(events, eventInfo{
			EventType:       ev.EventType,
			Ledger:          ev.Ledger,
			LedgerClosedAt:  ev.LedgerClosedAt,
			ContractID:      ev.ContractID,
			ID:              ev.ID,
			OpIndex:         ev.OpIndex,
			TxIndex:         ev.TxIndex,
			TransactionHash: ev.TransactionHash,
			TopicXDR:        ev.TopicXDR,
			TopicJSON:       ev.TopicJSON,
			ValueXDR:        ev.ValueXDR,
			ValueJSON:       ev.ValueJSON,
		})
	}
	return getEventsResponse{
		Events:                events,
		Cursor:                full.Cursor,
		LatestLedger:          full.LatestLedger,
		OldestLedger:          full.OldestLedger,
		LatestLedgerCloseTime: full.LatestLedgerCloseTime,
		OldestLedgerCloseTime: full.OldestLedgerCloseTime,
	}
}
