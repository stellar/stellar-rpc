package events

import (
	"fmt"

	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// StageSentinels maps a V4 top-level TransactionEvent stage to the
// (TxIdx, OpIdx) sentinels the v1 getEvents cursor encoding uses. applyIdx is
// the 1-based transaction apply index (consumed by the AfterTx arm). This is
// the SINGLE definition of the cursor-encoding policy for both storage
// backends: the full-history view path (LCMViewToPayloads) and the legacy
// SQL path (db/event.go's InsertEvents) both consume it, so a new stage or
// sentinel revision lands in one place and getEvents cursors stay compatible
// across backends by construction.
func StageSentinels(stage xdr.TransactionEventStage, applyIdx uint32) (uint32, uint32, error) {
	switch stage {
	case xdr.TransactionEventStageTransactionEventStageBeforeAllTxs:
		return 0, 0, nil
	case xdr.TransactionEventStageTransactionEventStageAfterAllTxs:
		return uint32(toid.TransactionMask), 0, nil
	case xdr.TransactionEventStageTransactionEventStageAfterTx:
		return applyIdx, uint32(toid.OperationMask), nil
	default:
		return 0, 0, fmt.Errorf("events: unhandled event stage %v", stage)
	}
}
