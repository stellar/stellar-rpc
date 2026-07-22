package events

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// TestStageSentinels pins the Stage→(TxIdx, OpIdx) cursor-sentinel mapping.
// StageSentinels is the SINGLE definition of this policy for both storage
// backends (PayloadsFromLedgerEvents and sqlitedb/event.go's InsertEvents), so these
// values ARE the v1 getEvents cursor-encoding contract:
//
//   - BeforeAllTxs → (0, 0): sorts before any real transaction (TxIdx >= 1);
//   - AfterTx      → (applyIdx, OperationMask): sorts after every operation
//     of its own transaction but before the next transaction;
//   - AfterAllTxs  → (TransactionMask, 0): the ledger tail, after every
//     real transaction.
func TestStageSentinels(t *testing.T) {
	const applyIdx = uint32(7)

	txIdx, opIdx, err := StageSentinels(
		xdr.TransactionEventStageTransactionEventStageBeforeAllTxs, applyIdx)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), txIdx, "BeforeAllTxs TxIdx")
	assert.Equal(t, uint32(0), opIdx, "BeforeAllTxs OpIdx")

	txIdx, opIdx, err = StageSentinels(
		xdr.TransactionEventStageTransactionEventStageAfterTx, applyIdx)
	require.NoError(t, err)
	assert.Equal(t, applyIdx, txIdx, "AfterTx TxIdx must be the 1-based apply index")
	assert.Equal(t, uint32(toid.OperationMask), opIdx, "AfterTx OpIdx")

	txIdx, opIdx, err = StageSentinels(
		xdr.TransactionEventStageTransactionEventStageAfterAllTxs, applyIdx)
	require.NoError(t, err)
	assert.Equal(t, uint32(toid.TransactionMask), txIdx, "AfterAllTxs TxIdx")
	assert.Equal(t, uint32(0), opIdx, "AfterAllTxs OpIdx")
}

// TestStageSentinels_CursorOrdering pins the ordering invariant the
// sentinels exist to provide: for any real transaction (1-based applyIdx)
// the (TxIdx, OpIdx) keys sort BeforeAllTxs < every op of every tx <
// that tx's AfterTx < the next tx < AfterAllTxs, under ascending
// (TxIdx, OpIdx) comparison — i.e. ascending getEvents cursor order.
func TestStageSentinels_CursorOrdering(t *testing.T) {
	beforeTx, beforeOp, err := StageSentinels(
		xdr.TransactionEventStageTransactionEventStageBeforeAllTxs, 1)
	require.NoError(t, err)
	afterAllTx, _, err := StageSentinels(
		xdr.TransactionEventStageTransactionEventStageAfterAllTxs, 1)
	require.NoError(t, err)

	const lastApplyIdx = uint32(toid.TransactionMask) - 1 // largest real tx index
	for _, applyIdx := range []uint32{1, 2, lastApplyIdx} {
		afterTxTx, afterTxOp, serr := StageSentinels(
			xdr.TransactionEventStageTransactionEventStageAfterTx, applyIdx)
		require.NoError(t, serr)

		// BeforeAllTxs sorts before any real tx's op events.
		assert.Less(t, beforeTx, applyIdx, "BeforeAllTxs must precede tx %d", applyIdx)
		assert.Equal(t, uint32(0), beforeOp)
		// AfterTx shares the tx's TxIdx and sorts after its largest
		// possible op index.
		assert.Equal(t, applyIdx, afterTxTx)
		assert.Greater(t, afterTxOp, uint32(toid.OperationMask)-1,
			"AfterTx must sort after every real op index")
		// AfterAllTxs sorts after every real tx.
		assert.Greater(t, afterAllTx, applyIdx, "AfterAllTxs must follow tx %d", applyIdx)
	}
}

// TestStageSentinels_UnknownStageErrors asserts an unrecognized stage is an
// error, never a silent (0, 0) — both backends rely on this to fail loudly
// when a new protocol stage appears before its sentinel is defined.
func TestStageSentinels_UnknownStageErrors(t *testing.T) {
	_, _, err := StageSentinels(xdr.TransactionEventStage(99), 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unhandled event stage")
}
