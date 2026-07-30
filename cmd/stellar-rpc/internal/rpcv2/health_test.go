package rpcv2

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// TestHealthState_ReadyLatches: readiness is false until the first commit is
// observed, then latches true and stays true (a stale close time does not unlatch
// it — "proven itself once").
func TestHealthState_ReadyLatches(t *testing.T) {
	var hs healthState
	assert.False(t, hs.Ready(), "no commit yet ⇒ not ready")

	hs.observe(time.Now().Unix())
	assert.True(t, hs.Ready(), "first commit latches ready")

	// An old close time (a stalled/lagging source) keeps ready latched.
	hs.observe(time.Now().Add(-time.Hour).Unix())
	assert.True(t, hs.Ready(), "ready stays latched after a stale commit")
}

// TestHealthState_ObserveNilSafe: the ingestion loop calls observe unconditionally,
// so a nil healthState (tests build the loop without one) must be a no-op, not a panic.
func TestHealthState_ObserveNilSafe(t *testing.T) {
	var hs *healthState
	require.NotPanics(t, func() { hs.observe(time.Now().Unix()) })
}

// TestRunIngestionLoop_FeedsHealthFromCommit: the ingestion loop feeds the
// readiness/health signal from the SAME per-ledger commit — after ingesting real
// ledgers with a known close time, the loop is ready and the recorded close time
// is that of the last committed ledger.
func TestRunIngestionLoop_FeedsHealthFromCommit(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	first := c.FirstLedger()
	closeTime := time.Now().Add(-2 * time.Second).Truncate(time.Second)

	stream := &fakeCoreStream{frames: map[uint32][]byte{
		first:     lcmBytesAtCloseTime(t, first, closeTime),
		first + 1: lcmBytesAtCloseTime(t, first+1, closeTime),
	}}
	stream.endErr = errors.New("end")

	cfg, _ := loopConfig(t, stream, cat, first)
	hs := &healthState{}
	cfg.Health = hs

	require.Error(t, runIngestionLoop(context.Background(), cfg))

	assert.True(t, hs.Ready(), "the loop's first commit latched readiness")
	got, ok := hs.LastCommitClose()
	require.True(t, ok)
	assert.Equal(t, closeTime.Unix(), got.Unix(), "health tracks the last committed ledger's close time")
}

// lcmBytesAtCloseTime is a zero-tx LCM (like rpcv2test.ZeroTxLCMBytes) with an explicit
// close time, so a test can drive the health signal off a known value.
func lcmBytesAtCloseTime(t *testing.T, seq uint32, closeTime time.Time) []byte {
	t.Helper()
	lcm := xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(closeTime.Unix())},
					LedgerSeq: xdr.Uint32(seq),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{Phases: nil},
			},
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}
