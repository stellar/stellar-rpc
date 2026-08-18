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
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

type stampRecordingSink struct {
	closingSink

	seqs       []uint32
	closeTimes []int64
}

func (s *stampRecordingSink) SetLatestLedger(seq uint32, closeTimeUnix int64) {
	s.seqs = append(s.seqs, seq)
	s.closeTimes = append(s.closeTimes, closeTimeUnix)
}

func (s *stampRecordingSink) PublishHandle(c chunk.ID, db *hotchunk.DB) {
	s.closingSink.PublishHandle(c, db)
}

func TestRunIngestionLoop_StampsCloseTimeOnCommit(t *testing.T) {
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
	sink := &stampRecordingSink{}
	cfg.Registry = sink

	require.Error(t, runIngestionLoop(context.Background(), cfg))

	require.Equal(t, []uint32{first, first + 1}, sink.seqs, "one stamp per committed ledger")
	assert.Equal(t, []int64{closeTime.Unix(), closeTime.Unix()}, sink.closeTimes,
		"the stamp carries the committed ledger's decoded close time")
}

// lcmBytesAtCloseTime is a zero-tx LCM (like rpcv2test.ZeroTxLCMBytes) with an explicit
// close time, so a test can drive the stamp off a known value.
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
