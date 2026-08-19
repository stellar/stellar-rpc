package rpcv2

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
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

func TestRunIngestionLoop_StampsCloseTimeOnCommit(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	first := c.FirstLedger()
	closeTime := time.Now().Add(-2 * time.Second).Truncate(time.Second)

	stream := &fakeCoreStream{frames: map[uint32][]byte{
		first:     rpcv2test.ZeroTxLCMBytesAt(t, first, closeTime.Unix()),
		first + 1: rpcv2test.ZeroTxLCMBytesAt(t, first+1, closeTime.Unix()),
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
