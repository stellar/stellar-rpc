package adapters

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

func TestMarkErr_NoMarkInstalled(t *testing.T) {
	err := errors.New("boom")
	assert.Same(t, err, markErr(context.Background(), err))
	assert.NoError(t, markErr(context.Background(), nil))
}

func TestMarkErr_PreservesTheErrorThroughTheContext(t *testing.T) {
	ctx, mark := WithErrorMark(context.Background())
	require.NoError(t, mark.Err())

	wrapped := fmt.Errorf("read ledger 5: %w", stores.ErrStoreClosed)
	assert.Same(t, wrapped, markErr(ctx, wrapped))
	assert.ErrorIs(t, mark.Err(), stores.ErrStoreClosed)

	rangeErr := &query.RangeError{Requested: 1, Oldest: 10, Latest: 20}
	_ = markErr(ctx, fmt.Errorf("scan: %w", rangeErr))
	var got *query.RangeError
	require.ErrorAs(t, mark.Err(), &got)
	assert.Same(t, rangeErr, got)
}

func TestMarkErr_LastErrorWins(t *testing.T) {
	ctx, mark := WithErrorMark(context.Background())
	_ = markErr(ctx, query.ErrUnavailable)
	_ = markErr(ctx, stores.ErrStoreClosed)
	assert.ErrorIs(t, mark.Err(), stores.ErrStoreClosed)
	assert.NotErrorIs(t, mark.Err(), query.ErrUnavailable)
}
