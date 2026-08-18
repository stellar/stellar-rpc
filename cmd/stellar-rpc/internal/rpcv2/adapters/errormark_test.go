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

func TestMarkErr_RecordsSentinels(t *testing.T) {
	t.Run("unavailable", func(t *testing.T) {
		ctx, mark := WithErrorMark(context.Background())
		wrapped := fmt.Errorf("resolve chunk: %w", query.ErrUnavailable)
		assert.Same(t, wrapped, markErr(ctx, wrapped))
		assert.True(t, mark.Transient())
		assert.False(t, mark.StoreClosed())
	})

	t.Run("store closed", func(t *testing.T) {
		ctx, mark := WithErrorMark(context.Background())
		wrapped := fmt.Errorf("read ledger 5: %w", stores.ErrStoreClosed)
		require.Same(t, wrapped, markErr(ctx, wrapped))
		assert.True(t, mark.Transient())
		assert.True(t, mark.StoreClosed())
	})

	t.Run("range error", func(t *testing.T) {
		ctx, mark := WithErrorMark(context.Background())
		rangeErr := &query.RangeError{Requested: 1, Oldest: 10, Latest: 20}
		wrapped := fmt.Errorf("scan: %w", rangeErr)
		require.Same(t, wrapped, markErr(ctx, wrapped))
		assert.Same(t, rangeErr, mark.RangeError())
		assert.False(t, mark.Transient())
	})

	t.Run("unrelated error leaves the mark clean", func(t *testing.T) {
		ctx, mark := WithErrorMark(context.Background())
		_ = markErr(ctx, errors.New("disk on fire"))
		assert.False(t, mark.Transient())
		assert.False(t, mark.StoreClosed())
		assert.Nil(t, mark.RangeError())
	})
}
