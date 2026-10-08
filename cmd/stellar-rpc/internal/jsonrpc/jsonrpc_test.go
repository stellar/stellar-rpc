package jsonrpc

import (
	"context"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
)

// A client disconnect cancels the handler context, except for sendTransaction,
// which keeps its duration limit.
func TestWrapWithLimiters_ClientDisconnect(t *testing.T) {
	for _, test := range []struct {
		method       string
		wantCanceled bool
	}{
		{protocol.SendTransactionMethodName, false},
		{protocol.GetHealthMethodName, true},
	} {
		t.Run(test.method, func(t *testing.T) {
			ctx, disconnect := context.WithCancel(t.Context())
			var deadline bool
			h := wrapWithLimiters(HandlerSpec{
				MethodName: test.method,
				Handler: func(hctx context.Context, _ *jrpc2.Request) (any, error) {
					disconnect()
					_, deadline = hctx.Deadline()
					return nil, hctx.Err()
				},
				QueueLimit:           1,
				RequestDurationLimit: time.Minute,
			}, host.MakeNoOpDaemon(), log.DefaultLogger)

			req := (&jrpc2.ParsedRequest{ID: "1", Method: test.method}).ToRequest()
			_, err := h(ctx, req)
			if test.wantCanceled {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
			assert.True(t, deadline, "duration limit not applied")
		})
	}
}
