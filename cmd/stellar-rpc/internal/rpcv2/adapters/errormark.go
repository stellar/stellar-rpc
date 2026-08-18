package adapters

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// The shared JSON-RPC handlers flatten every adapter error into a generic
// internal error — some paths keep only the message text, others drop even
// that — so an error's TYPE cannot survive the trip back through a handler.
// This side channel carries it out anyway: the v2 method table installs a mark
// on the request context before calling the handler, the adapters record the
// routing/lifecycle errors they hit on that mark, and the method table reads
// the mark after the handler fails to pick the right JSON-RPC error code.
//
// How the method table uses it:
//
//	ctx, mark := adapters.WithErrorMark(ctx)
//	result, err := handler(ctx, req)
//	if err != nil && mark.Transient() {
//		// self-healing condition: answer "temporarily unavailable, retry"
//		// instead of the handler's generic internal error
//	}
//
// The mark is only consulted when the handler ultimately fails, so an error an
// adapter hit but the handler recovered from cannot change a successful
// response. It CAN mislabel a failure whose final cause differs from the
// recorded one — accepted: both marked conditions are rare, and the worst case
// is telling a client to retry an error it would otherwise see as internal.

type errorMarkKey struct{}

// ErrorMark records which routing/lifecycle errors the adapters hit while
// serving one request. Written by the adapters, read by the method table.
// Fields are atomic so a future fan-out adapter can write from worker
// goroutines without a data race.
type ErrorMark struct {
	unavailable atomic.Bool
	storeClosed atomic.Bool
	rangeErr    atomic.Pointer[query.RangeError]
}

// WithErrorMark installs a fresh mark on ctx and returns both. The adapters
// find the mark through the context values every store call already carries.
func WithErrorMark(ctx context.Context) (context.Context, *ErrorMark) {
	mark := &ErrorMark{}
	return context.WithValue(ctx, errorMarkKey{}, mark), mark
}

// Transient reports whether the request hit a condition that self-heals on
// retry: a chunk with no serving store in the request's snapshot, or a store
// closed underneath an in-flight read.
func (m *ErrorMark) Transient() bool {
	return m.unavailable.Load() || m.storeClosed.Load()
}

// StoreClosed reports whether the request read a store that was closed
// underneath it — which means the request outlived the deletion grace period.
func (m *ErrorMark) StoreClosed() bool { return m.storeClosed.Load() }

// RangeError returns the recorded below-window range rejection, or nil.
func (m *ErrorMark) RangeError() *query.RangeError { return m.rangeErr.Load() }

// markErr records err on ctx's mark (when one is installed and err is a kind
// the method table maps) and returns err unchanged, so adapter error returns
// wrap in place: return markErr(ctx, err).
func markErr(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	mark, ok := ctx.Value(errorMarkKey{}).(*ErrorMark)
	if !ok {
		return err
	}
	var rangeErr *query.RangeError
	switch {
	case errors.As(err, &rangeErr):
		mark.rangeErr.Store(rangeErr)
	case errors.Is(err, query.ErrUnavailable):
		mark.unavailable.Store(true)
	case errors.Is(err, stores.ErrStoreClosed):
		mark.storeClosed.Store(true)
	}
	return err
}
