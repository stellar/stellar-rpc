package adapters

import (
	"context"
	"sync/atomic"
)

// The shared JSON-RPC handlers flatten every adapter error into a generic
// internal error — some paths keep only the message text, others drop even
// that — so an error's TYPE cannot survive the trip back through a handler.
// This side channel carries it out anyway: the v2 method table installs a mark
// on the request context before calling the handler, every public adapter
// method records the errors it returns on that mark, and the method table
// reads the mark after the handler fails to pick the right JSON-RPC error
// code. All classification (which errors matter, what code each becomes)
// lives in the method table; the adapters just preserve the error.
//
// How the method table uses it:
//
//	ctx, mark := adapters.WithErrorMark(ctx)
//	result, err := handler(ctx, req)
//	if err != nil && errors.Is(mark.Err(), query.ErrUnavailable) {
//		// answer "temporarily unavailable, retry" instead of the
//		// handler's generic internal error
//	}
//
// The mark is only consulted when the handler ultimately fails, so an error an
// adapter hit but the handler recovered from cannot change a successful
// response. It CAN mislabel a failure whose final cause differs from the
// recorded one — accepted: the mapped conditions are rare, and the worst case
// is telling a client to retry an error it would otherwise see as internal.

type errorMarkKey struct{}

// ErrorMark preserves the last error the adapters returned while serving one
// request. Written by the adapters, read by the method table. Atomic so a
// future fan-out adapter can write from worker goroutines without a data race.
type ErrorMark struct {
	err atomic.Pointer[error]
}

// WithErrorMark installs a fresh mark on ctx and returns both. The adapters
// find the mark through the context values every store call already carries.
func WithErrorMark(ctx context.Context) (context.Context, *ErrorMark) {
	mark := &ErrorMark{}
	return context.WithValue(ctx, errorMarkKey{}, mark), mark
}

// Err returns the last recorded error, or nil. Later errors win: when a
// handler retries past a failure, the recorded error is the one closest to
// the handler's final failure.
func (m *ErrorMark) Err() error {
	if p := m.err.Load(); p != nil {
		return *p
	}
	return nil
}

// markErr records err on ctx's mark (when one is installed) and returns err
// unchanged, so adapter error returns wrap in place: return markErr(ctx, err).
// Every error a public adapter method returns goes through this — no
// filtering here; the method table classifies.
func markErr(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if mark, ok := ctx.Value(errorMarkKey{}).(*ErrorMark); ok {
		mark.err.Store(&err)
	}
	return err
}
