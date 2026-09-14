package query

import (
	"context"
	"errors"
)

type viewKey struct{}

// ErrNoView means a read ran outside the serving wrapper, which is the only
// thing that installs a request's read view.
var ErrNoView = errors.New("query: context carries no read view; the serving wrapper installs one per request")

// WithView returns ctx carrying the request's read view. The serving wrapper
// acquires one view per request, installs it here before calling the handler,
// and releases it after the handler returns. Every read in the request goes
// through that one view, so a response never mixes two states of the serving
// world (e.g. a transaction looked up in one snapshot with a ledger range
// from a newer one). Like the ReadView it carries, the context serves one
// request on one goroutine.
func WithView(ctx context.Context, view *ReadView) context.Context {
	return context.WithValue(ctx, viewKey{}, view)
}

// ViewFrom returns the view WithView installed, or ErrNoView.
func ViewFrom(ctx context.Context) (*ReadView, error) {
	view, ok := ctx.Value(viewKey{}).(*ReadView)
	if !ok {
		return nil, ErrNoView
	}
	return view, nil
}
