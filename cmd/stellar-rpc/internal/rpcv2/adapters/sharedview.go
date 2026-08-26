package adapters

import (
	"context"
	"errors"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
)

type viewKey struct{}

// errNoView means an adapter call ran outside the serving wrapper — nothing
// else installs the request's read view.
var errNoView = errors.New("adapters: context carries no read view; the serving wrapper installs one per request")

// WithView returns ctx carrying the request's read view. The serving wrapper
// acquires one view per request, installs it here before calling the handler,
// and releases it after the handler returns. Every adapter call in the request
// reads through that one view, so a response never mixes two states of the
// serving world (e.g. a transaction looked up in one snapshot with a ledger
// range from a newer one). Like the ReadView it carries, the context serves
// one request on one goroutine.
func WithView(ctx context.Context, view *query.ReadView) context.Context {
	return context.WithValue(ctx, viewKey{}, view)
}

// ViewFrom returns the view WithView installed, or errNoView. It is exported
// for the getEventsV2 handler. That handler is not an adapter, but it takes
// its view from the request context the same way.
func ViewFrom(ctx context.Context) (*query.ReadView, error) {
	view, ok := ctx.Value(viewKey{}).(*query.ReadView)
	if !ok {
		return nil, errNoView
	}
	return view, nil
}
