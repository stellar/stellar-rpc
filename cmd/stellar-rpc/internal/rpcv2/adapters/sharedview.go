package adapters

import (
	"context"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
)

type sharedViewKey struct{}

// sharedView is the per-request read-view holder WithSharedView plants in the
// request context. It starts empty; the first adapter call that needs a view
// acquires it, and every later call in the same request reads through that same
// view — one snapshot per request, so a response never mixes two states of the
// serving world (e.g. a transaction looked up in one snapshot with a ledger
// range from a newer one). Like the ReadView it holds, it serves one request on
// one goroutine — no locking.
type sharedView struct {
	view *query.ReadView
}

// WithSharedView returns ctx carrying an empty holder, plus the release the
// serving wrapper must call after the handler returns:
//
//	ctx, release := adapters.WithSharedView(ctx)
//	result, err := handler(ctx, req)
//	release()
//
// release is a no-op if no adapter call ever acquired a view.
func WithSharedView(ctx context.Context) (context.Context, func()) {
	h := &sharedView{}
	return context.WithValue(ctx, sharedViewKey{}, h), func() {
		if h.view != nil {
			h.view.Release()
		}
	}
}

// acquireView returns the view an adapter call should read through. When ctx
// carries a holder, that is the request's shared view (acquired on first use)
// and the returned release is a no-op — WithSharedView's release owns it. When
// ctx carries none (adapters used outside the serving wrapper, e.g. direct use
// in tests), the call gets its own fresh view and release is that view's
// Release. Callers defer the returned release either way.
func acquireView(ctx context.Context, registry *query.Registry) (*query.ReadView, func(), error) {
	h, ok := ctx.Value(sharedViewKey{}).(*sharedView)
	if ok && h.view != nil {
		return h.view, func() {}, nil
	}
	view, err := registry.NewReadView()
	if err != nil {
		return nil, nil, err
	}
	if ok {
		h.view = view
		return view, func() {}, nil
	}
	return view, view.Release, nil
}
