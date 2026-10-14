package eventsapi

import (
	"context"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/eventquery"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
)

// ViewQuerier serves queryEvents from the query.ReadView the request
// pipeline attached to the context.
type ViewQuerier struct{}

func (ViewQuerier) Begin(ctx context.Context) (eventquery.Session, error) {
	view, err := query.ViewFrom(ctx)
	if err != nil {
		return nil, err
	}
	return viewSession{view}, nil
}

// viewSession adapts a ReadView. Close is a no-op: the view's lifetime is
// the request's, owned by the pipeline that attached it.
type viewSession struct{ *query.ReadView }

func (viewSession) Close() error { return nil }
