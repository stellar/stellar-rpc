//go:build xdr_hello_world

package preflight

import (
	"context"
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"
)

func getPreflightForXdrHelloWorld(ctx context.Context, params Parameters) (Preflight, error, bool) {
	switch params.OpBody.Type {
	case xdr.OperationTypeHelloWorld:
		return getHelloWorldPreflight(ctx, params)
	default:
		return Preflight{}, nil, false
	}
}

func getHelloWorldPreflight(_ context.Context, params Parameters) (Preflight, error, bool) {
	return Preflight{}, fmt.Errorf("unsupported operation type: %s", params.OpBody.Type.String()), true
}
