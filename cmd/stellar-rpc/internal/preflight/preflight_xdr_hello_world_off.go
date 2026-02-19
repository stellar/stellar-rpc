//go:build !xdr_hello_world

package preflight

import "context"

func getPreflightForXdrHelloWorld(_ context.Context, _ Parameters) (Preflight, error, bool) {
	return Preflight{}, nil, false
}
