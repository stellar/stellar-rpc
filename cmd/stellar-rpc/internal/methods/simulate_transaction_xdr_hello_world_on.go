//go:build xdr_hello_world

package methods

import (
	"errors"

	"github.com/stellar/go-stellar-sdk/xdr"
)

func simulateTransactionFootprintForXdrHelloWorld(opBody xdr.OperationBody) bool {
	switch opBody.Type {
	case xdr.OperationTypeHelloWorld:
		return true
	default:
		return false
	}
}

func validateAuthModeForXdrHelloWorld(opBody xdr.OperationBody, authModeRef *string) (error, bool) {
	switch opBody.Type {
	case xdr.OperationTypeHelloWorld:
		if *authModeRef != "" {
			return errors.New("cannot set authMode with non-InvokeHostFunction operations"), true
		}
		return nil, true
	default:
		return nil, false
	}
}
