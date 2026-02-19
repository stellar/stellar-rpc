//go:build !xdr_hello_world

package methods

import "github.com/stellar/go-stellar-sdk/xdr"

func simulateTransactionFootprintForXdrHelloWorld(_ xdr.OperationBody) bool {
	return false
}

func validateAuthModeForXdrHelloWorld(_ xdr.OperationBody, _ *string) (error, bool) {
	return nil, false
}
