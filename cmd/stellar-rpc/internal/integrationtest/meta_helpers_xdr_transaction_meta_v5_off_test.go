//go:build !xdr_transaction_meta_v5

package integrationtest

import "github.com/stellar/go-stellar-sdk/xdr"

func extractSorobanFeesFromMetaForXdrTransactionMetaV5(_ xdr.TransactionMeta) (xdr.SorobanTransactionMetaExtV1, bool) {
	return xdr.SorobanTransactionMetaExtV1{}, false
}

func extractReturnValueForXdrTransactionMetaV5(_ xdr.TransactionMeta) (xdr.ScVal, bool) {
	return xdr.ScVal{}, false
}
