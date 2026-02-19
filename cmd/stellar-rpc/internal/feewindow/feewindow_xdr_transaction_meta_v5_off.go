//go:build !xdr_transaction_meta_v5

package feewindow

import "github.com/stellar/go-stellar-sdk/xdr"

func extractSorobanFeesForXdrTransactionMetaV5(_ xdr.TransactionMeta) (xdr.SorobanTransactionMetaExtV1, bool) {
	return xdr.SorobanTransactionMetaExtV1{}, false
}
