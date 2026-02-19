//go:build xdr_transaction_meta_v5

package integrationtest

import "github.com/stellar/go-stellar-sdk/xdr"

func extractSorobanFeesFromMetaForXdrTransactionMetaV5(meta xdr.TransactionMeta) (xdr.SorobanTransactionMetaExtV1, bool) {
	switch meta.V {
	case 5:
		return *meta.V5.SorobanMeta.Ext.V1, true
	default:
		return xdr.SorobanTransactionMetaExtV1{}, false
	}
}

func extractReturnValueForXdrTransactionMetaV5(meta xdr.TransactionMeta) (xdr.ScVal, bool) {
	switch meta.V {
	case 5:
		return *meta.V5.SorobanMeta.ReturnValue, true
	default:
		return xdr.ScVal{}, false
	}
}
