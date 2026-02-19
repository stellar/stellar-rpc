//go:build xdr_transaction_meta_v5

package feewindow

import "github.com/stellar/go-stellar-sdk/xdr"

func extractSorobanFeesForXdrTransactionMetaV5(meta xdr.TransactionMeta) (xdr.SorobanTransactionMetaExtV1, bool) {
	switch meta.V {
	case 5:
		if meta.V5.SorobanMeta == nil || meta.V5.SorobanMeta.Ext.V != 1 {
			return xdr.SorobanTransactionMetaExtV1{}, false
		}
		return *meta.V5.SorobanMeta.Ext.V1, true
	default:
		return xdr.SorobanTransactionMetaExtV1{}, false
	}
}
