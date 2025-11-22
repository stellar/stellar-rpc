package methods

import (
	"encoding/json"

	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
)

func transactionToJSON(tx db.Transaction) (
	[]byte,
	[]byte,
	[]byte,
	error,
) {
	var err error
	var result, resultMeta, envelope []byte

	result, err = xdr2json.ConvertBytes(xdr.TransactionResult{}, tx.Result)
	if err != nil {
		return result, envelope, resultMeta, err
	}

	envelope, err = xdr2json.ConvertBytes(xdr.TransactionEnvelope{}, tx.Envelope)
	if err != nil {
		return result, envelope, resultMeta, err
	}

	resultMeta, err = xdr2json.ConvertBytes(xdr.TransactionMeta{}, tx.Meta)
	if err != nil {
		return result, envelope, resultMeta, err
	}

	return result, envelope, resultMeta, nil
}

func ledgerToJSON(chunk *db.LedgerMetadataChunk) ([]byte, []byte, error) {
	var err error
	var closeMetaJSON, headerJSON []byte

	closeMetaJSON, err = xdr2json.ConvertBytes(xdr.LedgerCloseMeta{}, chunk.Lcm)
	if err != nil {
		return nil, nil, err
	}

	headerJSON, err = xdr2json.ConvertInterface(chunk.Header)
	if err != nil {
		return nil, nil, err
	}

	return closeMetaJSON, headerJSON, nil
}

func jsonifySlice(xdr any, values [][]byte) ([]json.RawMessage, error) {
	result := make([]json.RawMessage, len(values))
	var err error

	for i, value := range values {
		result[i], err = xdr2json.ConvertBytes(xdr, value)
		if err != nil {
			return result, err
		}
	}

	return result, nil
}

// helper function to jsonify slices of slices like ContractEvents
func jsonifySliceOfSlices(xdr any, values [][][]byte) ([][]json.RawMessage, error) {
	jsonResult := make([][]json.RawMessage, 0, len(values))
	for _, slice := range values {
		convertedSlice, err := jsonifySlice(xdr, slice)
		if err != nil {
			return nil, err
		}
		jsonResult = append(jsonResult, convertedSlice)
	}
	return jsonResult, nil
}
