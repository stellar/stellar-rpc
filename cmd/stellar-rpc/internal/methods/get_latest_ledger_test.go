package methods

import (
	"context"
	"errors"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerbucketwindow"
)

const (
	expectedLatestLedgerSequence        uint32        = 960
	expectedLatestLedgerProtocolVersion uint32        = 20
	expectedLatestLedgerHashBytes       byte          = 42
	expectedLatestLedgerCloseTime       xdr.TimePoint = 125
)

type ConstantLedgerReader struct{}

func (ledgerReader *ConstantLedgerReader) GetLatestLedgerSequence(_ context.Context) (uint32, error) {
	return expectedLatestLedgerSequence, nil
}

func (ledgerReader *ConstantLedgerReader) GetLedgerRange(_ context.Context) (ledgerbucketwindow.LedgerRange, error) {
	return ledgerbucketwindow.LedgerRange{}, nil
}

func (ledgerReader *ConstantLedgerReader) NewTx(_ context.Context) (db.LedgerReaderTx, error) {
	return nil, errors.New("mock NewTx error")
}

func (ledgerReader *ConstantLedgerReader) GetLedger(_ context.Context,
	sequence uint32,
) (xdr.LedgerCloseMeta, bool, error) {
	return createLedger(expectedLatestLedgerHashBytes,
			sequence,
			expectedLatestLedgerProtocolVersion,
			expectedLatestLedgerCloseTime),
		true, nil
}

func (ledgerReader *ConstantLedgerReader) StreamAllLedgers(_ context.Context, _ db.StreamLedgerFn) error {
	return nil
}

func (ledgerReader *ConstantLedgerReader) StreamLedgerRange(
	_ context.Context,
	_ uint32,
	_ uint32,
	_ db.StreamLedgerFn,
) error {
	return nil
}

func MakeTxSet() xdr.GeneralizedTransactionSet {
	txset := xdr.GeneralizedTransactionSet{
		V: 1,
		V1TxSet: &xdr.TransactionSetV1{
			PreviousLedgerHash: xdr.Hash{},
			Phases:             nil,
		},
	}
	return txset
}

func MakeLedgerHeader(ledgerSequence uint32, protocolVersion uint32, closeTime xdr.TimePoint) xdr.LedgerHeader {
	header := xdr.LedgerHeader{
		LedgerSeq:     xdr.Uint32(ledgerSequence),
		LedgerVersion: xdr.Uint32(protocolVersion),
		ScpValue: xdr.StellarValue{
			CloseTime: closeTime,
			TxSetHash: xdr.Hash{},
			Upgrades:  nil,
		},
	}
	return header
}

func createLedger(hash byte, ledgerSeq uint32, protocolVersion uint32, closeTime xdr.TimePoint) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Hash:   xdr.Hash{hash},
				Header: MakeLedgerHeader(ledgerSeq, protocolVersion, closeTime),
			},
			TxSet:        MakeTxSet(), // minimal empty
			TxProcessing: nil,
			Ext:          xdr.LedgerCloseMetaExt{},
		},
	}
}

func TestGetLatestLedger(t *testing.T) {
	getLatestLedgerHandler := NewGetLatestLedgerHandler(&ConstantLedgerReader{})
	latestLedgerRespI, err := getLatestLedgerHandler(context.Background(), &jrpc2.Request{})
	require.NoError(t, err)
	require.IsType(t, protocol.GetLatestLedgerResponse{}, latestLedgerRespI)
	latestLedgerResp, ok := latestLedgerRespI.(protocol.GetLatestLedgerResponse)
	require.True(t, ok)

	expectedLedger := createLedger(expectedLatestLedgerHashBytes,
		expectedLatestLedgerSequence,
		expectedLatestLedgerProtocolVersion,
		expectedLatestLedgerCloseTime)

	var receivedHeader xdr.LedgerHeader
	err = xdr.SafeUnmarshalBase64(latestLedgerResp.LedgerHeader, &receivedHeader)
	require.NoError(t, err, "error unmarshaling received ledger header: %v", err)
	var receivedMetadata xdr.LedgerCloseMeta
	err = xdr.SafeUnmarshalBase64(latestLedgerResp.LedgerMetadata, &receivedMetadata)
	require.NoError(t, err, "error unmarshaling received ledger metadata: %v", err)

	assert.Equal(t, expectedLedger.LedgerHash().HexString(), latestLedgerResp.Hash)
	// Header check ensures sequence, protocol version, and close time match
	assert.Equal(t, expectedLedger.V1.LedgerHeader.Header, receivedHeader)
	assert.Equal(t, expectedLedger, receivedMetadata)
}
