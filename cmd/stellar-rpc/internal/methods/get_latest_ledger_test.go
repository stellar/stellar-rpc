package methods

import (
	"context"
	"errors"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerbucketwindow"
	"github.com/stellar/stellar-rpc/protocol"
)

const (
	expectedLatestLedgerSequence        uint32 = 960
	expectedLatestLedgerProtocolVersion uint32 = 21
	expectedLatestLedgerHashBytes       byte   = 42
)

type ConstantLedgerEntryReader struct{}

type ConstantLedgerEntryReaderTx struct{}

type ConstantLedgerReader struct{}

func (ledgerReader *ConstantLedgerReader) GetLedgerRange(_ context.Context) (ledgerbucketwindow.LedgerRange, error) {
	return ledgerbucketwindow.LedgerRange{}, nil
}

func (ledgerReader *ConstantLedgerReader) NewTx(_ context.Context) (db.LedgerReaderTx, error) {
	return nil, errors.New("mock NewTx error")
}

func (entryReader *ConstantLedgerEntryReader) GetLatestLedgerSequence(_ context.Context) (uint32, error) {
	return expectedLatestLedgerSequence, nil
}

func (entryReader *ConstantLedgerEntryReader) NewTx(_ context.Context, _ bool) (db.LedgerEntryReadTx, error) {
	return ConstantLedgerEntryReaderTx{}, nil
}

func (entryReaderTx ConstantLedgerEntryReaderTx) GetLatestLedgerSequence() (uint32, error) {
	return expectedLatestLedgerSequence, nil
}

func (entryReaderTx ConstantLedgerEntryReaderTx) GetLedgerEntries(_ ...xdr.LedgerKey) ([]db.LedgerKeyAndEntry, error) {
	return nil, nil
}

func (entryReaderTx ConstantLedgerEntryReaderTx) Done() error {
	return nil
}

func (ledgerReader *ConstantLedgerReader) GetLedger(_ context.Context,
	sequence uint32,
) (xdr.LedgerCloseMeta, bool, error) {
	return createLedger(sequence, expectedLatestLedgerProtocolVersion, expectedLatestLedgerHashBytes), true, nil
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

func createLedger(ledgerSequence uint32, protocolVersion uint32, hash byte) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Hash: xdr.Hash{hash},
				Header: xdr.LedgerHeader{
					LedgerSeq:     xdr.Uint32(ledgerSequence),
					LedgerVersion: xdr.Uint32(protocolVersion),
				},
			},
		},
	}
}

func TestGetLatestLedger(t *testing.T) {
	getLatestLedgerHandler := NewGetLatestLedgerHandler(&ConstantLedgerEntryReader{}, &ConstantLedgerReader{})
	latestLedgerRespI, err := getLatestLedgerHandler(context.Background(), &jrpc2.Request{})
	require.NoError(t, err)
	require.IsType(t, protocol.GetLatestLedgerResponse{}, latestLedgerRespI)
	latestLedgerResp, ok := latestLedgerRespI.(protocol.GetLatestLedgerResponse)
	require.True(t, ok)

	expectedLatestLedgerHashStr := xdr.Hash{expectedLatestLedgerHashBytes}.HexString()
	assert.Equal(t, expectedLatestLedgerHashStr, latestLedgerResp.Hash)

	assert.Equal(t, expectedLatestLedgerProtocolVersion, latestLedgerResp.ProtocolVersion)
	assert.Equal(t, expectedLatestLedgerSequence, latestLedgerResp.Sequence)
}
