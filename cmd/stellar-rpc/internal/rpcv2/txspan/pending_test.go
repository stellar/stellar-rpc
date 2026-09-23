package txspan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestStartBuildMatchesBuild(t *testing.T) {
	raw := lcmBytes(t, 2, 500, classicTx, sorobanTx, feeBumpTx, v0Tx, classicTx)
	txParts, err := ingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)

	want, err := Build(raw, txParts, passphrase)
	require.NoError(t, err)

	p := StartBuild(raw, passphrase)
	p.Provide(txParts)
	got, err := p.Join()
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestJoinBeforeProvideDoesNotBlock(t *testing.T) {
	raw := lcmBytes(t, 2, 501, classicTx, classicTx)
	p := StartBuild(raw, passphrase)

	got, err := p.Join()
	require.ErrorIs(t, err, ErrNoTxParts)
	assert.Nil(t, got)
}

func TestDiscardUnblocksABuildWaitingForTxParts(t *testing.T) {
	raw := lcmBytes(t, 2, 502, classicTx, classicTx)
	p := StartBuild(raw, passphrase)

	// A caller whose own walk failed never provides; Discard must still return.
	p.Discard()
	p.Discard() // idempotent

	got, err := p.Join()
	require.NoError(t, err)
	assert.Nil(t, got, "a consumed pending yields nothing")
}

func TestDiscardAfterProvideJoinsTheBuild(t *testing.T) {
	raw := lcmBytes(t, 2, 503, classicTx, feeBumpTx)
	txParts, err := ingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)

	p := StartBuild(raw, passphrase)
	p.Provide(txParts)
	p.Discard()

	got, err := p.Join()
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestJoinIsIdempotent(t *testing.T) {
	raw := lcmBytes(t, 2, 504, classicTx)
	txParts, err := ingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)

	p := StartBuild(raw, passphrase)
	p.Provide(txParts)
	first, err := p.Join()
	require.NoError(t, err)
	require.NotEmpty(t, first)

	second, err := p.Join()
	require.NoError(t, err)
	assert.Nil(t, second)

	p.Discard() // no-op after a join
}

// TestJoinReportsAPreparationFailure pins that a ledger the preparation half
// rejects still reports its error through Join, with Provide having happened.
func TestJoinReportsAPreparationFailure(t *testing.T) {
	raw := lcmBytes(t, 2, 505, classicTx)
	txParts, err := ingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)

	p := StartBuild(unknownVersionLCM(raw), passphrase)
	p.Provide(txParts)
	got, err := p.Join()
	require.ErrorIs(t, err, ErrUnsupportedLedger)
	assert.Nil(t, got)
}

// TestDiscardAfterAPreparationFailure pins the other order: the build already
// stopped on its own, and Discard must not wait for a hand-off.
func TestDiscardAfterAPreparationFailure(t *testing.T) {
	p := StartBuild(unknownVersionLCM(lcmBytes(t, 2, 506, classicTx)), passphrase)
	p.Discard()
}
