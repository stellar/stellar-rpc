package views_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/views"
)

// TestExtractTxHashes_MatchesStructPath cross-checks that the view-based
// ExtractTxHashes returns the same hashes, in the same apply order, as
// the struct path (lcm.CountTransactions / lcm.TransactionHash), each
// paired with the given ledgerSeq.
func TestExtractTxHashes_MatchesStructPath(t *testing.T) {
	const seq = 9001

	// A few txs with assorted metas; tx-hash extraction is independent
	// of meta contents, so any mix exercises the TxProcessing walk.
	metas := []xdr.TransactionMeta{
		txMetaWithOpEvents([][]xdr.ContractEvent{{buildContractEvent("a")}}),
		{V: 1, V1: &xdr.TransactionMetaV1{}},
		txMetaWithV3SorobanEvents([]xdr.ContractEvent{buildContractEvent("b")}),
	}
	lcm := buildLCM(t, seq, 1_700_000_777, metas)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)

	got, err := views.ExtractTxHashes(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)

	n := lcm.CountTransactions()
	require.Len(t, got, n, "tx count differs")

	for i := 0; i < n; i++ {
		want := lcm.TransactionHash(i)
		assert.Equal(t, [32]byte(want), got[i].Hash, "hash mismatch at tx %d", i)
		assert.Equal(t, uint32(seq), got[i].LedgerSeq, "ledgerSeq mismatch at tx %d", i)
	}
}

// TestExtractTxHashes_V0 exercises the LCM V0 dispatch arm (case 0),
// which uses a plain TransactionSet (not GeneralizedTransactionSet) and
// TransactionResultMeta. ExtractTxHashes supports V0 even though
// ExtractEvents does not. Parity is asserted against lcm.TransactionHash.
func TestExtractTxHashes_V0(t *testing.T) {
	const seq = 9101
	metas := []xdr.TransactionMeta{
		txMetaWithOpEvents([][]xdr.ContractEvent{{buildContractEvent("v0a")}}),
		{V: 1, V1: &xdr.TransactionMetaV1{}},
		txMetaWithV3SorobanEvents([]xdr.ContractEvent{buildContractEvent("v0b")}),
	}
	lcm := buildLCMV0(t, seq, 1_700_001_010, metas)
	require.Equal(t, int32(0), lcm.V, "fixture must be LCM V0")
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)

	got, err := views.ExtractTxHashes(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)

	n := lcm.CountTransactions()
	require.Len(t, got, n, "tx count differs")
	for i := 0; i < n; i++ {
		want := lcm.TransactionHash(i)
		assert.Equal(t, [32]byte(want), got[i].Hash, "hash mismatch at tx %d", i)
		assert.Equal(t, uint32(seq), got[i].LedgerSeq, "ledgerSeq mismatch at tx %d", i)
	}
}

// TestExtractTxHashes_V1 exercises the LCM V1 dispatch arm (case 1),
// LedgerCloseMetaV1 + the non-V1 TransactionResultMeta type. Parity is
// asserted against lcm.TransactionHash.
func TestExtractTxHashes_V1(t *testing.T) {
	const seq = 9102
	metas := []xdr.TransactionMeta{
		txMetaWithOpEvents([][]xdr.ContractEvent{{buildContractEvent("v1a")}}),
		{V: 2, V2: &xdr.TransactionMetaV2{}},
		txMetaWithV3SorobanEvents([]xdr.ContractEvent{buildContractEvent("v1b")}),
	}
	lcm := buildLCMVersion(t, 1, seq, 1_700_001_020, metas)
	require.Equal(t, int32(1), lcm.V, "fixture must be LCM V1")
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)

	got, err := views.ExtractTxHashes(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)

	n := lcm.CountTransactions()
	require.Len(t, got, n, "tx count differs")
	for i := 0; i < n; i++ {
		want := lcm.TransactionHash(i)
		assert.Equal(t, [32]byte(want), got[i].Hash, "hash mismatch at tx %d", i)
		assert.Equal(t, uint32(seq), got[i].LedgerSeq, "ledgerSeq mismatch at tx %d", i)
	}
}

// TestExtractTxHashes_Empty checks a zero-tx ledger yields no entries.
func TestExtractTxHashes_Empty(t *testing.T) {
	lcm := buildLCM(t, 9002, 1_700_000_888, nil)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	got, err := views.ExtractTxHashes(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	assert.Empty(t, got)
}

// BenchmarkExtractTxHashes measures the view extractor over a
// representative multi-tx LCM.
func BenchmarkExtractTxHashes(b *testing.B) {
	var metas []xdr.TransactionMeta
	for t := 0; t < 32; t++ {
		metas = append(metas, txMetaWithOpEvents([][]xdr.ContractEvent{{buildContractEvent("x")}}))
	}
	lcm := buildLCM(b, 5002, 1_700_002_000, metas)
	raw, err := lcm.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	view := xdr.LedgerCloseMetaView(raw)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := views.ExtractTxHashes(view); err != nil {
			b.Fatal(err)
		}
	}
}
