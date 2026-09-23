package txspan

import (
	"bytes"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// passphrase is the public network's, which every fixture and every ledger
// pack these tests read was produced against. The tx hashes that pair
// envelopes to apply-order elements depend on it.
const passphrase = network.PublicNetworkPassphrase

// ledgerCheck is what one differential run observed about a ledger.
type ledgerCheck struct {
	table      []byte
	lcmVersion uint8
	txs        int
	feeBumps   int
	// innerHits counts fee-bump transactions resolved by their INNER hash, the
	// path a four-byte prefix reaches through a second index entry.
	innerHits int
	soroban   int
	// lookups holds one duration per Lookup call the run made.
	lookups []time.Duration
}

// checkLedger is the differential the fixture tests run: every transaction of
// the ledger is checked against BOTH decode-and-walk entry points.
func checkLedger(t *testing.T, raw []byte) ledgerCheck {
	t.Helper()
	return checkLedgerWithin(t, raw, math.MaxInt)
}

// checkLedgerWithin proves two things about the table Build produced for a
// ledger: the spans point at exactly the bytes the decode-and-walk path hands
// back, and Lookup over those spans returns exactly the view that path
// produces — for every transaction, under both of a fee-bump's hashes.
//
// Every transaction is compared against LedgerTransactionViewRange's view of
// it. byHashBudget caps how many are ALSO compared against
// LedgerTransactionViewByHash, which is the entry point the read path
// replaces: that call walks the whole ledger, so checking every transaction of
// a six-thousand-transaction ledger is quadratic. Both SDK paths assemble a
// view the same way from the same parts, so a sample spread across the ledger
// pins them to each other while the per-transaction comparison stays linear.
func checkLedgerWithin(t *testing.T, raw []byte, byHashBudget int) ledgerCheck {
	t.Helper()
	lcm := xdr.LedgerCloseMetaView(raw)
	txParts, err := ingest.ExtractLedgerTxParts(lcm)
	require.NoError(t, err)
	views, err := ingest.LedgerTransactionViewRange(lcm, 0, 0, passphrase)
	require.NoError(t, err)
	require.Len(t, views, len(txParts))
	encoded, err := Build(raw, txParts, passphrase)
	require.NoError(t, err)
	tbl, err := Parse(encoded)
	require.NoError(t, err)

	header, err := ReadLedgerHeader(raw)
	require.NoError(t, err)
	// The table's two stamps are pairing checks, so the differential pins them
	// against the ledger's own header the way every read does.
	require.Equal(t, uint8(header.LCMVersion), tbl.LCMVersion())
	require.Equal(t, header.LedgerSeq, tbl.LedgerSeq())
	require.Equal(t, len(views), tbl.TxCount())
	assert.Zero(t, tbl.FrameCount())
	assertIndexSorted(t, tbl)

	out := ledgerCheck{table: encoded, lcmVersion: tbl.LCMVersion(), txs: len(views)}
	stride := byHashStride(len(views), byHashBudget)
	for k := range views {
		checkSpans(t, raw, tbl, k, views[k])
		// A fee-bump is always compared against the by-hash walk too: both of
		// its hashes must reach one view, and fee-bumps are rare enough on
		// every corpus that checking them all stays linear.
		alsoByHash := txParts[k].FeeBump || k%stride == 0
		out.lookups = append(out.lookups,
			checkLookup(t, raw, tbl, lcm, views[k], views[k].Hash, k, alsoByHash))
		if txParts[k].FeeBump {
			out.feeBumps++
			out.innerHits++
			out.lookups = append(out.lookups,
				checkLookup(t, raw, tbl, lcm, views[k], txParts[k].InnerHash, k, true))
		}
		if isSoroban(t, views[k].Envelope) {
			out.soroban++
		}
	}
	require.Equal(t, len(views)+out.feeBumps, tbl.IndexCount())
	assertElementsTile(t, tbl)
	return out
}

// checkSpans proves apply-order row k points at transaction k's two pieces and
// that the index routes its hash to k.
func checkSpans(t *testing.T, raw []byte, tbl Table, k int, view ingest.LedgerTransactionView) {
	t.Helper()
	row := tbl.Row(k)
	assert.Equal(t, view.Envelope, raw[row.EnvStart:row.EnvEnd], "envelope span for %x", view.Hash)
	checkElementBytes(t, raw, tbl, row, view)

	match := matchFor(t, raw, tbl, view.Hash)
	assert.Equal(t, k, match.ApplyIdx, "apply index for %x", view.Hash)
	assert.Equal(t, row, match.Row, "row reached through the index for %x", view.Hash)
}

// checkLookup runs the served differential for one hash: Lookup over the table
// must return exactly what decoding the ledger and walking it returns. The
// whole view is compared rather than a chosen list of fields, so a field added
// to the SDK's view cannot slip through uncompared. byHash additionally pins
// the by-hash walk — the entry point the read path replaces — to the range
// walk's view of the same transaction.
func checkLookup(
	t *testing.T, raw []byte, tbl Table, lcm xdr.LedgerCloseMetaView,
	want ingest.LedgerTransactionView, hash [32]byte, applyIdx int, byHash bool,
) time.Duration {
	t.Helper()
	if byHash {
		walked, found, err := ingest.LedgerTransactionViewByHash(lcm, hash, passphrase)
		require.NoError(t, err)
		require.True(t, found, "the by-hash walk does not find %x", hash)
		require.Equal(t, want, walked, "the two walks disagree on %x", hash)
	}

	start := time.Now()
	got, found, err := Lookup(raw, tbl, hash, passphrase)
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.True(t, found, "Lookup does not find %x", hash)

	require.Equal(t, want, got, "Lookup and the walk disagree on %x", hash)
	assert.Equal(t, int32(applyIdx)+1, got.ApplicationOrder, "application order for %x", hash)
	return elapsed
}

// byHashStride spreads budget by-hash comparisons across n transactions; it is
// never zero, so the modulo below is always defined.
func byHashStride(n, budget int) int {
	if budget <= 0 || n <= budget {
		return 1
	}
	return (n + budget - 1) / budget
}

// matchFor returns the one index match under hash whose element carries hash in
// its result pair — the disambiguation a reader performs, since a four-byte
// prefix collides and a fee-bump is routed under two prefixes.
func matchFor(t *testing.T, raw []byte, tbl Table, hash [32]byte) Match {
	t.Helper()
	candidates := slices.Collect(tbl.Find(hash))
	require.NotEmpty(t, candidates, "no index entry for %x", hash)
	var matched []Match
	for _, m := range candidates {
		if bytes.Equal(hashSlot(raw, tbl, m.Row), hash[:]) {
			matched = append(matched, m)
		}
	}
	require.Len(t, matched, 1, "entries whose element carries %x", hash)
	return matched[0]
}

// checkElementBytes parses the element the row points at with the view type the
// LCM version puts in txProcessing, and matches its pieces against the walk's.
// The sized element must be exactly the span, which is what proves deriving an
// element's end from its successor's start is sound.
func checkElementBytes(t *testing.T, raw []byte, tbl Table, row Row, view ingest.LedgerTransactionView) {
	t.Helper()
	elem := raw[row.ElemStart:row.ElemEnd]
	var (
		pair  xdr.TransactionResultPairView
		meta  xdr.TransactionMetaView
		sized []byte
		err   error
	)
	switch tbl.LCMVersion() {
	case 1:
		v := xdr.TransactionResultMetaView(elem)
		pair, err = v.Result()
		require.NoError(t, err)
		meta, err = v.TxApplyProcessing()
		require.NoError(t, err)
		sized, err = v.Raw()
	case 2:
		v := xdr.TransactionResultMetaV1View(elem)
		pair, err = v.Result()
		require.NoError(t, err)
		meta, err = v.TxApplyProcessing()
		require.NoError(t, err)
		sized, err = v.Raw()
	default:
		t.Fatalf("unexpected LCM version %d", tbl.LCMVersion())
	}
	require.NoError(t, err)
	assert.Len(t, sized, len(elem), "element span is not the element's exact extent")

	result, err := pair.Result()
	require.NoError(t, err)
	resultRaw, err := result.Raw()
	require.NoError(t, err)
	assert.Equal(t, view.Result, resultRaw)

	metaRaw, err := meta.Raw()
	require.NoError(t, err)
	assert.Equal(t, view.Meta, metaRaw)
}

// hashSlot returns the 32 bytes the element's result pair opens with, past the
// extension point the LCM version puts at the head of an element.
func hashSlot(raw []byte, tbl Table, row Row) []byte {
	off := row.ElemStart + uint32(tbl.ExtBytes())
	return raw[off : off+hashLen]
}

// assertElementsTile checks that the element spans cover the txProcessing array
// without gap or overlap, which is what lets a build size one element instead
// of all of them.
func assertElementsTile(t *testing.T, tbl Table) {
	t.Helper()
	for k := 0; k+1 < tbl.TxCount(); k++ {
		require.Equal(t, tbl.Row(k).ElemEnd, tbl.Row(k+1).ElemStart,
			"element %d does not abut element %d", k, k+1)
	}
}

// isSoroban mirrors the walk's soroban test: the transaction's Ext union
// discriminant is 1, read through the fee-bump's inner transaction when there
// is one. TX_V0 predates Soroban.
func isSoroban(t *testing.T, envelope []byte) bool {
	t.Helper()
	env := xdr.TransactionEnvelopeView(envelope)
	typ, err := env.Type()
	require.NoError(t, err)

	var v1 xdr.TransactionV1EnvelopeView
	switch typ {
	case xdr.EnvelopeTypeEnvelopeTypeTx:
		v1, err = env.V1()
		require.NoError(t, err)
	case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
		fb, ferr := env.FeeBump()
		require.NoError(t, ferr)
		fbTx, ferr := fb.Tx()
		require.NoError(t, ferr)
		inner, ferr := fbTx.InnerTx()
		require.NoError(t, ferr)
		v1, err = inner.V1()
		require.NoError(t, err)
	default:
		return false
	}
	tx, err := v1.Tx()
	require.NoError(t, err)
	ext, err := tx.Ext()
	require.NoError(t, err)
	disc, err := ext.V()
	require.NoError(t, err)
	return disc == 1
}
