package bench

import (
	"cmp"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
)

// The txhash pool takes one hash from each sampled ledger, so a pool over
// ledgers that each hold several transactions spans as many ledgers as hashes.
func TestBuildTxHashPoolTakesOneHashPerLedger(t *testing.T) {
	const numLedgers, txPerLedger, size = 20, 4, 8
	hotRoot, ledgerOf := ingestMultiTxHotChunk(t, numLedgers, txPerLedger)
	ds, release, err := openDataset(testLogger(), queryOptions{
		DataDir: hotRoot,
		Plan:    queryPlan{Types: []string{queryTypeTxHash}, Passphrase: network.PublicNetworkPassphrase},
	})
	require.NoError(t, err)
	defer release()

	pool, err := buildTxHashPool(context.Background(), testLogger(), ds, 0, defaultSeed, size)
	require.NoError(t, err)
	require.Len(t, pool.hashes, size)
	ledgers := map[uint32]struct{}{}
	for _, h := range pool.hashes {
		seq, ok := ledgerOf[h]
		require.True(t, ok, "hash %x is not in the fixture", h)
		ledgers[seq] = struct{}{}
	}
	assert.Len(t, ledgers, size)
}

// The same seed builds the same txhash pool.
func TestBuildTxHashPoolIsDeterministic(t *testing.T) {
	hotRoot, _ := ingestMultiTxHotChunk(t, 20, 4)
	ds, release, err := openDataset(testLogger(), queryOptions{
		DataDir: hotRoot,
		Plan:    queryPlan{Types: []string{queryTypeTxHash}, Passphrase: network.PublicNetworkPassphrase},
	})
	require.NoError(t, err)
	defer release()

	a, err := buildTxHashPool(context.Background(), testLogger(), ds, 0, defaultSeed, 8)
	require.NoError(t, err)
	b, err := buildTxHashPool(context.Background(), testLogger(), ds, 0, defaultSeed, 8)
	require.NoError(t, err)
	assert.Equal(t, a.hashes, b.hashes)
}

// ingestMultiTxHotChunk writes numLedgers ledgers of chunk 0, each holding
// txPerLedger transactions, into a hot database. It returns the --hot-dir and
// the ledger of each transaction hash.
func ingestMultiTxHotChunk(t *testing.T, numLedgers uint32, txPerLedger int) (string, map[[32]byte]uint32) {
	t.Helper()
	root := t.TempDir()
	layout := geometry.NewLayout(root)
	packPath := layout.LedgerPackPath(0)
	require.NoError(t, os.MkdirAll(filepath.Dir(packPath), 0o755))
	w, err := ledger.NewColdWriter(packPath, chunk.ID(0).FirstLedger(), ledger.ColdWriterOptions{})
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	ledgerOf := map[[32]byte]uint32{}
	first := chunk.ID(0).FirstLedger()
	for seq := first; seq < first+numLedgers; seq++ {
		envelopes := make([]xdr.TransactionEnvelope, txPerLedger)
		processing := make([]xdr.TransactionResultMetaV1, txPerLedger)
		for i := range envelopes {
			envelopes[i] = xdr.TransactionEnvelope{
				Type: xdr.EnvelopeTypeEnvelopeTypeTx,
				V1: &xdr.TransactionV1Envelope{Tx: xdr.Transaction{
					SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
				}},
			}
			hash, err := network.HashTransactionInEnvelope(envelopes[i], network.PublicNetworkPassphrase)
			require.NoError(t, err)
			ledgerOf[hash] = seq
			processing[i] = xdr.TransactionResultMetaV1{
				TxApplyProcessing: xdr.TransactionMeta{V: 4, V4: &xdr.TransactionMetaV4{}},
				Result: xdr.TransactionResultPair{
					TransactionHash: hash,
					Result: xdr.TransactionResult{Result: xdr.TransactionResultResult{
						Code: xdr.TransactionResultCodeTxSuccess, Results: &[]xdr.OperationResult{},
					}},
				},
			}
		}
		require.NoError(t, w.AppendLedger(seq, rpcv2test.V2LCMBytes(t, seq, 0, envelopes, processing)))
	}
	require.NoError(t, w.Commit())

	hotRoot := t.TempDir()
	require.NoError(t, runHot(context.Background(), testLogger(), hotOptions{
		Source:     sourceConfig{Kind: sourcePack, PackDir: layout.LedgersRoot()},
		StartChunk: 0,
		NumChunks:  1,
		NumLedgers: numLedgers,
		HotRoot:    hotRoot,
		OutDir:     filepath.Join(t.TempDir(), "csv"),
	}))
	return hotRoot, ledgerOf
}

// A canceled context stops the txhash pool build.
func TestBuildTxHashPoolStopsOnCancel(t *testing.T) {
	ds, release, err := openDataset(testLogger(), queryOptions{
		DataDir: ingestHotChunk(t),
		Plan:    queryPlan{Types: []string{queryTypeTxHash}, Passphrase: network.PublicNetworkPassphrase},
	})
	require.NoError(t, err)
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = buildTxHashPool(ctx, testLogger(), ds, 0, defaultSeed, defaultTxHashPoolSize)
	require.ErrorIs(t, err, context.Canceled)
}

// A txhash pool build fails under the wrong passphrase.
func TestBuildTxHashPoolWrongPassphrase(t *testing.T) {
	ds, release, err := openDataset(testLogger(), queryOptions{
		DataDir: ingestHotChunk(t),
		Plan:    queryPlan{Types: []string{queryTypeTxHash}, Passphrase: network.TestNetworkPassphrase},
	})
	require.NoError(t, err)
	defer release()

	_, err = buildTxHashPool(context.Background(), testLogger(), ds, 0, defaultSeed, defaultTxHashPoolSize)
	require.ErrorContains(t, err, "a wrong --network-passphrase")
}

// A probe whose hash getTransaction does not find blames the tx-hash index, not
// the passphrase.
func TestProbeTxHashNotFound(t *testing.T) {
	ds := &queryDataset{server: newTestReadServer(t, handler.Map{
		protocol.GetTransactionMethodName: fixedHandler(
			protocol.GetTransactionResponse{TransactionDetails: protocol.TransactionDetails{
				Status: protocol.TransactionStatusNotFound,
			}}, nil),
	})}
	err := probeTxHash(t.Context(), ds, [32]byte{1}, 2)
	require.ErrorContains(t, err, "tx-hash index does not resolve it")
	assert.NotContains(t, err.Error(), "--network-passphrase")
}

// A canceled probe returns the context error without the passphrase hint.
func TestProbeTxHashCanceled(t *testing.T) {
	ds := &queryDataset{server: newTestReadServer(t, handler.Map{
		protocol.GetTransactionMethodName: fixedHandler(nil, context.Canceled),
	})}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := probeTxHash(ctx, ds, [32]byte{1}, 2)
	require.ErrorIs(t, err, context.Canceled)
	assert.NotContains(t, err.Error(), "--network-passphrase")
}

// A canceled context stops the events pool build.
func TestBuildEventFilterPoolStopsOnCancel(t *testing.T) {
	ds, release, err := openDataset(testLogger(), queryOptions{
		DataDir: ingestHotChunk(t),
		Plan:    queryPlan{Types: []string{queryTypeEvents}},
	})
	require.NoError(t, err)
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = buildEventFilterPool(ctx, testLogger(), ds, defaultSeed)
	require.ErrorIs(t, err, context.Canceled)
}

// On a fixture with one contract and one topic, the events pool holds the
// unfiltered, frequent_contract and frequent_pair sets, and each matches the
// fixture's events.
func TestBuildEventFilterPool(t *testing.T) {
	ds, release, err := openDataset(testLogger(), queryOptions{
		DataDir: ingestHotChunk(t),
		Plan:    queryPlan{Types: []string{queryTypeEvents}},
	})
	require.NoError(t, err)
	defer release()

	pool, err := buildEventFilterPool(context.Background(), testLogger(), ds, defaultSeed)
	require.NoError(t, err)
	assert.Equal(t, "unfiltered,frequent_contract,frequent_pair", pool.labels())
	assert.Nil(t, pool.sets[0].filters)
	assert.Equal(t, pool.sets[1].filters[0].ContractID, pool.sets[2].filters[0].ContractID)
	assert.NotEmpty(t, pool.sets[2].filters[0].Topic0)
	for _, set := range pool.sets {
		assert.Equal(t, 2, eventsMatched(t, ds, set.filters), set.outcome.label())
	}
}

// On a fixture with contracts of different frequency, the events pool also
// holds the rare_contract and no_match sets, and each set returns its expected
// events through the queryEvents handler.
func TestBuildEventFilterPoolRareAndNoMatch(t *testing.T) {
	ds, release := openEventsFixture(t)
	defer release()

	pool, err := buildEventFilterPool(context.Background(), testLogger(), ds, defaultSeed)
	require.NoError(t, err)
	assert.Equal(t, "unfiltered,frequent_contract,frequent_pair,rare_contract,no_match", pool.labels())
	busy, rare := fixtureContractID(t, 0xaa), fixtureContractID(t, 0xcc)
	want := map[lookupOutcome]struct {
		contract, topic string
		events          int
	}{
		outcomeUnfiltered:       {"", "", 10},
		outcomeFrequentContract: {busy, "", 6},
		outcomeFrequentPair:     {busy, "transfer", 6},
		outcomeRareContract:     {rare, "", 1},
		outcomeNoMatch:          {busy, "mint", 0},
	}
	for _, set := range pool.sets {
		w := want[set.outcome]
		if w.contract != "" {
			require.Len(t, set.filters, 1)
			assert.Equal(t, w.contract, set.filters[0].ContractID, set.outcome.label())
			if w.topic != "" {
				assert.JSONEq(t, topicJSON(t, w.topic), string(set.filters[0].Topic0), set.outcome.label())
			}
		}
		assert.Equal(t, w.events, eventsMatched(t, ds, set.filters), set.outcome.label())
	}
}

// noMatchFilter finds no filter when every candidate topic matches the
// contract.
func TestNoMatchFilterOmittedWhenEveryCandidateMatches(t *testing.T) {
	ds, release := openEventsFixture(t)
	defer release()

	busy := fixtureContractID(t, 0xaa)
	other := fixtureContractID(t, 0xbb)
	topic := topicBase64(t, "transfer")
	_, found, err := noMatchFilter(context.Background(), ds, busy, []eventTermPair{{other, topic}})
	require.NoError(t, err)
	assert.False(t, found)
}

// filterMatches follows a HAS_MORE cursor until a page holds an event or the
// scan completes.
func TestFilterMatchesFollowsCursor(t *testing.T) {
	for _, tc := range []struct {
		name         string
		secondEvents int
		secondStatus string
		wantMatches  bool
	}{
		{"event on the second page", 1, protocol.ScanStatusHasMore, true},
		{"scan completes empty", 0, protocol.ScanStatusComplete, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ds, reqs := stubEventsDataset(t, func(req protocol.QueryEventsRequest) protocol.QueryEventsResponse {
				if req.Cursor == "" {
					return protocol.QueryEventsResponse{ScanStatus: protocol.ScanStatusHasMore, Cursor: "next"}
				}
				return protocol.QueryEventsResponse{
					Events: make([]protocol.EventInfo, tc.secondEvents), ScanStatus: tc.secondStatus,
				}
			})
			matches, err := filterMatches(t.Context(), ds, protocol.QueryEventsFilter{ContractID: "c"})
			require.NoError(t, err)
			assert.Equal(t, tc.wantMatches, matches)
			require.Len(t, *reqs, 2)
			assert.Equal(t, "next", (*reqs)[1].Cursor)
		})
	}
}

// noMatchFilter probes at most noMatchProbes candidates, and a candidate at
// the bound is still found.
func TestNoMatchFilterProbeBound(t *testing.T) {
	candidates := func(n int) []eventTermPair {
		pairs := make([]eventTermPair, n)
		for i := range pairs {
			pairs[i] = eventTermPair{contract: "other", topic: "t" + strconv.Itoa(i)}
		}
		return pairs
	}
	matchUnless := func(topic string) func(protocol.QueryEventsRequest) protocol.QueryEventsResponse {
		return func(req protocol.QueryEventsRequest) protocol.QueryEventsResponse {
			rsp := protocol.QueryEventsResponse{ScanStatus: protocol.ScanStatusComplete}
			if string(req.Filters[0].Topic0) != strconv.Quote(topic) {
				rsp.Events = make([]protocol.EventInfo, 1)
			}
			return rsp
		}
	}

	ds, reqs := stubEventsDataset(t, matchUnless(""))
	_, found, err := noMatchFilter(t.Context(), ds, "busy", candidates(noMatchProbes+1))
	require.NoError(t, err)
	assert.False(t, found)
	assert.Len(t, *reqs, noMatchProbes)

	last := "t" + strconv.Itoa(noMatchProbes-1)
	ds, reqs = stubEventsDataset(t, matchUnless(last))
	filter, found, err := noMatchFilter(t.Context(), ds, "busy", candidates(noMatchProbes))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "busy", filter.ContractID)
	assert.Equal(t, strconv.Quote(last), string(filter.Topic0))
	assert.Len(t, *reqs, noMatchProbes)
}

// stubEventsDataset returns a dataset whose queryEvents handler answers with
// respond, and the requests the handler received.
func stubEventsDataset(
	t *testing.T, respond func(protocol.QueryEventsRequest) protocol.QueryEventsResponse,
) (*queryDataset, *[]protocol.QueryEventsRequest) {
	t.Helper()
	var reqs []protocol.QueryEventsRequest
	ds := &queryDataset{FirstLedger: 2, LastLedger: 100, server: newTestReadServer(t, handler.Map{
		protocol.QueryEventsMethodName: func(_ context.Context, r *jrpc2.Request) (any, error) {
			var req protocol.QueryEventsRequest
			if err := r.UnmarshalParams(&req); err != nil {
				return nil, err
			}
			reqs = append(reqs, req)
			return respond(req), nil
		},
	})}
	return ds, &reqs
}

// openEventsFixture opens a hot dataset whose last ledger holds six events of
// contract 0xaa and three of contract 0xbb under the first topic "transfer",
// and one event of contract 0xcc under "mint".
func openEventsFixture(t *testing.T) (*queryDataset, func()) {
	t.Helper()
	evs := make([]xdr.ContractEvent, 0, 10)
	for range 6 {
		evs = append(evs, rpcv2test.SymbolContractEvent(xdr.ContractId{0xaa}, "d", "transfer"))
	}
	for range 3 {
		evs = append(evs, rpcv2test.SymbolContractEvent(xdr.ContractId{0xbb}, "d", "transfer"))
	}
	evs = append(evs, rpcv2test.SymbolContractEvent(xdr.ContractId{0xcc}, "d", "mint"))

	const numLedgers = 10
	root := t.TempDir()
	layout := geometry.NewLayout(root)
	packPath := layout.LedgerPackPath(0)
	require.NoError(t, os.MkdirAll(filepath.Dir(packPath), 0o755))
	w, err := ledger.NewColdWriter(packPath, chunk.ID(0).FirstLedger(), ledger.ColdWriterOptions{})
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	first := chunk.ID(0).FirstLedger()
	for seq := first; seq < first+numLedgers-1; seq++ {
		require.NoError(t, w.AppendLedger(seq, rpcv2test.ZeroTxLCMBytes(t, seq)))
	}
	// Every sampled page reads the last ledger, so the sample counts each of
	// its events the same number of times.
	last := first + numLedgers - 1
	require.NoError(t, w.AppendLedger(last, rpcv2test.EventsLCMBytes(t, last, evs...)))
	require.NoError(t, w.Commit())

	hotRoot := t.TempDir()
	require.NoError(t, runHot(context.Background(), testLogger(), hotOptions{
		Source:     sourceConfig{Kind: sourcePack, PackDir: layout.LedgersRoot()},
		StartChunk: 0,
		NumChunks:  1,
		NumLedgers: numLedgers,
		HotRoot:    hotRoot,
		OutDir:     filepath.Join(t.TempDir(), "csv"),
	}))
	ds, release, err := openDataset(testLogger(), queryOptions{
		DataDir: hotRoot,
		Plan:    queryPlan{Types: []string{queryTypeEvents}},
	})
	require.NoError(t, err)
	return ds, release
}

// eventsMatched returns how many events of ds's range queryEvents returns
// under filters.
func eventsMatched(t *testing.T, ds *queryDataset, filters []protocol.QueryEventsFilter) int {
	t.Helper()
	limit := uint(defaultEventsLimit)
	timing, err := ds.server.call(context.Background(), protocol.QueryEventsMethodName, protocol.QueryEventsRequest{
		MinLedger: ds.FirstLedger, MaxLedger: ds.LastLedger, Filters: filters, Limit: &limit,
	})
	require.NoError(t, err)
	return timing.items
}

// fixtureContractID returns the strkey of the contract ID whose first byte is
// b and the rest zero.
func fixtureContractID(t *testing.T, b byte) string {
	t.Helper()
	raw := xdr.ContractId{b}
	id, err := strkey.Encode(strkey.VersionByteContract, raw[:])
	require.NoError(t, err)
	return id
}

// topicBase64 returns the base64 XDR of the symbol topic sym.
func topicBase64(t *testing.T, sym string) string {
	t.Helper()
	s := xdr.ScSymbol(sym)
	raw, err := xdr.MarshalBase64(xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &s})
	require.NoError(t, err)
	return raw
}

// topicJSON returns the queryEvents topic filter value of the symbol topic sym.
func topicJSON(t *testing.T, sym string) string {
	t.Helper()
	return `"` + topicBase64(t, sym) + `"`
}

// byDescendingCount orders keys by count, most frequent first, and ties by
// compare.
func TestByDescendingCount(t *testing.T) {
	got := byDescendingCount(map[string]int{"b": 2, "a": 2, "c": 5}, cmp.Compare[string])
	assert.Equal(t, []string{"c", "a", "b"}, got)
}
