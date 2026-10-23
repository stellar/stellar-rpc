package bench

import (
	"context"
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/creachadair/jrpc2/handler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// ingestHotChunk writes 2×eventEvery ledgers of chunk 0 into a hot database
// and returns its --hot-dir.
func ingestHotChunk(t *testing.T) string {
	t.Helper()
	const numLedgers = 2 * eventEvery
	chunkID := chunk.ID(0)
	packDir, _ := writeSourcePack(t, t.TempDir(), chunkID, numLedgers)
	hotRoot := t.TempDir()
	require.NoError(t, runHot(context.Background(), testLogger(), hotOptions{
		Source:     sourceConfig{Kind: sourcePack, PackDir: packDir},
		StartChunk: chunkID,
		NumChunks:  1,
		NumLedgers: numLedgers,
		HotRoot:    hotRoot,
		OutDir:     filepath.Join(t.TempDir(), "csv"),
	}))
	return hotRoot
}

// testPlan is a plan with the default request flags and the given format.
func testPlan(format string) queryPlan {
	return queryPlan{
		Types:            allQueryTypes,
		LedgersLimit:     defaultLedgersLimit,
		TxPageLimit:      defaultTxPageLimit,
		EventsLimit:      defaultEventsLimit,
		NotFoundFraction: 0.5,
		TxHashPoolSize:   defaultTxHashPoolSize,
		Format:           format,
		Passphrase:       network.PublicNetworkPassphrase,
		Seed:             defaultSeed,
	}
}

// Every query type answers in both formats under a live context, reports both
// timings and its outcome, and stops with the context error once the context
// is done.
func TestQueryRequests(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	ds, release, err := openDataset(testLogger(), queryOptions{
		DataDir: hotRoot, Plan: testPlan(protocol.FormatBase64),
	})
	require.NoError(t, err)
	defer release()
	assert.Equal(t, chunk.ID(0).FirstLedger()+2*eventEvery-1, ds.LastLedger, "--sample-ledgers 0 keeps every ledger")

	for _, format := range queryFormats {
		plan := testPlan(format)
		for _, qtype := range allQueryTypes {
			t.Run(qtype+" "+format, func(t *testing.T) {
				req, err := newQueryRequest(context.Background(), testLogger(), ds, plan, qtype, map[string]string{})
				require.NoError(t, err)
				rng := rand.New(rand.NewPCG(defaultSeed, defaultSeed))
				outcomes := map[lookupOutcome]int{}
				events := 0
				for range 40 {
					timing, err := req(context.Background(), rng)
					require.NoError(t, err)
					assert.Positive(t, timing.handlerLatency)
					assert.LessOrEqual(t, timing.handlerLatency, timing.latency)
					outcomes[timing.outcome]++
					switch qtype {
					case queryTypeLedgers:
						assert.Equal(t, defaultLedgersLimit, timing.items)
					case queryTypeTxHash:
						assert.Equal(t, map[lookupOutcome]int{outcomeFound: 1, outcomeNotFound: 0}[timing.outcome],
							timing.items)
					case queryTypeEvents:
						assert.LessOrEqual(t, timing.items, defaultEventsLimit)
						events += timing.items
					}
				}
				switch qtype {
				case queryTypeTxHash:
					assert.Positive(t, outcomes[outcomeFound])
					assert.Positive(t, outcomes[outcomeNotFound])
					assert.Zero(t, outcomes[outcomeNone])
				case queryTypeEvents:
					assert.Positive(t, outcomes[outcomeUnfiltered])
					assert.Zero(t, outcomes[outcomeNone])
				default:
					assert.Equal(t, 40, outcomes[outcomeNone])
				}
				if qtype == queryTypeEvents {
					assert.Positive(t, events)
				}
			})
		}
	}
	for _, qtype := range allQueryTypes {
		t.Run(qtype+" stops on cancel", func(t *testing.T) {
			req, err := newQueryRequest(
				context.Background(), testLogger(), ds, testPlan(protocol.FormatBase64), qtype, map[string]string{})
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err = req(ctx, rand.New(rand.NewPCG(defaultSeed, defaultSeed)))
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}

// A txpage request returns at most --txpage-limit transactions.
func TestTxPageRequestLimit(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	ds, release, err := openDataset(testLogger(), queryOptions{
		DataDir: hotRoot, Plan: testPlan(protocol.FormatBase64),
	})
	require.NoError(t, err)
	defer release()
	// The fixture's transactions are in its first and its 101st ledger.
	ds.LastLedger = ds.FirstLedger
	for limit, want := range map[uint32]int{1: 1, defaultTxPageLimit: 2} {
		plan := testPlan(protocol.FormatBase64)
		plan.TxPageLimit = limit
		timing, err := txPageRequest(ds, plan)(context.Background(), rand.New(rand.NewPCG(defaultSeed, defaultSeed)))
		require.NoError(t, err)
		assert.Equal(t, want, timing.items, "--txpage-limit %d", limit)
	}
}

// --sample-ledgers ends the handlers' range: the latest ledger is the sample's
// last, and a txpage request reads no ledger past it.
func TestHotDatasetSampleBoundsHandlers(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	ds, release, err := openDataset(testLogger(), queryOptions{
		DataDir: hotRoot, SampleLedgers: 2, Plan: testPlan(protocol.FormatBase64),
	})
	require.NoError(t, err)
	defer release()
	require.Equal(t, chunk.ID(0).FirstLedger()+1, ds.LastLedger)

	var latest protocol.GetLatestLedgerResponse
	mustCallResult(t, ds.server, protocol.GetLatestLedgerMethodName, nil, &latest)
	assert.Equal(t, ds.LastLedger, latest.Sequence)

	ds.LastLedger = ds.FirstLedger
	timing, err := txPageRequest(ds, testPlan(protocol.FormatBase64))(
		context.Background(), rand.New(rand.NewPCG(defaultSeed, defaultSeed)))
	require.NoError(t, err)
	assert.Equal(t, 1, timing.items, "the transaction in the 101st ledger is past the sample")
}

// --sample-ledgers at or above the ingested span keeps every committed ledger.
func TestOpenHotDatasetSampleLedgers(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	plan := queryPlan{Passphrase: network.PublicNetworkPassphrase}
	committed := chunk.ID(0).FirstLedger() + 2*eventEvery - 1
	for _, sample := range []uint32{2 * eventEvery, 2*eventEvery + 1} {
		ds, release, err := openDataset(testLogger(), queryOptions{
			DataDir: hotRoot, SampleLedgers: sample, Plan: plan,
		})
		require.NoError(t, err)
		assert.Equal(t, committed, ds.LastLedger, "--sample-ledgers %d", sample)
		release()
	}
}

// pickStart returns a start whose span-long read stays inside the dataset's
// range, and the range's start when the span is wider.
func TestPickStart(t *testing.T) {
	ds := &queryDataset{FirstLedger: 10, LastLedger: 14}
	rng := rand.New(rand.NewPCG(1, 1))
	for _, span := range []uint32{1, 4, 5, 6} {
		for range 100 {
			lo := ds.pickStart(rng, span)
			require.GreaterOrEqual(t, lo, ds.FirstLedger, "span %d", span)
			require.LessOrEqual(t, lo+min(span, 5)-1, ds.LastLedger, "span %d", span)
			if span >= 5 {
				require.Equal(t, ds.FirstLedger, lo, "span %d", span)
			}
		}
	}
}

// A ledgers limit wider than the dataset reads the whole dataset and stays
// flagged as a fixed read range.
func TestLedgersLimitWiderThanDataset(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	plan := testPlan(protocol.FormatBase64)
	plan.Types = []string{queryTypeLedgers}
	ds, release, err := openDataset(testLogger(), queryOptions{
		DataDir: hotRoot, SampleLedgers: 5, Plan: plan,
	})
	require.NoError(t, err)
	defer release()
	require.Greater(t, plan.LedgersLimit, ds.LastLedger-ds.FirstLedger+1)

	timing, err := ledgersRequest(ds, plan)(context.Background(), rand.New(rand.NewPCG(defaultSeed, defaultSeed)))
	require.NoError(t, err)
	assert.Equal(t, 5, timing.items)

	settings := map[string]string{}
	warnFixedReadRange(testLogger(), ds, plan, settings)
	assert.Equal(t, queryTypeLedgers, settings["fixedReadRange"])
}

// A txhash request fails when the lookup outcome differs from the pool's
// expectation.
func TestTxHashRequestOutcomeMismatch(t *testing.T) {
	plan := testPlan(protocol.FormatBase64)
	ds, release, err := openDataset(testLogger(), queryOptions{DataDir: ingestHotChunk(t), Plan: plan})
	require.NoError(t, err)
	defer release()

	pool := &txHashPool{hashes: [][32]byte{{0xff}}}
	_, err = txHashRequest(ds, plan, pool)(context.Background(), rand.New(rand.NewPCG(defaultSeed, defaultSeed)))
	require.ErrorContains(t, err, "found=false, expected true")
}

// A txhash request fails when the pool expects not found and the handler finds
// the transaction.
func TestTxHashRequestUnexpectedFound(t *testing.T) {
	ds := &queryDataset{server: newTestReadServer(t, handler.Map{
		protocol.GetTransactionMethodName: fixedHandler(
			protocol.GetTransactionResponse{TransactionDetails: protocol.TransactionDetails{
				Status: protocol.TransactionStatusSuccess,
			}}, nil),
	})}
	pool := &txHashPool{hashes: [][32]byte{{0xff}}, notFoundFraction: 1}
	_, err := txHashRequest(ds, testPlan(protocol.FormatBase64), pool)(
		t.Context(), rand.New(rand.NewPCG(defaultSeed, defaultSeed)))
	require.ErrorContains(t, err, "found=true, expected false")
}

// An events request under the no_match set fails when queryEvents returns
// events.
func TestEventsRequestNoMatchReturnsEvents(t *testing.T) {
	ds, _ := stubEventsDataset(t, func(protocol.QueryEventsRequest) protocol.QueryEventsResponse {
		return protocol.QueryEventsResponse{Events: make([]protocol.EventInfo, 1)}
	})
	pool := &eventFilterPool{sets: []eventFilterSet{{
		outcome: outcomeNoMatch,
		filters: []protocol.QueryEventsFilter{{ContractID: "busy", Topic0: []byte(`"t"`)}},
	}}}
	_, err := eventsRequest(ds, testPlan(protocol.FormatBase64), pool)(
		t.Context(), rand.New(rand.NewPCG(defaultSeed, defaultSeed)))
	require.ErrorContains(t, err, "no_match filter")
	require.ErrorContains(t, err, "returned 1 events")
}
