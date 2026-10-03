package bench

import (
	"context"
	"math/rand/v2"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"

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

// Every query type reads under a live context. The ledgers and txpage scans
// stop with the context error once the context is done.
func TestQueryRequests(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	plan := queryPlan{
		Types:            allQueryTypes,
		LedgersSpan:      defaultLedgersSpan,
		TxPageSpan:       defaultTxPageSpan,
		TxPageLimit:      defaultTxPageLimit,
		EventsLimit:      defaultEventsLimit,
		NotFoundFraction: 0.5,
		Passphrase:       network.PublicNetworkPassphrase,
		Seed:             defaultSeed,
		TxHashPoolSize:   defaultTxHashPoolSize,
		Settings:         map[string]string{},
	}
	ds, release, err := openHotDataset(testLogger(), hotQueryOptions{HotRoot: hotRoot, Chunk: 0, Plan: plan})
	require.NoError(t, err)
	defer release()
	assert.Equal(t, chunk.ID(0).FirstLedger()+2*eventEvery-1, ds.LastLedger, "--sample-ledgers 0 keeps every ledger")

	for _, qtype := range allQueryTypes {
		t.Run(qtype, func(t *testing.T) {
			req, err := newQueryRequest(context.Background(), testLogger(), ds, plan, qtype)
			require.NoError(t, err)
			rng := rand.New(rand.NewPCG(defaultSeed, defaultSeed))
			outcomes := map[lookupOutcome]int{}
			events := 0
			for range 40 {
				timing, err := req(context.Background(), rng)
				require.NoError(t, err)
				switch qtype {
				case queryTypeLedgers:
					assert.Equal(t, defaultLedgersSpan, timing.items)
				case queryTypeTxHash:
					switch timing.outcome {
					case outcomeFound:
						assert.Equal(t, 1, timing.items)
					case outcomeNotFound:
						assert.Equal(t, 0, timing.items)
					default:
						t.Fatalf("txhash outcome %v", timing.outcome)
					}
					outcomes[timing.outcome]++
				case queryTypeEvents:
					assert.LessOrEqual(t, timing.items, plan.EventsLimit)
					events += timing.items
				}
			}
			if qtype == queryTypeTxHash {
				assert.Positive(t, outcomes[outcomeFound])
				assert.Positive(t, outcomes[outcomeNotFound])
			}
			if qtype == queryTypeEvents {
				assert.Positive(t, events)
			}
		})
	}

	for _, qtype := range []string{queryTypeLedgers, queryTypeTxPage} {
		t.Run(qtype+" stops on cancel", func(t *testing.T) {
			req, err := newQueryRequest(context.Background(), testLogger(), ds, plan, qtype)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err = req(ctx, rand.New(rand.NewPCG(defaultSeed, defaultSeed)))
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}

// errCounter is a context that is never done and counts its Err calls.
// txPageRequest calls Err once for each ledger the scan yields, so the count is
// the number of ledgers it read.
type errCounter struct {
	calls atomic.Int32
}

func (*errCounter) Deadline() (time.Time, bool) { return time.Time{}, false }

func (*errCounter) Done() <-chan struct{} { return nil }

func (c *errCounter) Err() error {
	c.calls.Add(1)
	return nil
}

func (*errCounter) Value(any) any { return nil }

// A txhash request fails when the lookup outcome differs from the pool's
// expectation.
func TestTxHashRequestOutcomeMismatch(t *testing.T) {
	ds, release, err := openHotDataset(testLogger(), hotQueryOptions{
		HotRoot: ingestHotChunk(t), Chunk: 0,
		Plan: queryPlan{Types: []string{queryTypeTxHash}, Passphrase: network.PublicNetworkPassphrase},
	})
	require.NoError(t, err)
	defer release()

	pool := &txHashPool{hashes: [][32]byte{{0xff}}, ledgerCount: 1}
	_, err = txHashRequest(ds, pool)(context.Background(), rand.New(rand.NewPCG(defaultSeed, defaultSeed)))
	require.ErrorContains(t, err, "found=false, expected true")
}

// A txpage request ends at the ledger that fills the page and reads no ledger
// after it.
func TestTxPageStopsAtFullPage(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	// The dataset holds two ledgers; only the first carries a transaction.
	plan := queryPlan{TxPageSpan: 2, TxPageLimit: 1, Passphrase: network.PublicNetworkPassphrase}
	ds, release, err := openHotDataset(testLogger(), hotQueryOptions{
		HotRoot: hotRoot, Chunk: 0, SampleLedgers: 2, Plan: plan,
	})
	require.NoError(t, err)
	defer release()
	require.Equal(t, chunk.ID(0).FirstLedger()+1, ds.LastLedger)

	ctx := &errCounter{}
	timing, err := txPageRequest(ds, plan)(ctx, rand.New(rand.NewPCG(defaultSeed, defaultSeed)))
	require.NoError(t, err)
	assert.Equal(t, 1, timing.items)
	assert.Equal(t, int32(1), ctx.calls.Load(), "the scan reads only the ledger that fills the page")
}

// --sample-ledgers at or above the ingested span keeps every committed ledger.
func TestOpenHotDatasetSampleLedgers(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	plan := queryPlan{Passphrase: network.PublicNetworkPassphrase}
	committed := chunk.ID(0).FirstLedger() + 2*eventEvery - 1
	for _, sample := range []uint32{2 * eventEvery, 2*eventEvery + 1} {
		ds, release, err := openHotDataset(testLogger(), hotQueryOptions{
			HotRoot: hotRoot, Chunk: 0, SampleLedgers: sample, Plan: plan,
		})
		require.NoError(t, err)
		assert.Equal(t, committed, ds.LastLedger, "--sample-ledgers %d", sample)
		release()
	}
}

// pickRange returns a span-long range inside the dataset's range, clamped to
// the range when the span is wider.
func TestPickRange(t *testing.T) {
	ds := &queryDataset{FirstLedger: 10, LastLedger: 14}
	rng := rand.New(rand.NewPCG(1, 1))
	for _, span := range []uint32{1, 4, 5, 6} {
		for range 100 {
			lo, hi := ds.pickRange(rng, span)
			require.GreaterOrEqual(t, lo, ds.FirstLedger, "span %d", span)
			require.LessOrEqual(t, hi, ds.LastLedger, "span %d", span)
			require.Equal(t, min(span, 5), hi-lo+1, "span %d", span)
			if span >= 5 {
				require.Equal(t, ds.FirstLedger, lo, "span %d", span)
			}
		}
	}
}
