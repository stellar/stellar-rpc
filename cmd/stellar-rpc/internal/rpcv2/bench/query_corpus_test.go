package bench

import (
	"context"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/adapters"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
)

// TestCorpusSpansManyLedgersOnADenseDataset pins the property the per-ledger cap
// exists for. On a dataset whose ledgers each carry more transactions than the
// cap, the pool still covers corpusTargetHashes/corpusMaxHashesPerLedger
// ledgers: without the cap it fills from the first few ledgers it reads, and
// every found lookup of the run then resolves against one of those few blobs.
func TestCorpusSpansManyLedgersOnADenseDataset(t *testing.T) {
	const txPerLedger = 4 * corpusMaxHashesPerLedger
	f, release := openDenseHotFixture(t, 64, txPerLedger)
	defer release()

	view, err := f.view()
	require.NoError(t, err)
	defer view.Release()

	s := newTxHashSampler(testRNG())
	require.NoError(t, s.sampleChunk(view, f.Chunks[0], f.FirstLedger, f.LastLedger))

	assert.GreaterOrEqual(t, len(s.hashes), corpusTargetHashes, "the pool did not fill")
	assert.GreaterOrEqual(t, len(s.ledgers), corpusTargetHashes/corpusMaxHashesPerLedger,
		"the pool covers too few ledgers")

	perLedger := hashesPerLedger(t, f, view, s.hashes)
	for seq, n := range perLedger {
		assert.LessOrEqual(t, n, corpusMaxHashesPerLedger, "ledger %d is over the per-ledger cap", seq)
	}
	assert.Len(t, perLedger, len(s.ledgers), "ledgers lists exactly the ledgers that contributed")
	assert.Len(t, uniqueHashes(s.hashes), len(s.hashes), "a hash is sampled at most once")
}

// TestCorpusPairsEachHashWithItsLedger checks the sampler's bookkeeping against
// the served by-hash path: every hash resolves to a ledger the sampler listed,
// and the ledgers it listed are exactly the ones the hashes resolve to. The
// draw picks hashes out of apply order, so a hash taken from one ledger and
// recorded against another would otherwise go unnoticed.
func TestCorpusPairsEachHashWithItsLedger(t *testing.T) {
	f, release := openDenseHotFixture(t, 64, 4*corpusMaxHashesPerLedger)
	defer release()

	view, err := f.view()
	require.NoError(t, err)
	defer view.Release()

	s := newTxHashSampler(testRNG())
	require.NoError(t, s.sampleChunk(view, f.Chunks[0], f.FirstLedger, f.LastLedger))
	require.NotEmpty(t, s.hashes)

	resolved := hashesPerLedger(t, f, view, s.hashes)
	seqs := make([]uint32, 0, len(resolved))
	for seq := range resolved {
		seqs = append(seqs, seq)
	}
	assert.ElementsMatch(t, s.ledgers, seqs, "the pool's hashes come from the ledgers the sampler listed")
}

// TestCorpusTakesEveryHashOfALedgerBelowTheCap pins what the cap does not do. A
// dataset whose ledgers hold fewer transactions than the cap contributes every
// one of them, and fills the pool by reading more ledgers instead.
func TestCorpusTakesEveryHashOfALedgerBelowTheCap(t *testing.T) {
	const txPerLedger = 4
	f, release := openDenseHotFixture(t, 256, txPerLedger)
	defer release()

	view, err := f.view()
	require.NoError(t, err)
	defer view.Release()

	s := newTxHashSampler(testRNG())
	require.NoError(t, s.sampleChunk(view, f.Chunks[0], f.FirstLedger, f.LastLedger))

	assert.GreaterOrEqual(t, len(s.hashes), corpusTargetHashes, "the pool did not fill")
	for seq, n := range hashesPerLedger(t, f, view, s.hashes) {
		assert.Equal(t, txPerLedger, n, "ledger %d did not contribute its whole transaction set", seq)
	}
	assert.Len(t, uniqueHashes(s.hashes), len(s.hashes), "a hash is sampled at most once")
}

// TestCorpusSkipsLedgersWithoutTransactions pins that a ledger carrying no
// transaction contributes nothing and counts as no coverage, so a dataset of
// mostly empty ledgers cannot report a wider corpus than it has.
func TestCorpusSkipsLedgersWithoutTransactions(t *testing.T) {
	const numLedgers = 400
	packDir, txLedgers := writeSourcePack(t, t.TempDir(), chunk.ID(0), numLedgers)
	f, release := openHotFixtureOverPack(t, packDir, numLedgers)
	defer release()

	view, err := f.view()
	require.NoError(t, err)
	defer view.Release()

	s := newTxHashSampler(testRNG())
	require.NoError(t, s.sampleChunk(view, f.Chunks[0], f.FirstLedger, f.LastLedger))

	require.NotEmpty(t, s.hashes)
	assert.LessOrEqual(t, len(s.ledgers), txLedgers, "only tx-bearing ledgers may contribute")
	assert.Len(t, s.hashes, len(s.ledgers), "each of these ledgers holds one transaction")
	for _, seq := range s.ledgers {
		assert.Zero(t, (seq-f.FirstLedger)%eventEvery, "ledger %d carries no transaction", seq)
	}
}

// TestSampleHashesFromLedgerDrawsARandomSubset pins the per-ledger draw: a
// random subset of the cap's size, not the ledger's opening transactions, and
// the whole set when the ledger holds fewer than the cap.
func TestSampleHashesFromLedgerDrawsARandomSubset(t *testing.T) {
	parts := make([]sdkingest.LedgerTxParts, 64)
	for i := range parts {
		parts[i].Hash[0] = byte(i)
	}

	first := sampleHashesFromLedger(rand.New(rand.NewPCG(1, 1)), parts)
	second := sampleHashesFromLedger(rand.New(rand.NewPCG(2, 2)), parts)
	require.Len(t, first, corpusMaxHashesPerLedger)
	require.Len(t, second, corpusMaxHashesPerLedger)
	assert.NotEqual(t, first, second, "two seeds must draw different subsets")
	assert.Len(t, uniqueHashes(first), len(first), "a draw takes each transaction at most once")

	inOrder := make([][32]byte, 0, corpusMaxHashesPerLedger)
	for _, p := range parts[:corpusMaxHashesPerLedger] {
		inOrder = append(inOrder, p.Hash)
	}
	assert.NotEqual(t, inOrder, first, "the draw is not the ledger's first transactions")

	few := parts[:3]
	whole := [][32]byte{few[0].Hash, few[1].Hash, few[2].Hash}
	assert.ElementsMatch(t, whole, sampleHashesFromLedger(testRNG(), few))
}

// TestCorpusLogsItsLedgerCoverage pins what the corpus build reports about
// itself: the hash count, the ledger count and the ledger range it spans, plus
// a warning when the whole pool came from one ledger, which makes every found
// lookup of the run read that ledger.
func TestCorpusLogsItsLedgerCoverage(t *testing.T) {
	t.Run("many ledgers", func(t *testing.T) {
		f, release := openDenseHotFixture(t, 8, 20)
		defer release()

		info, warnings := buildCorpusCapturingLogs(t, f)
		assert.Contains(t, info, "hashes over 8 ledgers spanning 2..9")
		assert.Empty(t, warnings)
	})

	t.Run("one ledger", func(t *testing.T) {
		f, release := openDenseHotFixture(t, 1, 20)
		defer release()

		info, warnings := buildCorpusCapturingLogs(t, f)
		assert.Contains(t, info, "hashes over 1 ledgers spanning 2..2")
		require.Len(t, warnings, 1)
		assert.Contains(t, warnings[0], "came from ledger 2 alone")
	})
}

// buildCorpusCapturingLogs builds the tx-hash corpus over f and returns the
// coverage line it logged and every warning it raised.
func buildCorpusCapturingLogs(t *testing.T, f *queryFixture) (string, []string) {
	t.Helper()
	logger := testLogger()
	done := logger.StartTest(logrus.InfoLevel)
	_, err := buildTxHashCorpus(context.Background(), logger, f, 0.1, defaultSeed)
	entries := done()
	require.NoError(t, err)

	var coverage string
	var warnings []string
	for _, e := range entries {
		if e.Level == logrus.WarnLevel {
			warnings = append(warnings, e.Message)
		}
		if strings.HasPrefix(e.Message, "txhash corpus:") {
			coverage = e.Message
		}
	}
	require.NotEmpty(t, coverage, "the corpus build logs its coverage")
	return coverage, warnings
}

// TestVerifySampledHashReportsThePassphrase pins which of the two checks reports a
// failure, since they send the operator to different places. A passphrase the
// dataset was not signed under fails the envelope pairing, inside the very
// ledger the hash was sampled from, and names --network-passphrase. A fixture
// whose tx-hash index cannot resolve the hash pairs fine and fails the served
// probe, which must not name the passphrase.
func TestVerifySampledHashReportsThePassphrase(t *testing.T) {
	f, release := openDenseHotFixture(t, 4, 4)
	defer release()

	view, err := f.view()
	require.NoError(t, err)
	defer view.Release()

	s := newTxHashSampler(testRNG())
	require.NoError(t, s.sampleChunk(view, f.Chunks[0], f.FirstLedger, f.LastLedger))
	require.NotEmpty(t, s.hashes)
	hash, seq := s.first()

	t.Run("the configured passphrase resolves it", func(t *testing.T) {
		require.NoError(t, verifySampledHashResolves(context.Background(), view, f, hash, seq))
	})

	t.Run("wrong passphrase", func(t *testing.T) {
		wrong := *f
		wrong.Passphrase = network.TestNetworkPassphrase
		err := verifySampledHashResolves(context.Background(), view, &wrong, hash, seq)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--network-passphrase")
		assert.NotContains(t, err.Error(), "probe of a known transaction hash failed")
	})
}

// TestVerifySampledHashReportsAProbeFailure covers the other side of the split:
// a cold fixture whose tx-hash window index is gone still pairs the envelope,
// because the passphrase is right, and fails on the served lookup. The report
// must name the probe and leave the passphrase out of it.
func TestVerifySampledHashReportsAProbeFailure(t *testing.T) {
	chunkID := chunk.ID(0)
	coldRoot := ingestColdChunk(t, chunkID)
	require.NoError(t, os.RemoveAll(geometry.NewLayout(coldRoot).TxHashIndexRoot()))

	// The types exclude txhash, so the open only warns about the absent index
	// and leaves the fixture to be probed here.
	f, release, err := openColdFixture(testLogger(), coldQueryOptions{
		ColdRoot:   coldRoot,
		StartChunk: chunkID,
		NumChunks:  1,
		Plan:       queryPlan{Types: []string{queryTypeLedgers}, Passphrase: network.PublicNetworkPassphrase},
	})
	require.NoError(t, err)
	defer release()

	view, err := f.view()
	require.NoError(t, err)
	defer view.Release()

	s := newTxHashSampler(testRNG())
	require.NoError(t, s.sampleChunk(view, chunkID, f.FirstLedger, f.LastLedger))
	require.NotEmpty(t, s.hashes)
	hash, seq := s.first()

	err = verifySampledHashResolves(context.Background(), view, f, hash, seq)
	require.ErrorContains(t, err, "probe of a known transaction hash failed")
	assert.Contains(t, err.Error(), "tx-hash index may be missing")
	assert.NotContains(t, err.Error(), "--network-passphrase")
}

// hashesPerLedger resolves every hash through the served by-hash path and
// counts how many of them landed in each ledger.
func hashesPerLedger(
	t *testing.T, f *queryFixture, view *query.ReadView, hashes [][32]byte,
) map[uint32]int {
	t.Helper()
	reader := adapters.NewTransactionReader(f.Passphrase, nil)
	ctx := adapters.WithView(context.Background(), view)
	counts := map[uint32]int{}
	for _, h := range hashes {
		tx, err := reader.GetTransaction(ctx, xdr.Hash(h))
		require.NoError(t, err, "sampled hash %x does not resolve", h)
		counts[tx.Ledger.Sequence]++
	}
	return counts
}

// uniqueHashes returns the distinct hashes of the slice.
func uniqueHashes(hashes [][32]byte) map[[32]byte]struct{} {
	set := make(map[[32]byte]struct{}, len(hashes))
	for _, h := range hashes {
		set[h] = struct{}{}
	}
	return set
}

// openDenseHotFixture ingests a pack whose ledgers each carry txPerLedger
// transactions into a hot database and opens the query fixture over it.
func openDenseHotFixture(t *testing.T, numLedgers uint32, txPerLedger int) (*queryFixture, func()) {
	t.Helper()
	packDir := writeDenseSourcePack(t, t.TempDir(), chunk.ID(0), numLedgers, txPerLedger)
	return openHotFixtureOverPack(t, packDir, numLedgers)
}

// openHotFixtureOverPack ingests numLedgers ledgers of chunk 0 from packDir
// into a fresh hot database and returns the query fixture over it, with the
// passphrase the fixture builders sign under.
func openHotFixtureOverPack(t *testing.T, packDir string, numLedgers uint32) (*queryFixture, func()) {
	t.Helper()
	chunkID := chunk.ID(0)
	hotRoot := t.TempDir()
	require.NoError(t, runHot(context.Background(), testLogger(), hotOptions{
		Source:     sourceConfig{Kind: sourcePack, PackDir: packDir},
		StartChunk: chunkID,
		NumChunks:  1,
		NumLedgers: numLedgers,
		HotRoot:    hotRoot,
		OutDir:     filepath.Join(t.TempDir(), "csv"),
	}))
	f, release, err := openHotFixture(testLogger(), hotQueryOptions{
		HotRoot: hotRoot,
		Chunk:   chunkID,
		Plan:    queryPlan{Passphrase: network.PublicNetworkPassphrase},
	})
	require.NoError(t, err)
	return f, release
}
