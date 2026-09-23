package txspan_test

import (
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/txspan"
)

// Environment for the real-data differential: a cold ledger pack and the
// ledger window inside it to check, written "firstSeq-count".
//
// The pack must be one this lineage reads: its ledgers' HEADER sequences have
// to equal the sequences the pack resolves them at, because IterateLedgers
// checks that on every ledger it yields. A synthetic corpus that numbered its
// records independently of its headers will not run here any more — that is
// the point of the check, not an obstacle to it.
const (
	packEnv  = "STELLAR_RPC_DIFF_PACK"
	rangeEnv = "STELLAR_RPC_DIFF_RANGE"
)

// byHashBudget is how many of a ledger's transactions are also compared
// against LedgerTransactionViewByHash. That walk is O(ledger) per call, so on
// a six-thousand-transaction ledger checking every transaction would be
// quadratic; every transaction is still compared against the range walk, which
// assembles the same views.
const byHashBudget = 8

// TestRealLedgerPackDifferential runs the same differential the synthetic
// fixtures run over real ledgers, which is where the shapes the fixtures
// cannot build — parallel phases, several components, large TxSets — actually
// occur. It skips unless a pack is named, since no pack ships with the tree.
func TestRealLedgerPackDifferential(t *testing.T) {
	packPath, rangeSpec := os.Getenv(packEnv), os.Getenv(rangeEnv)
	if packPath == "" || rangeSpec == "" {
		t.Skipf("set %s and %s (firstSeq-count) to run the real-data differential", packEnv, rangeEnv)
	}
	first, last := parseRange(t, rangeSpec)

	reader, err := ledger.OpenColdReader(packPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	var sum diffSummary
	for entry, iterErr := range reader.IterateLedgers(first, last) {
		require.NoError(t, iterErr)
		sum.add(t, entry.Bytes, txspan.CheckLedger(t, entry.Bytes, byHashBudget))
	}
	require.Positive(t, sum.ledgers, "the pack yielded no ledgers")
	sum.report(t, packPath, first, last)
}

// diffSummary accumulates what the run saw, so the differential reports the
// shape of the data it actually proved itself against rather than asserting
// numbers no fixture can predict.
type diffSummary struct {
	ledgers, txs, feeBumps, innerHits, soroban int
	lcmVersions                                []uint8
	tableBytes                                 []int
	build, walk                                []time.Duration
	lookups                                    []time.Duration
}

func (s *diffSummary) add(t *testing.T, raw []byte, got txspan.LedgerCheck) {
	t.Helper()
	s.ledgers++
	s.txs += got.Txs
	s.feeBumps += got.FeeBumps
	s.innerHits += got.InnerHits
	s.soroban += got.Soroban
	if !slices.Contains(s.lcmVersions, got.LCMVersion) {
		s.lcmVersions = append(s.lcmVersions, got.LCMVersion)
	}
	s.tableBytes = append(s.tableBytes, got.TableBytes)
	s.lookups = append(s.lookups, got.Lookups...)

	lcm := xdr.LedgerCloseMetaView(raw)
	txParts, err := ingest.ExtractLedgerTxParts(lcm)
	require.NoError(t, err)

	// Build is timed without the TxProcessing walk that produced txParts: the
	// ingest loop already holds that output, so the marginal cost of a table is
	// what this measures.
	s.build = append(s.build, timeIt(t, func() error {
		_, buildErr := txspan.Build(raw, txParts, txspan.Passphrase)
		return buildErr
	}))
	s.walk = append(s.walk, timeIt(t, func() error {
		_, walkErr := ingest.LedgerTransactionViewRange(lcm, 0, 0, txspan.Passphrase)
		return walkErr
	}))
}

func (s *diffSummary) report(t *testing.T, packPath string, first, last uint32) {
	t.Helper()
	slices.Sort(s.lcmVersions)
	minBytes, avgBytes, maxBytes := spread(s.tableBytes)
	t.Logf("pack %s ledgers [%d, %d]", packPath, first, last)
	t.Logf("ledgers=%d txs=%d feeBumpTxs=%d innerHashLookups=%d sorobanTxs=%d lcmVersions=%v",
		s.ledgers, s.txs, s.feeBumps, s.innerHits, s.soroban, s.lcmVersions)
	t.Logf("table bytes per ledger: min=%d avg=%d max=%d", minBytes, avgBytes, maxBytes)
	t.Logf("Build: p50=%s max=%s", p50(s.build), maxOf(s.build))
	t.Logf("Lookup (%d calls): p50=%s max=%s", len(s.lookups), p50(s.lookups), maxOf(s.lookups))
	t.Logf("LedgerTransactionViewRange walk: p50=%s max=%s", p50(s.walk), maxOf(s.walk))
}

func timeIt(t *testing.T, fn func() error) time.Duration {
	t.Helper()
	start := time.Now()
	err := fn()
	elapsed := time.Since(start)
	require.NoError(t, err)
	return elapsed
}

func p50(ds []time.Duration) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	sorted := slices.Clone(ds)
	slices.Sort(sorted)
	return sorted[len(sorted)/2]
}

func maxOf(ds []time.Duration) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	return slices.Max(ds)
}

func spread(values []int) (int, int, int) {
	total := 0
	for _, v := range values {
		total += v
	}
	return slices.Min(values), total / len(values), slices.Max(values)
}

func parseRange(t *testing.T, spec string) (uint32, uint32) {
	t.Helper()
	lo, count, ok := strings.Cut(spec, "-")
	require.True(t, ok, "%s must be firstSeq-count, got %q", rangeEnv, spec)
	firstSeq, err := strconv.ParseUint(lo, 10, 32)
	require.NoError(t, err)
	n, err := strconv.ParseUint(count, 10, 32)
	require.NoError(t, err)
	require.Positive(t, n, "%s count must be positive", rangeEnv)
	return uint32(firstSeq), uint32(firstSeq + n - 1)
}
