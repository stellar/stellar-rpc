package bench

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// TestNewQueryCommand builds the full command tree — executing every
// markRequired call, whose panic on a bad flag name this test exists to catch
// (main.go calls NewQueryCommand unconditionally at startup) — and pins each
// subcommand's required flags.
func TestNewQueryCommand(t *testing.T) {
	cmd := NewQueryCommand()
	require.Equal(t, "bench-query", cmd.Use)

	requiredBySubcommand := map[string][]string{
		"cold": {"start-chunk", "cold-dir"},
		"hot":  {"chunk", "hot-dir"},
	}
	subs := querySubcommands(t, cmd)
	for name, flags := range requiredBySubcommand {
		sub := subs[name]
		require.NotNil(t, sub, "subcommand %q missing", name)
		for _, fn := range flags {
			f := sub.Flags().Lookup(fn)
			require.NotNil(t, f, "%s: flag --%s missing", name, fn)
			require.Contains(t, f.Annotations, cobra.BashCompOneRequiredFlag,
				"%s: flag --%s not marked required", name, fn)
		}
	}
}

// TestQueryCommandAcceptsRunnerArgv parses the argv the campaign runner emits,
// verbatim and in its own order, through both subcommands. The runner is the
// only caller in the campaign, so this argv IS the flag surface's contract: a
// renamed flag, a dropped one, or a value format the flag cannot parse breaks a
// campaign leg, not a local invocation.
func TestQueryCommandAcceptsRunnerArgv(t *testing.T) {
	for _, tc := range []struct {
		name string
		argv []string
	}{
		{
			name: "cold",
			argv: []string{
				"cold",
				"--cold-dir=/bench/ds", "--start-chunk=7", "--num-chunks=1",
				"--types=ledgers,txpage,txhash,events",
				"--query-concurrency=1,4,16", "--iters=100", "--out=/bench/out",
			},
		},
		{
			name: "hot uncapped",
			argv: []string{
				"hot",
				"--hot-dir=/bench/hot", "--chunk=7",
				"--types=ledgers,txpage,txhash,events",
				"--query-concurrency=1,4,16", "--iters=200", "--warmup=20", "--out=/bench/out",
			},
		},
		{
			name: "hot capped",
			argv: []string{
				"hot",
				"--hot-dir=/bench/hot", "--chunk=7",
				"--types=ledgers,txpage,txhash,events",
				"--query-concurrency=1,4,16", "--iters=200", "--warmup=20",
				"--sample-ledgers=50000", "--out=/bench/out",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sub := querySubcommands(t, NewQueryCommand())[tc.argv[0]]
			require.NotNil(t, sub)
			require.NoError(t, sub.ParseFlags(tc.argv[1:]))

			types, err := sub.Flags().GetString("types")
			require.NoError(t, err)
			parsed, err := parseQueryTypes(types)
			require.NoError(t, err)
			require.Equal(t, allQueryTypes, parsed, "the runner sweeps every query type")

			concurrency, err := sub.Flags().GetString("query-concurrency")
			require.NoError(t, err)
			levels, err := parseConcurrency(concurrency)
			require.NoError(t, err)
			require.Equal(t, []int{1, 4, 16}, levels)
		})
	}
}

// TestParseQueryTypes pins the accepted and rejected --types values.
func TestParseQueryTypes(t *testing.T) {
	got, err := parseQueryTypes("events,ledgers")
	require.NoError(t, err)
	require.Equal(t, []string{"events", "ledgers"}, got, "the caller's order is kept")

	for _, bad := range []string{"", "ledgers,", "ledgers,ledgers", "ledger", "ledgers,txpages"} {
		_, err := parseQueryTypes(bad)
		require.Error(t, err, "--types=%q must be rejected", bad)
	}
}

// TestParseConcurrency pins the accepted and rejected --query-concurrency values.
func TestParseConcurrency(t *testing.T) {
	got, err := parseConcurrency("16,1,4")
	require.NoError(t, err)
	require.Equal(t, []int{16, 1, 4}, got, "the caller's order is kept")

	for _, bad := range []string{"", "1,", "0", "-1", "1,1", "four"} {
		_, err := parseConcurrency(bad)
		require.Error(t, err, "--query-concurrency=%q must be rejected", bad)
	}
}

// TestQuerySpecs pins the report schema the results converter parses: one CSV
// per swept type carrying total_c<W>, and driver.csv carrying the fixture open,
// each cell's <qtype>_c<W> wall-clock, and the peak RSS.
func TestQuerySpecs(t *testing.T) {
	specs := querySpecs([]string{queryTypeLedgers, queryTypeEvents}, []int{1, 4})

	names := make([]string, len(specs))
	byName := make(map[string][]string, len(specs))
	for i, s := range specs {
		names[i] = s.name
		byName[s.name] = s.rowOrder
	}
	require.Equal(t, []string{"ledgers", "events", "driver"}, names,
		"the swept types come first, in sweep order, then driver.csv")
	require.Equal(t, []string{"total_c1", "total_c4"}, byName["ledgers"])
	require.Equal(t, []string{"total_c1", "total_c4"}, byName["events"])
	require.Equal(t, []string{
		"open", "evict",
		"ledgers_c1", "ledgers_c4",
		"events_c1", "events_c4",
		"peak_rss_bytes",
	}, byName["driver"])
}

// TestQuerySpecsTxHashStages pins the found/miss rows the txhash file carries
// beside the blended cell the converter reads.
func TestQuerySpecsTxHashStages(t *testing.T) {
	specs := querySpecs([]string{queryTypeTxHash}, []int{1, 4})
	require.Equal(t, queryTypeTxHash, specs[0].name)
	require.Equal(t, []string{
		"total_c1", "total_c4",
		"found_c1", "found_c4",
		"miss_c1", "miss_c4",
	}, specs[0].rowOrder)
}

// TestQuerySinkWritesContractRows records one cell per type and checks the
// written CSVs carry the converter's header and row names.
func TestQuerySinkWritesContractRows(t *testing.T) {
	types := []string{queryTypeLedgers, queryTypeTxHash}
	concurrency := []int{1, 4}
	sink := newSchemaCSVSink(querySpecs(types, concurrency))
	sink.observe(fileDriver, driverQueryOpen, time.Millisecond, 1)
	for _, qtype := range types {
		for _, w := range concurrency {
			sink.observe(qtype, queryCellRow(w), time.Millisecond, 3)
			sink.observe(fileDriver, queryDriverRow(qtype, w), time.Second, 3)
		}
	}

	outDir := t.TempDir()
	written, err := sink.writeCSVs(outDir)
	require.NoError(t, err)
	require.Len(t, written, 3)

	for _, f := range sink.files() {
		names := make([]string, len(f.rows))
		for i, r := range f.rows {
			names[i] = r.name
		}
		switch f.name {
		case fileDriver:
			require.Equal(t, []string{
				"open", "ledgers_c1", "ledgers_c4", "txhash_c1", "txhash_c4",
			}, names)
		default:
			require.Equal(t, []string{"total_c1", "total_c4"}, names, "file %s", f.name)
		}
	}
}

// TestOpenColdFixtureServesFrozenChunks runs bench-ingest cold to produce a real
// frozen chunk, then opens the cold query fixture over that tree and reads
// through it. It is the check that the fixture rebuilds the catalog state the
// artifacts imply: bench-ingest throws its own catalog away, so a wrong
// rebuild would leave every chunk unservable.
func TestOpenColdFixtureServesFrozenChunks(t *testing.T) {
	chunkID := chunk.ID(0)
	packDir, _ := writeSourcePack(t, t.TempDir(), chunkID, chunk.LedgersPerChunk)
	coldRoot := t.TempDir()
	require.NoError(t, runCold(context.Background(), testLogger(), coldOptions{
		Source:     sourceConfig{Kind: sourcePack, PackDir: packDir},
		StartChunk: chunkID,
		NumChunks:  1,
		Workers:    1,
		ColdRoot:   coldRoot,
		OutDir:     filepath.Join(t.TempDir(), "csv"),
	}))

	f, release, err := openColdFixture(testLogger(), coldQueryOptions{
		ColdRoot:   coldRoot,
		StartChunk: chunkID,
		NumChunks:  1,
	})
	require.NoError(t, err)
	defer release()

	require.Equal(t, []chunk.ID{chunkID}, f.Chunks)
	assert.Equal(t, chunkID.FirstLedger(), f.FirstLedger)
	assert.Equal(t, chunkID.LastLedger(), f.LastLedger)

	view, err := f.view()
	require.NoError(t, err)
	defer view.Release()

	// The scan is the seam the ledgers and txpage benchmarks measure; running it
	// here proves routing resolved the chunk cold and the pack reader opened.
	from := chunkID.FirstLedger()
	scan, err := view.ScanLedgers(from, from+9)
	require.NoError(t, err)
	next := from
	for e, serr := range scan {
		require.NoError(t, serr)
		assert.Equal(t, next, e.Seq)
		next++
	}
	assert.Equal(t, from+10, next, "the scan yielded every ledger in the range")

	// The backfill built one partial window index; the fixture must have named
	// it, else the by-hash lookup has nothing to probe.
	coverages, err := view.ColdTxHashIndexCoverages()
	require.NoError(t, err)
	require.Len(t, coverages, 1)
	assert.Equal(t, chunkID, coverages[0].Lo)
	assert.Equal(t, chunkID, coverages[0].Hi)
}

// testQueryPlan is the sweep the end-to-end tests run: every type, two
// concurrency levels, few enough iterations to stay quick. The spans are small
// because the fixture chunk is synthetic, and the miss fraction is high enough
// that a short cell reliably produces both a found and a not-found lookup.
func testQueryPlan(iters int) queryPlan {
	return queryPlan{
		Types:        allQueryTypes,
		Concurrency:  []int{1, 2},
		Iters:        iters,
		LedgersSpan:  4,
		TxPageSpan:   2,
		TxPageLimit:  10,
		EventsLimit:  10,
		MissFraction: 0.5,
		Passphrase:   network.PublicNetworkPassphrase,
		Seed:         defaultSeed,
	}
}

// TestRunQueryCold is the cold end-to-end: ingest a chunk, then sweep every
// query type over its frozen artifacts and check the report the results
// converter will read — one CSV per type carrying a total_c<W> row per
// concurrency level, and driver.csv carrying the matching cell walls plus the
// setup rows.
func TestRunQueryCold(t *testing.T) {
	const iters = 5
	chunkID := chunk.ID(0)
	coldRoot := ingestColdChunk(t, chunkID)

	csvDir := filepath.Join(t.TempDir(), "csv")
	plan := testQueryPlan(iters)
	plan.Evict = true
	require.NoError(t, runQueryCold(context.Background(), testLogger(), coldQueryOptions{
		ColdRoot:   coldRoot,
		StartChunk: chunkID,
		NumChunks:  1,
		Plan:       plan,
		OutDir:     csvDir,
	}))

	assertQueryReport(t, csvDir, plan, iters)

	driver := readCSV(t, filepath.Join(csvDir, "driver.csv"))
	require.Contains(t, driver, "open")
	assert.EqualValues(t, 1, driver["open"]["n_items"], "one chunk opened")
	// One eviction pass runs per cell, but the sink drops zero-duration samples
	// and off Linux the pass is a no-op that finishes inside a timer tick — so
	// the row's presence and the artifacts it named are what can be asserted
	// everywhere, not the sample count.
	require.Contains(t, driver, "evict", "a cold cell evicts before it measures")
	assert.LessOrEqual(t, driver["evict"]["n"], int64(len(plan.Types)*len(plan.Concurrency)),
		"at most one eviction pass per cell")
	assert.Positive(t, driver["evict"]["n_items"], "the eviction pass named some artifacts")
}

// TestRunQueryHot is the hot end-to-end: ingest a chunk into a hot database,
// then sweep every query type against it. It also covers the warmup pass, which
// only the hot tier runs.
func TestRunQueryHot(t *testing.T) {
	const (
		ingested = 400
		iters    = 5
	)
	chunkID := chunk.ID(0)
	packDir, _ := writeSourcePack(t, t.TempDir(), chunkID, ingested)
	hotRoot := t.TempDir()
	require.NoError(t, runHot(context.Background(), testLogger(), hotOptions{
		Source:     sourceConfig{Kind: sourcePack, PackDir: packDir},
		StartChunk: chunkID,
		NumChunks:  1,
		NumLedgers: ingested,
		HotRoot:    hotRoot,
		OutDir:     filepath.Join(t.TempDir(), "csv"),
	}))

	csvDir := filepath.Join(t.TempDir(), "csv")
	plan := testQueryPlan(iters)
	plan.Warmup = 2
	require.NoError(t, runQueryHot(context.Background(), testLogger(), hotQueryOptions{
		HotRoot: hotRoot,
		Chunk:   chunkID,
		Plan:    plan,
		OutDir:  csvDir,
	}))

	assertQueryReport(t, csvDir, plan, iters)

	driver := readCSV(t, filepath.Join(csvDir, "driver.csv"))
	assert.NotContains(t, driver, "evict", "a hot run has no page-cache artifacts to evict")
}

// assertQueryReport checks a finished run's CSVs against the converter's
// contract: every swept cell present, its sample count exactly what the sweep
// promised (workers × iters), and every cell wall recorded in driver.csv. It
// also checks txhash's found/miss rows partition the blended row, which is the
// property that lets the split be reported without touching the cell the
// converter reads.
func assertQueryReport(t *testing.T, csvDir string, plan queryPlan, iters int) {
	t.Helper()
	driver := readCSV(t, filepath.Join(csvDir, "driver.csv"))

	for _, qtype := range plan.Types {
		rows := readCSV(t, filepath.Join(csvDir, qtype+".csv"))
		for _, w := range plan.Concurrency {
			cell := queryCellRow(w)
			require.Contains(t, rows, cell, "%s is missing its c%d cell", qtype, w)
			assert.EqualValues(t, w*iters, rows[cell]["n"],
				"%s c%d must hold one sample per request", qtype, w)

			wall := queryDriverRow(qtype, w)
			require.Contains(t, driver, wall, "driver.csv is missing %s", wall)
			assert.Positive(t, driver[wall]["total_ns"], "%s took no time", wall)
		}
		if qtype != queryTypeTxHash {
			continue
		}
		for _, w := range plan.Concurrency {
			found := rows[txHashStageFound+"_c"+strconv.Itoa(w)]
			miss := rows[txHashStageMiss+"_c"+strconv.Itoa(w)]
			assert.Equal(t, rows[queryCellRow(w)]["n"], found["n"]+miss["n"],
				"txhash c%d: the found and miss rows must partition the blended row", w)
		}
	}
}

// ingestColdChunk runs bench-ingest cold over a synthetic pack and returns the
// frozen artifact root, which is what a cold query run reads.
func ingestColdChunk(t *testing.T, chunkID chunk.ID) string {
	t.Helper()
	packDir, _ := writeSourcePack(t, t.TempDir(), chunkID, chunk.LedgersPerChunk)
	coldRoot := t.TempDir()
	require.NoError(t, runCold(context.Background(), testLogger(), coldOptions{
		Source:     sourceConfig{Kind: sourcePack, PackDir: packDir},
		StartChunk: chunkID,
		NumChunks:  1,
		Workers:    1,
		ColdRoot:   coldRoot,
		OutDir:     filepath.Join(t.TempDir(), "csv"),
	}))
	return coldRoot
}

// TestQueryRejectsWrongPassphrase pins the corpus build failing loudly on a
// passphrase the dataset was not signed under. Without the check every lookup
// would report not-found and the run would publish the miss path's latency
// under the hit path's name.
func TestQueryRejectsWrongPassphrase(t *testing.T) {
	chunkID := chunk.ID(0)
	coldRoot := ingestColdChunk(t, chunkID)

	plan := testQueryPlan(2)
	plan.Types = []string{queryTypeTxHash}
	plan.Passphrase = network.TestNetworkPassphrase
	err := runQueryCold(context.Background(), testLogger(), coldQueryOptions{
		ColdRoot:   coldRoot,
		StartChunk: chunkID,
		NumChunks:  1,
		Plan:       plan,
		OutDir:     filepath.Join(t.TempDir(), "csv"),
	})
	require.ErrorContains(t, err, "--network-passphrase")
}

// TestEvictionStateRecordsPlatform pins what invocation.json says about
// eviction: what was asked for, and whether this platform could do it.
func TestEvictionStateRecordsPlatform(t *testing.T) {
	assert.Equal(t, "off", evictionState(false))
	if evictSupported {
		assert.Equal(t, "on", evictionState(true))
		return
	}
	assert.Equal(t, "unsupported-on-this-platform", evictionState(true))
}

// TestOpenColdFixtureRejectsMissingChunk pins the open failing with the chunk
// named when the dataset does not cover the requested range, rather than
// deferring an unreadable ErrUnavailable to every query.
func TestOpenColdFixtureRejectsMissingChunk(t *testing.T) {
	_, _, err := openColdFixture(testLogger(), coldQueryOptions{
		ColdRoot:   t.TempDir(),
		StartChunk: chunk.ID(4),
		NumChunks:  1,
	})
	require.ErrorContains(t, err, "no ledger pack")
}

// TestOpenHotFixtureServesHotChunk runs bench-ingest hot to produce a real hot
// database, then opens the hot query fixture over it and reads through it. It
// also pins the sampled range: the fixture trusts what the database committed,
// not the chunk's nominal span, and --sample-ledgers narrows that further.
func TestOpenHotFixtureServesHotChunk(t *testing.T) {
	const ingested = 50
	chunkID := chunk.ID(0)
	packDir, _ := writeSourcePack(t, t.TempDir(), chunkID, ingested)
	hotRoot := t.TempDir()
	require.NoError(t, runHot(context.Background(), testLogger(), hotOptions{
		Source:     sourceConfig{Kind: sourcePack, PackDir: packDir},
		StartChunk: chunkID,
		NumChunks:  1,
		NumLedgers: ingested,
		HotRoot:    hotRoot,
		OutDir:     filepath.Join(t.TempDir(), "csv"),
	}))
	first := chunkID.FirstLedger()

	t.Run("uncapped sample", func(t *testing.T) {
		f, release, err := openHotFixture(testLogger(), hotQueryOptions{HotRoot: hotRoot, Chunk: chunkID})
		require.NoError(t, err)
		defer release()

		require.Equal(t, []chunk.ID{chunkID}, f.Chunks)
		assert.Equal(t, first, f.FirstLedger)
		assert.Equal(t, first+ingested-1, f.LastLedger, "the sampled range is what the database holds")

		view, err := f.view()
		require.NoError(t, err)
		defer view.Release()

		scan, err := view.ScanLedgers(first, f.LastLedger)
		require.NoError(t, err)
		seqs := 0
		for _, serr := range scan {
			require.NoError(t, serr)
			seqs++
		}
		assert.Equal(t, ingested, seqs)

		// One published handle means one hot index for the by-hash lookup.
		assert.Len(t, view.HotTxHashIndexes(), 1)
	})

	t.Run("capped sample", func(t *testing.T) {
		f, release, err := openHotFixture(testLogger(), hotQueryOptions{
			HotRoot: hotRoot, Chunk: chunkID, SampleLedgers: 10,
		})
		require.NoError(t, err)
		defer release()
		assert.Equal(t, first+9, f.LastLedger)
	})

	t.Run("cap past what was ingested", func(t *testing.T) {
		f, release, err := openHotFixture(testLogger(), hotQueryOptions{
			HotRoot: hotRoot, Chunk: chunkID, SampleLedgers: chunk.LedgersPerChunk,
		})
		require.NoError(t, err)
		defer release()
		assert.Equal(t, first+ingested-1, f.LastLedger, "a cap past the committed span is clamped")
	})
}

// TestOpenHotFixtureRejectsMissingDatabase pins the ready-key open refusing to
// fabricate an empty database for a chunk that was never ingested.
func TestOpenHotFixtureRejectsMissingDatabase(t *testing.T) {
	_, _, err := openHotFixture(testLogger(), hotQueryOptions{HotRoot: t.TempDir(), Chunk: chunk.ID(3)})
	require.Error(t, err)
}

// querySubcommands indexes a command's children by Use.
func querySubcommands(t *testing.T, cmd *cobra.Command) map[string]*cobra.Command {
	t.Helper()
	subs := make(map[string]*cobra.Command, len(cmd.Commands()))
	for _, sub := range cmd.Commands() {
		subs[sub.Use] = sub
	}
	return subs
}
