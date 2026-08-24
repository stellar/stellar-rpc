package bench

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
		"open",
		"ledgers_c1", "ledgers_c4",
		"events_c1", "events_c4",
		"peak_rss_bytes",
	}, byName["driver"])
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

// TestRunQueryColdWritesSetupReport drives runQueryCold over a real frozen
// chunk with the sweep the campaign runner asks for. The per-type bodies are
// #856 B2, so the sweep stops at the first cell and the run writes the PARTIAL
// report: this pins that the setup rows still land and that the error names the
// type. Replace the unimplemented assertions with the per-cell rows when B2
// fills the bodies in.
func TestRunQueryColdWritesSetupReport(t *testing.T) {
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

	csvDir := filepath.Join(t.TempDir(), "csv")
	err := runQueryCold(context.Background(), testLogger(), coldQueryOptions{
		ColdRoot:   coldRoot,
		StartChunk: chunkID,
		NumChunks:  1,
		Plan:       queryPlan{Types: allQueryTypes, Concurrency: []int{1, 4, 16}, Iters: 100},
		OutDir:     csvDir,
	})
	require.ErrorIs(t, err, errQueryTypeUnimplemented)
	require.ErrorContains(t, err, queryTypeLedgers, "the error names the type that stopped the sweep")

	driver := readCSV(t, filepath.Join(csvDir, "driver.csv"))
	require.Contains(t, driver, "open")
	assert.EqualValues(t, 1, driver["open"]["n"])
	assert.EqualValues(t, 1, driver["open"]["n_items"], "one chunk opened")
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
