package bench

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

// minLatencyRowSamples is the sample count below which a latency row gets a
// warning.
const minLatencyRowSamples = 100

// queryRun is one bench query run's scenarios: the dataset they read, the
// plan, the report they add to, the load generator's time source and the run
// settings.
type queryRun struct {
	logger   *supportlog.Entry
	ds       *queryDataset
	plan     queryPlan
	report   *queryReport
	clock    scenarioClock
	settings map[string]string
	// cpuProfile, when set, is the --cpuprofile path that scenarioProfilePath
	// derives each scenario's CPU profile path from.
	cpuProfile string
	// evict, when set, runs before each scenario.
	evict func(context.Context) error
}

// scenarios runs every type at every rate. A type's pool is built once and
// shared by every rate.
func (r *queryRun) scenarios(ctx context.Context) error {
	for _, qtype := range r.plan.Types {
		req, err := newQueryRequest(ctx, r.logger, r.ds, r.plan, qtype, r.settings)
		if err != nil {
			return fmt.Errorf("prepare the %s benchmark: %w", qtype, err)
		}
		for _, rps := range r.plan.TargetRPS {
			if err := r.scenario(ctx, qtype, rps, req); err != nil {
				return fmt.Errorf("query %s at %s rps: %w", qtype, formatRPS(rps), err)
			}
			if err := ctx.Err(); err != nil {
				return err
			}
		}
	}
	return nil
}

// scenario runs one type at one rate: r.evict when it is set, then plan.Warmup
// unmeasured iterations and the measured ones at rps over plan.Duration. The
// seed mixes in the type.
//
// A cancel after the first measured iteration adds the partial scenario to the
// report, logs it PARTIAL and returns the context error. A cancel before it
// adds nothing. A failed measured request fails the run after the scenario is
// added. With cpuProfile set, the measured iterations are CPU-profiled.
func (r *queryRun) scenario(ctx context.Context, qtype string, rps float64, req queryRequest) error {
	p := r.plan
	sc := scenarioReport{queryType: qtype, targetRPS: rps}
	if r.evict != nil {
		start := r.clock.now()
		if err := r.evict(ctx); err != nil {
			if isContextErr(err) {
				r.logger.Warnf("query %s at %s rps canceled during page-cache eviction; no rows added",
					qtype, formatRPS(rps))
			}
			return err
		}
		sc.pageCacheEvict = r.clock.now().Sub(start)
		sc.evicted = true
	}
	r.logger.Infof("query %s at %s rps for %s, %d warmup iterations",
		qtype, formatRPS(rps), p.Duration, p.Warmup)
	var prof *cpuProfile
	var onMeasure func() error
	if r.cpuProfile != "" {
		prof = &cpuProfile{path: scenarioProfilePath(r.cpuProfile, qtype, rps)}
		onMeasure = prof.start
	}
	res, err := runConstantArrivalRate(ctx, r.clock, rps, p.Duration, p.Warmup, scenarioSeed(p.Seed, qtype), req,
		onMeasure)
	if res.planned == 0 {
		prof.discard(r.logger) // it covers no measured iteration
	} else {
		prof.stop(r.logger)
	}
	if err != nil && !isContextErr(err) {
		return err // a bad argument or a profile that failed to start leaves no result
	}
	if err != nil && res.planned == 0 {
		r.logger.Warnf("query %s at %s rps canceled before its first measured iteration; no rows added",
			qtype, formatRPS(rps))
		return err
	}
	sc.result = res
	warnThinSamples(r.logger, r.report.add(sc))
	if err != nil {
		r.logger.Warnf("query %s at %s rps is PARTIAL: canceled after %d measured iterations",
			qtype, formatRPS(rps), res.planned)
		return err
	}
	if m := res.measured; m.failed > 0 {
		return fmt.Errorf("%d of %d requests failed: %w", m.failed, m.started, m.firstErr)
	}
	return nil
}

// scenarioProfilePath returns the CPU profile path of the qtype scenario at
// rps: base with ".<qtype>.<rps>rps" inserted before its extension.
func scenarioProfilePath(base, qtype string, rps float64) string {
	ext := filepath.Ext(base)
	return strings.TrimSuffix(base, ext) + "." + qtype + "." + formatRPS(rps) + "rps" + ext
}

// isContextErr reports whether err is a context cancel or deadline.
func isContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// warnThinSamples logs a warning for each latency row of sc with fewer than
// minLatencyRowSamples samples.
func warnThinSamples(logger *supportlog.Entry, sc scenarioSummary) {
	for _, row := range sc.latency {
		if row.metric == metricLatency && row.agg.n < minLatencyRowSamples {
			logger.Warnf("%-8s target_rps=%-8s latency outcome=%s has %d samples, fewer than %d",
				sc.queryType, formatRPS(sc.targetRPS), row.outcome, row.agg.n, minLatencyRowSamples)
		}
	}
}

// scenarioSeed returns the seed for qtype's scenarios. Each type gets its own
// seed, and the seed does not depend on the order of --types.
func scenarioSeed(base int64, qtype string) int64 {
	return base + int64(slices.Index(allQueryTypes, qtype))
}

// warnFixedReadRange logs the configured types whose requests all start at the
// dataset's first ledger and records them in settings under fixedReadRange.
// pickStart returns that ledger when a ledgers request's limit covers the whole
// dataset, or for any type that picks a start when the dataset holds one
// ledger, so every request reads the same ledgers and their percentiles
// measure a cached read. txhash requests pick a hash, not a start.
func warnFixedReadRange(logger *supportlog.Entry, ds *queryDataset, p queryPlan, settings map[string]string) {
	room := ds.LastLedger - ds.FirstLedger + 1
	var fixed []string
	for _, qtype := range p.Types {
		span := uint32(1)
		switch qtype {
		case queryTypeLedgers:
			span = p.LedgersLimit
		case queryTypeTxHash:
			continue
		}
		if span >= room {
			fixed = append(fixed, qtype)
		}
	}
	if len(fixed) == 0 {
		return
	}
	logger.Warnf("dataset holds %d ledgers; %s read the same fixed range every request (limit >= range), "+
		"so their percentiles measure a cached read", room, strings.Join(fixed, " and "))
	settings["fixedReadRange"] = strings.Join(fixed, ",")
}

// runQueryBench is the bench query body: open the dataset, run the
// scenarios, write the report. The open time goes into setupNs.storeOpen. A
// failure after the dataset opens still writes the CSVs of the scenarios added
// so far, logged as PARTIAL, but no bench.txt. A non-empty cpuProfile writes
// one CPU profile per scenario at the path scenarioProfilePath returns.
func runQueryBench(
	ctx context.Context, logger *supportlog.Entry, env runEnv, p queryPlan, cpuProfile string,
	open func() (*queryDataset, func(), error),
) error {
	start := time.Now()
	ds, release, err := open()
	if err != nil {
		return err
	}
	defer release()
	env.SetupTimes["storeOpen"] = time.Since(start)
	evict := cacheControls(logger, p, ds, env.Settings)
	logger.Infof("serving ledgers [%d, %d] over %d %s chunk(s)", ds.FirstLedger, ds.LastLedger, len(ds.Chunks), ds.Tier)
	env.Settings["tier"] = ds.Tier
	warnFixedReadRange(logger, ds, p, env.Settings)
	env.Settings["format"] = p.Format
	env.Settings["handlerConcurrency"] = strconv.Itoa(ds.server.bridge.MaxHandlers())

	report := &queryReport{tier: ds.Tier}
	run := &queryRun{
		logger: logger, ds: ds, plan: p, report: report, clock: timerClock{}, settings: env.Settings,
		cpuProfile: cpuProfile, evict: evict,
	}
	runErr := run.scenarios(ctx)
	written, err := report.write(env.OutDir)
	if runErr != nil {
		if err != nil {
			logger.Warnf("writing the PARTIAL report: %v", err)
		}
		if len(written) > 0 {
			logger.Warnf("run incomplete: wrote %d PARTIAL CSVs to %s (they cover only the scenarios that ran)",
				len(written), env.OutDir)
		}
		return runErr
	}
	if err != nil {
		return err
	}
	path, err := report.writeBench(env.OutDir)
	if err != nil {
		return err
	}
	written = append(written, path)
	report.logSummary(logger)
	logger.Infof("wrote %d report files to %s", len(written), env.OutDir)
	return nil
}
