package bench

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

// minLatencyRowSamples is the sample count below which a latency row gets a
// warning.
const minLatencyRowSamples = 100

// queryRun is one bench query run's scenarios: the dataset they read, the
// plan, the report they add to and the load generator's time source.
type queryRun struct {
	logger *supportlog.Entry
	ds     *queryDataset
	plan   queryPlan
	report *queryReport
	clock  scenarioClock
}

// scenarios runs every type at every rate.
func (r *queryRun) scenarios(ctx context.Context) error {
	for _, qtype := range r.plan.Types {
		req, err := newQueryRequest(r.ds, r.plan, qtype)
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

// scenario runs one type at one rate: plan.Warmup unmeasured iterations, then
// the measured ones at rps over plan.Duration. The seed mixes in the type.
//
// A cancel after the first measured iteration adds the partial scenario to the
// report, logs it PARTIAL and returns the context error. A cancel before it
// adds nothing. A failed measured request fails the run after the scenario is
// added.
func (r *queryRun) scenario(ctx context.Context, qtype string, rps float64, req queryRequest) error {
	p := r.plan
	sc := scenarioReport{queryType: qtype, targetRPS: rps}
	r.logger.Infof("query %s at %s rps for %s, %d warmup iterations",
		qtype, formatRPS(rps), p.Duration, p.Warmup)
	res, err := runConstantArrivalRate(ctx, r.clock, rps, p.Duration, p.Warmup, scenarioSeed(p.Seed, qtype), req)
	if err != nil && !isContextErr(err) {
		return err // a bad argument leaves no result
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

// isContextErr reports whether err is a context cancel or deadline.
func isContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// warnThinSamples logs a warning for each latency row of sc with fewer than
// minLatencyRowSamples samples.
func warnThinSamples(logger *supportlog.Entry, sc scenarioSummary) {
	for _, row := range sc.latency {
		if row.metric == metricLatency && row.agg.n < minLatencyRowSamples {
			logger.Warnf("%-8s target_rps=%-8s latency has %d samples, fewer than %d",
				sc.queryType, formatRPS(sc.targetRPS), row.agg.n, minLatencyRowSamples)
		}
	}
}

// scenarioSeed returns the seed for qtype's scenarios. Each type gets its own
// seed, and the seed does not depend on the order of --types.
func scenarioSeed(base int64, qtype string) int64 {
	return base + int64(slices.Index(allQueryTypes, qtype))
}

// warnFixedReadRange logs the configured types whose span covers the whole
// dataset and records them in settings.fixedReadRange. pickStart returns the
// range's first ledger for those, so every request reads the same ledgers and
// their percentiles measure a cached read.
func warnFixedReadRange(logger *supportlog.Entry, ds *queryDataset, p queryPlan) {
	room := ds.LastLedger - ds.FirstLedger + 1
	var fixed []string
	for _, qtype := range p.Types {
		var span uint32
		switch qtype {
		case queryTypeLedgers:
			span = p.LedgersSpan
		case queryTypeTxPage:
			span = p.TxPageSpan
		}
		if span >= room {
			fixed = append(fixed, qtype)
		}
	}
	if len(fixed) == 0 {
		return
	}
	logger.Warnf("dataset holds %d ledgers; %s read the same fixed range every request (span >= range), "+
		"so their percentiles measure a cached read", room, strings.Join(fixed, " and "))
	p.Settings["fixedReadRange"] = strings.Join(fixed, ",")
}

// runQueryBench is the body both subcommands share: open the dataset, run the
// scenarios, write the report. The open time goes into setupNs.storeOpen. A
// failure after the dataset opens still writes the scenarios added so far,
// logged as PARTIAL.
func runQueryBench(
	ctx context.Context, logger *supportlog.Entry, env runEnv, p queryPlan,
	open func() (*queryDataset, func(), error),
) error {
	start := time.Now()
	ds, release, err := open()
	if err != nil {
		return err
	}
	defer release()
	env.SetupTimes["storeOpen"] = time.Since(start)
	logger.Infof("serving ledgers [%d, %d] over %d chunk(s)", ds.FirstLedger, ds.LastLedger, len(ds.Chunks))
	warnFixedReadRange(logger, ds, p)

	report := &queryReport{}
	run := &queryRun{logger: logger, ds: ds, plan: p, report: report, clock: timerClock{}}
	runErr := run.scenarios(ctx)
	written, err := report.write(env.OutDir)
	if runErr != nil {
		if err != nil {
			logger.Warnf("writing the PARTIAL report: %v", err)
		}
		if len(written) > 0 {
			logger.Warnf("run incomplete: wrote %d PARTIAL CSVs to %s (rows cover only the scenarios that ran)",
				len(written), env.OutDir)
		}
		return runErr
	}
	if err != nil {
		return err
	}
	report.logSummary(logger)
	logger.Infof("wrote %d CSVs to %s", len(written), env.OutDir)
	return nil
}
