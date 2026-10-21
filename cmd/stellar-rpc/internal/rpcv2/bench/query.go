package bench

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
)

// Read-shape flag defaults.
const (
	defaultLedgersLimit     = 10
	defaultTxPageLimit      = 200
	defaultEventsLimit      = 10
	defaultNotFoundFraction = 0.12
	defaultSeed             = 1
)

// Scenario flag defaults.
const (
	defaultScenarioDuration = 60 * time.Second
	defaultTargetRPS        = "10"
)

// maxTargetRPS is the highest rate --target-rps accepts.
const maxTargetRPS = 100_000

// maxTxHashPoolSize is the largest --txhash-pool-size.
const maxTxHashPoolSize = 1_000_000

// queryFlags is the bench query flag set that shapes the requests, beyond --out
// and the profiling flags newBenchCommand binds. The spellings and value
// formats of --types, --target-rps, --duration and --warmup are the campaign
// runner's argv contract.
type queryFlags struct {
	types     string
	targetRPS string
	duration  time.Duration
	warmup    int

	ledgersLimit     uint32
	txPageLimit      uint32
	eventsLimit      uint32
	notFoundFraction float64
	txHashPoolSize   int
	format           string
	passphrase       string
	seed             int64
	evict            bool
}

func (f *queryFlags) bind(cmd *cobra.Command) {
	fs := cmd.Flags()
	fs.StringVar(&f.types, "types", strings.Join(allQueryTypes, ","),
		"comma-separated query types to run: "+strings.Join(allQueryTypes, " | "))
	fs.StringVar(&f.targetRPS, "target-rps", defaultTargetRPS,
		"comma-separated target rates, in requests per second, at most "+
			strconv.Itoa(maxTargetRPS)+", e.g. 0.5,1,2")
	fs.DurationVar(&f.duration, "duration", defaultScenarioDuration,
		"measured duration of each --target-rps scenario; the --warmup iterations run before it")
	fs.IntVar(&f.warmup, "warmup", 0,
		"unmeasured iterations per scenario, started at the scenario's rate before measurement starts")
	fs.Uint32Var(&f.ledgersLimit, "ledgers-limit", defaultLedgersLimit,
		"getLedgers limit of one ledgers request, in [1, "+strconv.FormatUint(uint64(maxLedgersLimit()), 10)+
			"] (1 = a point read)")
	fs.Uint32Var(&f.txPageLimit, "txpage-limit", defaultTxPageLimit,
		"getTransactions limit of one txpage request, in [1, "+strconv.FormatUint(uint64(maxTxPageLimit()), 10)+"]")
	fs.Uint32Var(&f.eventsLimit, "events-limit", defaultEventsLimit,
		"queryEvents limit of one events request, in [1, "+strconv.FormatUint(uint64(maxEventsLimit()), 10)+"]")
	fs.Float64Var(&f.notFoundFraction, "not-found-fraction", defaultNotFoundFraction,
		"share of txhash lookups for a hash that is not in the dataset, in [0, 1]")
	fs.IntVar(&f.txHashPoolSize, "txhash-pool-size", defaultTxHashPoolSize,
		"txhash pool size, in [1, "+strconv.Itoa(maxTxHashPoolSize)+"]; the pool can hold fewer hashes")
	fs.StringVar(&f.format, "format", protocol.FormatBase64,
		"response format of every request: "+strings.Join(queryFormats, " | "))
	fs.StringVar(&f.passphrase, "network-passphrase", network.PublicNetworkPassphrase,
		"network passphrase the dataset's transactions were signed under; txhash and "+
			"txpage need it to pair envelopes, and a wrong one fails the txhash pool build")
	fs.Int64Var(&f.seed, "seed", defaultSeed,
		"seed for the work each request picks, so a re-run reads the same ledgers")
	fs.BoolVar(&f.evict, "evict-page-cache", true,
		"request best-effort OS page-cache eviction of a cold dataset's frozen files before each scenario (Linux only)")
}

// plan parses and validates the flags.
func (f *queryFlags) plan() (queryPlan, error) {
	types, err := parseQueryTypes(f.types)
	if err != nil {
		return queryPlan{}, err
	}
	rates, err := parseTargetRPS(f.targetRPS)
	if err != nil {
		return queryPlan{}, err
	}
	if err = f.checkScenarioFlags(rates); err != nil {
		return queryPlan{}, err
	}
	if err = f.checkRequestFlags(); err != nil {
		return queryPlan{}, err
	}
	if f.passphrase == "" {
		return queryPlan{}, errors.New("--network-passphrase is required")
	}
	return queryPlan{
		Types:            types,
		TargetRPS:        rates,
		Duration:         f.duration,
		Warmup:           f.warmup,
		LedgersLimit:     f.ledgersLimit,
		TxPageLimit:      f.txPageLimit,
		EventsLimit:      f.eventsLimit,
		NotFoundFraction: f.notFoundFraction,
		TxHashPoolSize:   f.txHashPoolSize,
		Format:           f.format,
		Passphrase:       f.passphrase,
		Seed:             f.seed,
		Evict:            f.evict,
	}, nil
}

// checkScenarioFlags checks --duration and --warmup, and that each rate gives
// a scenario that runConstantArrivalRate can run.
func (f *queryFlags) checkScenarioFlags(rates []float64) error {
	switch {
	case f.duration <= 0:
		return fmt.Errorf("--duration must be > 0, got %v", f.duration)
	case f.warmup < 0:
		return fmt.Errorf("--warmup must be >= 0, got %d", f.warmup)
	}
	for _, rps := range rates {
		if _, _, err := validateScenario(rps, f.duration, f.warmup); err != nil {
			return fmt.Errorf("--target-rps, --duration and --warmup: %w", err)
		}
	}
	return nil
}

// checkRequestFlags checks the flags that set what each request reads. A limit
// above the daemon's cap is an error, because the handler would reject every
// request.
func (f *queryFlags) checkRequestFlags() error {
	switch maxLedgers, maxTxs := maxLedgersLimit(), maxTxPageLimit(); {
	case f.ledgersLimit < 1 || uint(f.ledgersLimit) > maxLedgers:
		return fmt.Errorf("--ledgers-limit must be in [1, %d], got %d", maxLedgers, f.ledgersLimit)
	case f.txPageLimit < 1 || uint(f.txPageLimit) > maxTxs:
		return fmt.Errorf("--txpage-limit must be in [1, %d], got %d", maxTxs, f.txPageLimit)
	case f.eventsLimit < 1 || uint(f.eventsLimit) > maxEventsLimit():
		return fmt.Errorf("--events-limit must be in [1, %d], got %d", maxEventsLimit(), f.eventsLimit)
	case !(f.notFoundFraction >= 0 && f.notFoundFraction <= 1): // also rejects NaN
		return fmt.Errorf("--not-found-fraction must be in [0, 1], got %v", f.notFoundFraction)
	case f.txHashPoolSize < 1 || f.txHashPoolSize > maxTxHashPoolSize:
		return fmt.Errorf("--txhash-pool-size must be in [1, %d], got %d", maxTxHashPoolSize, f.txHashPoolSize)
	case !slices.Contains(queryFormats, f.format):
		return fmt.Errorf("--format: unknown format %q (want %s)", f.format, strings.Join(queryFormats, " | "))
	}
	return nil
}

// parseQueryTypes splits --types, in the caller's order. A repeat is an error:
// it would duplicate a scenario's rows.
func parseQueryTypes(s string) ([]string, error) {
	fields := strings.Split(s, ",")
	types := make([]string, 0, len(fields))
	for _, f := range fields {
		qtype := strings.TrimSpace(f)
		if qtype == "" {
			return nil, fmt.Errorf("--types has an empty entry: %q", s)
		}
		if !slices.Contains(allQueryTypes, qtype) {
			return nil, fmt.Errorf("--types: unknown query type %q (want %s)",
				qtype, strings.Join(allQueryTypes, " | "))
		}
		if slices.Contains(types, qtype) {
			return nil, fmt.Errorf("--types repeats %q, which would duplicate its scenario rows", qtype)
		}
		types = append(types, qtype)
	}
	return types, nil
}

// parseTargetRPS splits --target-rps into rates, in the caller's order. A repeat
// is an error: it would duplicate a scenario's rows.
func parseTargetRPS(s string) ([]float64, error) {
	fields := strings.Split(s, ",")
	rates := make([]float64, 0, len(fields))
	for _, f := range fields {
		field := strings.TrimSpace(f)
		if field == "" {
			return nil, fmt.Errorf("--target-rps has an empty entry: %q", s)
		}
		rps, err := strconv.ParseFloat(field, 64)
		if err != nil {
			return nil, fmt.Errorf("--target-rps: %q is not a number: %w", field, err)
		}
		if err := checkTargetRPS(rps); err != nil {
			return nil, err
		}
		if slices.Contains(rates, rps) {
			return nil, fmt.Errorf("--target-rps repeats %v, which would duplicate its scenario rows", rps)
		}
		rates = append(rates, rps)
	}
	return rates, nil
}

// checkTargetRPS checks one --target-rps rate: positive, finite and at most
// maxTargetRPS.
func checkTargetRPS(rps float64) error {
	if rps <= 0 || math.IsNaN(rps) || math.IsInf(rps, 0) {
		return fmt.Errorf("--target-rps rates must be positive and finite, got %v", rps)
	}
	if rps > maxTargetRPS {
		return fmt.Errorf("--target-rps rates must be <= %v, got %v", float64(maxTargetRPS), rps)
	}
	return nil
}
