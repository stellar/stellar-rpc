package backfill

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"
)

// Backend is an EXTERNAL ledger source the backfill freeze path fetches from and
// may have to wait for: it streams ledgers (ledgerbackend.LedgerStream) and
// reports its current frontier via Tip. The local frozen .pack is NOT a Backend —
// it is complete, so backfillSource constructs a bare ledgerbackend.LedgerStream
// (ledger.NewPackStream) for it, with nothing to wait on.
//
// Implementations (one wired now, one deferred):
//
//	bsbSource     (now)   Tip = datastore.FindLatestLedgerSequence (the lake frontier)
//	captiveSource (later) Tip = historyarchive GetRootHAS().CurrentLedger
//
// Captive core is the only deferred piece, and the interface is the seam that
// keeps the path open: it is one new struct (embed ledgerbackend.NewCaptiveCoreStream
// plus a history-archive Tip) and one daemon factory case, with zero changes to
// ProcessConfig, backfillSource, waitForCoverage, or ingest.WriteColdChunk.
//
// Tip is its own method because the SDK's LedgerBackend.GetLatestLedgerSequence is
// NOT the frontier we want — it is prepared-relative (errors before PrepareRange on
// a BufferedStorageBackend, and otherwise returns the buffer's latest, not the
// source's frontier). Each impl therefore supplies its own frontier query.
type Backend interface {
	ledgerbackend.LedgerStream
	Tip(ctx context.Context) (uint32, error)
}

// Default buffered-storage tuning, applied when a NewBSBBackend caller leaves a
// field zero. The SDK's BufferedStorageBackend rejects BufferSize == 0 (and
// requires NumWorkers <= BufferSize), so zero values must be filled before
// streaming.
//
// These are the BACKFILL-workload values the rpc-hack bulk-ingest benchmarking
// converged on, not the daemon's serving-path defaults: the pubnet lake stores one
// ledger per object, so download throughput is request-latency-bound and scales
// with workers, and a deep prefetch buffer keeps the download overlapped with
// ingest.
//
// The retry defaults are deliberately non-zero: a multi-day full-history backfill
// will hit transient object-store errors with certainty, and with RetryLimit 0 a
// single one fails the whole chunk. Disabling retries entirely is therefore not
// expressible through the zero value — pass an explicit RetryLimit for a different
// policy.
const (
	defaultBSBBufferSize = 5000
	defaultBSBNumWorkers = 50
	defaultBSBRetryLimit = 3
	defaultBSBRetryWait  = 5 * time.Second
)

// bsbSource is the Backend over an SDK datastore (the pubnet lake): a
// buffered-storage stream for the ledgers, plus a long-lived datastore handle used
// only for the frontier Tip. One code path serves GCS, S3, and Filesystem — they
// differ only in datastore.DataStoreConfig.
type bsbSource struct {
	ledgerbackend.LedgerStream // NewBufferedStorageStream over cfg

	ds datastore.DataStore // long-lived; tip only; Closed by the daemon
}

var _ Backend = (*bsbSource)(nil)

// NewBSBBackend returns a Backend over the SDK datastore described by cfg. The
// ledger stream owns its own per-iteration datastore lifecycle (the SDK closes it
// when iteration ends); the separate, long-lived ds is used only for the frontier
// Tip and is owned and Closed by the caller (the daemon). Zero bsb fields fall back
// to the benchmarked backfill defaults.
func NewBSBBackend(
	ds datastore.DataStore,
	cfg datastore.DataStoreConfig,
	bsb ledgerbackend.BufferedStorageBackendConfig,
) Backend {
	// Fill zero values: the SDK backend rejects BufferSize == 0 and requires
	// NumWorkers <= BufferSize, so a zero-value config would fail before reading
	// any ledger, and a zero retry policy would fail whole chunks on the first
	// transient error.
	if bsb.BufferSize == 0 {
		bsb.BufferSize = defaultBSBBufferSize
	}
	if bsb.NumWorkers == 0 {
		bsb.NumWorkers = defaultBSBNumWorkers
	}
	if bsb.NumWorkers > bsb.BufferSize {
		bsb.NumWorkers = bsb.BufferSize
	}
	if bsb.RetryLimit == 0 {
		bsb.RetryLimit = defaultBSBRetryLimit
	}
	if bsb.RetryWait == 0 {
		bsb.RetryWait = defaultBSBRetryWait
	}
	return &bsbSource{
		LedgerStream: ledgerbackend.NewBufferedStorageStream(bsb, cfg, nil),
		ds:           ds,
	}
}

// NewBSBBackendFromConfig opens the datastore described by dsCfg and returns a BSB
// Backend over it, plus a close func that releases the long-lived Tip datastore handle
// at shutdown. The datastore type (GCS/S3/Filesystem/...) is whatever dsCfg.Type names —
// any SDK datastore works. The batch schema is read from the lake manifest by the
// stream, so dsCfg need not set Schema.
func NewBSBBackendFromConfig(
	ctx context.Context,
	dsCfg datastore.DataStoreConfig,
	bsb ledgerbackend.BufferedStorageBackendConfig,
) (Backend, func(), error) {
	ds, err := datastore.NewDataStore(ctx, dsCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("open datastore (type %q): %w", dsCfg.Type, err)
	}
	return NewBSBBackend(ds, dsCfg, bsb), func() { _ = ds.Close() }, nil
}

// Tip reports the lake's current frontier — the latest ledger present in the
// datastore — independent of any prepared range.
func (s *bsbSource) Tip(ctx context.Context) (uint32, error) {
	return datastore.FindLatestLedgerSequence(ctx, s.ds)
}

// Default coverage-poll cadence, used by backfillSource. A backfill of past history
// almost never waits (the lake is far ahead), but near the tip the freeze must
// block until the lake catches up.
const (
	defaultCoveragePollInterval = time.Second
	defaultCoverageTimeout      = 5 * time.Minute
)

// waitForCoverage blocks until b's Tip reaches target, polling on interval up to
// timeout. It returns nil once covered; ErrBackendCoverageTimeout if the tip stays
// below target until the timeout (coverage lag); a "backend tip query" error if a
// Tip call itself fails (surfaced straight up — waitForCoverage does not retry the
// tip, but the backfill task's withRetries retries the whole task); or ctx's error
// if ctx is canceled first. interval and timeout must be positive — the sole caller
// passes defaultCoveragePollInterval/defaultCoverageTimeout, and tests pass their
// own explicit values.
func waitForCoverage(ctx context.Context, b Backend, target uint32, interval, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	poll := func() error {
		// Bound each tip query by the overall deadline — the parent ctx may carry no
		// deadline, and the backoff caps total retry time, not a single in-flight
		// call, so a hung backend would otherwise block past timeout.
		tipCtx, cancel := context.WithDeadline(ctx, deadline)
		defer cancel()
		tip, err := b.Tip(tipCtx)
		if err != nil {
			// A tip-query failure is fatal — don't retry a broken backend.
			return backoff.Permanent(fmt.Errorf("backend tip query: %w", err))
		}
		if tip >= target {
			return nil
		}
		// Retryable. This is the error backoff.Retry returns once MaxElapsedTime
		// stops the loop, so callers still classify the timeout via ErrBackendCoverageTimeout.
		return fmt.Errorf("%w: tip %d < needed %d after %s", ErrBackendCoverageTimeout, tip, target, timeout)
	}

	// A constant interval bounded by timeout (MaxElapsedTime) and ctx (cancellation).
	// WithMaxElapsedTime only composes with ExponentialBackOff, so a unit multiplier
	// and zero randomization reduce it to a constant poll.
	bo := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(interval),
		backoff.WithMaxInterval(interval),
		backoff.WithMultiplier(1),
		backoff.WithRandomizationFactor(0),
		backoff.WithMaxElapsedTime(timeout),
	)
	return backoff.Retry(poll, backoff.WithContext(bo, ctx))
}
