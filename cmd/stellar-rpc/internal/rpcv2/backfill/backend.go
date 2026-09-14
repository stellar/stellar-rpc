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
// Two implementations: bsbSource here (Tip = datastore.FindLatestLedgerSequence,
// the lake frontier) and the daemon layer's captiveSource for a no-lake
// deployment (captive core replays each chunk's bounded range; Tip is the
// history archives' root HAS). Only a deployment with neither a datastore nor
// archives has no Backend — then nothing below the pinned floor is fillable and
// backfillSource errors on any backend-only chunk.
//
// Tip is its own method because the SDK's LedgerBackend.GetLatestLedgerSequence is
// NOT the frontier we want — it is prepared-relative (errors before PrepareRange on
// a BufferedStorageBackend, and otherwise returns the buffer's latest, not the
// source's frontier). Each impl therefore supplies its own frontier query.
type Backend interface {
	ledgerbackend.LedgerStream
	Tip(ctx context.Context) (uint32, error)
}

// Default buffered-storage tuning. Exported because the config package fills
// them into an unset [backfill.bsb] section and the bench command uses them as
// its flag defaults — the values are defined once, here.
const (
	// Every value below is PER CHUNK TASK, and backfill runs [backfill].workers
	// tasks at once — totals multiply by the worker count.

	// DefaultBSBPrefetchBytes caps one task's prefetch in bytes — the bound
	// that tracks memory, since object size varies ~700x across pubnet history
	// while a count does not. Throughput is flat across an 8x range around it.
	DefaultBSBPrefetchBytes = 32 << 20

	// DefaultBSBDownloads is one task's concurrent object downloads. 12-50
	// measure identically, and aggregates to 1600 requests are error-free, so
	// it is chosen small.
	DefaultBSBDownloads = 25

	// DefaultBSBPrefetchObjects is one task's prefetch depth in objects. It
	// bounds per-object overhead; memory is DefaultBSBPrefetchBytes' job.
	DefaultBSBPrefetchObjects = 5000

	DefaultBSBMaxRetries = 3
	DefaultBSBRetryWait  = 5 * time.Second
)

// FillBSBDefaults fills unset (zero) fields; explicitly set fields are
// honored. There is no way to disable the byte cap: zero means unset, and a
// caller wanting it effectively unbounded sets a huge value.
func FillBSBDefaults(
	bsb ledgerbackend.BufferedStorageBackendConfig,
) ledgerbackend.BufferedStorageBackendConfig {
	if bsb.BufferSize == 0 {
		bsb.BufferSize = DefaultBSBPrefetchObjects
	}
	if bsb.BufferBytes == 0 {
		bsb.BufferBytes = DefaultBSBPrefetchBytes
	}
	if bsb.NumWorkers == 0 {
		bsb.NumWorkers = DefaultBSBDownloads
	}
	if bsb.NumWorkers > bsb.BufferSize {
		bsb.NumWorkers = bsb.BufferSize
	}
	return bsb
}

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
// Tip and is owned and Closed by the caller (the daemon).
//
// A zero BufferSize or NumWorkers falls back to the defaults above (the SDK
// backend rejects BufferSize == 0). The retry fields are used VERBATIM —
// RetryLimit 0 really means no retries — so a caller wanting the default retry
// policy must pass DefaultBSBMaxRetries / DefaultBSBRetryWait explicitly, as
// the daemon's config layer does for an unset [backfill.bsb].
func NewBSBBackend(
	ds datastore.DataStore,
	cfg datastore.DataStoreConfig,
	bsb ledgerbackend.BufferedStorageBackendConfig,
) Backend {
	return &bsbSource{
		LedgerStream: ledgerbackend.NewBufferedStorageStream(FillBSBDefaults(bsb), cfg, nil),
		ds:           ds,
	}
}

// NewBSBBackendFromConfig opens the datastore described by dsCfg and returns a BSB
// Backend over it, plus a close func that releases the long-lived Tip datastore handle
// at shutdown. The datastore type (GCS/S3/Filesystem/...) is whatever dsCfg.Type names —
// any SDK datastore works. The batch schema is read from the lake manifest by the
// stream, so dsCfg need not set Schema.
//
// When dsCfg.NetworkPassphrase is non-empty, the lake's manifest is verified
// against dsCfg eagerly, here: the stream re-runs the same check on every open
// anyway, and failing now turns a wrong-network lake into one clear startup
// error instead of a retried chunk-task crash loop. A lake with no manifest
// skips the comparison — provided dsCfg carries the schema (LoadSchema needs
// one of the two).
func NewBSBBackendFromConfig(
	ctx context.Context,
	dsCfg datastore.DataStoreConfig,
	bsb ledgerbackend.BufferedStorageBackendConfig,
) (Backend, func(), error) {
	ds, err := datastore.NewDataStore(ctx, dsCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("open datastore (type %q): %w", dsCfg.Type, err)
	}
	if dsCfg.NetworkPassphrase != "" {
		if _, err := datastore.LoadSchema(ctx, ds, dsCfg); err != nil {
			_ = ds.Close()
			return nil, nil, fmt.Errorf("verify datastore manifest (type %q): %w", dsCfg.Type, err)
		}
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
			// A tip-query failure stops this poll loop immediately (no point
			// re-polling a broken backend); the error surfaces and fails the
			// pass (the process exits; the orchestrator restarts).
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
