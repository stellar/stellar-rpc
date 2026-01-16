package ingest

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/stellar/go-stellar-sdk/historyarchive"
	backends "github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/feewindow"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/util"
)

const (
	maxRetries = 5
)

var errEmptyArchives = errors.New("cannot start ingestion without history archives, " +
	"wait until first history archives are published")

type Config struct {
	Logger            *log.Entry
	DB                db.ReadWriter
	FeeWindows        *feewindow.FeeWindows
	NetworkPassPhrase string
	Archive           historyarchive.ArchiveInterface
	LedgerBackend     backends.LedgerBackend
	Timeout           time.Duration
	OnIngestionRetry  backoff.Notify
	Daemon            interfaces.Daemon
}

func NewService(cfg Config) *Service {
	service := newService(cfg)
	return service
}

func newService(cfg Config) *Service {
	// ingestionDurationMetric is a metric for measuring the latency of ingestion
	ingestionDurationMetric := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: cfg.Daemon.MetricsNamespace(), Subsystem: "ingest", Name: "ledger_ingestion_duration_seconds",
		Help:       "ledger ingestion durations, sliding window = 10m",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}, //nolint:mnd
	},
		[]string{"type"},
	)
	// latestLedgerMetric is a metric for measuring the latest ingested ledger
	latestLedgerMetric := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: cfg.Daemon.MetricsNamespace(), Subsystem: "ingest", Name: "local_latest_ledger",
		Help: "sequence number of the latest ledger ingested by this ingesting instance",
	})

	// ledgerStatsMetric is a metric which measures statistics on all ledger entries ingested by stellar rpc
	ledgerStatsMetric := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: cfg.Daemon.MetricsNamespace(), Subsystem: "ingest", Name: "ledger_stats_total",
			Help: "counters of different ledger stats",
		},
		[]string{"type"},
	)

	cfg.Daemon.MetricsRegistry().MustRegister(
		ingestionDurationMetric,
		latestLedgerMetric,
		ledgerStatsMetric)

	service := &Service{
		logger:            cfg.Logger,
		db:                cfg.DB,
		feeWindows:        cfg.FeeWindows,
		ledgerBackend:     cfg.LedgerBackend,
		networkPassPhrase: cfg.NetworkPassPhrase,
		timeout:           cfg.Timeout,
		metrics: Metrics{
			ingestionDurationMetric: ingestionDurationMetric,
			latestLedgerMetric:      latestLedgerMetric,
			ledgerStatsMetric:       ledgerStatsMetric,
		},
	}

	return service
}

func StartService(service *Service, cfg Config) {
	ctx, done := context.WithCancel(context.Background())
	service.done = done
	service.wg.Add(1)
	panicGroup := util.UnrecoverablePanicGroup.Log(cfg.Logger)
	panicGroup.Go(func() {
		defer service.wg.Done()
		// Retry running ingestion every second for 5 seconds.
		constantBackoff := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), maxRetries)
		// Don't want to keep retrying if the context gets canceled.
		contextBackoff := backoff.WithContext(constantBackoff, ctx)
		err := backoff.RetryNotify(
			func() error {
				err := service.run(ctx, cfg.Archive)
				if errors.Is(err, errEmptyArchives) {
					// keep retrying until history archives are published
					constantBackoff.Reset()
				}
				return err
			},
			contextBackoff,
			cfg.OnIngestionRetry)
		if err != nil && !errors.Is(err, context.Canceled) {
			service.logger.WithError(err).Fatal("could not run ingestion")
		}
	})
}

type Metrics struct {
	ingestionDurationMetric *prometheus.SummaryVec
	latestLedgerMetric      prometheus.Gauge
	ledgerStatsMetric       *prometheus.CounterVec
}

type Service struct {
	logger            *log.Entry
	db                db.ReadWriter
	feeWindows        *feewindow.FeeWindows
	ledgerBackend     backends.LedgerBackend
	timeout           time.Duration
	networkPassPhrase string
	done              context.CancelFunc
	wg                sync.WaitGroup
	metrics           Metrics
	latestIngestedSeq uint32
}

func (s *Service) Close() error {
	s.done()
	s.wg.Wait()
	return nil
}

func (s *Service) run(ctx context.Context, archive historyarchive.ArchiveInterface) error {
	nextLedgerSeq, err := s.getNextLedgerSequence(ctx, archive)
	if err != nil {
		return err
	}

	for ; ; nextLedgerSeq++ {
		if err := s.ingest(ctx, nextLedgerSeq); err != nil {
			return err
		}
	}
}

func (s *Service) getNextLedgerSequence(ctx context.Context,
	archive historyarchive.ArchiveInterface,
) (uint32, error) {
	var nextLedgerSeq uint32
	curLedgerSeq, err := s.db.GetLatestLedgerSequence(ctx)
	switch {
	case err == nil:
		nextLedgerSeq = curLedgerSeq + 1

	case errors.Is(err, db.ErrEmptyDB):
		root, rootErr := archive.GetRootHAS()
		// DB is empty, check latest available ledger in History Archives
		if rootErr != nil {
			return 0, rootErr
		}
		if root.CurrentLedger == 0 {
			return 0, errEmptyArchives
		}
		nextLedgerSeq = root.CurrentLedger

	default:
		return 0, err
	}
	prepareRangeCtx, cancelPrepareRange := context.WithTimeout(ctx, s.timeout)
	defer cancelPrepareRange()
	return nextLedgerSeq,
		s.ledgerBackend.PrepareRange(prepareRangeCtx, backends.UnboundedRange(nextLedgerSeq))
}

func (s *Service) ingest(ctx context.Context, sequence uint32) error {
	s.logger.Infof("Ingesting ledger %d", sequence)
	ledgerCloseMeta, err := s.ledgerBackend.GetLedger(ctx, sequence)
	if err != nil {
		return err
	}

	startTime := time.Now()
	tx, err := s.db.NewTx(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			s.logger.WithError(err).Warn("could not rollback ingest write transactions")
		}
	}()

	if err := s.ingestLedgerCloseMeta(tx, ledgerCloseMeta); err != nil {
		return err
	}

	// Abstracted from ingestLedgerCloseMeta to allow fee window ingestion to be optional
	startTime = time.Now()
	if err := s.feeWindows.IngestFees(ledgerCloseMeta); err != nil {
		return err
	}
	s.metrics.ingestionDurationMetric.
		With(prometheus.Labels{"type": "fee-window"}).
		Observe(time.Since(startTime).Seconds())

	durationMetrics := map[string]time.Duration{}
	if err := tx.Commit(ledgerCloseMeta, durationMetrics); err != nil {
		return err
	}
	for key, duration := range durationMetrics {
		s.metrics.ingestionDurationMetric.
			With(prometheus.Labels{"type": key}).
			Observe(duration.Seconds())
	}

	s.logger.
		WithField("duration", time.Since(startTime).Seconds()).
		Debugf("Ingested ledger %d", sequence)

	s.metrics.ingestionDurationMetric.
		With(prometheus.Labels{"type": "total"}).
		Observe(time.Since(startTime).Seconds())
	if sequence > s.latestIngestedSeq {
		s.latestIngestedSeq = sequence
		s.metrics.latestLedgerMetric.Set(float64(sequence))
	}
	return nil
}

// Ingests a range of ledgers from a provided ledgerBackend
// Does NOT call ingestLedgerCloseMeta for each ledger as these metrics aren't suitable for backfilling
func (s *Service) ingestRange(ctx context.Context, backend backends.LedgerBackend, seqRange backends.Range) error {
	s.logger.Infof("Ingesting ledgers [%d, %d]", seqRange.From(), seqRange.To())
	var ledgerCloseMeta xdr.LedgerCloseMeta

	startTime := time.Now()
	tx, err := s.db.NewTx(ctx)
	if err != nil {
		return err
	}

	if err := backend.PrepareRange(ctx, seqRange); err != nil {
		return err
	}

	defer func() {
		if err := tx.Rollback(); err != nil {
			s.logger.WithError(err).Warn("could not rollback ingest write transactions")
		}
	}()

	for seq := seqRange.From(); seq <= seqRange.To(); seq++ {
		ledgerCloseMeta, err = backend.GetLedger(ctx, seq)
		if err != nil {
			return err
		}
		if err := s.ingestLedgerCloseMeta(tx, ledgerCloseMeta); err != nil {
			return err
		}
	}

	durationMetrics := map[string]time.Duration{}
	if err := tx.Commit(ledgerCloseMeta, durationMetrics); err != nil {
		return err
	}
	for key, duration := range durationMetrics {
		s.metrics.ingestionDurationMetric.
			With(prometheus.Labels{"type": key}).
			Observe(duration.Seconds())
	}

	s.logger.
		WithField("duration", time.Since(startTime).Seconds()).
		Debugf("Ingested ledgers [%d, %d]", seqRange.From(), seqRange.To())

	s.metrics.ingestionDurationMetric.
		With(prometheus.Labels{"type": "total"}).
		Observe(time.Since(startTime).Seconds())
	if seqRange.To() > s.latestIngestedSeq {
		s.latestIngestedSeq = seqRange.To()
		s.metrics.latestLedgerMetric.Set(float64(seqRange.To()))
	}
	return nil
}

func (s *Service) ingestLedgerCloseMeta(tx db.WriteTx, ledgerCloseMeta xdr.LedgerCloseMeta) error {
	startTime := time.Now()
	if err := tx.LedgerWriter().InsertLedger(ledgerCloseMeta); err != nil {
		return err
	}
	s.metrics.ingestionDurationMetric.
		With(prometheus.Labels{"type": "ledger_close_meta"}).
		Observe(time.Since(startTime).Seconds())

	startTime = time.Now()
	if err := tx.TransactionWriter().InsertTransactions(ledgerCloseMeta); err != nil {
		return err
	}
	s.metrics.ingestionDurationMetric.
		With(prometheus.Labels{"type": "transactions"}).
		Observe(time.Since(startTime).Seconds())

	startTime = time.Now()
	if err := tx.EventWriter().InsertEvents(ledgerCloseMeta); err != nil {
		return err
	}
	s.metrics.ingestionDurationMetric.
		With(prometheus.Labels{"type": "events"}).
		Observe(time.Since(startTime).Seconds())

	return nil
}
