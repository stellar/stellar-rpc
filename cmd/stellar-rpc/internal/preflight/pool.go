//nolint:funcorder // worker lifecycle helpers are grouped for readability
package preflight

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
)

var (
	ErrPreflightQueueFull = errors.New("preflight queue full")
	// ErrSimulationFailed is returned when the worker process running a
	// simulation exceeded its memory or time limit, or stopped.
	ErrSimulationFailed = errors.New("simulation exceeded its resource limits or failed")
	errPoolClosed       = errors.New("preflight worker pool is closed")
)

// WorkerPool runs each simulation in one of WorkerCount worker processes. A
// call takes an idle worker and runs its simulation on it.
type WorkerPool struct {
	logger                     *log.Entry
	ctx                        context.Context //nolint:containedctx // ends at Close
	cancel                     context.CancelFunc
	idle                       chan *workerProcess
	queue                      chan struct{} // a slot for each call waiting for a worker
	concurrentRequestsMetric   prometheus.Gauge
	errorFullCounter           prometheus.Counter
	durationMetric             *prometheus.SummaryVec
	ledgerEntriesFetchedMetric prometheus.Summary
	workerExitsMetric          *prometheus.CounterVec

	// Close cancels ctx under mu, and addWorker starts nothing once ctx is
	// canceled, so wg cannot grow from zero while Close waits on it.
	mu sync.Mutex
	wg sync.WaitGroup

	workerConfig workerConfig
	limits       poolLimits
}

type WorkerPoolConfig struct {
	Daemon            host.Daemon
	WorkerCount       uint
	JobQueueCapacity  uint
	EnableDebug       bool
	NetworkPassphrase string
	Logger            *log.Entry
	// CoreURL is captive core's query server, where the workers read ledger
	// entries.
	CoreURL            string
	CoreRequestTimeout time.Duration
}

// poolLimits are fixed in production. Tests change them.
type poolLimits struct {
	maxResponseBytes int
	minRestartDelay  time.Duration
	maxRestartDelay  time.Duration
}

func defaultPoolLimits() poolLimits {
	return poolLimits{
		maxResponseBytes: maxResponseBytes,
		minRestartDelay:  100 * time.Millisecond,
		maxRestartDelay:  10 * time.Second,
	}
}

func NewPreflightWorkerPool(cfg WorkerPoolConfig) *WorkerPool {
	return newWorkerPool(cfg, defaultPoolLimits())
}

func newWorkerPool(cfg WorkerPoolConfig, limits poolLimits) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())
	preflightWP := WorkerPool{
		logger: cfg.Logger,
		ctx:    ctx,
		cancel: cancel,
		idle:   make(chan *workerProcess, cfg.WorkerCount),
		queue:  make(chan struct{}, cfg.JobQueueCapacity),
		workerConfig: workerConfig{
			NetworkPassphrase:  cfg.NetworkPassphrase,
			EnableDebug:        cfg.EnableDebug,
			CoreURL:            cfg.CoreURL,
			CoreRequestTimeout: cfg.CoreRequestTimeout,
		},
		limits: limits,
	}
	requestQueueMetric := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: cfg.Daemon.MetricsNamespace(),
		Subsystem: "preflight_pool",
		Name:      "queue_length",
		Help:      "number of preflight requests in the queue",
	}, func() float64 {
		return float64(len(preflightWP.queue))
	})
	preflightWP.concurrentRequestsMetric = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: cfg.Daemon.MetricsNamespace(),
		Subsystem: "preflight_pool",
		Name:      "concurrent_requests",
		Help:      "number of preflight requests currently running",
	})
	preflightWP.errorFullCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: cfg.Daemon.MetricsNamespace(),
		Subsystem: "preflight_pool",
		Name:      "queue_full_errors",
		Help:      "number of preflight full queue errors",
	})
	preflightWP.durationMetric = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  cfg.Daemon.MetricsNamespace(),
		Subsystem:  "preflight_pool",
		Name:       "request_ledger_get_duration_seconds",
		Help:       "preflight request duration broken down by status",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}, //nolint:mnd
	}, []string{"status", "type"})
	preflightWP.ledgerEntriesFetchedMetric = prometheus.NewSummary(prometheus.SummaryOpts{
		Namespace:  cfg.Daemon.MetricsNamespace(),
		Subsystem:  "preflight_pool",
		Name:       "request_ledger_entries_fetched",
		Help:       "ledger entries fetched by simulate transaction calls",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}, //nolint:mnd
	})
	preflightWP.workerExitsMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: cfg.Daemon.MetricsNamespace(),
		Subsystem: "preflight_pool",
		Name:      "worker_exits_total",
		Help:      "number of preflight worker processes that exited, by cause",
	}, []string{"cause"})
	cfg.Daemon.MetricsRegistry().MustRegister(
		requestQueueMetric,
		preflightWP.concurrentRequestsMetric,
		preflightWP.errorFullCounter,
		preflightWP.durationMetric,
		preflightWP.ledgerEntriesFetchedMetric,
		preflightWP.workerExitsMetric,
	)
	for slot := range cfg.WorkerCount {
		preflightWP.addWorker(slot)
	}
	return &preflightWP
}

// addWorker starts a worker for slot in the background, unless the pool has
// closed, and makes it idle once it is ready. A worker that fails to start
// is started again after a delay that doubles from minRestartDelay up to
// maxRestartDelay.
func (pwp *WorkerPool) addWorker(slot uint) {
	pwp.mu.Lock()
	defer pwp.mu.Unlock()
	if pwp.ctx.Err() != nil {
		return
	}
	pwp.wg.Go(func() {
		for delay := pwp.limits.minRestartDelay; ; delay = min(2*delay, pwp.limits.maxRestartDelay) {
			w, err := pwp.startWorker(slot)
			switch {
			case err == nil:
				pwp.idle <- w
				return
			case pwp.ctx.Err() != nil:
				return
			}
			pwp.logger.WithError(err).WithFields(log.F{"preflight_worker": slot, "retry_in": delay}).
				Error("could not start a preflight worker")
			select {
			case <-pwp.ctx.Done():
				return
			case <-time.After(delay):
			}
		}
	})
}

// takeWorker takes an idle worker, waiting for one until ctx ends or the
// pool closes. A worker that exited while idle is replaced, and the call
// takes another.
func (pwp *WorkerPool) takeWorker(ctx context.Context) (*workerProcess, error) {
	for {
		w, err := pwp.nextIdle(ctx)
		if err != nil {
			return nil, err
		}
		select {
		case <-w.exited:
			pwp.replace(w, w.exitCause(nil))
		default:
			return w, nil
		}
	}
}

// nextIdle takes an idle worker. When there is none, the call waits in the
// queue, unless the queue is full.
func (pwp *WorkerPool) nextIdle(ctx context.Context) (*workerProcess, error) {
	select {
	case w := <-pwp.idle:
		return w, nil
	default:
	}
	select {
	case pwp.queue <- struct{}{}:
	default:
		pwp.errorFullCounter.Inc()
		return nil, ErrPreflightQueueFull
	}
	defer func() { <-pwp.queue }()
	select {
	case w := <-pwp.idle:
		return w, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-pwp.ctx.Done():
		return nil, errPoolClosed
	}
}

// replace counts and logs the exit of w, unless the pool closed and killed
// it, and starts another worker in its slot.
func (pwp *WorkerPool) replace(w *workerProcess, cause string) {
	if pwp.ctx.Err() == nil {
		pwp.workerExitsMetric.WithLabelValues(cause).Inc()
		w.logger.WithFields(log.F{"cause": cause, "status": w.cmd.ProcessState.String()}).
			Warn("preflight worker exited; starting another")
	}
	pwp.addWorker(w.slot)
}

// Close stops the workers. Callers still waiting for a simulation get an
// error.
func (pwp *WorkerPool) Close() {
	pwp.mu.Lock()
	pwp.cancel()
	pwp.mu.Unlock()
	pwp.wg.Wait()
}

type GetterParameters struct {
	BucketListSize  uint64
	SourceAccount   xdr.AccountIdView
	OperationBody   xdr.OperationBodyView
	Footprint       xdr.LedgerFootprintView
	ResourceConfig  protocol.ResourceConfig
	AuthMode        string
	UseUpgradedAuth bool
	ProtocolVersion uint32
	LedgerSeq       uint32
	LedgerTime      uint64
	Format          string
}

// GetPreflight runs a simulation in a worker.
func (pwp *WorkerPool) GetPreflight(ctx context.Context, params GetterParameters) (Response, error) {
	// A call whose context has ended does not run: takeWorker's select could
	// still pick an idle worker.
	if err := ctx.Err(); err != nil {
		return Response{}, err
	}
	w, err := pwp.takeWorker(ctx)
	if err != nil {
		return Response{}, err
	}
	// A worker can become idle just as ctx ends.
	if err := ctx.Err(); err != nil {
		pwp.idle <- w
		return Response{}, err
	}
	pwp.concurrentRequestsMetric.Inc()
	defer pwp.concurrentRequestsMetric.Dec()
	startTime := time.Now()
	response, err := w.simulate(ctx, params, pwp.limits.maxResponseBytes)
	if err != nil {
		w.stop()
		pwp.replace(w, w.exitCause(err))
		pwp.durationMetric.With(
			prometheus.Labels{"type": "all", "status": "error"},
		).Observe(time.Since(startTime).Seconds())
		return Response{}, ErrSimulationFailed
	}
	pwp.idle <- w
	status := "ok"
	if response.Error != "" {
		err = errors.New(response.Error)
		status = "error"
	}
	pwp.durationMetric.With(
		prometheus.Labels{"type": "all", "status": status},
	).Observe(time.Since(startTime).Seconds())
	if response.LedgerEntriesFetched > 0 {
		pwp.durationMetric.With(
			prometheus.Labels{"type": "db", "status": status},
		).Observe(response.LedgerEntriesTime.Seconds())
	}
	pwp.ledgerEntriesFetchedMetric.Observe(float64(response.LedgerEntriesFetched))
	return Response{body: response.Body, failed: response.Failed}, err
}
