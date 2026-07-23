//nolint:funcorder // daemon lifecycle helpers are grouped for readability
package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	runtimePprof "runtime/pprof"
	"sync"
	"syscall"
	"time"

	"github.com/go-chi/chi"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/stellar/go-stellar-sdk/clients/stellarcore"
	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/ingest/loadtest"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	supporthttp "github.com/stellar/go-stellar-sdk/support/http"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/support/storage"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/preflight"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcdatastore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/feewindow"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/sqlitedb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/util"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/version"
)

const (
	defaultReadTimeout         = 5 * time.Second
	defaultShutdownGracePeriod = 10 * time.Second

	// Since our default retention window will be 7 days (7*17,280 ledgers),
	// choose a random 5-digit prime to have irregular logging intervals at each
	// halfish-day of processing
	inMemoryInitializationLedgerLogPeriod = 10_099
)

type Daemon struct {
	core                *ledgerbackend.CaptiveStellarCore
	coreClient          *CoreClientWithMetrics
	coreQueryingClient  host.FastCoreClient
	ingestService       *ingest.Service
	db                  *sqlitedb.DB
	jsonRPCHandler      *rpcv1.Handler
	logger              *supportlog.Entry
	preflightWorkerPool *preflight.WorkerPool
	listener            net.Listener
	server              *http.Server
	adminListener       net.Listener
	adminServer         *http.Server
	closeOnce           sync.Once
	closeError          error
	done                chan struct{}
	metricsRegistry     *prometheus.Registry
	dataStore           datastore.DataStore
	dataStoreSchema     datastore.DataStoreSchema
}

func (d *Daemon) GetDB() *sqlitedb.DB {
	return d.db
}

func (d *Daemon) GetEndpointAddrs() (net.TCPAddr, *net.TCPAddr) {
	//nolint:forcetypeassert
	addr := d.listener.Addr().(*net.TCPAddr)
	var adminAddr *net.TCPAddr
	if d.adminListener != nil {
		//nolint:forcetypeassert
		adminAddr = d.adminListener.Addr().(*net.TCPAddr)
	}
	return *addr, adminAddr
}

func (d *Daemon) close() {
	shutdownCtx, shutdownRelease := context.WithTimeout(context.Background(), defaultShutdownGracePeriod)
	defer shutdownRelease()
	var closeErrors []error

	if err := d.server.Shutdown(shutdownCtx); err != nil {
		d.logger.WithError(err).Error("error during Soroban JSON RPC server Shutdown")
		closeErrors = append(closeErrors, err)
	}
	if d.adminServer != nil {
		if err := d.adminServer.Shutdown(shutdownCtx); err != nil {
			d.logger.WithError(err).Error("error during Soroban JSON admin server Shutdown")
			closeErrors = append(closeErrors, err)
		}
	}

	if err := d.ingestService.Close(); err != nil {
		d.logger.WithError(err).Error("error closing ingestion service")
		closeErrors = append(closeErrors, err)
	}
	if err := d.core.Close(); err != nil {
		d.logger.WithError(err).Error("error closing captive core")
		closeErrors = append(closeErrors, err)
	}
	d.jsonRPCHandler.Close()
	if err := d.db.Close(); err != nil {
		d.logger.WithError(err).Error("Error closing db")
		closeErrors = append(closeErrors, err)
	}
	d.preflightWorkerPool.Close()

	if d.dataStore != nil {
		err := d.dataStore.Close()
		if err != nil {
			d.logger.WithError(err).Error("error closing datastore")
			closeErrors = append(closeErrors, err)
		}
	}

	d.closeError = errors.Join(closeErrors...)
	close(d.done)
}

func (d *Daemon) Close() error {
	d.closeOnce.Do(d.close)
	return d.closeError
}

// newCaptiveCore creates a new captive core backend instance and returns it.
func newCaptiveCore(cfg *config.Config, logger *supportlog.Entry) (*ledgerbackend.CaptiveStellarCore, error) {
	queryServerParams := &ledgerbackend.HTTPQueryServerParams{
		Port:            cfg.CaptiveCoreHTTPQueryPort,
		ThreadPoolSize:  cfg.CaptiveCoreHTTPQueryThreadPoolSize,
		SnapshotLedgers: cfg.CaptiveCoreHTTPQuerySnapshotLedgers,
	}

	httpPort := uint(cfg.CaptiveCoreHTTPPort)
	captiveCoreTomlParams := ledgerbackend.CaptiveCoreTomlParams{
		HTTPPort:                           &httpPort,
		HistoryArchiveURLs:                 cfg.HistoryArchiveURLs,
		NetworkPassphrase:                  cfg.NetworkPassphrase,
		Strict:                             true,
		EnforceSorobanDiagnosticEvents:     true,
		EnforceSorobanTransactionMetaExtV1: true,
		EmitUnifiedEvents:                  true,
		EmitUnifiedEventsBeforeProtocol22:  false,
		CoreBinaryPath:                     cfg.StellarCoreBinaryPath,
		HTTPQueryServerParams:              queryServerParams,
	}
	captiveCoreToml, err := ledgerbackend.NewCaptiveCoreTomlFromFile(cfg.CaptiveCoreConfigPath, captiveCoreTomlParams)
	if err != nil {
		logger.WithError(err).Fatal("Invalid captive core toml")
	}

	captiveConfig := ledgerbackend.CaptiveCoreConfig{
		BinaryPath:          cfg.StellarCoreBinaryPath,
		StoragePath:         cfg.CaptiveCoreStoragePath,
		NetworkPassphrase:   cfg.NetworkPassphrase,
		HistoryArchiveURLs:  cfg.HistoryArchiveURLs,
		CheckpointFrequency: cfg.CheckpointFrequency,
		Log:                 logger.WithField("subservice", "stellar-core"),
		Toml:                captiveCoreToml,
		UserAgent:           cfg.ExtendedUserAgent("captivecore"),
	}
	return ledgerbackend.NewCaptive(captiveConfig)
}

func MustNew(cfg *config.Config, logger *supportlog.Entry) *Daemon {
	logger = setupLogger(cfg, logger)
	core := mustCreateCaptiveCore(cfg, logger)
	historyArchive := mustCreateHistoryArchive(cfg, logger)
	metricsRegistry := prometheus.NewRegistry()

	daemon := &Daemon{
		logger:             logger,
		core:               core,
		db:                 mustOpenDatabase(cfg, logger, metricsRegistry),
		done:               make(chan struct{}),
		metricsRegistry:    metricsRegistry,
		coreClient:         newCoreClientWithMetrics(createStellarCoreClient(cfg), metricsRegistry),
		coreQueryingClient: createHighperfStellarCoreClient(cfg),
	}

	feewindows := daemon.mustInitializeStorage(cfg)

	// Create the read-writer once and reuse in ingest service/backfill
	rw := sqlitedb.NewReadWriter(
		logger,
		daemon.db,
		daemon,
		cfg.HistoryRetentionWindow,
		cfg.NetworkPassphrase,
	)
	if cfg.ServeLedgersFromDatastore {
		daemon.dataStore, daemon.dataStoreSchema = mustCreateDataStore(cfg, logger)
	}
	var ingestCfg ingest.Config
	daemon.ingestService, ingestCfg = createIngestService(cfg, logger, daemon, feewindows, historyArchive, rw)
	if cfg.Backfill {
		backfillMeta, err := ingest.NewBackfillMeta(
			logger,
			daemon.ingestService,
			sqlitedb.NewLedgerReader(daemon.db),
			daemon.dataStore,
			daemon.dataStoreSchema,
		)
		if err != nil {
			logger.WithError(err).Fatal("failed to create backfill metadata")
		}
		if err := backfillMeta.RunBackfill(cfg); err != nil {
			logger.WithError(err).Fatal("failed to backfill ledgers")
		}
		// Clear the DB cache and fee windows so they re-populate from the database
		daemon.db.ResetCache()
		feewindows.Reset()
	}
	// Start ingestion service only after backfill is complete
	daemon.ingestService.Start(ingestCfg)

	daemon.preflightWorkerPool = createPreflightWorkerPool(cfg, logger, daemon)
	daemon.jsonRPCHandler = createJSONRPCHandler(cfg, logger, daemon, feewindows)

	daemon.setupHTTPServers(cfg)
	daemon.registerMetrics()

	return daemon
}

func mustCreateDataStore(cfg *config.Config, logger *supportlog.Entry) (datastore.DataStore,
	datastore.DataStoreSchema,
) {
	dataStore, err := datastore.NewDataStore(context.Background(), cfg.DataStoreConfig)
	if err != nil {
		logger.WithError(err).Fatal("failed to initialize datastore")
	}

	schema, err := datastore.LoadSchema(context.Background(), dataStore, cfg.DataStoreConfig)
	if err != nil {
		logger.WithError(err).Fatal("failed to retrieve datastore schema ")
	}

	return dataStore, schema
}

func setupLogger(cfg *config.Config, logger *supportlog.Entry) *supportlog.Entry {
	logger.SetLevel(cfg.LogLevel)
	if cfg.LogFormat == config.LogFormatJSON {
		logger.UseJSONFormatter()
	}
	logger.WithFields(supportlog.F{
		versionLabel: version.Version,
		commitLabel:  version.CommitHash,
	}).Info("starting Stellar RPC")
	return logger
}

func mustCreateCaptiveCore(cfg *config.Config, logger *supportlog.Entry) *ledgerbackend.CaptiveStellarCore {
	core, err := newCaptiveCore(cfg, logger)
	if err != nil {
		logger.WithError(err).Fatal("could not create captive core")
	}
	return core
}

func mustCreateHistoryArchive(cfg *config.Config, logger *supportlog.Entry) *historyarchive.ArchiveInterface {
	if len(cfg.HistoryArchiveURLs) == 0 {
		logger.Fatal("no history archives URLs were provided")
	}

	historyArchive, err := historyarchive.NewArchivePool(
		cfg.HistoryArchiveURLs,
		historyarchive.ArchiveOptions{
			Logger:              logger,
			NetworkPassphrase:   cfg.NetworkPassphrase,
			CheckpointFrequency: cfg.CheckpointFrequency,
			ConnectOptions: storage.ConnectOptions{
				Context:   context.Background(),
				UserAgent: cfg.HistoryArchiveUserAgent,
			},
		},
	)
	if err != nil {
		logger.WithError(err).Fatal("could not connect to history archive")
	}
	return &historyArchive
}

func mustOpenDatabase(cfg *config.Config, logger *supportlog.Entry, metricsRegistry *prometheus.Registry) *sqlitedb.DB {
	dbConn, err := sqlitedb.OpenSQLiteDBWithPrometheusMetrics(
		cfg.SQLiteDBPath, host.PrometheusNamespace, "db", metricsRegistry)
	if err != nil {
		logger.WithError(err).Fatal("could not open database")
	}
	return dbConn
}

func createStellarCoreClient(cfg *config.Config) stellarcore.Client {
	return stellarcore.Client{
		URL:  cfg.StellarCoreURL,
		HTTP: &http.Client{Timeout: cfg.CoreRequestTimeout},
	}
}

func createHighperfStellarCoreClient(cfg *config.Config) host.FastCoreClient {
	return &stellarcore.Client{
		URL:  fmt.Sprintf("http://localhost:%d", cfg.CaptiveCoreHTTPQueryPort),
		HTTP: &http.Client{Timeout: cfg.CoreRequestTimeout},
	}
}

func createIngestService(cfg *config.Config, logger *supportlog.Entry, daemon *Daemon,
	feewindows *feewindow.FeeWindows, historyArchive *historyarchive.ArchiveInterface, rw sqlitedb.ReadWriter,
) (*ingest.Service, ingest.Config) {
	onIngestionRetry := func(err error, _ time.Duration) {
		logger.WithError(err).Error("could not run ingestion. Retrying")
	}

	var backend ledgerbackend.LedgerBackend = daemon.core
	if cfg.IngestLoadTest.Enabled() {
		// CustomSetValue/MarshalTOML doesn't apply DefaultValue, so fall back here.
		frequency := cfg.IngestLoadTest.Frequency
		if frequency == 0 {
			frequency = config.DefaultIngestLoadTestFrequency
		}
		logger.
			WithField("files", cfg.IngestLoadTest.Files).
			WithField("max_ledgers_per_file", cfg.IngestLoadTest.MaxLedgersPerFile).
			WithField("close_time", frequency).
			Warn("Ingestion will run with load testing")

		backend = loadtest.NewLedgerBackend(loadtest.LedgerBackendConfig{
			NetworkPassphrase:   cfg.NetworkPassphrase,
			LedgersFilePaths:    cfg.IngestLoadTest.Files,
			LedgerCloseDuration: frequency,
			MaxLedgersPerFile:   cfg.IngestLoadTest.MaxLedgersPerFile,
		})
	}

	ingestCfg := ingest.Config{
		Logger:            logger,
		DB:                rw,
		NetworkPassPhrase: cfg.NetworkPassphrase,
		Archive:           *historyArchive,
		LedgerBackend:     backend,
		Timeout:           cfg.IngestionTimeout,
		OnIngestionRetry:  onIngestionRetry,
		OnLedgerIngested:  cfg.IngestLoadTest.OnLedgerIngested,
		Daemon:            daemon,
		FeeWindows:        feewindows,
	}
	return ingest.NewService(ingestCfg), ingestCfg
}

func createPreflightWorkerPool(cfg *config.Config, logger *supportlog.Entry, daemon *Daemon) *preflight.WorkerPool {
	return preflight.NewPreflightWorkerPool(
		preflight.WorkerPoolConfig{
			Daemon:            daemon,
			WorkerCount:       cfg.PreflightWorkerCount,
			JobQueueCapacity:  cfg.PreflightWorkerQueueSize,
			EnableDebug:       cfg.PreflightEnableDebug,
			NetworkPassphrase: cfg.NetworkPassphrase,
			Logger:            logger,
		},
	)
}

func createJSONRPCHandler(cfg *config.Config, logger *supportlog.Entry, daemon *Daemon,
	feewindows *feewindow.FeeWindows,
) *rpcv1.Handler {
	var dataStoreLedgerReader rpcdatastore.LedgerReader
	if cfg.ServeLedgersFromDatastore {
		dataStoreLedgerReader = rpcdatastore.NewLedgerReader(cfg.BufferedStorageBackendConfig, daemon.dataStore,
			daemon.dataStoreSchema)
	}

	rpcHandler := rpcv1.NewJSONRPCHandler(cfg, rpcv1.HandlerParams{
		Daemon:                daemon,
		FeeStatWindows:        feewindows,
		Logger:                logger,
		LedgerReader:          sqlitedb.NewLedgerReader(daemon.db),
		TransactionReader:     sqlitedb.NewTransactionReader(logger, daemon.db, cfg.NetworkPassphrase),
		EventReader:           sqlitedb.NewEventReader(logger, daemon.db, cfg.NetworkPassphrase),
		PreflightGetter:       daemon.preflightWorkerPool,
		DataStoreLedgerReader: dataStoreLedgerReader,
	})
	return &rpcHandler
}

func (d *Daemon) setupHTTPServers(cfg *config.Config) {
	var err error
	var listenConfig net.ListenConfig
	d.listener, err = listenConfig.Listen(context.Background(), "tcp", cfg.Endpoint)
	if err != nil {
		d.logger.WithError(err).WithField("endpoint", cfg.Endpoint).Fatal("cannot listen on endpoint")
	}
	d.server = &http.Server{
		Handler:     createHTTPHandler(d.logger, d.jsonRPCHandler),
		ReadTimeout: defaultReadTimeout,
	}

	if cfg.AdminEndpoint != "" {
		d.setupAdminServer(cfg)
	}
}

func createHTTPHandler(logger *supportlog.Entry, jsonRPCHandler *rpcv1.Handler) http.Handler {
	httpHandler := supporthttp.NewAPIMux(logger)
	httpHandler.Handle("/", jsonRPCHandler)
	return httpHandler
}

func (d *Daemon) setupAdminServer(cfg *config.Config) {
	var err error
	adminMux := createAdminMux(d.logger, d.metricsRegistry)
	var listenConfig net.ListenConfig
	d.adminListener, err = listenConfig.Listen(context.Background(), "tcp", cfg.AdminEndpoint)
	if err != nil {
		d.logger.WithError(err).WithField("endpoint", cfg.AdminEndpoint).Fatal("cannot listen on admin endpoint")
	}
	d.adminServer = &http.Server{Handler: adminMux} //nolint:gosec
}

func createAdminMux(logger *supportlog.Entry, metricsRegistry *prometheus.Registry) *chi.Mux {
	adminMux := supporthttp.NewMux(logger)
	adminMux.HandleFunc("/debug/pprof/", pprof.Index)
	adminMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	adminMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	adminMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	adminMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	for _, profile := range runtimePprof.Profiles() {
		adminMux.Handle("/debug/pprof/"+profile.Name(), pprof.Handler(profile.Name()))
	}
	adminMux.Handle("/metrics", promhttp.HandlerFor(metricsRegistry, promhttp.HandlerOpts{}))
	return adminMux
}

// mustInitializeStorage initializes the storage using what was on the DB
func (d *Daemon) mustInitializeStorage(cfg *config.Config) *feewindow.FeeWindows {
	readTxMetaCtx, cancelReadTxMeta := context.WithTimeout(context.Background(), cfg.IngestionTimeout)
	defer cancelReadTxMeta()

	feeWindows := feewindow.NewFeeWindows(
		cfg.ClassicFeeStatsLedgerRetentionWindow,
		cfg.SorobanFeeStatsLedgerRetentionWindow,
		cfg.NetworkPassphrase,
		d.db,
	)

	// In load-test mode the existing DB is treated as opaque carrier state for
	// ingestion timing; skip the fee-stat / migration backfill
	if cfg.IngestLoadTest.Enabled() {
		return feeWindows
	}

	// 1. First, identify the ledger range for database migrations based on the
	//    ledger retention window. Since we don't do "partial" migrations (all or
	//    nothing), this represents the entire range of ledger metas we store.
	retentionRange, err := sqlitedb.GetMigrationLedgerRange(readTxMetaCtx, d.db, cfg.HistoryRetentionWindow)
	if err != nil {
		d.logger.WithError(err).Fatal("could not get ledger range for migration")
	}

	// 2. Then, we build migrations for transactions and events, also incorporating the fee windows.
	//    If there are migrations to do, this has no effect, since migration windows are larger than
	//    the fee window. In the absence of migrations, though, this means the ingestion
	//    range is just the fee stat range.
	dataMigrations := d.buildMigrations(readTxMetaCtx, cfg, retentionRange, feeWindows)
	ledgerSeqRange := dataMigrations.ApplicableRange()

	//
	// 3. Apply all migrations, including fee stat analysis.
	//
	var initialSeq, currentSeq uint32
	err = sqlitedb.NewLedgerReader(d.db).StreamLedgerRange(
		readTxMetaCtx,
		ledgerSeqRange.First,
		ledgerSeqRange.Last,
		func(txMeta xdr.LedgerCloseMeta) error {
			currentSeq = txMeta.LedgerSequence()
			if initialSeq == 0 {
				initialSeq = currentSeq
				d.logger.
					WithField("first", initialSeq).
					WithField("last", ledgerSeqRange.Last).
					Info("Initializing in-memory store")
			} else if (currentSeq-initialSeq)%inMemoryInitializationLedgerLogPeriod == 0 {
				d.logger.
					WithField("seq", currentSeq).
					WithField("last", ledgerSeqRange.Last).
					Debug("Still initializing in-memory store")
			}

			if err := dataMigrations.Apply(readTxMetaCtx, txMeta); err != nil {
				d.logger.WithError(err).Fatal("could not apply migration for ledger ", currentSeq)
			}

			return nil
		})
	if err != nil {
		d.logger.WithError(err).Fatal("Could not obtain txmeta cache from the database")
	}

	if err := dataMigrations.Commit(readTxMetaCtx); err != nil {
		d.logger.WithError(err).Fatal("Could not commit data migrations")
	}

	if currentSeq != 0 {
		d.logger.
			WithField("first", retentionRange.First).
			WithField("last", retentionRange.Last).
			Info("Finished initializing in-memory store and applying DB data migrations")
	}

	return feeWindows
}

func (d *Daemon) buildMigrations(ctx context.Context, cfg *config.Config, retentionRange sqlitedb.LedgerSeqRange,
	feeWindows *feewindow.FeeWindows,
) sqlitedb.MultiMigration {
	// There are two windows in play here:
	//  - the ledger retention window, which describes the range of txmeta
	//    to keep relative to the latest "ledger tip" of the network
	//  - the fee stats window, which describes a *subset* of the prior
	//    ledger retention window on which to perform fee analysis
	//
	// If the fee window *exceeds* the retention window, this doesn't make any
	// sense since it implies the user wants to store N amount of actual
	// historical data and M > N amount of ledgers just for fee processing,
	// which is nonsense from a performance standpoint. We prevent this:
	maxFeeRetentionWindow := max(
		cfg.ClassicFeeStatsLedgerRetentionWindow,
		cfg.SorobanFeeStatsLedgerRetentionWindow)
	if maxFeeRetentionWindow > cfg.HistoryRetentionWindow {
		d.logger.Fatalf(
			"Fee stat analysis window (%d) cannot exceed history retention window (%d).",
			maxFeeRetentionWindow, cfg.HistoryRetentionWindow)
	}

	dataMigrations, err := sqlitedb.BuildMigrations(
		ctx, d.logger, d.db, cfg.NetworkPassphrase, retentionRange)
	if err != nil {
		d.logger.WithError(err).Fatal("could not build migrations")
	}

	feeStatsRange, err := sqlitedb.GetMigrationLedgerRange(ctx, d.db, maxFeeRetentionWindow)
	if err != nil {
		d.logger.WithError(err).Fatal("could not get ledger range for fee stats")
	}

	// By treating the fee window *as if* it's a migration, we can make the interface here clean.
	dataMigrations.Append(feeWindows.AsMigration(feeStatsRange))
	return dataMigrations
}

func (d *Daemon) Run() {
	d.logger.WithField("addr", d.listener.Addr().String()).Info("starting HTTP server")

	panicGroup := util.NewUnrecoverablePanicGroup()
	panicGroupWithLog := panicGroup.Log(d.logger)
	panicGroupWithLog.Go(func() {
		if err := d.server.Serve(d.listener); !errors.Is(err, http.ErrServerClosed) {
			d.logger.WithError(err).Fatal("soroban JSON RPC server encountered fatal error")
		}
	})

	if d.adminServer != nil {
		d.logger.
			WithField("addr", d.adminListener.Addr().String()).
			Info("starting Admin HTTP server")
		panicGroupWithLog.Go(func() {
			if err := d.adminServer.Serve(d.adminListener); !errors.Is(err, http.ErrServerClosed) {
				d.logger.WithError(err).Error("soroban admin server encountered fatal error")
			}
		})
	}

	// Shutdown gracefully when we receive an interrupt signal. First
	// server.Shutdown closes all open listeners, then closes all idle
	// connections. Finally, it waits a grace period (10s here) for connections
	// to return to idle and then shut down.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-signals:
		d.Close()
	case <-d.done:
		return
	}
}

// Ensure the daemon conforms to the interface
var _ host.Daemon = (*Daemon)(nil)
