//nolint:funcorder // daemon lifecycle helpers are grouped for readability
package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

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
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/jsonrpc"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/preflight"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcdatastore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/feewindow"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/sqlitedb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/util"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/version"
)

const (
	defaultShutdownGracePeriod = 10 * time.Second

	// Since our default retention window will be 7 days (7*17,280 ledgers),
	// choose a random 5-digit prime to have irregular logging intervals at each
	// halfish-day of processing
	inMemoryInitializationLedgerLogPeriod = 10_099
)

type Daemon struct {
	core                *ledgerbackend.CaptiveStellarCore
	coreClient          *host.CoreClientWithMetrics
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
	// cancelArchive ends every history archive request in flight. Without it,
	// close would wait for the archive's own retries, which with an unreachable
	// archive is minutes.
	cancelArchive   context.CancelFunc
	metricsRegistry *prometheus.Registry
	dataStore       datastore.DataStore
	dataStoreSchema datastore.DataStoreSchema
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

// close releases every component the daemon holds. It also runs when New
// fails part way through, so each step skips a component New did not get to.
func (d *Daemon) close() {
	shutdownCtx, shutdownRelease := context.WithTimeout(context.Background(), defaultShutdownGracePeriod)
	defer shutdownRelease()
	var closeErrors []error

	closeErrors = append(closeErrors, shutdownServer(shutdownCtx, d.server, d.listener)...)
	closeErrors = append(closeErrors, shutdownServer(shutdownCtx, d.adminServer, d.adminListener)...)
	for _, err := range closeErrors {
		d.logger.WithError(err).Error("error during HTTP server shutdown")
	}

	// Order matters. The ingestion worker can be parked inside a blocking
	// captive-core startup command that only closing the backend interrupts, so
	// cancel ingestion first, then close captive core, and only then wait for
	// the worker. Waiting before closing captive core stalls shutdown for as
	// long as stellar-core takes to exit by itself, which with an unreachable
	// history archive is minutes.
	if d.ingestService != nil {
		d.ingestService.Stop()
	}
	if d.cancelArchive != nil {
		d.cancelArchive()
	}
	if d.core != nil {
		if err := d.core.Close(); err != nil {
			d.logger.WithError(err).Error("error closing captive core")
			closeErrors = append(closeErrors, err)
		}
	}
	if d.ingestService != nil {
		d.ingestService.Wait()
	}
	if d.jsonRPCHandler != nil {
		d.jsonRPCHandler.Close()
	}
	if d.db != nil {
		if err := d.db.Close(); err != nil {
			d.logger.WithError(err).Error("Error closing db")
			closeErrors = append(closeErrors, err)
		}
	}
	if d.preflightWorkerPool != nil {
		d.preflightWorkerPool.Close()
	}
	if d.dataStore != nil {
		if err := d.dataStore.Close(); err != nil {
			d.logger.WithError(err).Error("error closing datastore")
			closeErrors = append(closeErrors, err)
		}
	}

	d.closeError = errors.Join(closeErrors...)
}

// shutdownServer drains server and closes listener. Both are nil when New
// failed before binding them. Shutdown closes the listener itself once Serve
// has run, so the explicit Close is for a listener that was bound but never
// served; the error it returns in the served case is ignored.
func shutdownServer(ctx context.Context, server *http.Server, listener net.Listener) []error {
	if listener == nil {
		return nil
	}
	var errs []error
	if err := server.Shutdown(ctx); err != nil {
		errs = append(errs, err)
	}
	if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		errs = append(errs, err)
	}
	return errs
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
		return nil, fmt.Errorf("invalid captive core toml: %w", err)
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
	core, err := ledgerbackend.NewCaptive(captiveConfig)
	if err != nil {
		return nil, fmt.Errorf("could not create captive core: %w", err)
	}
	return core, nil
}

// New opens every component of the daemon, binds the HTTP listeners and
// starts ingestion. Run serves the listeners. On failure New closes what it
// opened before the failure and returns the error.
//
// ctx bounds startup: a canceled ctx makes a step such as a backfill stop,
// and New then fails with an error that wraps ctx.Err(). ctx must stay alive
// for as long as the daemon runs, since history archive requests made during
// ingestion also run on it.
func New(ctx context.Context, cfg *config.Config, logger *supportlog.Entry) (*Daemon, error) {
	d := &Daemon{
		logger:          setupLogger(cfg, logger),
		metricsRegistry: prometheus.NewRegistry(),
	}
	if err := d.open(ctx, cfg); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			// The SQLite driver reports a canceled query in its own words, so
			// name the cause for callers that check errors.Is.
			err = fmt.Errorf("startup interrupted: %w: %w", ctxErr, err)
		}
		return nil, errors.Join(err, d.Close())
	}
	return d, nil
}

func (d *Daemon) open(ctx context.Context, cfg *config.Config) error {
	var err error
	if d.core, err = newCaptiveCore(cfg, d.logger); err != nil {
		return err
	}
	archiveCtx, cancelArchive := context.WithCancel(ctx)
	d.cancelArchive = cancelArchive
	historyArchive, err := createHistoryArchive(archiveCtx, cfg, d.logger)
	if err != nil {
		return err
	}
	if d.db, err = openDatabase(cfg, d.metricsRegistry); err != nil { //nolint:contextcheck // the opener takes no ctx
		return err
	}
	d.coreClient = host.NewCoreClientWithMetrics(createStellarCoreClient(cfg), d.metricsRegistry, host.PrometheusNamespace)
	d.coreQueryingClient = createHighperfStellarCoreClient(cfg)

	feewindows, err := d.initializeStorage(ctx, cfg)
	if err != nil {
		return err
	}
	// Create the read-writer once and reuse in ingest service/backfill
	rw := sqlitedb.NewReadWriter(d.logger, d.db, d, cfg.HistoryRetentionWindow, cfg.NetworkPassphrase)
	if cfg.ServeLedgersFromDatastore {
		if d.dataStore, d.dataStoreSchema, err = createDataStore(ctx, cfg); err != nil {
			return err
		}
	}
	var ingestCfg ingest.Config
	// Ingestion outlives startup and is stopped by Close, not by ctx.
	d.ingestService, ingestCfg = createIngestService(cfg, d.logger, d, feewindows, historyArchive, rw)
	if err := d.backfillAndFinalize(ctx, cfg, feewindows); err != nil {
		return err
	}
	d.preflightWorkerPool = createPreflightWorkerPool(cfg, d.logger, d)
	d.jsonRPCHandler = createJSONRPCHandler(cfg, d.logger, d, feewindows)
	// Bind the listeners before ingestion starts: a port already in use is the
	// common startup mistake, and failing on it must not launch captive core.
	if err := d.listen(ctx, cfg); err != nil {
		return err
	}
	// Start ingestion only after backfill is complete
	d.ingestService.Start(ingestCfg) //nolint:contextcheck // see above
	d.registerMetrics()
	return nil
}

// backfillAndFinalize runs the configured backfill and restores the canonical
// schema after a bulk-load, including one interrupted by a crash.
func (d *Daemon) backfillAndFinalize(ctx context.Context, cfg *config.Config, feewindows *feewindow.FeeWindows) error {
	if cfg.Backfill {
		if err := d.prepareBulkLoadIfEmpty(ctx); err != nil {
			return err
		}
		if err := d.backfill(ctx, cfg, feewindows); err != nil {
			return err
		}
	}

	// Must finish before ingestService.Start to avoid starving it.
	finalizeStart := time.Now()
	if err := sqlitedb.FinalizeBulkLoad(ctx, d.db, cfg.SQLiteDBPath, d.logger); err != nil {
		return fmt.Errorf("failed to finalize backfill bulk-load: %w", err)
	}
	// The backfill perf-eval runner keys off this line; keep it stable
	d.logger.WithField("duration", time.Since(finalizeStart).String()).Info("Bulk-load finalize complete")

	if cfg.Backfill {
		// Top-up frontfill after finalize so captive core starts nearer the live tip
		return d.backfill(ctx, cfg, feewindows)
	}
	return nil
}

// prepareBulkLoadIfEmpty reshapes the schema of a fresh DB for the bulk-load.
// FinalizeBulkLoad restores it.
func (d *Daemon) prepareBulkLoadIfEmpty(ctx context.Context) error {
	_, err := sqlitedb.NewLedgerReader(d.db).GetLedgerRange(ctx)
	switch {
	case errors.Is(err, store.ErrEmptyDB):
		if err := sqlitedb.PrepareBulkLoad(ctx, d.db, d.logger); err != nil {
			return fmt.Errorf("failed to prepare database for backfill bulk-load: %w", err)
		}
	case err != nil:
		return fmt.Errorf("failed to check database emptiness for backfill: %w", err)
	}
	return nil
}

func createDataStore(ctx context.Context, cfg *config.Config) (datastore.DataStore, datastore.DataStoreSchema, error) {
	dataStore, err := datastore.NewDataStore(ctx, cfg.DataStoreConfig)
	if err != nil {
		return nil, datastore.DataStoreSchema{}, fmt.Errorf("failed to initialize datastore: %w", err)
	}

	schema, err := datastore.LoadSchema(ctx, dataStore, cfg.DataStoreConfig)
	if err != nil {
		return nil, datastore.DataStoreSchema{}, errors.Join(
			fmt.Errorf("failed to retrieve datastore schema: %w", err), dataStore.Close())
	}

	return dataStore, schema, nil
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

func createHistoryArchive(ctx context.Context, cfg *config.Config, logger *supportlog.Entry,
) (historyarchive.ArchiveInterface, error) {
	if len(cfg.HistoryArchiveURLs) == 0 {
		return nil, errors.New("no history archives URLs were provided")
	}

	historyArchive, err := historyarchive.NewArchivePool(
		cfg.HistoryArchiveURLs,
		historyarchive.ArchiveOptions{
			Logger:              logger,
			NetworkPassphrase:   cfg.NetworkPassphrase,
			CheckpointFrequency: cfg.CheckpointFrequency,
			ConnectOptions: storage.ConnectOptions{
				Context:   ctx,
				UserAgent: cfg.HistoryArchiveUserAgent,
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not connect to history archive: %w", err)
	}
	return historyArchive, nil
}

func openDatabase(cfg *config.Config, metricsRegistry *prometheus.Registry) (*sqlitedb.DB, error) {
	dbConn, err := sqlitedb.OpenSQLiteDBWithPrometheusMetrics(
		cfg.SQLiteDBPath, host.PrometheusNamespace, "db", metricsRegistry)
	if err != nil {
		return nil, fmt.Errorf("could not open database: %w", err)
	}
	return dbConn, nil
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
	feewindows *feewindow.FeeWindows, historyArchive historyarchive.ArchiveInterface, rw sqlitedb.ReadWriter,
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
		Archive:           historyArchive,
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
		EventReader:           sqlitedb.NewEventReader(logger, daemon.db),
		PreflightGetter:       daemon.preflightWorkerPool,
		DataStoreLedgerReader: dataStoreLedgerReader,
	})
	return &rpcHandler
}

// listen binds the JSON-RPC listener and, when configured, the admin listener.
// Run serves them.
func (d *Daemon) listen(ctx context.Context, cfg *config.Config) error {
	var err error
	var listenConfig net.ListenConfig
	d.listener, err = listenConfig.Listen(ctx, "tcp", cfg.Endpoint)
	if err != nil {
		return fmt.Errorf("cannot listen on endpoint %s: %w", cfg.Endpoint, err)
	}
	d.server = &http.Server{
		Handler:     createHTTPHandler(d.logger, d.jsonRPCHandler),
		ReadTimeout: jsonrpc.DefaultHTTPReadTimeout,
		IdleTimeout: jsonrpc.DefaultHTTPIdleTimeout,
	}

	if cfg.AdminEndpoint == "" {
		return nil
	}
	d.adminListener, err = listenConfig.Listen(ctx, "tcp", cfg.AdminEndpoint)
	if err != nil {
		return fmt.Errorf("cannot listen on admin endpoint %s: %w", cfg.AdminEndpoint, err)
	}
	d.adminServer = &http.Server{
		Handler:     jsonrpc.NewAdminMux(d.logger, d.metricsRegistry),
		ReadTimeout: jsonrpc.DefaultHTTPReadTimeout,
		IdleTimeout: jsonrpc.DefaultHTTPIdleTimeout,
	}
	return nil
}

func createHTTPHandler(logger *supportlog.Entry, jsonRPCHandler *rpcv1.Handler) http.Handler {
	httpHandler := supporthttp.NewAPIMux(logger)
	httpHandler.Handle("/", jsonRPCHandler)
	return httpHandler
}

// initializeStorage initializes the storage using what was on the DB
func (d *Daemon) initializeStorage(ctx context.Context, cfg *config.Config) (*feewindow.FeeWindows, error) {
	readTxMetaCtx, cancelReadTxMeta := context.WithTimeout(ctx, cfg.IngestionTimeout)
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
		return feeWindows, nil
	}

	// 1. First, identify the ledger range for database migrations based on the
	//    ledger retention window. Since we don't do "partial" migrations (all or
	//    nothing), this represents the entire range of ledger metas we store.
	retentionRange, err := sqlitedb.GetMigrationLedgerRange(readTxMetaCtx, d.db, cfg.HistoryRetentionWindow)
	if err != nil {
		return nil, fmt.Errorf("could not get ledger range for migration: %w", err)
	}

	// 2. Then, we build migrations for transactions and events, also incorporating the fee windows.
	//    If there are migrations to do, this has no effect, since migration windows are larger than
	//    the fee window. In the absence of migrations, though, this means the ingestion
	//    range is just the fee stat range.
	dataMigrations, err := d.buildMigrations(readTxMetaCtx, cfg, retentionRange, feeWindows)
	if err != nil {
		return nil, err
	}
	ledgerSeqRange := dataMigrations.ApplicableRange()

	//
	// 3. Apply all migrations, including fee stat analysis.
	//
	var initialSeq, currentSeq uint32
	reader := sqlitedb.NewLedgerReader(d.db)
	// buildMigrations opened a DB transaction that Apply and Commit roll back
	// when they fail. A failure between them has to roll it back here, or the
	// connection stays in the transaction past db.Close.
	for l, err := range reader.ScanLedgers(readTxMetaCtx, ledgerSeqRange.First, ledgerSeqRange.Last) {
		if err != nil {
			return nil, errors.Join(fmt.Errorf("could not obtain txmeta cache from the database: %w", err),
				d.db.Rollback())
		}
		var txMeta xdr.LedgerCloseMeta
		if err := txMeta.UnmarshalBinary(l.Raw); err != nil {
			return nil, errors.Join(fmt.Errorf("could not decode ledger %d: %w", l.Sequence, err), d.db.Rollback())
		}
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
			return nil, fmt.Errorf("could not apply migration for ledger %d: %w", currentSeq, err)
		}
	}

	if err := dataMigrations.Commit(readTxMetaCtx); err != nil {
		return nil, fmt.Errorf("could not commit data migrations: %w", err)
	}

	if currentSeq != 0 {
		d.logger.
			WithField("first", retentionRange.First).
			WithField("last", retentionRange.Last).
			Info("Finished initializing in-memory store and applying DB data migrations")
	}

	return feeWindows, nil
}

func (d *Daemon) backfill(ctx context.Context, cfg *config.Config, feeWindows *feewindow.FeeWindows) error {
	backfillMeta, err := ingest.NewBackfillMeta(
		ctx,
		d.logger,
		d.ingestService,
		sqlitedb.NewLedgerReader(d.db),
		d.dataStore,
		d.dataStoreSchema,
	)
	if err != nil {
		return fmt.Errorf("failed to create backfill metadata: %w", err)
	}
	if err := backfillMeta.RunBackfill(ctx, cfg); err != nil {
		return fmt.Errorf("failed to backfill ledgers: %w", err)
	}

	// Reset the fee windows so they re-populate from the database
	feeWindows.Reset()
	return nil
}

func (d *Daemon) buildMigrations(ctx context.Context, cfg *config.Config, retentionRange sqlitedb.LedgerSeqRange,
	feeWindows *feewindow.FeeWindows,
) (sqlitedb.MultiMigration, error) {
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
		return sqlitedb.MultiMigration{}, fmt.Errorf(
			"fee stat analysis window (%d) cannot exceed history retention window (%d)",
			maxFeeRetentionWindow, cfg.HistoryRetentionWindow)
	}

	dataMigrations, err := sqlitedb.BuildMigrations(
		ctx, d.logger, d.db, cfg.NetworkPassphrase, retentionRange)
	if err != nil {
		return sqlitedb.MultiMigration{}, fmt.Errorf("could not build migrations: %w", err)
	}

	feeStatsRange, err := sqlitedb.GetMigrationLedgerRange(ctx, d.db, maxFeeRetentionWindow)
	if err != nil {
		return sqlitedb.MultiMigration{}, fmt.Errorf("could not get ledger range for fee stats: %w", err)
	}

	// By treating the fee window *as if* it's a migration, we can make the interface here clean.
	dataMigrations.Append(feeWindows.AsMigration(feeStatsRange))
	return dataMigrations, nil
}

// Run serves the JSON-RPC and admin endpoints until ctx is canceled or a
// component fails for good, then closes the daemon.
//
// A canceled ctx is a shutdown request, not a failure: Run returns nil, and
// an error from Close itself is logged rather than returned. Run returns the
// first error from the JSON-RPC server, the admin server, or ingestion after
// Close has run. A failure that is pending at the moment of a shutdown
// request is still returned.
func (d *Daemon) Run(ctx context.Context) error {
	// One slot per server, so a Serve goroutine never blocks on its send. A
	// goroutine sends once: its Serve error, or the panic that replaced it.
	serverFailed := make(chan error, 2) //nolint:mnd
	panicGroup := util.NewRecoverablePanicGroup(d.logger, func(err error) {
		serverFailed <- fmt.Errorf("HTTP server: %w", err)
	})
	d.logger.WithField("addr", d.listener.Addr().String()).Info("starting HTTP server")
	panicGroup.Go(func() { serve(serverFailed, "soroban JSON RPC server", d.server, d.listener) })
	if d.adminServer != nil {
		d.logger.WithField("addr", d.adminListener.Addr().String()).Info("starting Admin HTTP server")
		panicGroup.Go(func() { serve(serverFailed, "soroban admin server", d.adminServer, d.adminListener) })
	}

	var err error
	select {
	case <-ctx.Done():
	case err = <-serverFailed:
	case err = <-d.ingestService.Failed():
	}
	d.Close()
	if err == nil {
		// select picks at random among ready cases, so a failure that was
		// pending next to the shutdown request may have lost the draw.
		select {
		case err = <-serverFailed:
		case err = <-d.ingestService.Failed():
		default:
		}
	}
	return err
}

// serve runs server on listener. Shutdown ends Serve with ErrServerClosed,
// which is not a failure; any other end is sent on failed.
func serve(failed chan<- error, name string, server *http.Server, listener net.Listener) {
	err := server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return
	}
	failed <- fmt.Errorf("%s encountered fatal error: %w", name, err)
}

// Ensure the daemon conforms to the interface
var _ host.Daemon = (*Daemon)(nil)
