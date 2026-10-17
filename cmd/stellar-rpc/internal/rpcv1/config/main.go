//nolint:funcorder // lifecycle helpers are grouped before validation for readability
package config

import (
	"time"

	"github.com/pelletier/go-toml"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/version"
)

// Config represents the configuration of a stellar-rpc server
type Config struct {
	ConfigPath string

	Strict bool

	StellarCoreURL                      string
	Backfill                            bool
	CaptiveCoreStoragePath              string
	StellarCoreBinaryPath               string
	CaptiveCoreConfigPath               string
	CaptiveCoreHTTPPort                 uint16
	CaptiveCoreHTTPQueryPort            uint16
	CaptiveCoreHTTPQueryThreadPoolSize  uint16
	CaptiveCoreHTTPQuerySnapshotLedgers uint16

	Endpoint                                       string
	AdminEndpoint                                  string
	CheckpointFrequency                            uint32
	CoreRequestTimeout                             time.Duration
	DefaultEventsLimit                             uint
	DefaultTransactionsLimit                       uint
	DefaultLedgersLimit                            uint
	FriendbotURL                                   string
	HistoryArchiveURLs                             []string
	HistoryArchiveUserAgent                        string
	IngestionTimeout                               time.Duration
	LogFormat                                      LogFormat
	LogLevel                                       logrus.Level
	MaxEventsLimit                                 uint
	MaxTransactionsLimit                           uint
	MaxLedgersLimit                                uint
	MaxHealthyLedgerLatency                        time.Duration
	NetworkPassphrase                              string
	Network                                        string
	PreflightWorkerCount                           uint
	PreflightWorkerQueueSize                       uint
	PreflightEnableDebug                           bool
	SQLiteDBPath                                   string
	HistoryRetentionWindow                         uint32
	SorobanFeeStatsLedgerRetentionWindow           uint32
	ClassicFeeStatsLedgerRetentionWindow           uint32
	RequestBacklogGlobalQueueLimit                 uint
	RequestBacklogGetHealthQueueLimit              uint
	RequestBacklogGetEventsQueueLimit              uint
	RequestBacklogGetNetworkQueueLimit             uint
	RequestBacklogGetVersionInfoQueueLimit         uint
	RequestBacklogGetLatestLedgerQueueLimit        uint
	RequestBacklogGetLedgerEntriesQueueLimit       uint
	RequestBacklogGetTransactionQueueLimit         uint
	RequestBacklogGetTransactionsQueueLimit        uint
	RequestBacklogGetLedgersQueueLimit             uint
	RequestBacklogSendTransactionQueueLimit        uint
	RequestBacklogSimulateTransactionQueueLimit    uint
	RequestBacklogGetFeeStatsTransactionQueueLimit uint
	RequestExecutionWarningThreshold               time.Duration
	MaxRequestExecutionDuration                    time.Duration
	MaxGetHealthExecutionDuration                  time.Duration
	MaxGetEventsExecutionDuration                  time.Duration
	MaxGetNetworkExecutionDuration                 time.Duration
	MaxGetVersionInfoExecutionDuration             time.Duration
	MaxGetLatestLedgerExecutionDuration            time.Duration
	MaxGetLedgerEntriesExecutionDuration           time.Duration
	MaxGetTransactionExecutionDuration             time.Duration
	MaxGetTransactionsExecutionDuration            time.Duration
	MaxGetLedgersExecutionDuration                 time.Duration
	MaxSendTransactionExecutionDuration            time.Duration
	MaxSimulateTransactionExecutionDuration        time.Duration
	MaxGetFeeStatsExecutionDuration                time.Duration

	ServeLedgersFromDatastore    bool
	BufferedStorageBackendConfig ledgerbackend.BufferedStorageBackendConfig
	DataStoreConfig              datastore.DataStoreConfig

	IngestLoadTest IngestLoadTestConfig

	// We memoize these, so they bind to pflags correctly
	optionsCache *Options
	flagset      *pflag.FlagSet
}

// IngestLoadTestConfig groups the options for ingesting from pre-generated synthetic
// ledger bundles. If no files are given, normal captive-core ingestion runs.
type IngestLoadTestConfig struct {
	// Files are .xdr.zstd bundles of LedgerCloseMeta records produced by
	// stellar-core's apply-load, replayed in order.
	Files []string `toml:"files"`
	// Frequency paces ingestion, replaying one synthetic ledger per duration.
	// Zero means "use DefaultIngestLoadTestFrequency".
	Frequency time.Duration `toml:"frequency"`
	// MaxLedgersPerFile optionally caps how many ledgers are replayed from each
	// file in Files. Zero replays every ledger in every file.
	MaxLedgersPerFile uint32 `toml:"max_ledgers_per_file"`
	// OnLedgerIngested is a test-only seam, never set from TOML/flags (hence
	// toml:"-"): the load-test harness sets it so the daemon reports exact
	// per-ledger ingest timing via ingest.Config.OnLedgerIngested. Nil in
	// production.
	OnLedgerIngested func(seq uint32, d time.Duration) `toml:"-"`
}

func (cfg IngestLoadTestConfig) Enabled() bool {
	return len(cfg.Files) > 0
}

const DefaultIngestLoadTestFrequency = 2 * time.Second

func (cfg *Config) ExtendedUserAgent(extension string) string {
	if cfg.HistoryArchiveUserAgent == "" {
		return extension
	}
	return cfg.HistoryArchiveUserAgent + "/" + extension
}

func (cfg *Config) SetValues(lookupEnv func(string) (string, bool)) error {
	return cfg.SetValuesWithLoader(lookupEnv, toml.LoadFile)
}

// SetValuesWithLoader is SetValues with the config file read through loadTOML.
// The caller can inspect the loaded tree before the config takes values from
// it, so the file is read once.
func (cfg *Config) SetValuesWithLoader(
	lookupEnv func(string) (string, bool),
	loadTOML func(path string) (*toml.Tree, error),
) error {
	// We start with the defaults
	if err := cfg.loadDefaults(); err != nil {
		return err
	}

	// Then we load from the environment variables and cli flags, to try to find
	// the config file path
	if err := cfg.loadEnv(lookupEnv); err != nil {
		return err
	}
	if err := cfg.loadFlags(); err != nil {
		return err
	}

	// If we specified a config file, we load that
	if cfg.ConfigPath != "" {
		// Merge in the config file flags
		tree, err := loadTOML(cfg.ConfigPath)
		if err != nil {
			return err
		}
		if err := applyTree(tree, cfg.Strict, cfg); err != nil {
			return err
		}

		// Load from cli flags and environment variables again, to overwrite what we
		// got from the config file
		if err := cfg.loadEnv(lookupEnv); err != nil {
			return err
		}
		if err := cfg.loadFlags(); err != nil {
			return err
		}
	}

	return nil
}

// loadDefaults populates the config with default values
func (cfg *Config) loadDefaults() error {
	for _, option := range cfg.options() {
		if option.DefaultValue != nil {
			if err := option.setValue(option.DefaultValue); err != nil {
				return err
			}
		}
	}
	cfg.HistoryArchiveUserAgent = "stellar-rpc/" + version.Version
	return nil
}

// loadEnv populates the config with values from the environment variables
func (cfg *Config) loadEnv(lookupEnv func(string) (string, bool)) error {
	for _, option := range cfg.options() {
		key, ok := option.getEnvKey()
		if !ok {
			continue
		}
		value, ok := lookupEnv(key)
		if !ok {
			continue
		}
		if err := option.setValue(value); err != nil {
			return err
		}
	}
	return nil
}

// loadFlags populates the config with values from the cli flags
func (cfg *Config) loadFlags() error {
	for _, option := range cfg.options() {
		if option.flag == nil || !option.flag.Changed {
			continue
		}
		val, err := option.GetFlag(cfg.flagset)
		if err != nil {
			return err
		}
		if err := option.setValue(val); err != nil {
			return err
		}
	}
	return nil
}

func (cfg *Config) Validate() error {
	return cfg.options().Validate()
}
