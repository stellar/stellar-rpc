//nolint:mnd,lll // lots of magic constants and long lines due to default values and help strings
package config

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"time"

	"github.com/pelletier/go-toml"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/strutils"
)

const (
	// OneDayOfLedgers is (roughly) a 24 hour window of ledgers.
	OneDayOfLedgers   = 17280
	SevenDayOfLedgers = OneDayOfLedgers * 7

	defaultHTTPEndpoint             = "localhost:8000"
	defaultCaptiveCoreHTTPPort      = 11626 // regular queries like /info
	defaultCaptiveCoreHTTPQueryPort = 11628
)

// TODO: refactor and remove the linter exceptions
//
//nolint:funlen,cyclop,maintidx
func (cfg *Config) options() Options {
	if cfg.optionsCache != nil {
		return *cfg.optionsCache
	}
	defaultStellarCoreBinaryPath, _ := exec.LookPath("stellar-core")
	cfg.optionsCache = &Options{
		{
			Name: "config-path",
			// TODO: deprecate and rename to STELLAR_RPC_
			EnvVar:    "SOROBAN_RPC_CONFIG_PATH",
			TomlKey:   "-",
			Usage:     "File path to the toml configuration file",
			ConfigKey: &cfg.ConfigPath,
		},
		{
			Name: "config-strict",
			// TODO: deprecate and rename to STELLAR_RPC_
			EnvVar:       "SOROBAN_RPC_CONFIG_STRICT",
			TomlKey:      "STRICT",
			Usage:        "Enable strict toml configuration file parsing. This will prevent unknown fields in the config toml from being parsed.",
			ConfigKey:    &cfg.Strict,
			DefaultValue: false,
		},
		{
			Name:         "endpoint",
			Usage:        "Endpoint to listen and serve on",
			ConfigKey:    &cfg.Endpoint,
			DefaultValue: defaultHTTPEndpoint,
		},
		{
			Name:      "admin-endpoint",
			Usage:     "Admin endpoint to listen and serve on. WARNING: this should not be accessible from the Internet and does not use TLS. \"\" (default) disables the admin server",
			ConfigKey: &cfg.AdminEndpoint,
		},
		{
			Name:      "stellar-core-url",
			Usage:     "URL used to query Stellar Core (local captive core by default)",
			ConfigKey: &cfg.StellarCoreURL,
			Validate: func(_ *Option) error {
				// This is a bit awkward. We're actually setting a default, but we
				// can't do that until the config is fully parsed, so we do it as a
				// validator here.
				if cfg.StellarCoreURL == "" {
					cfg.StellarCoreURL = fmt.Sprintf("http://localhost:%d", cfg.CaptiveCoreHTTPPort)
				}
				return nil
			},
		},
		{
			Name:         "stellar-core-timeout",
			Usage:        "Timeout used when submitting requests to stellar-core",
			ConfigKey:    &cfg.CoreRequestTimeout,
			DefaultValue: 2 * time.Second,
		},
		{
			Name:         "stellar-captive-core-http-port",
			Usage:        "HTTP port for Captive Core to listen on (0 disables the HTTP server)",
			ConfigKey:    &cfg.CaptiveCoreHTTPPort,
			DefaultValue: uint16(defaultCaptiveCoreHTTPPort),
		},
		{
			Name:         "stellar-captive-core-http-query-port",
			Usage:        "HTTP port for Captive Core to listen on for high-performance queries like /getledgerentry (must not conflict with CAPTIVE_CORE_HTTP_PORT)",
			ConfigKey:    &cfg.CaptiveCoreHTTPQueryPort,
			DefaultValue: uint16(defaultCaptiveCoreHTTPQueryPort),
			Validate:     positive,
		},
		{
			Name:         "stellar-captive-core-http-query-thread-pool-size",
			Usage:        "Number of threads to use by Captive Core's high-performance query server",
			ConfigKey:    &cfg.CaptiveCoreHTTPQueryThreadPoolSize,
			DefaultValue: uint16(runtime.NumCPU()), //nolint:gosec
		},
		{
			Name:         "stellar-captive-core-http-query-snapshot-ledgers",
			Usage:        "Size of ledger history in Captive Core's high-performance query server (don't touch unless you know what you are doing)",
			ConfigKey:    &cfg.CaptiveCoreHTTPQuerySnapshotLedgers,
			DefaultValue: uint16(4),
		},
		{
			Name:         "log-level",
			Usage:        "minimum log severity (debug, info, warn, error) to log",
			ConfigKey:    &cfg.LogLevel,
			DefaultValue: logrus.InfoLevel,
			CustomSetValue: func(option *Option, i interface{}) error {
				switch v := i.(type) {
				case nil:
					return nil
				case string:
					ll, err := logrus.ParseLevel(v)
					if err != nil {
						return fmt.Errorf("could not parse %s: %q", option.Name, v)
					}
					cfg.LogLevel = ll
				case logrus.Level:
					cfg.LogLevel = v
				case *logrus.Level:
					cfg.LogLevel = *v
				default:
					return fmt.Errorf("could not parse %s: %q", option.Name, v)
				}
				return nil
			},
			MarshalTOML: func(_ *Option) (interface{}, error) {
				return cfg.LogLevel.String(), nil
			},
		},
		{
			Name:         "log-format",
			Usage:        "format used for output logs (json or text)",
			ConfigKey:    &cfg.LogFormat,
			DefaultValue: LogFormatText,
			CustomSetValue: func(option *Option, i interface{}) error {
				switch v := i.(type) {
				case nil:
					return nil
				case string:
					if err := cfg.LogFormat.UnmarshalText([]byte(v)); err != nil {
						return fmt.Errorf("could not parse %s: %w", option.Name, err)
					}
				case LogFormat:
					cfg.LogFormat = v
				case *LogFormat:
					cfg.LogFormat = *v
				default:
					return fmt.Errorf("could not parse %s: %q", option.Name, v)
				}
				return nil
			},
			MarshalTOML: func(_ *Option) (interface{}, error) {
				return cfg.LogFormat.String()
			},
		},
		{
			Name:         "stellar-core-binary-path",
			Usage:        "path to stellar core binary",
			ConfigKey:    &cfg.StellarCoreBinaryPath,
			DefaultValue: defaultStellarCoreBinaryPath,
			Validate:     required,
		},
		{
			Name:      "captive-core-config-path",
			Usage:     "path to additional configuration for the Stellar Core configuration file used by captive core. It must, at least, include enough details to define a quorum set",
			ConfigKey: &cfg.CaptiveCoreConfigPath,
			Validate:  required,
		},
		{
			Name:      "captive-core-storage-path",
			Usage:     "Storage location for Captive Core bucket data",
			ConfigKey: &cfg.CaptiveCoreStoragePath,
			CustomSetValue: func(option *Option, i interface{}) error {
				switch v := i.(type) {
				case string:
					if v == "" || v == "." {
						cwd, err := os.Getwd()
						if err != nil {
							return fmt.Errorf("unable to determine the current directory: %w", err)
						}
						v = cwd
					}
					cfg.CaptiveCoreStoragePath = v
					return nil
				case nil:
					cwd, err := os.Getwd()
					if err != nil {
						return fmt.Errorf("unable to determine the current directory: %w", err)
					}
					cfg.CaptiveCoreStoragePath = cwd
					return nil
				default:
					return fmt.Errorf("could not parse %s: %v", option.Name, v)
				}
			},
		},
		{
			Name:      "history-archive-urls",
			Usage:     "comma-separated list of stellar history archives to connect with",
			ConfigKey: &cfg.HistoryArchiveURLs,
			Validate:  required,
		},
		{
			Name:      "friendbot-url",
			Usage:     "The friendbot URL to be returned by getNetwork endpoint",
			ConfigKey: &cfg.FriendbotURL,
		},
		{
			Name:      "network-passphrase",
			Usage:     "Network passphrase of the Stellar network transactions should be signed for. Commonly used values are \"" + network.FutureNetworkPassphrase + "\", \"" + network.TestNetworkPassphrase + "\" and \"" + network.PublicNetworkPassphrase + "\"",
			ConfigKey: &cfg.NetworkPassphrase,
			Validate:  required,
		},
		{
			Name:      "db-path",
			Usage:     "SQLite DB path",
			ConfigKey: &cfg.SQLiteDBPath,
			// TODO: deprecate and rename to stellar_rpc.sqlite
			DefaultValue: "soroban_rpc.sqlite",
		},
		{
			Name:         "ingestion-timeout",
			Usage:        "Ingestion Timeout when bootstrapping data (checkpoint and in-memory initialization) and preparing ledger reads",
			ConfigKey:    &cfg.IngestionTimeout,
			DefaultValue: 50 * time.Minute,
		},
		{
			Name:         "checkpoint-frequency",
			Usage:        "establishes how many ledgers exist between checkpoints, do NOT change this unless you really know what you are doing",
			ConfigKey:    &cfg.CheckpointFrequency,
			DefaultValue: uint32(64),
		},
		{
			Name: "history-retention-window",
			Usage: fmt.Sprintf(
				"configures history retention window for transactions and events, expressed in number of ledgers,"+
					" the default value is %d which corresponds to about 7 days of history",
				SevenDayOfLedgers),
			ConfigKey:    &cfg.HistoryRetentionWindow,
			DefaultValue: uint32(SevenDayOfLedgers),
			Validate:     positive,
		},
		{
			Name:         "classic-fee-stats-retention-window",
			Usage:        "configures classic fee stats retention window expressed in number of ledgers",
			ConfigKey:    &cfg.ClassicFeeStatsLedgerRetentionWindow,
			DefaultValue: uint32(10),
			Validate:     positive,
		},
		{
			Name:         "soroban-fee-stats-retention-window",
			Usage:        "configures soroban inclusion fee stats retention window expressed in number of ledgers",
			ConfigKey:    &cfg.SorobanFeeStatsLedgerRetentionWindow,
			DefaultValue: uint32(50),
			Validate:     positive,
		},
		{
			Name:         "max-events-limit",
			Usage:        "Maximum amount of events allowed in a single getEvents response",
			ConfigKey:    &cfg.MaxEventsLimit,
			DefaultValue: uint(10000),
		},
		{
			Name:         "default-events-limit",
			Usage:        "Default cap on the amount of events included in a single getEvents response",
			ConfigKey:    &cfg.DefaultEventsLimit,
			DefaultValue: uint(100),
			Validate: func(_ *Option) error {
				if cfg.DefaultEventsLimit > cfg.MaxEventsLimit {
					return fmt.Errorf(
						"default-events-limit (%v) cannot exceed max-events-limit (%v)",
						cfg.DefaultEventsLimit,
						cfg.MaxEventsLimit,
					)
				}
				return nil
			},
		},
		{
			Name:         "max-transactions-limit",
			Usage:        "Maximum amount of transactions allowed in a single getTransactions response",
			ConfigKey:    &cfg.MaxTransactionsLimit,
			DefaultValue: uint(200),
		},
		{
			Name:         "default-transactions-limit",
			Usage:        "Default cap on the amount of transactions included in a single getTransactions response",
			ConfigKey:    &cfg.DefaultTransactionsLimit,
			DefaultValue: uint(50),
			Validate: func(_ *Option) error {
				if cfg.DefaultTransactionsLimit > cfg.MaxTransactionsLimit {
					return fmt.Errorf(
						"default-transactions-limit (%v) cannot exceed max-transactions-limit (%v)",
						cfg.DefaultTransactionsLimit,
						cfg.MaxTransactionsLimit,
					)
				}
				return nil
			},
		},
		{
			Name:         "max-ledgers-limit",
			Usage:        "Maximum amount of ledgers allowed in a single getLedgers response",
			ConfigKey:    &cfg.MaxLedgersLimit,
			DefaultValue: uint(200),
		},
		{
			Name:         "default-ledgers-limit",
			Usage:        "Default cap on the amount of ledgers included in a single getLedgers response",
			ConfigKey:    &cfg.DefaultLedgersLimit,
			DefaultValue: uint(50),
			Validate: func(_ *Option) error {
				if cfg.DefaultLedgersLimit > cfg.MaxLedgersLimit {
					return fmt.Errorf(
						"default-ledgers-limit (%v) cannot exceed max-ledgers-limit (%v)",
						cfg.DefaultLedgersLimit,
						cfg.MaxLedgersLimit,
					)
				}
				return nil
			},
		},
		{
			Name: "max-healthy-ledger-latency",
			Usage: "maximum ledger latency (i.e. time elapsed since the last known ledger closing time) considered to be healthy" +
				" (used for the /health endpoint)",
			ConfigKey:    &cfg.MaxHealthyLedgerLatency,
			DefaultValue: 30 * time.Second,
		},
		{
			Name:         "preflight-worker-count",
			Usage:        "Number of workers (read goroutines) used to compute preflights for the simulateTransaction endpoint. Defaults to the number of CPUs.",
			ConfigKey:    &cfg.PreflightWorkerCount,
			DefaultValue: uint(runtime.NumCPU()),
			Validate:     positive,
		},
		{
			Name:         "preflight-worker-queue-size",
			Usage:        "Maximum number of outstanding preflight requests for the simulateTransaction endpoint. Defaults to the number of CPUs.",
			ConfigKey:    &cfg.PreflightWorkerQueueSize,
			DefaultValue: uint(runtime.NumCPU()),
			Validate:     positive,
		},
		{
			Name:         "preflight-enable-debug",
			Usage:        "Enable debug information in preflighting (provides more detailed errors). It should not be enabled in production deployments.",
			ConfigKey:    &cfg.PreflightEnableDebug,
			DefaultValue: true,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("request-backlog-global-queue-limit"),
			Usage:        "Maximum number of outstanding requests",
			ConfigKey:    &cfg.RequestBacklogGlobalQueueLimit,
			DefaultValue: uint(5000),
			Validate:     positive,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("request-backlog-get-health-queue-limit"),
			Usage:        "Maximum number of outstanding GetHealth requests",
			ConfigKey:    &cfg.RequestBacklogGetHealthQueueLimit,
			DefaultValue: uint(1000),
			Validate:     positive,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("request-backlog-get-events-queue-limit"),
			Usage:        "Maximum number of outstanding GetEvents requests",
			ConfigKey:    &cfg.RequestBacklogGetEventsQueueLimit,
			DefaultValue: uint(1000),
			Validate:     positive,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("request-backlog-get-network-queue-limit"),
			Usage:        "Maximum number of outstanding GetNetwork requests",
			ConfigKey:    &cfg.RequestBacklogGetNetworkQueueLimit,
			DefaultValue: uint(1000),
			Validate:     positive,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("request-backlog-get-version-info-queue-limit"),
			Usage:        "Maximum number of outstanding GetVersionInfo requests",
			ConfigKey:    &cfg.RequestBacklogGetVersionInfoQueueLimit,
			DefaultValue: uint(1000),
			Validate:     positive,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("request-backlog-get-latest-ledger-queue-limit"),
			Usage:        "Maximum number of outstanding GetLatestsLedger requests",
			ConfigKey:    &cfg.RequestBacklogGetLatestLedgerQueueLimit,
			DefaultValue: uint(1000),
			Validate:     positive,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("request-backlog-get-ledger-entries-queue-limit"),
			Usage:        "Maximum number of outstanding GetLedgerEntries requests",
			ConfigKey:    &cfg.RequestBacklogGetLedgerEntriesQueueLimit,
			DefaultValue: uint(1000),
			Validate:     positive,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("request-backlog-get-transaction-queue-limit"),
			Usage:        "Maximum number of outstanding GetTransaction requests",
			ConfigKey:    &cfg.RequestBacklogGetTransactionQueueLimit,
			DefaultValue: uint(1000),
			Validate:     positive,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("request-backlog-get-transactions-queue-limit"),
			Usage:        "Maximum number of outstanding GetTransactions requests",
			ConfigKey:    &cfg.RequestBacklogGetTransactionsQueueLimit,
			DefaultValue: uint(1000),
			Validate:     positive,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("request-backlog-get-ledgers-queue-limit"),
			Usage:        "Maximum number of outstanding getLedgers requests",
			ConfigKey:    &cfg.RequestBacklogGetLedgersQueueLimit,
			DefaultValue: uint(1000),
			Validate:     positive,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("request-backlog-send-transaction-queue-limit"),
			Usage:        "Maximum number of outstanding SendTransaction requests",
			ConfigKey:    &cfg.RequestBacklogSendTransactionQueueLimit,
			DefaultValue: uint(500),
			Validate:     positive,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("request-backlog-simulate-transaction-queue-limit"),
			Usage:        "Maximum number of outstanding SimulateTransaction requests",
			ConfigKey:    &cfg.RequestBacklogSimulateTransactionQueueLimit,
			DefaultValue: uint(100),
			Validate:     positive,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("request-backlog-get-fee-stats-queue-limit"),
			Usage:        "Maximum number of outstanding GetFeeStats requests",
			ConfigKey:    &cfg.RequestBacklogGetFeeStatsTransactionQueueLimit,
			DefaultValue: uint(100),
			Validate:     positive,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("request-execution-warning-threshold"),
			Usage:        "The request execution warning threshold is the predetermined maximum duration of time that a request can take to be processed before a warning would be generated",
			ConfigKey:    &cfg.RequestExecutionWarningThreshold,
			DefaultValue: 5 * time.Second,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("max-request-execution-duration"),
			Usage:        "The max request execution duration is the predefined maximum duration of time allowed for processing a request. When that time elapses, the server would return 504 and abort the request's execution",
			ConfigKey:    &cfg.MaxRequestExecutionDuration,
			DefaultValue: 25 * time.Second,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("max-get-health-execution-duration"),
			Usage:        "The maximum duration of time allowed for processing a getHealth request. When that time elapses, the rpc server would return -32001 and abort the request's execution",
			ConfigKey:    &cfg.MaxGetHealthExecutionDuration,
			DefaultValue: 5 * time.Second,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("max-get_events-execution-duration"),
			Usage:        "The maximum duration of time allowed for processing a getEvents request. When that time elapses, the rpc server would return -32001 and abort the request's execution",
			ConfigKey:    &cfg.MaxGetEventsExecutionDuration,
			DefaultValue: 10 * time.Second,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("max-get-network-execution-duration"),
			Usage:        "The maximum duration of time allowed for processing a getNetwork request. When that time elapses, the rpc server would return -32001 and abort the request's execution",
			ConfigKey:    &cfg.MaxGetNetworkExecutionDuration,
			DefaultValue: 5 * time.Second,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("max-get-version-info-execution-duration"),
			Usage:        "The maximum duration of time allowed for processing a getVersionInfo request. When that time elapses, the rpc server would return -32001 and abort the request's execution",
			ConfigKey:    &cfg.MaxGetVersionInfoExecutionDuration,
			DefaultValue: 5 * time.Second,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("max-get-latest-ledger-execution-duration"),
			Usage:        "The maximum duration of time allowed for processing a getLatestLedger request. When that time elapses, the rpc server would return -32001 and abort the request's execution",
			ConfigKey:    &cfg.MaxGetLatestLedgerExecutionDuration,
			DefaultValue: 5 * time.Second,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("max-get_ledger-entries-execution-duration"),
			Usage:        "The maximum duration of time allowed for processing a getLedgerEntries request. When that time elapses, the rpc server would return -32001 and abort the request's execution",
			ConfigKey:    &cfg.MaxGetLedgerEntriesExecutionDuration,
			DefaultValue: 5 * time.Second,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("max-get-transaction-execution-duration"),
			Usage:        "The maximum duration of time allowed for processing a getTransaction request. When that time elapses, the rpc server would return -32001 and abort the request's execution",
			ConfigKey:    &cfg.MaxGetTransactionExecutionDuration,
			DefaultValue: 5 * time.Second,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("max-get-transactions-execution-duration"),
			Usage:        "The maximum duration of time allowed for processing a getTransactions request. When that time elapses, the rpc server would return -32001 and abort the request's execution",
			ConfigKey:    &cfg.MaxGetTransactionsExecutionDuration,
			DefaultValue: 5 * time.Second,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("max-get-ledgers-execution-duration"),
			Usage:        "The maximum duration of time allowed for processing a getLedgers request. When that time elapses, the rpc server would return -32001 and abort the request's execution",
			ConfigKey:    &cfg.MaxGetLedgersExecutionDuration,
			DefaultValue: 5 * time.Second,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("max-send-transaction-execution-duration"),
			Usage:        "The maximum duration of time allowed for processing a sendTransaction request. When that time elapses, the rpc server would return -32001 and abort the request's execution",
			ConfigKey:    &cfg.MaxSendTransactionExecutionDuration,
			DefaultValue: 15 * time.Second,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("max-simulate-transaction-execution-duration"),
			Usage:        "The maximum duration of time allowed for processing a simulateTransaction request. When that time elapses, the rpc server would return -32001 and abort the request's execution",
			ConfigKey:    &cfg.MaxSimulateTransactionExecutionDuration,
			DefaultValue: 15 * time.Second,
		},
		{
			TomlKey:      strutils.KebabToConstantCase("max-get-fee-stats-execution-duration"),
			Usage:        "The maximum duration of time allowed for processing a getFeeStats request. When that time elapses, the rpc server would return -32001 and abort the request's execution",
			ConfigKey:    &cfg.MaxGetFeeStatsExecutionDuration,
			DefaultValue: 5 * time.Second,
		},
		{
			Name:         "serve-ledgers-from-datastore",
			TomlKey:      strutils.KebabToConstantCase("serve-ledgers-from-datastore"),
			Usage:        "Fetch historical ledgers from the datastore if they're not available locally.",
			ConfigKey:    &cfg.ServeLedgersFromDatastore,
			DefaultValue: false,
		},
		{
			TomlKey:   "buffered_storage_backend_config",
			ConfigKey: &cfg.BufferedStorageBackendConfig,
			Usage:     "Buffered storage backend configuration for reading ledger data from the datastore.",
			CustomSetValue: func(option *Option, i interface{}) error {
				tree, ok := i.(*toml.Tree)
				if !ok {
					return fmt.Errorf("invalid type for buffered_storage_backend_config:"+
						" expected toml table, got %T", i)
				}

				tomlBytes, err := toml.Marshal(tree.ToMap())
				if err != nil {
					return fmt.Errorf("unable to serialize buffered_storage_backend_config: %w", err)
				}

				return toml.Unmarshal(tomlBytes, option.ConfigKey)
			},
			MarshalTOML: func(_ *Option) (interface{}, error) {
				tomlBytes, err := toml.Marshal(cfg.BufferedStorageBackendConfig)
				if err != nil {
					return nil, fmt.Errorf("unable to marshal buffered_storage_backend_config to toml: %w", err)
				}
				return toml.LoadBytes(tomlBytes)
			},
		},
		{
			TomlKey:   "datastore_config",
			ConfigKey: &cfg.DataStoreConfig,
			Usage:     "External datastore configuration including type, bucket name and schema.",
			CustomSetValue: func(option *Option, i interface{}) error {
				tree, ok := i.(*toml.Tree)
				if !ok {
					return fmt.Errorf("invalid type for datastore_config: expected toml table, got %T", i)
				}

				tomlBytes, err := toml.Marshal(tree.ToMap())
				if err != nil {
					return fmt.Errorf("unable to serialize datastore_config: %w", err)
				}

				return toml.Unmarshal(tomlBytes, option.ConfigKey)
			},
			MarshalTOML: func(_ *Option) (interface{}, error) {
				tomlBytes, err := toml.Marshal(cfg.DataStoreConfig)
				if err != nil {
					return nil, fmt.Errorf("unable to marshal datastore_config: %w", err)
				}
				return toml.LoadBytes(tomlBytes)
			},
			Validate: func(_ *Option) error {
				if cfg.ServeLedgersFromDatastore {
					// performs a basic check to verify credentials, bucket name and access.
					dataStore, err := datastore.NewDataStore(context.Background(), cfg.DataStoreConfig)
					if err != nil {
						return fmt.Errorf("failed to initialize datastore: %w", err)
					}
					defer dataStore.Close()
				}
				return nil
			},
		},
	}
	return *cfg.optionsCache
}

type missingRequiredOptionError struct {
	strErr string
	usage  string
}

func (e missingRequiredOptionError) Error() string {
	return e.strErr
}

func required(option *Option) error {
	value := reflect.ValueOf(option.ConfigKey).Elem()

	isSet := value.Kind() == reflect.Slice && value.Len() > 0 || value.Kind() != reflect.Slice && !value.IsZero()

	if isSet {
		return nil
	}

	var waysToSet []string
	if option.Name != "" && option.Name != "-" {
		waysToSet = append(waysToSet, fmt.Sprintf("specify --%s on the command line", option.Name))
	}
	if option.EnvVar != "" && option.EnvVar != "-" {
		waysToSet = append(waysToSet, fmt.Sprintf("set the %s environment variable", option.EnvVar))
	}

	if tomlKey, hasTomlKey := option.getTomlKey(); hasTomlKey {
		waysToSet = append(waysToSet, fmt.Sprintf("set %s in the config file", tomlKey))
	}

	advice := ""
	switch len(waysToSet) {
	case 1:
		advice = fmt.Sprintf(" Please %s.", waysToSet[0])
	case 2:
		advice = fmt.Sprintf(" Please %s or %s.", waysToSet[0], waysToSet[1])
	case 3:
		advice = fmt.Sprintf(" Please %s, %s, or %s.", waysToSet[0], waysToSet[1], waysToSet[2])
	}

	return missingRequiredOptionError{strErr: fmt.Sprintf("%s is required.%s", option.Name, advice), usage: option.Usage}
}

func positive(option *Option) error {
	switch v := option.ConfigKey.(type) {
	case *int, *int8, *int16, *int32, *int64:
		if reflect.ValueOf(v).Elem().Int() <= 0 {
			return fmt.Errorf("%s must be positive", option.Name)
		}
	case *uint, *uint8, *uint16, *uint32, *uint64:
		if reflect.ValueOf(v).Elem().Uint() <= 0 {
			return fmt.Errorf("%s must be positive", option.Name)
		}
	default:
		return fmt.Errorf("%s is not a positive integer", option.Name)
	}
	return nil
}
