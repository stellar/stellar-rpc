package main

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
)

func TestSampleConfig_ParsesStrict(t *testing.T) {
	data, err := os.ReadFile("rpc-v2-sample-config.toml")
	require.NoError(t, err)

	cfg, err := config.ParseConfig(data)
	require.NoError(t, err)

	assert.Equal(t, "/var/stellar/rpc-v2", cfg.Storage.DefaultDataDir)
	assert.Equal(t, config.DefaultEndpoint, cfg.Service.Endpoint)
	assert.Equal(t, "GCS", cfg.Backfill.DataStore.Type)
	assert.Equal(t, "/etc/stellar/captive-core.toml", cfg.Ingestion.CaptiveCoreConfig)

	// The sample spells out the compiled defaults; drift between the two would
	// make the sample lie about what an absent key means.
	assert.Equal(t, config.DefaultMaxConcurrentRequests, *cfg.Service.MaxConcurrentRequests)
	assert.Equal(t, config.DefaultMethodQueueLimit, *cfg.Service.Methods.GetLedgers.QueueLimit)
	assert.Equal(t, config.DefaultScanMethodMaxExecutionDuration, *cfg.Service.Methods.GetLedgers.MaxExecutionDuration)
	assert.Equal(t, config.DefaultGetFeeStatsQueueLimit, *cfg.Service.Methods.GetFeeStats.QueueLimit)
	assert.Equal(t, config.DefaultClassicFeeWindowLedgers, *cfg.Service.FeeStats.ClassicFeeWindowLedgers)
	assert.Equal(t, config.DefaultMaxHealthyLedgerLatency, *cfg.Service.Methods.GetHealth.MaxHealthyLedgerLatency)
	assert.Equal(t, 30*time.Second, *cfg.Service.Methods.GetHealth.MaxHealthyLedgerLatency)

	assert.Equal(t, uint32(backfill.DefaultBSBBufferSize), *cfg.Backfill.BSB.BufferSize)
	assert.Equal(t, uint32(backfill.DefaultBSBNumWorkers), *cfg.Backfill.BSB.NumWorkers)
	assert.Equal(t, uint32(backfill.DefaultBSBMaxRetries), *cfg.Backfill.BSB.MaxRetries)
	assert.Equal(t, backfill.DefaultBSBRetryWait, *cfg.Backfill.BSB.RetryWait)
}
