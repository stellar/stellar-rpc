package daemon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/network"
)

func TestWriteTimeout(t *testing.T) {
	require.Equal(t, 25*time.Second+defaultResponseWriteTime, writeTimeout(25*time.Second))
	require.Zero(t, writeTimeout(network.RequestDurationLimiterNoLimit))
	require.Zero(t, writeTimeout(network.RequestDurationLimiterNoLimit-time.Second))
}
