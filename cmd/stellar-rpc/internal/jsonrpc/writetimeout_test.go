package jsonrpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/network"
)

func TestWriteTimeout(t *testing.T) {
	require.Equal(t, 25*time.Second+responseWriteTime, WriteTimeout(25*time.Second))
	require.Zero(t, WriteTimeout(network.RequestDurationLimiterNoLimit))
	require.Zero(t, WriteTimeout(network.RequestDurationLimiterNoLimit-time.Second))
}
