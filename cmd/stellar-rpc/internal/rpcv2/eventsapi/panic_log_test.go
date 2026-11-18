package eventsapi

import (
	"errors"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
)

func TestWithPanicLogsValueAndStack(t *testing.T) {
	logger := supportlog.New()
	logged := logger.StartTest(logrus.ErrorLevel)
	panicked := &packfile.PanicError{Op: "events: cold read", Value: "boom", Stack: []byte("goroutine 7")}
	err := fmt.Errorf("query: %w", panicked)
	withPanic(logger.WithError(err), err).Error("failed")
	plain := errors.New("plain")
	withPanic(logger.WithError(plain), plain).Error("failed")

	entries := logged()
	require.Len(t, entries, 2)
	require.Equal(t, "boom", entries[0].Data["panic"])
	require.Equal(t, "goroutine 7", entries[0].Data["stack"])
	require.NotContains(t, err.Error(), "goroutine")
	require.NotContains(t, entries[1].Data, "panic")
}
