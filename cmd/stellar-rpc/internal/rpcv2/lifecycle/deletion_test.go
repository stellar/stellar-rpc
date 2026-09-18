package lifecycle

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/observability"
)

type failedDestroyCounter struct {
	observability.NopMetrics

	failed int
}

func (c *failedDestroyCounter) FailedDestroy() { c.failed++ }

func TestDestroyAll_CountsFailedDestroys(t *testing.T) {
	metrics := &failedDestroyCounter{}
	cfg := Config{ExecConfig: backfill.ExecConfig{Logger: silentLogger(), Metrics: metrics}}

	p := &pendingDeletions{}
	p.add("stuck handle", func() error { return errReaderInFlight })
	p.add("fine", func() error { return nil })
	p.add("also stuck", func() error { return errors.New("unlink failed") })
	p.destroyAll(context.Background(), cfg)

	assert.Equal(t, 2, metrics.failed)
}
