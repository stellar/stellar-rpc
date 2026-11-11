package network

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/creachadair/jrpc2"

	"github.com/stellar/go-stellar-sdk/support/log"
)

const maxDuration = time.Duration(math.MaxInt64)

const RequestDurationLimiterNoLimit = maxDuration

// The increasingCounter is a subset of prometheus.Counter, and it allows us to mock the
// counter usage for testing purposes without requiring the implementation of the true
// prometheus.Counter.
type increasingCounter interface {
	// Inc increments the counter by 1. Use Add to increment it by arbitrary
	// non-negative values.
	Inc()
}

type requestDurationLimiter struct {
	warningThreshold time.Duration
	limitThreshold   time.Duration
	logger           *log.Entry
	warningCounter   increasingCounter
	limitCounter     increasingCounter
}

type RPCRequestDurationLimiter struct {
	requestDurationLimiter

	jrpcDownstreamHandler jrpc2.Handler
}

func MakeJrpcRequestDurationLimiter(
	downstream jrpc2.Handler,
	warningThreshold time.Duration,
	limitThreshold time.Duration,
	warningCounter increasingCounter,
	limitCounter increasingCounter,
	logger *log.Entry,
) *RPCRequestDurationLimiter {
	// make sure the warning threshold is less then the limit threshold; otherwise, just set it to the limit threshold.
	if warningThreshold > limitThreshold {
		warningThreshold = limitThreshold
	}

	return &RPCRequestDurationLimiter{
		jrpcDownstreamHandler: downstream,
		requestDurationLimiter: requestDurationLimiter{
			warningThreshold: warningThreshold,
			limitThreshold:   limitThreshold,
			logger:           logger,
			warningCounter:   warningCounter,
			limitCounter:     limitCounter,
		},
	}
}

// Handle runs the request on a context that ends at the limit and waits for
// the handler to return, so a request holds nothing once it is answered. A
// request that ran to the limit gets ErrRequestExceededProcessingLimitThreshold
// in place of its result; one past the warning threshold is counted and logged.
func (q *RPCRequestDurationLimiter) Handle(ctx context.Context, req *jrpc2.Request) (any, error) {
	if q.limitThreshold == RequestDurationLimiterNoLimit {
		// if specified max duration, pass-through
		return q.jrpcDownstreamHandler(ctx, req)
	}
	requestCtx, requestCtxCancel := context.WithTimeout(ctx, q.limitThreshold)
	defer requestCtxCancel()

	start := time.Now()
	res, err := q.jrpcDownstreamHandler(requestCtx, req)
	elapsed := time.Since(start)
	switch {
	// A handler that ends with its deadline can return before elapsed reaches the limit.
	case q.limitThreshold != 0 && elapsed >= q.limitThreshold, errors.Is(requestCtx.Err(), context.DeadlineExceeded):
		return nil, q.limited(ctx, req)
	case q.warningThreshold != 0 && elapsed >= q.warningThreshold:
		q.passed(req, "warning", q.warningThreshold, q.warningCounter)
	}
	return res, err
}

// passed counts and logs a request that ran past a threshold.
func (q *RPCRequestDurationLimiter) passed(
	req *jrpc2.Request, name string, threshold time.Duration, counter increasingCounter,
) {
	if counter != nil {
		counter.Inc()
	}
	if q.logger != nil {
		q.logger.Infof("Request processing for %s exceed %s threshold of %v", req.Method(), name, threshold)
	}
}

// limited counts and logs a request that passed the limit, and returns its
// error.
func (q *RPCRequestDurationLimiter) limited(ctx context.Context, req *jrpc2.Request) error {
	if q.limitCounter != nil {
		q.limitCounter.Inc()
	}
	if q.logger != nil {
		q.logger.Infof("Request processing for %s exceed limiting threshold of %v", req.Method(), q.limitThreshold)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return ErrRequestExceededProcessingLimitThreshold
}

// The errors below are this repo's implementation-defined JSON-RPC
// codes, kept together so every -3200x code is defined in one place.

var ErrRequestExceededProcessingLimitThreshold = jrpc2.Error{
	Code:    -32001,
	Message: "request exceeded processing limit threshold",
}

var ErrFailToProcessDueToInternalIssue = jrpc2.Error{
	Code:    -32003, // internal error
	Message: "request failed to process due to internal issue",
}

// ErrPreflightQueueFull fails a simulateTransaction call that finds every
// preflight worker busy and no place left to wait for one. A client may retry.
// It is a pointer so that the response's message is Message alone, without
// the code that jrpc2.Error.Error adds.
var ErrPreflightQueueFull = &jrpc2.Error{
	Code:    -32004,
	Message: "preflight queue full; retry later",
}
