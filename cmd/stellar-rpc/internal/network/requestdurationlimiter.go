package network

import (
	"context"
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

// Handle enforces warning and limit thresholds for JSON-RPC request execution.
// TODO: this function is too complicated we should fix this and remove the nolint:gocognit
//
//nolint:gocognit,cyclop
func (q *RPCRequestDurationLimiter) Handle(ctx context.Context, req *jrpc2.Request) (any, error) {
	if q.limitThreshold == RequestDurationLimiterNoLimit {
		// if specified max duration, pass-through
		return q.jrpcDownstreamHandler(ctx, req)
	}
	var warningCh <-chan time.Time
	if q.warningThreshold != time.Duration(0) && q.warningThreshold < q.limitThreshold {
		warningCh = time.NewTimer(q.warningThreshold).C
	}
	var limitCh <-chan time.Time
	if q.limitThreshold != time.Duration(0) {
		limitCh = time.NewTimer(q.limitThreshold).C
	}
	type requestResultOutput struct {
		data any
		err  error
	}
	requestCompleted := make(chan requestResultOutput, 1)
	requestCtx, requestCtxCancel := context.WithTimeout(ctx, q.limitThreshold)
	defer requestCtxCancel()

	go func() {
		defer func() {
			if err := recover(); err != nil {
				q.logger.Errorf("Request for method %s resulted in an error : %v", req.Method(), err)
			}
			close(requestCompleted)
		}()
		var res requestResultOutput
		res.data, res.err = q.jrpcDownstreamHandler(requestCtx, req)
		requestCompleted <- res
	}()

	warn := false
	for {
		select {
		case <-warningCh:
			// warn
			warn = true
		case <-limitCh:
			// limit
			requestCtxCancel()
			if q.limitCounter != nil {
				q.limitCounter.Inc()
			}
			if q.logger != nil {
				q.logger.Infof("Request processing for %s exceed limiting threshold of %v", req.Method(), q.limitThreshold)
			}
			if ctxErr := ctx.Err(); ctxErr == nil {
				return nil, ErrRequestExceededProcessingLimitThreshold
			} else {
				return nil, ctxErr
			}
		case requestRes, ok := <-requestCompleted:
			if warn {
				if q.warningCounter != nil {
					q.warningCounter.Inc()
				}
				if q.logger != nil {
					q.logger.Infof("Request processing for %s exceed warning threshold of %v", req.Method(), q.warningThreshold)
				}
			}
			if ok {
				return requestRes.data, requestRes.err
			} else {
				// request panicked ?
				return nil, ErrFailToProcessDueToInternalIssue
			}
		}
	}
}

// The three errors below are this repo's implementation-defined JSON-RPC
// codes, kept together so every -3200x code is defined in one place.

var ErrRequestExceededProcessingLimitThreshold = jrpc2.Error{
	Code:    -32001,
	Message: "request exceeded processing limit threshold",
}

var ErrFailToProcessDueToInternalIssue = jrpc2.Error{
	Code:    -32003, // internal error
	Message: "request failed to process due to internal issue",
}
