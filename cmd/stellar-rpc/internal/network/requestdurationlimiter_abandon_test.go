package network

import (
	"context"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/creachadair/jrpc2/jhttp"
	"github.com/stretchr/testify/require"
)

// TestJRPCRequestDurationLimiter_TimeoutAbandonsHandlerGoroutine pins the
// limiter's cooperative-cancellation semantics: when the limit threshold
// fires, the limiter cancels the request context and returns the timeout
// error to the client, but it does not wait for or stop the handler
// goroutine. A handler stuck in a call that cannot observe context
// cancellation (e.g. a CGO RocksDB read blocked in the kernel) keeps
// running after the client already received its timeout response.
//
// Any deletion-safety design gating resource destruction on "requests
// finish by their deadline" must account for this: the deadline bounds the
// response time, not the handler goroutine's lifetime.
func TestJRPCRequestDurationLimiter_TimeoutAbandonsHandlerGoroutine(t *testing.T) {
	ctx := t.Context()
	addr, redirector, shutdown := createTestServer(ctx)
	defer shutdown()
	hoistFunction := bindRPCHoist(redirector)

	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	// Carries the ctx.Err() the handler observes once released.
	handlerDone := make(chan error, 1)

	// Ignores its context entirely, simulating a read blocked below the
	// context-checking layer. Blocks until the test releases it.
	blockedHandler := handler.New(func(ctx context.Context, _ *jrpc2.Request) (any, error) {
		close(handlerStarted)
		<-releaseHandler
		handlerDone <- ctx.Err()
		return "late", nil
	})

	logCounter := makeTestLogCounter()
	*hoistFunction = MakeJrpcRequestDurationLimiter(
		blockedHandler,
		time.Second/20,
		time.Second/10,
		&TestingCounter{},
		&TestingCounter{},
		logCounter.Entry()).Handle

	channel := jhttp.NewChannel("http://"+addr+"/", nil)
	client := jrpc2.NewClient(channel, nil)
	defer client.Close()

	// The client gets the timeout error once the limit threshold fires.
	var res any
	err := client.CallResult(ctx, "method", struct{ i int }{1}, &res)
	var jrpcError *jrpc2.Error
	require.ErrorAs(t, err, &jrpcError)
	require.Equal(t, ErrRequestExceededProcessingLimitThreshold.Code, jrpcError.Code)

	// The handler goroutine is still running: it started, and it has not
	// completed, even though the client already has its response.
	<-handlerStarted
	select {
	case <-handlerDone:
		t.Fatal("handler completed before the timeout response; it should still be blocked")
	default:
	}

	// Release the handler. It runs to completion only now, and the only
	// stop signal it ever received was context cancellation.
	close(releaseHandler)
	select {
	case ctxErr := <-handlerDone:
		require.Error(t, ctxErr, "request context should have been cancelled at the limit threshold")
	case <-time.After(time.Second):
		t.Fatal("handler goroutine never completed after release")
	}
}
