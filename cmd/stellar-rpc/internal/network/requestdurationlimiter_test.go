package network

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/creachadair/jrpc2/jhttp"
	"github.com/stretchr/testify/require"
)

type TestServerHandlerWrapper struct {
	f func(http.ResponseWriter, *http.Request)
}

func (h *TestServerHandlerWrapper) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	h.f(res, req)
}

func createTestServer(ctx context.Context) (string, *TestServerHandlerWrapper, context.CancelFunc) {
	ipAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	listener, _ := net.ListenTCP("tcp", ipAddr)
	handlerRedirector := &TestServerHandlerWrapper{}
	server := http.Server{
		Handler:           handlerRedirector,
		ReadHeaderTimeout: 10 * time.Second,
	}

	serverDown := make(chan error)
	go func() {
		serverDown <- server.Serve(listener)
	}()

	return listener.Addr().String(), handlerRedirector, func() {
		server.Shutdown(ctx) //nolint:errcheck
		<-serverDown
	}
}

func TestHTTPRequestDurationLimiter_Limiting(t *testing.T) {
	ctx := t.Context()
	addr, redirector, shutdown := createTestServer(ctx)
	longExecutingHandler := &TestServerHandlerWrapper{
		f: func(res http.ResponseWriter, req *http.Request) {
			select {
			case <-req.Context().Done():
				return
			case <-time.After(time.Second * 10):
			}
			n, err := res.Write([]byte{1, 2, 3})
			require.Equal(t, 3, n)
			require.NoError(t, err)
		},
	}
	warningCounter := TestingCounter{}
	limitCounter := TestingCounter{}
	logCounter := makeTestLogCounter()
	redirector.f = MakeHTTPRequestDurationLimiter(
		longExecutingHandler,
		time.Second/20,
		time.Second/10,
		&warningCounter,
		&limitCounter,
		logCounter.Entry()).ServeHTTP

	client := http.Client{}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	bytes, err := io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	require.NoError(t, err)
	require.Equal(t, []byte{}, bytes)
	require.Equal(t, http.StatusGatewayTimeout, resp.StatusCode)
	require.Zero(t, warningCounter.count)
	require.Equal(t, int64(1), limitCounter.count)
	require.Equal(t, [7]int{0, 0, 0, 0, 1, 0, 0}, logCounter.writtenLogEntries)
	shutdown()
}

func TestHTTPRequestDurationLimiter_NoLimiting(t *testing.T) {
	ctx := t.Context()
	addr, redirector, shutdown := createTestServer(ctx)
	longExecutingHandler := &TestServerHandlerWrapper{
		f: func(res http.ResponseWriter, req *http.Request) {
			select {
			case <-req.Context().Done():
				return
			case <-time.After(time.Second / 10):
			}
			n, err := res.Write([]byte{1, 2, 3})
			require.Equal(t, 3, n)
			require.NoError(t, err)
		},
	}
	warningCounter := TestingCounter{}
	limitCounter := TestingCounter{}
	logCounter := makeTestLogCounter()
	redirector.f = MakeHTTPRequestDurationLimiter(
		longExecutingHandler,
		time.Second*5,
		time.Second*10,
		&warningCounter,
		&limitCounter,
		logCounter.Entry()).ServeHTTP

	client := http.Client{}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	bytes, err := io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3}, bytes)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Zero(t, warningCounter.count)
	require.Zero(t, limitCounter.count)
	require.Equal(t, [7]int{0, 0, 0, 0, 0, 0, 0}, logCounter.writtenLogEntries)
	shutdown()
}

func TestHTTPRequestDurationLimiter_NoLimiting_Warn(t *testing.T) {
	ctx := t.Context()
	addr, redirector, shutdown := createTestServer(ctx)
	longExecutingHandler := &TestServerHandlerWrapper{
		f: func(res http.ResponseWriter, req *http.Request) {
			select {
			case <-req.Context().Done():
				return
			case <-time.After(time.Second / 5):
			}
			n, err := res.Write([]byte{1, 2, 3})
			require.Equal(t, 3, n)
			require.NoError(t, err)
		},
	}
	warningCounter := TestingCounter{}
	limitCounter := TestingCounter{}
	logCounter := makeTestLogCounter()
	redirector.f = MakeHTTPRequestDurationLimiter(
		longExecutingHandler,
		time.Second/10,
		time.Second*10,
		&warningCounter,
		&limitCounter,
		logCounter.Entry()).ServeHTTP

	client := http.Client{}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	bytes, err := io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3}, bytes)
	require.Equal(t, resp.StatusCode, http.StatusOK)
	require.Equal(t, int64(1), warningCounter.count)
	require.Zero(t, limitCounter.count)
	require.Equal(t, [7]int{0, 0, 0, 0, 1, 0, 0}, logCounter.writtenLogEntries)
	shutdown()
}

type JRPCHandlerFunc func(ctx context.Context, r *jrpc2.Request) (interface{}, error)

func bindRPCHoist(redirector *TestServerHandlerWrapper) *JRPCHandlerFunc {
	var hoistFunction JRPCHandlerFunc

	bridgeMap := handler.Map{
		"method": handler.New(func(ctx context.Context, r *jrpc2.Request) (interface{}, error) {
			return hoistFunction(ctx, r)
		}),
	}

	redirector.f = jhttp.NewBridge(bridgeMap, &jhttp.BridgeOptions{}).ServeHTTP
	return &hoistFunction
}

func TestJRPCRequestDurationLimiter_Limiting(t *testing.T) {
	ctx := t.Context()
	addr, redirector, shutdown := createTestServer(ctx)
	hoistFunction := bindRPCHoist(redirector)

	longExecutingHandler := handler.New(func(ctx context.Context, _ *jrpc2.Request) (interface{}, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second * 10):
		}
		return "", nil
	})

	warningCounter := TestingCounter{}
	limitCounter := TestingCounter{}
	logCounter := makeTestLogCounter()
	*hoistFunction = MakeJrpcRequestDurationLimiter(
		longExecutingHandler,
		time.Second/20,
		time.Second/10,
		&warningCounter,
		&limitCounter,
		logCounter.Entry()).Handle

	ch := jhttp.NewChannel("http://"+addr+"/", nil)
	client := jrpc2.NewClient(ch, nil)
	defer client.Close()

	var res interface{}
	req := struct {
		i int
	}{1}
	err := client.CallResult(ctx, "method", req, &res)
	require.Error(t, err)
	var jrpcError *jrpc2.Error
	require.True(t, errors.As(err, &jrpcError))
	require.Equal(t, ErrRequestExceededProcessingLimitThreshold.Code, jrpcError.Code)
	require.Nil(t, res)
	require.Zero(t, warningCounter.count)
	require.Equal(t, int64(1), limitCounter.count)
	require.Equal(t, [7]int{0, 0, 0, 0, 1, 0, 0}, logCounter.writtenLogEntries)
	shutdown()
}

func TestJRPCRequestDurationLimiter_NoLimiting(t *testing.T) {
	ctx := t.Context()
	addr, redirector, shutdown := createTestServer(ctx)
	hoistFunction := bindRPCHoist(redirector)

	returnString := "ok"
	longExecutingHandler := handler.New(func(ctx context.Context, _ *jrpc2.Request) (interface{}, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second / 10):
		}
		return returnString, nil
	})

	warningCounter := TestingCounter{}
	limitCounter := TestingCounter{}
	logCounter := makeTestLogCounter()
	*hoistFunction = MakeJrpcRequestDurationLimiter(
		longExecutingHandler,
		time.Second*5,
		time.Second*10,
		&warningCounter,
		&limitCounter,
		logCounter.Entry()).Handle

	ch := jhttp.NewChannel("http://"+addr+"/", nil)
	client := jrpc2.NewClient(ch, nil)
	defer client.Close()

	var res interface{}
	req := struct {
		i int
	}{1}
	err := client.CallResult(ctx, "method", req, &res)
	require.NoError(t, err)
	require.Equal(t, returnString, res)
	require.Zero(t, warningCounter.count)
	require.Zero(t, limitCounter.count)
	require.Equal(t, [7]int{0, 0, 0, 0, 0, 0, 0}, logCounter.writtenLogEntries)
	shutdown()
}

func TestJRPCRequestDurationLimiter_NoLimiting_Warn(t *testing.T) {
	ctx := t.Context()
	addr, redirector, shutdown := createTestServer(ctx)
	hoistFunction := bindRPCHoist(redirector)

	returnString := "ok"
	longExecutingHandler := handler.New(func(ctx context.Context, _ *jrpc2.Request) (interface{}, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second / 5):
		}
		return returnString, nil
	})

	warningCounter := TestingCounter{}
	limitCounter := TestingCounter{}
	logCounter := makeTestLogCounter()
	*hoistFunction = MakeJrpcRequestDurationLimiter(
		longExecutingHandler,
		time.Second/10,
		time.Second*10,
		&warningCounter,
		&limitCounter,
		logCounter.Entry()).Handle

	ch := jhttp.NewChannel("http://"+addr+"/", nil)
	client := jrpc2.NewClient(ch, nil)
	defer client.Close()

	var res interface{}
	req := struct {
		i int
	}{1}
	err := client.CallResult(ctx, "method", req, &res)
	require.NoError(t, err)
	require.Equal(t, returnString, res)
	require.Equal(t, int64(1), warningCounter.count)
	require.Zero(t, limitCounter.count)
	require.Equal(t, [7]int{0, 0, 0, 0, 1, 0, 0}, logCounter.writtenLogEntries)
	shutdown()
}

func TestHTTPRequestDurationLimiter_Panicing(t *testing.T) {
	ctx := t.Context()
	addr, redirector, shutdown := createTestServer(ctx)
	longExecutingHandler := &TestServerHandlerWrapper{
		f: func(res http.ResponseWriter, req *http.Request) {
			panic("test panic")
		},
	}

	logCounter := makeTestLogCounter()
	redirector.f = MakeHTTPRequestDurationLimiter(
		longExecutingHandler,
		time.Second*10,
		time.Second*10,
		nil,
		nil,
		logCounter.Entry()).ServeHTTP

	client := http.Client{}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	bytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	require.Equal(t, []byte{}, bytes)
	require.Equal(t, [7]int{0, 0, 0, 7, 0, 0, 0}, logCounter.writtenLogEntries)
	shutdown()
}
