package jsonrpc

import (
	"net/http/pprof"
	runtimePprof "runtime/pprof"
	"time"

	"github.com/go-chi/chi"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	supporthttp "github.com/stellar/go-stellar-sdk/support/http"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/network"
)

// DefaultHTTPReadTimeout and DefaultHTTPIdleTimeout are the http.Server
// timeouts both daemons set on their serving and admin listeners.
//
// The idle timeout has to be set explicitly: when it is zero, net/http falls
// back to ReadTimeout for idle keep-alive connections, so every pooled client
// connection that sat idle for more than 5s got closed under it, and the
// client's next request on that connection failed with EOF or a reset.
const (
	DefaultHTTPReadTimeout = 5 * time.Second
	DefaultHTTPIdleTimeout = 2 * time.Minute
)

const responseWriteTime = 30 * time.Second // to deliver a response after the execution limit

// WriteTimeout is the http.Server write timeout both daemons set on their
// serving listeners: the request execution limit plus responseWriteTime, or 0
// (no timeout) when the sum would overflow. The bridge gives each response its
// own deadline, from its size.
func WriteTimeout(maxExecution time.Duration) time.Duration {
	if maxExecution > network.RequestDurationLimiterNoLimit-responseWriteTime {
		return 0
	}
	return maxExecution + responseWriteTime
}

// NewAdminMux is the operator-facing mux both daemons serve on their admin
// endpoint: the pprof handlers plus Prometheus /metrics over the given
// gatherer (a single registry, or prometheus.Gatherers to merge several).
func NewAdminMux(logger *log.Entry, metrics prometheus.Gatherer) *chi.Mux {
	adminMux := supporthttp.NewMux(logger)
	adminMux.HandleFunc("/debug/pprof/", pprof.Index)
	adminMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	adminMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	adminMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	adminMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	for _, profile := range runtimePprof.Profiles() {
		adminMux.Handle("/debug/pprof/"+profile.Name(), pprof.Handler(profile.Name()))
	}
	adminMux.Handle("/metrics", promhttp.HandlerFor(metrics, promhttp.HandlerOpts{}))
	return adminMux
}
