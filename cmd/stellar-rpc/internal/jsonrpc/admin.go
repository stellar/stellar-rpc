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
)

// DefaultHTTPReadTimeout is the http.Server ReadTimeout both daemons use for
// their serving and admin listeners.
const DefaultHTTPReadTimeout = 5 * time.Second

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
