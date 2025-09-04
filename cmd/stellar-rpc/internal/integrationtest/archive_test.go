package integrationtest

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
)

func TestArchiveUserAgent(t *testing.T) {
	userAgents := sync.Map{}
	historyArchive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		agent := r.Header["User-Agent"][0]
		t.Log("agent", agent)
		userAgents.Store(agent, "")
		if r.URL.Path == "/.well-known/stellar-history.json" || r.URL.Path == "/history/00/00/00/history-0000001f.json" {
			_, _ = w.Write([]byte(`{
    "version": 1,
    "server": "stellar-core 21.0.1 (dfd3dbff1d9cad4dc31e022de6ac2db731b4b326)",
    "currentLedger": 31,
    "networkPassphrase": "Standalone Network ; February 2017",
    "currentBuckets": []
}`))
			return
		}
		// emulate a problem with the archive
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer historyArchive.Close()
	url, err := url.Parse(historyArchive.URL)
	require.NoError(t, err)
	historyHostPort := url.Host

	cfg := &infrastructure.TestConfig{
		OnlyRPC: &infrastructure.TestOnlyRPCConfig{
			CorePorts: infrastructure.TestCorePorts{
				CoreArchiveHostPort: historyHostPort,
			},
			DontWait: true,
		},
	}

	infrastructure.NewTest(t, cfg)

	require.Eventually(t,
		func() bool {
			_, ok1 := userAgents.Load("stellar-rpc/0.0.0")
			_, ok2 := userAgents.Load("stellar-rpc/0.0.0/captivecore")
			return ok1 && ok2
		},
		5*time.Second,
		time.Second,
	)
}
