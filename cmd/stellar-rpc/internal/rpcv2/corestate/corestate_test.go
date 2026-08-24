package corestate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/clients/stellarcore"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

func validConfig() Config {
	return Config{
		CoreURL:        "http://localhost:11626",
		QueryPort:      11628,
		RequestTimeout: 2 * time.Second,
		Registry:       prometheus.NewRegistry(),
	}
}

func TestNew_WiresBothClientsAtTheirPorts(t *testing.T) {
	registry := prometheus.NewRegistry()
	cfg := validConfig()
	cfg.CoreURL = "http://core.internal:8080"
	cfg.QueryPort = 21628
	cfg.RequestTimeout = 7 * time.Second
	cfg.Registry = registry

	d, err := New(context.Background(), cfg)
	require.NoError(t, err)

	submit, ok := d.CoreClient().(*host.CoreClientWithMetrics)
	require.True(t, ok, "the submission path must carry the txsub metrics wrapper")
	assert.Equal(t, "http://core.internal:8080", submit.URL,
		"submissions go to the configured admin URL")
	assert.Equal(t, 7*time.Second, submit.HTTP.(*http.Client).Timeout) //nolint:forcetypeassert

	query, ok := d.FastCoreClient().(*stellarcore.Client)
	require.True(t, ok)
	assert.Equal(t, "http://localhost:21628", query.URL,
		"entry lookups go to the local query port")
	assert.Equal(t, 7*time.Second, query.HTTP.(*http.Client).Timeout) //nolint:forcetypeassert

	assert.NotSame(t, submit.HTTP, query.HTTP,
		"one http.Client per server, so the metrics-wrapped submission path never shares a Client with queries")
	assert.Same(t, registry, d.MetricsRegistry(),
		"handlers must register on the daemon's real registry, not a throwaway")
	assert.Equal(t, host.PrometheusNamespace, d.MetricsNamespace())
}

func TestCoreClient_PublishesTxsubMetrics(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"PENDING"}`)
	}))
	defer srv.Close()

	registry := prometheus.NewRegistry()
	cfg := validConfig()
	cfg.CoreURL = srv.URL
	cfg.Registry = registry

	d, err := New(context.Background(), cfg)
	require.NoError(t, err)

	envelope := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{
				SourceAccount: xdr.MustMuxedAddress("GDKXE2OZMJIPOSLNA6N6F2BVCI3O777I2OOC4BV7VOYUEHYX7RTRYA7Y"),
				Operations: []xdr.Operation{{Body: xdr.OperationBody{
					Type:           xdr.OperationTypeBumpSequence,
					BumpSequenceOp: &xdr.BumpSequenceOp{BumpTo: 1},
				}}},
			},
		},
	}
	blob, err := xdr.MarshalBase64(envelope)
	require.NoError(t, err)

	resp, err := d.CoreClient().SubmitTransaction(context.Background(), blob)
	require.NoError(t, err)
	assert.Equal(t, "PENDING", resp.Status)

	families, err := registry.Gather()
	require.NoError(t, err)
	names := make([]string, 0, len(families))
	for _, family := range families {
		names = append(names, family.GetName())
	}
	assert.Contains(t, names, "soroban_rpc_txsub_submission_duration_seconds")
	assert.Contains(t, names, "soroban_rpc_txsub_operation_count")
}

func TestNew_NamespaceOverride(t *testing.T) {
	cfg := validConfig()
	cfg.Namespace = "custom_ns"
	d, err := New(context.Background(), cfg)
	require.NoError(t, err)
	assert.Equal(t, "custom_ns", d.MetricsNamespace())
}

func TestNew_RejectsIncompleteConfig(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"no core URL", func(c *Config) { c.CoreURL = "" }, "CoreURL"},
		{"no query port", func(c *Config) { c.QueryPort = 0 }, "QueryPort"},
		{"zero timeout means no timeout at all", func(c *Config) { c.RequestTimeout = 0 }, "RequestTimeout"},
		{"negative timeout", func(c *Config) { c.RequestTimeout = -time.Second }, "RequestTimeout"},
		{"no registry", func(c *Config) { c.Registry = nil }, "Registry"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			tc.mutate(&cfg)
			_, err := New(context.Background(), cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// fakeCoreBinary writes an executable that answers `version` like stellar-core
// does — the first line of stdout is the version string.
func fakeCoreBinary(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "stellar-core")
	require.NoError(t, os.WriteFile(path, []byte(script), 0o755))
	return path
}

func TestCoreVersion_ReadFromTheBinary(t *testing.T) {
	cfg := validConfig()
	cfg.StellarCoreBinaryPath = fakeCoreBinary(t, "#!/bin/sh\necho 'stellar-core 22.1.0 (deadbeef)'\n")

	d, err := New(context.Background(), cfg)
	require.NoError(t, err)
	assert.Equal(t, "stellar-core 22.1.0 (deadbeef)", d.CoreVersion())
}

func TestCoreVersion_EmptyWhenNoBinaryPath(t *testing.T) {
	d, err := New(context.Background(), validConfig())
	require.NoError(t, err)
	assert.Empty(t, d.CoreVersion())
}

// A binary that cannot be run is not a startup failure: the version is a
// cosmetic field of getVersionInfo, so the daemon still comes up.
func TestCoreVersion_UnreadableBinaryIsNotFatal(t *testing.T) {
	cfg := validConfig()
	cfg.StellarCoreBinaryPath = filepath.Join(t.TempDir(), "does-not-exist")

	d, err := New(context.Background(), cfg)
	require.NoError(t, err)
	assert.Empty(t, d.CoreVersion())
}

func TestCoreVersion_HangingBinaryDoesNotBlockStartup(t *testing.T) {
	cfg := validConfig()
	cfg.StellarCoreBinaryPath = fakeCoreBinary(t, "#!/bin/sh\nsleep 300\n")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	d, err := New(ctx, cfg)
	require.NoError(t, err)
	assert.Empty(t, d.CoreVersion())
	assert.Less(t, time.Since(start), 30*time.Second,
		"a binary that never exits must be killed at the deadline, not waited out")
}

func TestCoreVersion_WrapperChildHoldingStdoutDoesNotBlockStartup(t *testing.T) {
	cfg := validConfig()
	// The wrapper exits at once, but its background child inherits stdout and
	// holds the pipe open — only WaitDelay unblocks the read.
	cfg.StellarCoreBinaryPath = fakeCoreBinary(t, "#!/bin/sh\necho 'stellar-core 99.0.0'\nsleep 300 &\n")

	start := time.Now()
	d, err := New(context.Background(), cfg)
	require.NoError(t, err)
	assert.Empty(t, d.CoreVersion())
	assert.Less(t, time.Since(start), 30*time.Second,
		"a grandchild holding the stdout pipe must not stall startup")
}

// stubLedgerReader answers only GetLatestLedgerSequence; the entry getter needs
// nothing else from the store.
type stubLedgerReader struct{ latest uint32 }

func (s stubLedgerReader) GetLatestLedgerSequence(context.Context) (uint32, error) {
	return s.latest, nil
}

func (s stubLedgerReader) GetLedger(context.Context, uint32) (xdr.LedgerCloseMeta, bool, error) {
	return xdr.LedgerCloseMeta{}, false, errors.New("unused")
}

func (s stubLedgerReader) GetLedgerView(context.Context, uint32) (xdr.LedgerCloseMetaView, bool, error) {
	return nil, false, errors.New("unused")
}

func (s stubLedgerReader) GetLedgerRange(context.Context) (store.LedgerRange, error) {
	return store.LedgerRange{}, errors.New("unused")
}

func (s stubLedgerReader) StreamLedgerRange(
	context.Context, uint32, uint32, store.StreamLedgerFn,
) error {
	return errors.New("unused")
}

func (s stubLedgerReader) NewTx(context.Context) (store.LedgerReaderTx, error) {
	return nil, errors.New("unused")
}

// The entry getter must reach core's QUERY server (not the admin URL) and must
// ask for the ledger this daemon last committed, so a read agrees with what the
// daemon's other responses report even while core runs ahead.
func TestLedgerEntryGetter_QueriesTheQueryPortAtTheDaemonsLedger(t *testing.T) {
	var gotPath, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		gotPath, gotBody = r.URL.Path, string(body)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"ledgerSeq":1234,"entries":[]}`)
	}))
	defer srv.Close()

	cfg := validConfig()
	cfg.QueryPort = serverPort(t, srv)
	// A deliberately unroutable admin URL: any request that lands here instead of
	// on the query port fails the test rather than passing by accident.
	cfg.CoreURL = "http://127.0.0.1:1"

	d, err := New(context.Background(), cfg)
	require.NoError(t, err)

	getter := d.LedgerEntryGetter(stubLedgerReader{latest: 1234})
	key := xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeAccount,
		Account: &xdr.LedgerKeyAccount{
			AccountId: xdr.MustAddress("GDKXE2OZMJIPOSLNA6N6F2BVCI3O777I2OOC4BV7VOYUEHYX7RTRYA7Y"),
		},
	}

	entries, atLedger, err := getter.GetLedgerEntries(context.Background(), []xdr.LedgerKey{key})
	require.NoError(t, err)
	assert.Empty(t, entries)
	assert.Equal(t, uint32(1234), atLedger)
	assert.Equal(t, "/getledgerentry", gotPath)
	assert.Contains(t, gotBody, "ledgerSeq=1234",
		"the daemon's own latest ledger is what core is asked about")
}

// serverPort is the port an httptest server bound, so a client built from a bare
// port number can be pointed at it.
func serverPort(t *testing.T, srv *httptest.Server) uint {
	t.Helper()
	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	port, err := strconv.ParseUint(u.Port(), 10, 32)
	require.NoError(t, err)
	return uint(port)
}
