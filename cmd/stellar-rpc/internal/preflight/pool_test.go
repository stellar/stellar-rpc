package preflight

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	coreproto "github.com/stellar/go-stellar-sdk/protocols/stellarcore"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
)

// failStartEnv makes the workers of a test exit before they are ready.
const failStartEnv = "PREFLIGHT_TEST_WORKER_FAIL_START"

// The pool tests run their workers from this test binary.
func TestMain(m *testing.M) {
	if isWorker() {
		if os.Getenv(failStartEnv) != "" {
			os.Exit(1)
		}
		os.Exit(workerMain())
	}
	os.Exit(m.Run())
}

// newCoreServer answers /getledgerentry from mockLedgerEntries, the way
// captive core's query server does. onRequest, when set, runs first and can
// delay a request or answer it instead.
func newCoreServer(t *testing.T, onRequest func(w http.ResponseWriter) bool) *httptest.Server {
	getter, err := newInMemoryLedgerEntryGetter(mockLedgerEntries, latestSimulateTransactionLedgerSeq)
	require.NoError(t, err)
	core := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Core reads the form whatever the content type, and the client sends none.
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		form, err := url.ParseQuery(string(body))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if onRequest != nil && !onRequest(w) {
			return
		}
		response := coreproto.GetLedgerEntryResponse{Ledger: latestSimulateTransactionLedgerSeq}
		for _, key := range form["key"] {
			entry, ok := getter.entries[key]
			if !ok {
				response.Entries = append(response.Entries,
					coreproto.LedgerEntryResponse{State: coreproto.LedgerEntryStateNotFound})
				continue
			}
			entryB64, err := xdr.MarshalBase64(entry)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			found := coreproto.LedgerEntryResponse{State: coreproto.LedgerEntryStateLive, Entry: entryB64}
			if t := entry.Data.Type; t == xdr.LedgerEntryTypeContractData || t == xdr.LedgerEntryTypeContractCode {
				found.LiveUntilLedgerSeq = entryTTLValue
			}
			response.Entries = append(response.Entries, found)
		}
		_ = json.NewEncoder(w).Encode(response)
	}))
	t.Cleanup(core.Close)
	return core
}

// holdFirstSimulationRead returns a core hook that holds the first read until
// release is called, and a channel that closes when that read arrives.
func holdFirstSimulationRead() (func(http.ResponseWriter) bool, <-chan struct{}, func()) {
	held, released := make(chan struct{}), make(chan struct{})
	var holding atomic.Bool
	var release sync.Once
	return func(http.ResponseWriter) bool {
			if holding.CompareAndSwap(false, true) {
				close(held)
				<-released
			}
			return true
		}, held, func() {
			release.Do(func() { close(released) })
		}
}

func newTestPool(t *testing.T, core *httptest.Server, limits poolLimits) (*WorkerPool, *test.Hook) {
	logger := log.New()
	logger.SetLevel(logrus.DebugLevel)
	logger.SetOutput(io.Discard)
	logs := &test.Hook{}
	logger.AddHook(logs)
	pool := newWorkerPool(WorkerPoolConfig{
		Daemon:             host.MakeNoOpDaemon(),
		WorkerCount:        1,
		JobQueueCapacity:   1,
		EnableDebug:        true,
		NetworkPassphrase:  "foo",
		Logger:             logger,
		CoreURL:            core.URL,
		CoreRequestTimeout: 10 * time.Second,
	}, limits)
	t.Cleanup(pool.Close)
	return pool, logs
}

func testSimulation(t testing.TB, format string) GetterParameters {
	params := getPreflightParameters(t, supportedProtocolVersions[len(supportedProtocolVersions)-1])
	return GetterParameters{
		BucketListSize:  params.BucketListSize,
		SourceAccount:   params.SourceAccount,
		OperationBody:   params.OpBody,
		AuthMode:        params.AuthMode,
		ProtocolVersion: params.ProtocolVersion,
		LedgerSeq:       latestSimulateTransactionLedgerSeq,
		LedgerTime:      params.LedgerTime,
		Format:          format,
	}
}

// simulateAsync runs a simulation on pool and sends its error.
func simulateAsync(ctx context.Context, t *testing.T, pool *WorkerPool) <-chan error {
	params := testSimulation(t, protocol.FormatJSON)
	done := make(chan error, 1)
	go func() {
		_, err := pool.GetPreflight(ctx, params)
		done <- err
	}()
	return done
}

// decode decodes a response the pool returned.
func decode(t *testing.T, encoded Response) protocol.SimulateTransactionResponse {
	t.Helper()
	var response protocol.SimulateTransactionResponse
	require.NoError(t, json.Unmarshal(encoded.JSON(), &response))
	require.Equal(t, response.Error != "", encoded.Failed())
	return response
}

func requireSimulates(t *testing.T, pool *WorkerPool) {
	t.Helper()
	encoded, err := pool.GetPreflight(t.Context(), testSimulation(t, protocol.FormatJSON))
	require.NoError(t, err)
	response := decode(t, encoded)
	require.Empty(t, response.Error)
	require.Len(t, response.Results, 1)
	require.JSONEq(t, `{"vec":[{"symbol":"Hello"},{"symbol":"world"}]}`,
		string(response.Results[0].ReturnValueJSON))
	require.EqualValues(t, latestSimulateTransactionLedgerSeq, response.LatestLedger)
}

func exits(pool *WorkerPool, cause string) float64 {
	var metric dto.Metric
	_ = pool.workerExitsMetric.WithLabelValues(cause).Write(&metric)
	return metric.GetCounter().GetValue()
}

// samples is how many observations a summary or histogram holds.
func samples(t *testing.T, metric prometheus.Metric) uint64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, metric.Write(&m))
	return m.GetSummary().GetSampleCount() + m.GetHistogram().GetSampleCount()
}

// workerPID waits until the last worker the pool started is not the one
// with pid skip, and returns its pid.
func workerPID(t *testing.T, logs *test.Hook, skip int) int {
	t.Helper()
	var pid int
	require.Eventually(t, func() bool {
		for _, entry := range logs.AllEntries() {
			if p, ok := entry.Data["pid"].(int); ok && entry.Message == "started a preflight worker" {
				pid = p
			}
		}
		return pid != 0 && pid != skip
	}, time.Minute, 10*time.Millisecond)
	return pid
}

func TestPoolSimulatesInWorker(t *testing.T) {
	pool, _ := newTestPool(t, newCoreServer(t, nil), defaultPoolLimits())

	requireSimulates(t, pool)
	encoded, err := pool.GetPreflight(t.Context(), testSimulation(t, protocol.FormatBase64))
	require.NoError(t, err)
	response := decode(t, encoded)
	require.Empty(t, response.Error)
	require.NotNil(t, response.Results[0].ReturnValueXDR)
	require.NotEmpty(t, response.TransactionDataXDR)
	require.NotEmpty(t, response.EventsXDR)
}

func TestPoolReportsSimulationErrors(t *testing.T) {
	pool, _ := newTestPool(t, newCoreServer(t, nil), defaultPoolLimits())

	params := testSimulation(t, protocol.FormatJSON)
	params.AuthMode = "bogus"
	_, err := pool.GetPreflight(t.Context(), params)
	require.EqualError(t, err, "invalid auth mode: 'bogus'")

	params = testSimulation(t, protocol.FormatJSON)
	params.OperationBody = invokeContractOpBody(t, "bar")
	encoded, err := pool.GetPreflight(t.Context(), params)
	require.NoError(t, err)
	response := decode(t, encoded)
	require.Equal(t, "HostError: Error(WasmVm, MissingValue)", response.Error)
	require.NotEmpty(t, response.EventsJSON)
	require.Zero(t, exits(pool, exitCrash))
}

// Core answers a request for a ledger it no longer keeps with a 404.
func TestPoolFailsSimulationWhenLedgerIsGone(t *testing.T) {
	var reads atomic.Int32
	core := newCoreServer(t, func(w http.ResponseWriter) bool {
		if reads.Add(1) <= 3 {
			return true
		}
		http.Error(w, "Ledger not found", http.StatusNotFound)
		return false
	})
	pool, _ := newTestPool(t, core, defaultPoolLimits())

	_, err := pool.GetPreflight(t.Context(), testSimulation(t, protocol.FormatJSON))
	require.ErrorContains(t, err, "could not read the ledger state at ledger 2")
	require.ErrorContains(t, err, "404")
}

// A call whose context has already ended does not run, even on an idle
// worker.
func TestPoolDoesNotRunACallWhoseContextEnded(t *testing.T) {
	pool, _ := newTestPool(t, newCoreServer(t, nil), defaultPoolLimits())
	require.Eventually(t, func() bool { return len(pool.idle) == 1 }, time.Minute, time.Millisecond)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := pool.GetPreflight(ctx, testSimulation(t, protocol.FormatJSON))
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, samples(t, pool.ledgerEntriesFetchedMetric))
	require.Len(t, pool.idle, 1)
}

// A worker that exits while idle is replaced before a call gets it.
func TestPoolReplacesWorkerThatExitedWhileIdle(t *testing.T) {
	pool, logs := newTestPool(t, newCoreServer(t, nil), defaultPoolLimits())
	w := <-pool.idle
	require.NoError(t, w.cmd.Process.Kill())
	<-w.exited
	pool.idle <- w

	requireSimulates(t, pool)
	assert.InDelta(t, 1, exits(pool, exitCrash), 0)
	require.NotEqual(t, w.cmd.Process.Pid, workerPID(t, logs, 0))
}

func TestPoolReplacesCrashedWorker(t *testing.T) {
	hold, held, release := holdFirstSimulationRead()
	defer release()
	pool, logs := newTestPool(t, newCoreServer(t, hold), defaultPoolLimits())
	pid := workerPID(t, logs, 0)

	failed := simulateAsync(context.Background(), t, pool)
	<-held
	require.NoError(t, syscall.Kill(pid, syscall.SIGKILL))
	require.ErrorIs(t, <-failed, ErrSimulationFailed)
	assert.InDelta(t, 1, exits(pool, exitCrash), 0)

	release()
	workerPID(t, logs, pid)
	requireSimulates(t, pool)
}

// Closing the pool kills a worker that is still simulating, even for a call
// without a deadline, and fails the calls waiting for a worker.
func TestPoolCloseKillsBusyWorker(t *testing.T) {
	hold, held, release := holdFirstSimulationRead()
	defer release()
	pool, logs := newTestPool(t, newCoreServer(t, hold), defaultPoolLimits())
	pid := workerPID(t, logs, 0)

	failed := simulateAsync(context.Background(), t, pool)
	<-held
	waiting := simulateAsync(context.Background(), t, pool)
	require.Eventually(t, func() bool { return len(pool.queue) == 1 }, time.Minute, time.Millisecond)
	closed := make(chan struct{})
	go func() {
		pool.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return")
	}
	require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH)
	require.Error(t, <-failed)
	require.ErrorIs(t, <-waiting, errPoolClosed)
	assert.Zero(t, exits(pool, exitCrash))
}

// The pool kills a worker that is still simulating when its caller's deadline
// passes.
func TestPoolKillsWorkerAtTheCallersDeadline(t *testing.T) {
	hold, held, release := holdFirstSimulationRead()
	defer release()
	pool, logs := newTestPool(t, newCoreServer(t, hold), defaultPoolLimits())
	pid := workerPID(t, logs, 0)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	failed := simulateAsync(ctx, t, pool)
	<-held
	// A stopped worker cannot answer, like one still running the host.
	require.NoError(t, syscall.Kill(pid, syscall.SIGSTOP))
	require.ErrorIs(t, <-failed, ErrSimulationFailed)
	workerPID(t, logs, pid)
	assert.InDelta(t, 1, exits(pool, exitDeadline), 0)

	release()
	requireSimulates(t, pool)
}

// A worker stops waiting for a ledger read before its caller's deadline, and
// answers instead of being killed.
func TestPoolWorkerStopsReadingBeforeTheCallersDeadline(t *testing.T) {
	hold, _, release := holdFirstSimulationRead()
	defer release()
	pool, logs := newTestPool(t, newCoreServer(t, hold), defaultPoolLimits())
	pid := workerPID(t, logs, 0)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := pool.GetPreflight(ctx, testSimulation(t, protocol.FormatJSON))
	require.ErrorContains(t, err, "could not read the ledger state")
	require.ErrorContains(t, err, context.DeadlineExceeded.Error())
	require.NoError(t, syscall.Kill(pid, 0))
	assert.Zero(t, exits(pool, exitDeadline))

	release()
	requireSimulates(t, pool)
}

// A caller that stops waiting does not end its simulation, so leaving cannot
// cost a worker.
func TestPoolFinishesSimulationWhoseCallerLeft(t *testing.T) {
	hold, held, release := holdFirstSimulationRead()
	defer release()
	pool, logs := newTestPool(t, newCoreServer(t, hold), defaultPoolLimits())
	pid := workerPID(t, logs, 0)

	ctx, cancel := context.WithCancel(t.Context())
	left := simulateAsync(ctx, t, pool)
	<-held
	cancel()
	release()
	require.NoError(t, <-left)

	requireSimulates(t, pool)
	require.Equal(t, pid, workerPID(t, logs, 0))
	assert.Zero(t, exits(pool, exitCrash)+exits(pool, exitDeadline))
}

func TestPoolRejectsOversizedResponse(t *testing.T) {
	limits := defaultPoolLimits()
	limits.maxResponseBytes = 100
	pool, _ := newTestPool(t, newCoreServer(t, nil), limits)

	_, err := pool.GetPreflight(t.Context(), testSimulation(t, protocol.FormatJSON))
	require.ErrorIs(t, err, ErrSimulationFailed)
	assert.InDelta(t, 1, exits(pool, exitInvalidResponse), 0)
}

func TestPoolDelaysRestartsOfWorkersThatFailToStart(t *testing.T) {
	t.Setenv(failStartEnv, "1")
	limits := defaultPoolLimits()
	limits.minRestartDelay = 10 * time.Millisecond
	limits.maxRestartDelay = 40 * time.Millisecond
	pool, logs := newTestPool(t, newCoreServer(t, nil), limits)

	var delays []time.Duration
	require.Eventually(t, func() bool {
		delays = delays[:0]
		for _, entry := range logs.AllEntries() {
			if delay, ok := entry.Data["retry_in"].(time.Duration); ok {
				delays = append(delays, delay)
			}
		}
		return len(delays) >= 5
	}, time.Minute, 10*time.Millisecond)
	ms := time.Millisecond
	require.Equal(t, []time.Duration{10 * ms, 20 * ms, 40 * ms, 40 * ms, 40 * ms}, delays[:5])

	closed := make(chan struct{})
	go func() {
		pool.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return")
	}
}

func TestReadFrameRejectsOversizedFrameWithoutReadingIt(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, writeFrame(&buf, strings.Repeat("x", 100)))
	frame := buf.Bytes()

	err := readFrame(bytes.NewReader(frame[:4]), len(frame)-5, new(string))
	require.ErrorIs(t, err, errFrameTooLarge)

	var value string
	require.NoError(t, readFrame(bytes.NewReader(frame), len(frame)-4, &value))
	require.Equal(t, strings.Repeat("x", 100), value)
}

func TestReadFrameErrors(t *testing.T) {
	var value string
	require.ErrorIs(t, readFrame(bytes.NewReader(nil), 10, &value), io.EOF)
	require.ErrorIs(t, readFrame(bytes.NewReader([]byte{0, 0, 0, 5, '"'}), 10, &value), io.ErrUnexpectedEOF)
	require.ErrorIs(t, readFrame(bytes.NewReader([]byte{0, 0, 0, 1, '{'}), 10, &value), errInvalidFrame)
}

func TestWorkerOutputIsLogged(t *testing.T) {
	logger := log.New()
	logger.SetOutput(io.Discard)
	logs := &test.Hook{}
	logger.AddHook(logs)
	w := &workerProcess{logger: logger}

	w.logLine([]byte(`{"level":"warning","msg":"careful","time":"now","key":"value"}`))
	w.logLine([]byte("memory allocation of 1048576 bytes failed"))

	entries := logs.AllEntries()
	require.Len(t, entries, 2)
	require.Equal(t, logrus.WarnLevel, entries[0].Level)
	require.Equal(t, "careful", entries[0].Message)
	require.Equal(t, "value", entries[0].Data["key"])
	require.Equal(t, "memory allocation of 1048576 bytes failed", entries[1].Message)
}

func TestReadLineTruncatesLongLines(t *testing.T) {
	long := strings.Repeat("a", 100)
	reader := bufio.NewReaderSize(strings.NewReader(long+"\nnext\n"), 16)
	line, err := readLine(reader)
	require.NoError(t, err)
	require.Equal(t, long[:16], string(line))
	line, err = readLine(reader)
	require.NoError(t, err)
	require.Equal(t, "next", string(line))
	_, err = readLine(reader)
	require.ErrorIs(t, err, io.EOF)
}
