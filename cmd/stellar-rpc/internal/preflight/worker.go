package preflight

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/stellar/go-stellar-sdk/clients/stellarcore"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerentries"
)

// workerArg, as the first argument, starts the binary as a preflight worker.
// It looks like a flag so that a test binary without the TestMain hook fails
// on it instead of running its tests.
const workerArg = "--preflight-worker"

// RunIfWorker runs the process as a preflight worker, and exits, when it was
// started as one.
func RunIfWorker() {
	if isWorker() {
		os.Exit(workerMain())
	}
}

func isWorker() bool {
	return len(os.Args) > 1 && os.Args[1] == workerArg
}

// The parent passes a worker the read end of a pipe for requests as file
// descriptor 3, and the write end of a pipe for responses as file
// descriptor 4. Anything the worker writes to stdout or stderr ends up in the
// parent's log.
const (
	requestsFD  = 3
	responsesFD = 4
)

// workerMain runs the simulations the parent sends until it closes the
// request pipe, and returns the process exit code.
func workerMain() int {
	// The parent stops its workers itself.
	signal.Ignore(syscall.SIGINT, syscall.SIGTERM)
	logger := log.New()
	logger.UseJSONFormatter()
	return runWorker(os.NewFile(requestsFD, "requests"), os.NewFile(responsesFD, "responses"), logger)
}

// workerConfig is the first frame a worker reads.
type workerConfig struct {
	NetworkPassphrase  string
	EnableDebug        bool
	CoreURL            string
	CoreRequestTimeout time.Duration
	MemoryLimit        uint64
}

// workerReady is the first frame a worker writes.
type workerReady struct{}

// simulationRequest asks a worker to simulate.
type simulationRequest struct {
	GetterParameters

	// Timeout is the time the call had left when the pool sent the request,
	// zero if the call has no deadline. It is relative, so that the worker's
	// deadline does not move when the wall clock is set.
	Timeout time.Duration
}

// replyMargin is how long before its call's deadline a worker stops reading
// the ledger, so that it answers before the pool kills it.
const replyMargin = 100 * time.Millisecond

type simulationResponse struct {
	// Body is the response, encoded as JSON. It follows in a frame of its
	// own, which the parent passes on without decoding it.
	Body []byte `json:"-"`
	// Failed says the response carries an error.
	Failed bool
	// Error is set, and Body empty, when the simulation could not run or its
	// result could not be converted.
	Error                string
	LedgerEntriesFetched uint32
	LedgerEntriesTime    time.Duration
	// MemoryLimit is the heap limit in force when the simulation finished, 0
	// when there was none yet. PeakMemory is the most heap the simulation
	// library held during the simulation and the conversion of its result.
	MemoryLimit uint64
	PeakMemory  uint64
}

func runWorker(in io.Reader, out io.Writer, logger *log.Entry) int {
	var cfg workerConfig
	if err := readFrame(in, maxFrameBytes, &cfg); err != nil {
		logger.WithError(err).Error("could not read the preflight worker config")
		return 1
	}
	core := &stellarcore.Client{
		URL:  cfg.CoreURL,
		HTTP: &http.Client{Timeout: cfg.CoreRequestTimeout},
	}
	limitMemory(cfg.MemoryLimit)
	if err := writeFrame(out, workerReady{}); err != nil {
		return 1
	}
	for {
		var request simulationRequest
		if err := readFrame(in, maxFrameBytes, &request); err != nil {
			if errors.Is(err, io.EOF) {
				return 0
			}
			logger.WithError(err).Error("could not read a simulation request")
			return 1
		}
		response := simulate(cfg, core, logger, request)
		if writeFrame(out, response) != nil || writeRawFrame(out, response.Body) != nil {
			return 1
		}
	}
}

func simulate(
	cfg workerConfig, core *stellarcore.Client, logger *log.Entry, request simulationRequest,
) simulationResponse {
	entries := &entryReader{getter: ledgerentries.NewLedgerEntryAtGetter(core, request.LedgerSeq)}
	ctx := context.Background()
	if request.Timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, request.Timeout-replyMargin)
		defer cancel()
	}
	resetMemoryPeak()
	preflight, err := GetPreflight(ctx, Parameters{
		Logger:            logger,
		SourceAccount:     request.SourceAccount,
		OpBody:            request.OperationBody,
		Footprint:         request.Footprint,
		NetworkPassphrase: cfg.NetworkPassphrase,
		LedgerEntryGetter: entries,
		LedgerSeq:         request.LedgerSeq,
		LedgerTime:        request.LedgerTime,
		BucketListSize:    request.BucketListSize,
		ResourceConfig:    request.ResourceConfig,
		EnableDebug:       cfg.EnableDebug,
		AuthMode:          request.AuthMode,
		UseUpgradedAuth:   request.UseUpgradedAuth,
		ProtocolVersion:   request.ProtocolVersion,
	})
	var formatted protocol.SimulateTransactionResponse
	if err == nil {
		formatted, err = formatResponse(preflight, request.Format, request.LedgerSeq)
	}
	var encoded Response
	if err == nil {
		encoded, err = EncodeResponse(formatted)
	}
	response := simulationResponse{
		Body:                 encoded.JSON(),
		Failed:               encoded.Failed(),
		LedgerEntriesFetched: entries.fetched,
		LedgerEntriesTime:    entries.duration,
		MemoryLimit:          memoryLimit(),
		PeakMemory:           memoryPeak(),
	}
	if err != nil {
		response.Error = err.Error()
	}
	return response
}

// entryReader reads ledger entries for one simulation and counts the reads.
type entryReader struct {
	getter   ledgerentries.LedgerEntryGetter
	fetched  uint32
	duration time.Duration
}

func (r *entryReader) GetLedgerEntries(
	ctx context.Context, keys []xdr.LedgerKeyView,
) ([]ledgerentries.LedgerKeyAndEntry, uint32, error) {
	start := time.Now()
	entries, ledger, err := r.getter.GetLedgerEntries(ctx, keys)
	r.duration += time.Since(start)
	r.fetched += uint32(len(keys)) //nolint:gosec // len() is never negative
	return entries, ledger, err
}
