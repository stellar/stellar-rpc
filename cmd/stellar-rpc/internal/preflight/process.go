package preflight

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"time"

	"github.com/stellar/go-stellar-sdk/support/log"
)

// The causes the worker exit metric counts.
const (
	exitCrash           = "crash"
	exitDeadline        = "deadline"
	exitInvalidResponse = "invalid_response"
)

const (
	maxLogLineBytes = 64 << 10
	startupTimeout  = 30 * time.Second
)

var (
	errDeadline       = errors.New("the simulation passed its deadline")
	errStartupTimeout = errors.New("the worker did not start in time")
)

// workerProcess is the parent's handle on one worker.
type workerProcess struct {
	cmd       *exec.Cmd
	requests  *os.File
	responses *os.File
	slot      uint
	logger    *log.Entry
	// exited is closed once the worker has exited, its output is logged, its
	// pipes are closed and cmd.ProcessState is set.
	exited chan struct{}
}

// startWorker starts a worker for slot. Closing the pool kills it.
func (pwp *WorkerPool) startWorker(slot uint) (*workerProcess, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	if runtime.GOOS == "linux" {
		// The running binary, even after an upgrade replaces the file at its
		// path, which os.Executable returns.
		executable = "/proc/self/exe"
	}
	cmd := exec.CommandContext(pwp.ctx, executable, workerArg)
	cmd.Args[0] = os.Args[0]
	output, outputW, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	requestsR, requests, err := os.Pipe()
	if err != nil {
		closeAll(output, outputW)
		return nil, err
	}
	responses, responsesW, err := os.Pipe()
	if err != nil {
		closeAll(output, outputW, requestsR, requests)
		return nil, err
	}
	cmd.Stdout, cmd.Stderr = outputW, outputW
	cmd.ExtraFiles = []*os.File{requestsR, responsesW} // requestsFD, responsesFD
	err = cmd.Start()
	closeAll(outputW, requestsR, responsesW)
	if err != nil {
		closeAll(output, requests, responses)
		return nil, err
	}

	w := &workerProcess{
		cmd:       cmd,
		requests:  requests,
		responses: responses,
		slot:      slot,
		logger:    pwp.logger.WithFields(log.F{"preflight_worker": slot, "pid": cmd.Process.Pid}),
		exited:    make(chan struct{}),
	}
	pwp.wg.Go(func() {
		w.logOutput(output)
		_ = cmd.Wait()
		closeAll(requests, responses)
		close(w.exited)
	})
	if err := w.handshake(pwp.workerConfig); err != nil {
		w.stop()
		return nil, fmt.Errorf("the preflight worker did not start (%s): %w", cmd.ProcessState, err)
	}
	w.logger.Debug("started a preflight worker")
	return w, nil
}

// handshake sends a started worker its config and waits until it is ready.
func (w *workerProcess) handshake(cfg workerConfig) error {
	err := w.setDeadline(time.Now().Add(startupTimeout))
	if err == nil {
		err = writeFrame(w.requests, cfg)
	}
	if err == nil {
		err = readFrame(w.responses, maxFrameBytes, &workerReady{})
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return errStartupTimeout
	}
	return err
}

// simulate runs request on w. It fails when ctx's deadline passes, but not
// when ctx is canceled: a caller that leaves does not end its simulation.
func (w *workerProcess) simulate(
	ctx context.Context, request GetterParameters, maxResponseBytes int,
) (simulationResponse, error) {
	deadline, hasDeadline := ctx.Deadline()
	frame := simulationRequest{GetterParameters: request}
	if hasDeadline {
		frame.Timeout = time.Until(deadline)
	}
	var response simulationResponse
	err := w.setDeadline(deadline)
	if err == nil {
		err = writeFrame(w.requests, frame)
	}
	if err == nil {
		err = readFrame(w.responses, maxFrameBytes, &response)
	}
	if err == nil {
		response.Body, err = readRawFrame(w.responses, maxResponseBytes)
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return simulationResponse{}, errDeadline
	}
	return response, err
}

// setDeadline makes reads and writes on w's pipes fail once t passes. The
// zero time means never.
func (w *workerProcess) setDeadline(t time.Time) error {
	return errors.Join(w.requests.SetWriteDeadline(t), w.responses.SetReadDeadline(t))
}

// exitCause says why w exited, given the error of the request it failed, if
// any.
func (w *workerProcess) exitCause(err error) string {
	switch {
	case errors.Is(err, errDeadline):
		return exitDeadline
	case errors.Is(err, errFrameTooLarge), errors.Is(err, errInvalidFrame):
		return exitInvalidResponse
	default:
		return exitCrash
	}
}

// stop kills w and waits until it has exited.
func (w *workerProcess) stop() {
	_ = w.cmd.Process.Kill()
	<-w.exited
}

// logOutput logs each line the worker writes to stdout or stderr. Lines from
// the worker's JSON logger keep their level and fields.
func (w *workerProcess) logOutput(output io.ReadCloser) {
	defer output.Close()
	reader := bufio.NewReaderSize(output, maxLogLineBytes)
	for {
		line, err := readLine(reader)
		if len(line) > 0 {
			w.logLine(line)
		}
		if err != nil {
			return
		}
	}
}

func (w *workerProcess) logLine(line []byte) {
	var fields map[string]any
	if json.Unmarshal(line, &fields) != nil {
		w.logger.Warn(string(line))
		return
	}
	msg, _ := fields["msg"].(string)
	level, _ := fields["level"].(string)
	delete(fields, "msg")
	delete(fields, "level")
	delete(fields, "time")
	entry := w.logger.WithFields(fields)
	switch level {
	case "trace", "debug":
		entry.Debug(msg)
	case "info":
		entry.Info(msg)
	case "warning":
		entry.Warn(msg)
	default:
		entry.Error(msg)
	}
}

// readLine reads a line without its line ending. It truncates a line longer
// than r's buffer.
func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		line = bytes.Clone(line)
		for errors.Is(err, bufio.ErrBufferFull) {
			_, err = r.ReadSlice('\n')
		}
	}
	return bytes.TrimRight(line, "\r\n"), err
}

func closeAll(files ...*os.File) {
	for _, f := range files {
		_ = f.Close()
	}
}
