package util

import (
	"fmt"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/stellar/go-stellar-sdk/support/log"
)

type PanicGroup struct {
	log                *log.Entry
	logPanicsToStdErr  bool
	exitProcessOnPanic bool
	onPanic            func(error)
}

func NewUnrecoverablePanicGroup() PanicGroup {
	return PanicGroup{
		logPanicsToStdErr:  true,
		exitProcessOnPanic: true,
	}
}

// NewRecoverablePanicGroup logs a panic in one of its goroutines with the
// call stack, then hands it to onPanic as an error. The process keeps running,
// so the caller can shut everything down in order instead of exiting at once.
func NewRecoverablePanicGroup(logger *log.Entry, onPanic func(error)) *PanicGroup {
	return &PanicGroup{log: logger, onPanic: onPanic}
}

func (pg *PanicGroup) Log(log *log.Entry) *PanicGroup {
	return &PanicGroup{
		log:                log,
		logPanicsToStdErr:  pg.logPanicsToStdErr,
		exitProcessOnPanic: pg.exitProcessOnPanic,
		onPanic:            pg.onPanic,
	}
}

// Go spins a goroutine with clear upfront definitions for what should be done in the case of an internal panic.
func (pg *PanicGroup) Go(fn func()) {
	go pg.Run(fn)
}

// Run calls fn on the calling goroutine with the same panic handling as Go.
// A caller that must do something after the panic was handled, such as
// marking a WaitGroup done, wraps Run instead of fn.
func (pg *PanicGroup) Run(fn func()) {
	defer pg.recoverRoutine(fn)
	fn()
}

func (pg *PanicGroup) recoverRoutine(fn func()) {
	recoverRes := recover()
	if recoverRes == nil {
		return
	}
	cs := getPanicCallStack(recoverRes, fn)
	if len(cs) == 0 {
		return
	}
	if pg.log != nil {
		// A recoverable panic has no stderr copy, so its stack must survive
		// a logger set to level error.
		logLine := pg.log.Warn
		if pg.onPanic != nil {
			logLine = pg.log.Error
		}
		for _, line := range cs {
			logLine(line)
		}
	}
	if pg.logPanicsToStdErr {
		for _, line := range cs {
			fmt.Fprintln(os.Stderr, line)
		}
	}
	if pg.onPanic != nil {
		pg.onPanic(fmt.Errorf("panic: %v", recoverRes))
		return
	}

	if pg.exitProcessOnPanic {
		os.Exit(1)
	}
}

func getPanicCallStack(recoverRes any, fn func()) []string {
	functionName := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	return CallStack(recoverRes, functionName, "(*PanicGroup).Run", 10)
}

// CallStack returns an array of strings representing the current call stack. The method is
// tuned for the purpose of panic handler, and used as a helper in constructing the list of entries we want
// to write to the log / stderr / telemetry.

func CallStack(
	recoverRes any,
	topLevelFunctionName string,
	lastCallstackMethod string,
	unwindStackLines int,
) []string {
	callStack := []string{}
	if topLevelFunctionName != "" {
		callStack = append(callStack, fmt.Sprintf("%v when calling %v", recoverRes, topLevelFunctionName))
	} else {
		callStack = append(callStack, fmt.Sprintf("%v", recoverRes))
	}
	// while we're within the recoverRoutine, the debug.Stack() would return the
	// call stack where the panic took place.
	callStackStrings := string(debug.Stack())
	stackFields := strings.FieldsFunc(callStackStrings, func(r rune) bool {
		return r == '\n' || r == '\t'
	})
	for i, callStackLine := range stackFields {
		// skip the first (unwindStackLines) entries, since these are the "debug.Stack()" entries, which aren't really useful.
		if i < unwindStackLines {
			continue
		}
		callStack = append(callStack, callStackLine)
		// once we reached the limiter entry, stop.
		if strings.Contains(callStackLine, lastCallstackMethod) {
			break
		}
	}
	return callStack
}
