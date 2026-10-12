package util

import (
	"fmt"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/stellar/go-stellar-sdk/support/log"
)

// PanicGroup recovers a panic in a function it runs, logs the call stack at
// level error, and hands the panic to onPanic as an error. The process keeps
// running, so the caller can shut everything down in order instead of exiting
// at once.
type PanicGroup struct {
	log     *log.Entry
	onPanic func(error)
}

func NewPanicGroup(logger *log.Entry, onPanic func(error)) *PanicGroup {
	return &PanicGroup{log: logger, onPanic: onPanic}
}

// Go runs fn on a new goroutine with the same panic handling as Run.
func (pg *PanicGroup) Go(fn func()) {
	go pg.Run(fn)
}

// Run calls fn on the calling goroutine and handles a panic in it. A caller
// that must do something after the panic was handled, such as marking a
// WaitGroup done, wraps Run instead of fn.
func (pg *PanicGroup) Run(fn func()) {
	defer pg.recoverRoutine(fn)
	fn()
}

func (pg *PanicGroup) recoverRoutine(fn func()) {
	recoverRes := recover()
	if recoverRes == nil {
		return
	}
	if pg.log != nil {
		for _, line := range getPanicCallStack(recoverRes, fn) {
			pg.log.Error(line)
		}
	}
	if pg.onPanic != nil {
		pg.onPanic(fmt.Errorf("panic: %v", recoverRes))
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
