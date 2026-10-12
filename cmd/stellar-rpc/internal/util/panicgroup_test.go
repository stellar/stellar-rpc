package util

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/support/log"
)

func TestTrivialPanicGroup(_ *testing.T) {
	ch := make(chan int)

	panicGroup := PanicGroup{}
	panicGroup.Go(func() { ch <- 1 })

	<-ch
}

type TestLogsCounter struct {
	entry             *log.Entry
	mu                sync.Mutex
	writtenLogEntries [logrus.TraceLevel + 1]int
}

func makeTestLogCounter() *TestLogsCounter {
	out := &TestLogsCounter{
		entry: log.New(),
	}
	out.entry.AddHook(out)
	out.entry.SetLevel(logrus.DebugLevel)
	// The hook counts the lines; keep them out of the test output.
	out.entry.SetOutput(io.Discard)
	return out
}

func (te *TestLogsCounter) Entry() *log.Entry {
	return te.entry
}

func (te *TestLogsCounter) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
		logrus.TraceLevel,
	}
}

func (te *TestLogsCounter) Fire(e *logrus.Entry) error {
	te.mu.Lock()
	defer te.mu.Unlock()
	te.writtenLogEntries[e.Level]++
	return nil
}

func (te *TestLogsCounter) GetLevel(i int) int {
	te.mu.Lock()
	defer te.mu.Unlock()
	return te.writtenLogEntries[i]
}

func PanicingFunctionA(w *int) {
	*w = 0
}

func IndirectPanicingFunctionB() {
	PanicingFunctionA(nil)
}

func IndirectPanicingFunctionC() {
	IndirectPanicingFunctionB()
}

func TestPanicGroupLog(t *testing.T) {
	logCounter := makeTestLogCounter()
	panicGroup := PanicGroup{
		log: logCounter.Entry(),
	}
	panicGroup.Go(IndirectPanicingFunctionC)
	// wait until we get all the log entries.
	waitStarted := time.Now()
	for time.Since(waitStarted) < 5*time.Second {
		if logCounter.GetLevel(int(logrus.ErrorLevel)) >= 9 {
			return
		}
		time.Sleep(1 * time.Millisecond)
	}
	t.FailNow()
}

func TestPanicGroupReportsThePanic(t *testing.T) {
	logCounter := makeTestLogCounter()
	failed := make(chan error, 1)
	panicGroup := NewPanicGroup(logCounter.Entry(), func(err error) { failed <- err })
	panicGroup.Go(IndirectPanicingFunctionC)

	select {
	case err := <-failed:
		require.ErrorContains(t, err, "panic: ")
		require.ErrorContains(t, err, "nil pointer dereference")
	case <-time.After(5 * time.Second):
		t.Fatal("the panic was not reported")
	}
	require.GreaterOrEqual(t, logCounter.GetLevel(int(logrus.ErrorLevel)), 2, "the call stack was not logged at error")
}
