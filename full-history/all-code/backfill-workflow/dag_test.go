package backfill

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// Test helpers
// =============================================================================

// simpleTask is a test task that calls a function on Execute.
type simpleTask struct {
	id TaskID
	fn func(ctx context.Context) error
}

func (t *simpleTask) ID() TaskID                          { return t.id }
func (t *simpleTask) Execute(ctx context.Context) error { return t.fn(ctx) }

func newSimpleTask(id string, fn func(ctx context.Context) error) *simpleTask {
	return &simpleTask{id: TaskID(id), fn: fn}
}

func newNoopTask(id string) *simpleTask {
	return &simpleTask{id: TaskID(id), fn: func(context.Context) error { return nil }}
}

// =============================================================================
// DAG Scheduler Tests
// =============================================================================

func TestDAGEmpty(t *testing.T) {
	dag := NewDAG()
	err := dag.Execute(context.Background(), 4)
	if err != nil {
		t.Fatalf("empty DAG should not error: %v", err)
	}
}

func TestDAGSingleTask(t *testing.T) {
	var executed bool
	dag := NewDAG()
	dag.AddTask(newSimpleTask("a", func(context.Context) error {
		executed = true
		return nil
	}))

	err := dag.Execute(context.Background(), 1)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !executed {
		t.Error("task should have executed")
	}
}

func TestDAGLinearChain(t *testing.T) {
	// a → b → c : must execute in order
	var order []string
	var mu sync.Mutex
	record := func(name string) func(context.Context) error {
		return func(context.Context) error {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return nil
		}
	}

	dag := NewDAG()
	dag.AddTask(newSimpleTask("a", record("a")))
	dag.AddTask(newSimpleTask("b", record("b")), "a")
	dag.AddTask(newSimpleTask("c", record("c")), "b")

	err := dag.Execute(context.Background(), 1)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(order) != 3 || order[0] != "a" || order[1] != "b" || order[2] != "c" {
		t.Errorf("expected [a b c], got %v", order)
	}
}

func TestDAGDiamondDeps(t *testing.T) {
	// a ──► b ──► d
	// a ──► c ──► d
	// d must execute after both b and c.
	var completedBefore atomic.Int32
	var dStarted atomic.Bool

	dag := NewDAG()
	dag.AddTask(newNoopTask("a"))
	dag.AddTask(newSimpleTask("b", func(context.Context) error {
		time.Sleep(10 * time.Millisecond)
		completedBefore.Add(1)
		return nil
	}), "a")
	dag.AddTask(newSimpleTask("c", func(context.Context) error {
		time.Sleep(10 * time.Millisecond)
		completedBefore.Add(1)
		return nil
	}), "a")
	dag.AddTask(newSimpleTask("d", func(context.Context) error {
		dStarted.Store(true)
		if completedBefore.Load() != 2 {
			return fmt.Errorf("d started before b and c completed")
		}
		return nil
	}), "b", "c")

	err := dag.Execute(context.Background(), 4)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !dStarted.Load() {
		t.Error("task d should have executed")
	}
}

func TestDAGConcurrency(t *testing.T) {
	// 4 independent tasks, maxWorkers=2. At most 2 should run concurrently.
	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32

	dag := NewDAG()
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("t%d", i)
		dag.AddTask(newSimpleTask(id, func(context.Context) error {
			c := concurrent.Add(1)
			if c > maxConcurrent.Load() {
				maxConcurrent.Store(c)
			}
			time.Sleep(20 * time.Millisecond)
			concurrent.Add(-1)
			return nil
		}))
	}

	err := dag.Execute(context.Background(), 2)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if maxConcurrent.Load() > 2 {
		t.Errorf("max concurrent = %d, want <= 2", maxConcurrent.Load())
	}
}

func TestDAGErrorStopsNewTasks(t *testing.T) {
	// a errors → b (depends on a) should not execute
	var bExecuted atomic.Bool

	dag := NewDAG()
	dag.AddTask(newSimpleTask("a", func(context.Context) error {
		return fmt.Errorf("intentional error")
	}))
	dag.AddTask(newSimpleTask("b", func(context.Context) error {
		bExecuted.Store(true)
		return nil
	}), "a")

	err := dag.Execute(context.Background(), 2)
	if err == nil {
		t.Fatal("expected error")
	}
	if bExecuted.Load() {
		t.Error("b should not execute after a fails")
	}
}

func TestDAGErrorFromIndependentTask(t *testing.T) {
	// Two independent tasks: a succeeds, b errors.
	dag := NewDAG()
	dag.AddTask(newSimpleTask("a", func(context.Context) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	}))
	dag.AddTask(newSimpleTask("b", func(context.Context) error {
		return fmt.Errorf("b failed")
	}))

	err := dag.Execute(context.Background(), 2)
	if err == nil {
		t.Fatal("expected error from task b")
	}
}

func TestDAGContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	dag := NewDAG()
	dag.AddTask(newNoopTask("a"))

	err := dag.Execute(ctx, 1)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

func TestDAGBackfillShape(t *testing.T) {
	// Simulate the backfill DAG shape:
	// process_chunk(0..2) → build_txhash_index(0)
	// All 3 chunks must complete before the build task runs.
	var chunksCompleted atomic.Int32
	var buildRanOK atomic.Bool

	dag := NewDAG()
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("process_chunk(0,%d)", i)
		dag.AddTask(newSimpleTask(id, func(context.Context) error {
			time.Sleep(5 * time.Millisecond)
			chunksCompleted.Add(1)
			return nil
		}))
	}

	dag.AddTask(newSimpleTask("build_txhash_index(0)", func(context.Context) error {
		if chunksCompleted.Load() != 3 {
			return fmt.Errorf("build started before all chunks completed")
		}
		buildRanOK.Store(true)
		return nil
	}), "process_chunk(0,0)", "process_chunk(0,1)", "process_chunk(0,2)")

	err := dag.Execute(context.Background(), 4)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !buildRanOK.Load() {
		t.Error("build task should have run")
	}
}

func TestDAGMultiRangeShape(t *testing.T) {
	// Two indexes with 2 chunks each. Both build tasks must wait for
	// their respective process_chunk tasks.
	var r0done, r1done atomic.Int32

	dag := NewDAG()
	for r := 0; r < 2; r++ {
		for i := 0; i < 2; i++ {
			rID := r
			dag.AddTask(newSimpleTask(
				fmt.Sprintf("process(%d,%d)", r, i),
				func(context.Context) error {
					if rID == 0 {
						r0done.Add(1)
					} else {
						r1done.Add(1)
					}
					return nil
				},
			))
		}
	}

	dag.AddTask(newSimpleTask("build(0)", func(context.Context) error {
		if r0done.Load() != 2 {
			return fmt.Errorf("range 0 build started before chunks done")
		}
		return nil
	}), "process(0,0)", "process(0,1)")

	dag.AddTask(newSimpleTask("build(1)", func(context.Context) error {
		if r1done.Load() != 2 {
			return fmt.Errorf("range 1 build started before chunks done")
		}
		return nil
	}), "process(1,0)", "process(1,1)")

	err := dag.Execute(context.Background(), 4)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
}

func TestDAGFutureEventsShape(t *testing.T) {
	// Simulates the future DAG shape when events are added:
	//
	//   process_chunk(0..2) ──► build_txhash_index(0) ──┐
	//                        └─► build_events_index(0) ──┴──► complete_index(0)
	//
	// Both build tasks depend on all process_chunk tasks.
	// complete_index depends on both build tasks.
	var chunksDone atomic.Int32
	var txhashBuilt, eventsBuilt atomic.Bool
	var completeRanOK atomic.Bool

	dag := NewDAG()

	// 3 process_chunk tasks
	for i := 0; i < 3; i++ {
		dag.AddTask(newSimpleTask(fmt.Sprintf("process_chunk(0,%d)", i), func(context.Context) error {
			time.Sleep(5 * time.Millisecond)
			chunksDone.Add(1)
			return nil
		}))
	}

	// build_txhash_index depends on all 3 chunks
	dag.AddTask(newSimpleTask("build_txhash_index(0)", func(context.Context) error {
		if chunksDone.Load() != 3 {
			return fmt.Errorf("txhash build started before all chunks done")
		}
		time.Sleep(10 * time.Millisecond)
		txhashBuilt.Store(true)
		return nil
	}), "process_chunk(0,0)", "process_chunk(0,1)", "process_chunk(0,2)")

	// build_events_index depends on all 3 chunks (parallel to txhash)
	dag.AddTask(newSimpleTask("build_events_index(0)", func(context.Context) error {
		if chunksDone.Load() != 3 {
			return fmt.Errorf("events build started before all chunks done")
		}
		time.Sleep(10 * time.Millisecond)
		eventsBuilt.Store(true)
		return nil
	}), "process_chunk(0,0)", "process_chunk(0,1)", "process_chunk(0,2)")

	// complete_index depends on both build tasks
	dag.AddTask(newSimpleTask("complete_index(0)", func(context.Context) error {
		if !txhashBuilt.Load() {
			return fmt.Errorf("complete_index started before txhash build")
		}
		if !eventsBuilt.Load() {
			return fmt.Errorf("complete_index started before events build")
		}
		completeRanOK.Store(true)
		return nil
	}), "build_txhash_index(0)", "build_events_index(0)")

	err := dag.Execute(context.Background(), 4)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !completeRanOK.Load() {
		t.Error("complete_index should have executed")
	}
}

func TestDAGFutureEventsMultiRange(t *testing.T) {
	// Two indexes with the full future shape. Verifies that both indexes
	// can interleave — index 1's build tasks can start even while index 0's
	// complete_index hasn't fired yet.
	//
	// Index 0: process(0,0..1) → build_txhash(0) + build_events(0) → complete(0)
	// Index 1: process(1,0..1) → build_txhash(1) + build_events(1) → complete(1)

	var completed [2]atomic.Bool

	dag := NewDAG()
	for r := 0; r < 2; r++ {
		rID := r
		p0 := fmt.Sprintf("process(%d,0)", r)
		p1 := fmt.Sprintf("process(%d,1)", r)
		btx := fmt.Sprintf("build_txhash(%d)", r)
		bev := fmt.Sprintf("build_events(%d)", r)

		dag.AddTask(newSimpleTask(p0, func(context.Context) error { return nil }))
		dag.AddTask(newSimpleTask(p1, func(context.Context) error { return nil }))

		dag.AddTask(newSimpleTask(btx, func(context.Context) error { return nil }), TaskID(p0), TaskID(p1))
		dag.AddTask(newSimpleTask(bev, func(context.Context) error { return nil }), TaskID(p0), TaskID(p1))

		dag.AddTask(newSimpleTask(fmt.Sprintf("complete(%d)", r), func(context.Context) error {
			completed[rID].Store(true)
			return nil
		}), TaskID(btx), TaskID(bev))
	}

	err := dag.Execute(context.Background(), 4)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	for r := 0; r < 2; r++ {
		if !completed[r].Load() {
			t.Errorf("range %d complete_index did not execute", r)
		}
	}
}

func TestDAGPanicsOnDuplicateID(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on duplicate task ID")
		}
	}()

	dag := NewDAG()
	dag.AddTask(newNoopTask("a"))
	dag.AddTask(newNoopTask("a")) // Should panic
}

func TestDAGPanicsOnMissingDep(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on missing dependency")
		}
	}()

	dag := NewDAG()
	dag.AddTask(newNoopTask("a"), "nonexistent") // Should panic
}

func TestDAGRetry_TransientFailure(t *testing.T) {
	var attempts atomic.Int32

	dag := NewDAG()
	dag.AddTask(newSimpleTask("test", func(ctx context.Context) error {
		n := attempts.Add(1)
		if n < 3 {
			return fmt.Errorf("transient error")
		}
		return nil
	}))

	err := dag.Execute(context.Background(), 1, WithMaxRetries(3))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if attempts.Load() != 3 {
		t.Errorf("attempts = %d, want 3", attempts.Load())
	}
}

func TestDAGRetry_PermanentFailure(t *testing.T) {
	var attempts atomic.Int32

	dag := NewDAG()
	dag.AddTask(newSimpleTask("test", func(ctx context.Context) error {
		attempts.Add(1)
		return fmt.Errorf("permanent error")
	}))

	err := dag.Execute(context.Background(), 1, WithMaxRetries(3))
	if err == nil {
		t.Fatal("expected error")
	}
	if attempts.Load() != 3 {
		t.Errorf("attempts = %d, want 3 (exhausted retries)", attempts.Load())
	}
}

func TestDAGRetry_DefaultNoRetry(t *testing.T) {
	var attempts atomic.Int32

	dag := NewDAG()
	dag.AddTask(newSimpleTask("test", func(ctx context.Context) error {
		attempts.Add(1)
		return fmt.Errorf("error")
	}))

	// No WithMaxRetries — default is 1 attempt.
	err := dag.Execute(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error")
	}
	if attempts.Load() != 1 {
		t.Errorf("attempts = %d, want 1", attempts.Load())
	}
}
