package backfill

import (
	"context"
	"fmt"
	"sync"
)

// =============================================================================
// DAG — Dependency-Driven Task Scheduler
// =============================================================================
//
// The DAG schedules tasks with explicit dependency edges and bounded concurrency.
// Tasks are added via AddTask with their dependencies. Execute runs all tasks,
// dispatching each task only after all its dependencies have completed.
//
// The DAG is built once at startup after resume triage and executed to completion.
// On the first task error, the context is cancelled and no new tasks are dispatched.
// Execute waits for all in-flight tasks to finish before returning.
//
// The scheduler treats tasks as black boxes. It calls task.Execute(ctx) and
// waits for it to return. What happens inside Execute is the task's business —
// a task may be single-threaded (process_chunk) or spawn 100+ goroutines
// internally (build_txhash_index's RecSplit pipeline). The DAG only controls
// how many tasks run concurrently (via the semaphore), not what they do.
//
// Current task types (see tasks.go for the Execute implementations):
//
//	process_chunk(chunk_id)          — processChunkTask.Execute()
//	build_txhash_index(index_id)     — buildTxHashIndexTask.Execute()
//
// Future extensibility (when events are added):
//
//	build_events_index(index_id)  — Index cadence, depends on all chunks
//	complete_index(index_id)      — Index cadence, depends on all build_* tasks

// TaskID uniquely identifies a task in the DAG.
type TaskID string

// Task is a unit of work executed by the DAG scheduler.
type Task interface {
	ID() TaskID
	Execute(ctx context.Context) error
}

// DAG is a directed acyclic graph of tasks.
type DAG struct {
	tasks       map[TaskID]Task
	deps        map[TaskID][]TaskID
	insertOrder []TaskID
}

// NewDAG creates an empty DAG.
func NewDAG() *DAG {
	return &DAG{
		tasks: make(map[TaskID]Task),
		deps:  make(map[TaskID][]TaskID),
	}
}

// AddTask registers a task with its dependencies. Dependencies must already
// be registered. Panics on duplicate task IDs or unregistered dependencies.
func (d *DAG) AddTask(task Task, deps ...TaskID) {
	id := task.ID()
	if _, exists := d.tasks[id]; exists {
		panic(fmt.Sprintf("DAG: duplicate task ID: %s", id))
	}
	for _, dep := range deps {
		if _, exists := d.tasks[dep]; !exists {
			panic(fmt.Sprintf("DAG: task %s depends on unregistered task %s", id, dep))
		}
	}
	d.tasks[id] = task
	d.deps[id] = deps
	d.insertOrder = append(d.insertOrder, id)
}

// Len returns the number of tasks in the DAG.
func (d *DAG) Len() int { return len(d.tasks) }

// Execute runs all tasks respecting dependencies with bounded concurrency.
//
// maxWorkers limits the number of tasks executing simultaneously. Set to
// cfg.Backfill.Workers (default 40) to control concurrent task execution.
// process_chunk tasks fill the pool first; as chunks complete, build_txhash_index
// tasks claim freed slots once all their chunk dependencies are satisfied.
//
// On the first task error, the context is cancelled and no new tasks start.
// Execute waits for all in-flight tasks to finish before returning.
func (d *DAG) Execute(ctx context.Context, maxWorkers int) error {
	if len(d.tasks) == 0 {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Build in-degree map and reverse dependency index.
	inDegree := make(map[TaskID]int, len(d.tasks))
	dependents := make(map[TaskID][]TaskID, len(d.tasks))
	for id := range d.tasks {
		inDegree[id] = len(d.deps[id])
		for _, dep := range d.deps[id] {
			dependents[dep] = append(dependents[dep], id)
		}
	}

	var (
		mu       sync.Mutex
		firstErr error
		wg       sync.WaitGroup
	)
	sem := make(chan struct{}, maxWorkers)

	var dispatch func(id TaskID)
	dispatch = func(id TaskID) {
		mu.Lock()
		if firstErr != nil {
			mu.Unlock()
			return
		}
		mu.Unlock()

		wg.Add(1)
		go func() {
			defer wg.Done()

			// Acquire worker slot (or exit on cancellation).
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()

			err := d.tasks[id].Execute(ctx)

			mu.Lock()
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("task %s: %w", id, err)
					cancel()
				}
				mu.Unlock()
				return
			}

			// Decrement dependents' in-degrees and dispatch newly ready tasks.
			var ready []TaskID
			for _, depID := range dependents[id] {
				inDegree[depID]--
				if inDegree[depID] == 0 {
					ready = append(ready, depID)
				}
			}
			mu.Unlock()

			for _, readyID := range ready {
				dispatch(readyID)
			}
		}()
	}

	// Dispatch all initially ready tasks (in-degree 0).
	for _, id := range d.insertOrder {
		if inDegree[id] == 0 {
			dispatch(id)
		}
	}

	wg.Wait()

	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}
