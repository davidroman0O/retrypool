package retrypool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/davidroman0O/retrypool/logs"
)

// Define an IncrementWorker that increments a counter
type IncrementWorker struct {
	mu      sync.Mutex
	counter int
}

func (w *IncrementWorker) Run(ctx context.Context, data int) error {
	w.mu.Lock()
	w.counter += data
	w.mu.Unlock()
	return nil
}

// Define a FlakyWorker that fails a certain number of times before succeeding
type FlakyWorker struct {
	failuresLeft int
	mu           sync.Mutex
	count        int
}

func (w *FlakyWorker) Run(ctx context.Context, data int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failuresLeft > 0 {
		w.failuresLeft--
		return errors.New("temporary error")
	}
	return nil
}

// Define a FailingWorker that always fails
type FailingWorker struct {
	mu      sync.Mutex
	counter int
}

func (w *FailingWorker) Run(ctx context.Context, data int) error {
	w.mu.Lock()
	w.counter++
	w.mu.Unlock()
	return errors.New("task failed")
}

// Define a testPanicWorker that panics during execution
type testPanicWorker struct{}

func (w *testPanicWorker) Run(ctx context.Context, data int) error {
	panic("worker panic")
}

// Define a SlowWorker that sleeps
type SlowWorker struct {
	mu         sync.Mutex
	processing bool
	started    chan struct{}
}

func (w *SlowWorker) Run(ctx context.Context, data int) error {
	fmt.Println("called", data)
	w.mu.Lock()
	w.processing = true
	if w.started != nil {
		close(w.started) // Signal that processing has started
	}
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		w.processing = false
		w.mu.Unlock()
	}()
	select {
	case <-time.After(500 * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Define a worker that counts the number of times Run is called
type CountingWorker struct {
	mu      sync.Mutex
	counter int
}

func (w *CountingWorker) Run(ctx context.Context, data int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(100 * time.Millisecond):
		w.mu.Lock()
		w.counter++
		w.mu.Unlock()
	}
	return nil
}

func (w *CountingWorker) GetCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.counter
}

// Define a worker that sleeps longer than max duration per attempt
type VerySlowWorker struct {
	mu      sync.Mutex
	counter int
}

func (w *VerySlowWorker) Run(ctx context.Context, data int) error {
	w.mu.Lock()
	w.counter++
	w.mu.Unlock()
	time.Sleep(300 * time.Millisecond)
	return errors.New("task failed")
}

// Define a worker that panics on first task, succeeds on second
type PanicThenSuccessWorker struct {
	mu         sync.Mutex
	panicCount int
}

func (w *PanicThenSuccessWorker) Run(ctx context.Context, data int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.panicCount == 0 {
		w.panicCount++
		panic("worker panic")
	}
	return nil
}

// Define a worker that records the data it processes
type RecordingWorker struct {
	mu    sync.Mutex
	tasks []int
}

func (w *RecordingWorker) Run(ctx context.Context, data int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(100 * time.Millisecond):
		w.mu.Lock()
		w.tasks = append(w.tasks, data)
		w.mu.Unlock()
	}
	return nil
}

// TestAddWorkerDynamically verifies that adding a worker during task processing
// results in the worker starting to process tasks immediately.
func TestAddWorkerDynamically(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create initial worker
	initialWorker := &RecordingWorker{}

	// Create pool with initial worker
	pool := New(ctx, []Worker[int]{initialWorker})

	// Dispatch tasks
	for i := 0; i < 10; i++ {
		err := pool.Submit(i)
		if err != nil {
			t.Fatalf("Failed to dispatch task %d: %v", i, err)
		}
	}

	// Wait a moment to let initialWorker start processing
	time.Sleep(100 * time.Millisecond)

	// Add new worker dynamically
	newWorker := &RecordingWorker{}
	newWorkerID := pool.AddWorker(newWorker)

	if newWorkerID <= 0 {
		t.Errorf("Expected new worker ID to be greater than 0, got %d", newWorkerID)
	}

	// Wait for tasks to be processed
	err := pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 10*time.Millisecond)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	if err := pool.Shutdown(); err != nil {
		if err != context.Canceled {
			t.Fatalf("Failed to close pool: %v", err)
		}
	}

	// Check newWorker processed tasks
	newWorker.mu.Lock()
	newWorkerTaskCount := len(newWorker.tasks)
	newWorker.mu.Unlock()

	if newWorkerTaskCount == 0 {
		t.Errorf("Expected new worker to process tasks")
	}
}

// TestRemoveWorkerDuringProcessing verifies that removing a worker during processing
// results in its tasks being reassigned and processed by other workers.
func TestRemoveWorkerDuringProcessing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	worker1 := &IncrementWorker{}
	worker2 := &IncrementWorker{}

	pool := New(ctx, []Worker[int]{worker1, worker2}, WithLogLevel[int](logs.LevelDebug))

	// Dispatch tasks
	for i := 0; i < 10; i++ {
		err := pool.Submit(1)
		if err != nil {
			t.Fatalf("Failed to dispatch task %d: %v", i, err)
		}
	}

	// Wait a moment to let workers start processing
	time.Sleep(100 * time.Millisecond)

	// Remove worker1
	err := pool.RemoveWorker(0)
	if err != nil {
		t.Fatalf("Failed to remove worker: %v", err)
	}

	// Wait for tasks to be processed
	err = pool.WaitWithCallback(ctx, func(q, p, d int) bool {
		return q > 0 || p > 0
	}, 10*time.Millisecond)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	pool.Shutdown()

	totalCounter := worker1.counter + worker2.counter
	if totalCounter != 10 {
		t.Errorf("Expected total counter to be 10, got %d", totalCounter)
	}

	if worker1.counter == 0 {
		t.Errorf("Expected worker1 to have processed tasks before removal")
	}

	if worker2.counter == 0 {
		t.Errorf("Expected worker2 to process tasks after removal")
	}
}

// TestInterruptWorkerWithRemoveWorkerOption verifies that interrupting a worker
// with the WithRemoveWorker option removes the worker and reassigns its tasks.
func TestInterruptWorkerWithRemoveWorkerOption(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	worker1 := &IncrementWorker{}
	worker2 := &IncrementWorker{}

	pool := New(ctx, []Worker[int]{worker1, worker2}, WithLogLevel[int](logs.LevelDebug))

	// Dispatch tasks
	for i := 0; i < 10; i++ {
		err := pool.Submit(1)
		if err != nil {
			t.Fatalf("Failed to dispatch task %d: %v", i, err)
		}
	}

	// Wait a moment
	time.Sleep(100 * time.Millisecond)

	// Interrupt worker1 with WithRemoveWorker
	err := pool.InterruptWorker(0, WithRemoveWorker())
	if err != nil {
		t.Fatalf("Failed to interrupt and remove worker: %v", err)
	}

	// Wait for tasks to be processed
	err = pool.WaitWithCallback(ctx, func(q, p, d int) bool {
		fmt.Println(q, p, d)
		return q > 0 || p > 0
	}, 10*time.Millisecond)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	pool.Shutdown()

	totalCounter := worker1.counter + worker2.counter
	if totalCounter != 10 {
		t.Errorf("Expected total counter to be 10, got %d", totalCounter)
	}

	if worker1.counter == 0 {
		t.Errorf("Expected worker1 to have processed tasks before removal")
	}

	if worker2.counter == 0 {
		t.Errorf("Expected worker2 to process tasks after removal")
	}
}

// TestTaskRetriesUpToMaxAttempts verifies that tasks retry up to the maximum number of attempts.
func TestTaskRetriesUpToMaxAttempts(t *testing.T) {
	ctx := context.Background()

	worker := &FailingWorker{}

	pool := New(ctx, []Worker[int]{worker},
		WithAttempts[int](3),
		WithRetryIf[int](func(err error) bool { return true }),
	)

	// Dispatch task
	err := pool.Submit(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	// Wait for pool to finish
	err = pool.WaitWithCallback(ctx, func(q, p, d int) bool {
		return q > 0 || p > 0
	}, 10*time.Millisecond)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	pool.Shutdown()

	if worker.counter != 3 {
		t.Errorf("Expected 3 attempts, got %d", worker.counter)
	}

	deadTasks := pool.DeadTasks()
	if len(deadTasks) != 1 {
		t.Errorf("Expected 1 dead task, got %d", len(deadTasks))
	}
}

// TestCustomDelayTypeFunction tests that a custom delay type function is used
// to calculate delays between retries.
func TestCustomDelayTypeFunction(t *testing.T) {
	ctx := context.Background()

	worker := &FlakyWorker{failuresLeft: 3}

	var delays []time.Duration
	pool := New(ctx, []Worker[int]{worker},
		WithAttempts[int](4),
		WithRetryIf[int](func(err error) bool { return true }),
		WithDelayType[int](func(n int, err error, c *Config[int]) time.Duration {
			delay := time.Duration(n) * 100 * time.Millisecond
			delays = append(delays, delay)
			return delay
		}),
	)

	// Dispatch task
	err := pool.Submit(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	// Wait for pool to finish
	err = pool.WaitWithCallback(ctx, func(q, p, d int) bool {
		return q > 0 || p > 0
	}, 50*time.Millisecond)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	pool.Shutdown()

	expectedDelays := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		300 * time.Millisecond,
	}

	if len(delays) != len(expectedDelays) {
		t.Fatalf("Expected %d delays, got %d", len(expectedDelays), len(delays))
	}

	for i, expectedDelay := range expectedDelays {
		if delays[i] != expectedDelay {
			t.Errorf("Expected delay %v, got %v", expectedDelay, delays[i])
		}
	}
}

// TestImmediateRetryOption verifies that tasks with WithImmediateRetry are retried immediately.
func TestImmediateRetryOption(t *testing.T) {
	ctx := context.Background()

	// Worker that fails once, then succeeds
	worker := &FlakyWorker{failuresLeft: 1}

	pool := New(ctx, []Worker[int]{worker},
		WithAttempts[int](2),
		WithRetryIf[int](func(err error) bool { return true }),
	)

	// Dispatch task with immediate retry
	err := pool.Submit(1, WithImmediateRetry[int]())
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	// Wait for pool to finish
	err = pool.WaitWithCallback(ctx, func(q, p, d int) bool {
		return q > 0 || p > 0
	}, 10*time.Millisecond)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	pool.Shutdown()

	if worker.failuresLeft != 0 {
		t.Errorf("Expected worker to have no failures left, got %d", worker.failuresLeft)
	}
}

// TestTaskRespectsTimeLimit verifies that tasks respect their total time limit.
func TestTaskRespectsTimeLimit(t *testing.T) {
	ctx := context.Background()

	worker := &VerySlowWorker{}

	pool := New(ctx, []Worker[int]{worker},
		WithAttempts[int](10),
		WithRetryIf[int](func(err error) bool { return true }),
	)

	// Dispatch task with time limit and max duration per attempt
	err := pool.Submit(1,
		WithTimeLimit[int](500*time.Millisecond),
		WithMaxContextDuration[int](200*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	// Wait for pool to finish
	err = pool.WaitWithCallback(ctx, func(q, p, d int) bool {
		return q > 0 || p > 0
	}, 50*time.Millisecond)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	pool.Shutdown()

	deadTasks := pool.DeadTasks()
	if len(deadTasks) != 1 {
		t.Errorf("Expected 1 dead task, got %d", len(deadTasks))
	}

	if worker.counter == 0 {
		t.Errorf("Expected worker to have attempted task")
	}
}

// TestTaskCancellationOnContextCancel verifies that tasks are canceled when context is canceled.
func TestTaskCancellationOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	worker := &SlowWorker{}

	pool := New(ctx, []Worker[int]{worker},
		WithAttempts[int](UnlimitedAttempts),
		WithRetryIf[int](func(err error) bool { return true }),
	)

	for i := 0; i < 10; i++ {
		// Dispatch task
		err := pool.Submit(i)
		if err != nil {
			t.Fatalf("Failed to dispatch task: %v", err)
		}
	}

	// Cancel context after delay
	time.AfterFunc(2*time.Second, cancel)

	// Wait for pool to finish
	err := pool.WaitWithCallback(ctx, func(q, p, d int) bool {
		return q > 0 || p > 0
	}, 10*time.Millisecond)

	if err != context.Canceled {
		t.Errorf("Expected context.Canceled error, got %v", err)
	}

	pool.Shutdown()

	// Check task is in dead tasks
	deadTasks := pool.DeadTasks()
	if len(deadTasks) == 0 {
		t.Errorf("Expected many dead tasks, got %d", len(deadTasks))
	}
}

// TestCallbacksAreInvoked verifies that OnTaskSuccess, OnTaskFailure, and OnNewDeadTask callbacks are invoked.
func TestCallbacksAreInvoked(t *testing.T) {
	ctx := context.Background()

	worker := &FlakyWorker{
		failuresLeft: 1,
	}

	var successCalled bool
	var failureCalled bool
	var newDeadTaskCalled bool

	var mu sync.Mutex

	pool := New(ctx, []Worker[int]{worker},
		WithAttempts[int](2),
		WithRetryIf[int](func(err error) bool { return true }),
		WithOnTaskSuccess[int](func(c WorkerController[int], id int, w Worker[int], t *TaskWrapper[int]) {
			mu.Lock()
			successCalled = true
			mu.Unlock()
		}),
		WithOnTaskFailure[int](func(c WorkerController[int], id int, w Worker[int], t *TaskWrapper[int], e error) DeadTaskAction {
			mu.Lock()
			failureCalled = true
			mu.Unlock()
			return DeadTaskActionRetry
		}),
		WithOnNewDeadTask[int](func(t *DeadTask[int]) {
			mu.Lock()
			newDeadTaskCalled = true
			mu.Unlock()
		}),
	)

	// Dispatch task
	err := pool.Submit(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	// Wait for pool to finish
	err = pool.WaitWithCallback(ctx, func(q, p, d int) bool {
		return q > 0 || p > 0
	}, 10*time.Millisecond)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	pool.Shutdown()

	mu.Lock()
	defer mu.Unlock()
	if !successCalled {
		t.Errorf("Expected OnTaskSuccess to be called")
	}

	if !failureCalled {
		t.Errorf("Expected OnTaskFailure to be called")
	}

	if newDeadTaskCalled {
		t.Errorf("Did not expect OnNewDeadTask to be called")
	}
}

// TestPanicRecoveryInWorker tests that a panic in the worker's Run method is recovered and handled.
func TestPanicRecoveryInWorker(t *testing.T) {
	ctx := context.Background()

	worker := &testPanicWorker{}

	var panicHandled bool

	pool := New(ctx, []Worker[int]{worker},
		WithPanicHandler[int](func(task int, v interface{}, stackTrace string) {
			panicHandled = true
		}),
	)

	// Dispatch task
	err := pool.Submit(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	// Wait for pool to finish
	err = pool.WaitWithCallback(ctx, func(q, p, d int) bool {
		return q > 0 || p > 0
	}, 10*time.Millisecond)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	pool.Shutdown()

	if !panicHandled {
		t.Errorf("Expected panic handler to be called")
	}
}

// TestPoolContinuesAfterPanic verifies that the pool continues processing after a panic in a worker.
func TestPoolContinuesAfterPanic(t *testing.T) {
	ctx := context.Background()

	worker := &PanicThenSuccessWorker{}

	var panicHandled bool

	pool := New(ctx, []Worker[int]{worker},
		WithPanicHandler[int](func(task int, v interface{}, stackTrace string) {
			panicHandled = true
		}),
	)

	// Dispatch tasks
	err := pool.Submit(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task 1: %v", err)
	}

	// Wait for a moment
	time.Sleep(100 * time.Millisecond)

	err = pool.Submit(2)
	if err != nil {
		t.Fatalf("Failed to dispatch task 2: %v", err)
	}

	// Wait for pool to finish
	err = pool.WaitWithCallback(ctx, func(q, p, d int) bool {
		return q > 0 || p > 0
	}, 50*time.Millisecond)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	pool.Shutdown()

	if !panicHandled {
		t.Errorf("Expected panic handler to be called")
	}

	deadTasks := pool.DeadTasks()
	if len(deadTasks) != 0 {
		t.Errorf("Expected 0 dead tasks, got %d", len(deadTasks))
	}
}

// TestOnTaskFailureDeadTaskAction tests that the OnTaskFailure callback returns the correct DeadTaskAction.
func TestOnTaskFailureDeadTaskAction(t *testing.T) {
	ctx := context.Background()

	worker := &FailingWorker{}

	actionSequence := []DeadTaskAction{
		DeadTaskActionRetry,
		DeadTaskActionForceRetry,
		DeadTaskActionAddToDeadTasks,
	}
	actionIndex := 0

	pool := New(ctx, []Worker[int]{worker},
		WithAttempts[int](5),
		WithRetryIf[int](func(err error) bool { return true }),
		WithOnTaskFailure[int](func(c WorkerController[int], id int, w Worker[int], t *TaskWrapper[int], e error) DeadTaskAction {
			action := actionSequence[actionIndex]
			actionIndex++
			return action
		}),
	)

	// Dispatch task
	err := pool.Submit(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	// Wait for pool to finish
	err = pool.WaitWithCallback(ctx, func(q, p, d int) bool {
		return q > 0 || p > 0
	}, 50*time.Millisecond)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	pool.Shutdown()

	deadTasks := pool.DeadTasks()
	if len(deadTasks) != 1 {
		t.Errorf("Expected 1 dead task, got %d", len(deadTasks))
	}
}

// TestRangeTasksIteratesAllTasks tests that RangeTasks correctly iterates over all tasks with accurate statuses.
func TestRangeTasksIteratesAllTasks(t *testing.T) {
	ctx := context.Background()

	worker := &CountingWorker{}

	pool := New(ctx, []Worker[int]{worker})

	// Dispatch multiple tasks
	for i := 0; i < 5; i++ {
		err := pool.Submit(i)
		if err != nil {
			t.Fatalf("Failed to dispatch task %d: %v", i, err)
		}
	}

	// Allow some tasks to be processed
	time.Sleep(100 * time.Millisecond)

	var queuedTasks, processingTasks int

	pool.RangeTasks(func(data TaskWrapper[int], workerID int, status TaskStatus) bool {
		if status == TaskStatusQueued {
			queuedTasks++
		} else if status == TaskStatusProcessing {
			processingTasks++
		}
		return true
	})

	if queuedTasks == 0 {
		t.Errorf("Expected some tasks to be queued")
	}

	if processingTasks == 0 {
		t.Errorf("Expected some tasks to be processing")
	}

	pool.Shutdown()
}

// TestCombinedFeaturesWithDynamicWorkers tests a complex scenario involving dynamic workers and task retries.
func TestCombinedFeaturesWithDynamicWorkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	worker1 := &IncrementWorker{}
	worker2 := &IncrementWorker{}

	pool := New(ctx, []Worker[int]{worker1})

	// Dispatch tasks
	for i := 0; i < 50; i++ {
		err := pool.Submit(i)
		if err != nil {
			t.Fatalf("Failed to dispatch task %d: %v", i, err)
		}
	}

	// Add a new worker dynamically
	newWorkerID := pool.AddWorker(worker2)
	if newWorkerID <= 0 {
		t.Errorf("Expected new worker ID to be greater than 0, got %d", newWorkerID)
	}

	// Interrupt worker1 and reassign its task
	err := pool.InterruptWorker(0, WithReassignTask())
	if err != nil {
		t.Fatalf("Failed to interrupt worker: %v", err)
	}

	// Wait for tasks to be processed
	err = pool.WaitWithCallback(ctx, func(q, p, d int) bool {
		return q > 0 || p > 0
	}, 50*time.Millisecond)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	pool.Shutdown()

	// Check that tasks were processed and retried
	if worker1.counter == 0 && worker2.counter == 0 {
		t.Errorf("Expected tasks to be processed by workers")
	}
}
