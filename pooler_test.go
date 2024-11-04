package retrypool

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
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

	fmt.Println("called", data)
	w.mu.Lock()
	w.counter += data
	w.mu.Unlock()
	return nil
}

// Define an SlowIncrementWorker that increments a counter
type SlowIncrementWorker struct {
	mu      sync.Mutex
	counter int
}

func (w *SlowIncrementWorker) Run(ctx context.Context, data int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(100 * time.Millisecond):
		fmt.Println("called", data)
		w.mu.Lock()
		w.counter += data
		w.mu.Unlock()
	}
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

func (w *SlowWorker) IsProcessing() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.processing
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
		fmt.Println("called", data)
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

	var worker1Tasks, worker2Tasks []int
	var taskMu sync.Mutex

	// Create pool with initial worker
	pool := New(ctx, []Worker[int]{initialWorker},
		WithOnTaskSuccess[int](func(_ WorkerController[int], workerID int, _ Worker[int], task *TaskWrapper[int]) {
			taskMu.Lock()
			if workerID == 0 {
				worker1Tasks = append(worker1Tasks, task.Data())
			} else {
				worker2Tasks = append(worker2Tasks, task.Data())
			}
			taskMu.Unlock()
		}),
	)

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

	t.Logf("Worker1 tasks: %v", worker1Tasks)
	t.Logf("Worker2 tasks: %v", worker2Tasks)

	if len(worker2Tasks) == 0 {
		t.Error("Second worker didn't process any tasks")
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
	if len(deadTasks) != 1 {
		t.Errorf("Expected 1 dead tasks, got %d", len(deadTasks))
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

// Define at package level
type TestSlowIncrementWorker struct {
	*IncrementWorker
}

func NewTestSlowIncrementWorker() *TestSlowIncrementWorker {
	return &TestSlowIncrementWorker{
		IncrementWorker: &IncrementWorker{},
	}
}

func (w *TestSlowIncrementWorker) Run(ctx context.Context, data int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(50 * time.Millisecond):
	}
	return w.IncrementWorker.Run(ctx, data)
}

// TestCombinedFeaturesWithDynamicWorkers tests a complex scenario involving dynamic workers and task retries.
func TestCombinedFeaturesWithDynamicWorkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	worker1 := NewTestSlowIncrementWorker()
	worker2 := NewTestSlowIncrementWorker()

	// Track task completion order and worker assignment
	type taskResult struct {
		workerID  int
		timestamp time.Time
	}
	var completedTasks sync.Map
	var taskCount int32

	pool := New(ctx, []Worker[int]{worker1},
		WithLogLevel[int](logs.LevelDebug),
		WithAttempts[int](1), // No retries to keep things simple
		WithOnTaskSuccess[int](func(_ WorkerController[int], workerID int, _ Worker[int], task *TaskWrapper[int]) {
			completedTasks.Store(task.Data(), taskResult{
				workerID:  workerID,
				timestamp: time.Now(),
			})
			atomic.AddInt32(&taskCount, 1)
		}),
	)

	// Submit first batch
	for i := 0; i < 5; i++ {
		if err := pool.Submit(i); err != nil {
			t.Fatalf("Failed to dispatch task %d: %v", i, err)
		}
	}

	// Let some first batch tasks complete
	time.Sleep(150 * time.Millisecond)

	// Add second worker
	worker2ID := pool.AddWorker(worker2)
	if worker2ID != 1 {
		t.Errorf("Expected worker2 ID to be 1, got %d", worker2ID)
	}

	// Submit second batch
	for i := 5; i < 10; i++ {
		if err := pool.Submit(i); err != nil {
			t.Fatalf("Failed to dispatch task %d: %v", i, err)
		}
	}

	// Wait for at least one second batch task to start
	time.Sleep(100 * time.Millisecond)

	// Interrupt first worker
	err := pool.InterruptWorker(0, WithReassignTask())
	if err != nil {
		t.Fatalf("Failed to interrupt worker: %v", err)
	}

	// Wait for completion with timeout
	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()

	completed := make(map[int]taskResult)
	for len(completed) < 10 {
		select {
		case <-timeout.C:
			t.Fatalf("Timeout waiting for tasks. Completed %d/10: %v", len(completed), completed)
		case <-time.After(50 * time.Millisecond):
			completedTasks.Range(func(k, v interface{}) bool {
				completed[k.(int)] = v.(taskResult)
				return true
			})
		}
	}

	// Let any final operations complete
	time.Sleep(100 * time.Millisecond)

	pool.Shutdown()

	// Analyze results
	byWorker := make(map[int][]int)
	for taskID, result := range completed {
		byWorker[result.workerID] = append(byWorker[result.workerID], taskID)
	}

	// Sort tasks for consistent output
	for _, tasks := range byWorker {
		sort.Ints(tasks)
	}

	t.Logf("Tasks by worker0: %v", byWorker[0])
	t.Logf("Tasks by worker1: %v", byWorker[1])

	// Verify distribution
	if len(byWorker[0]) == 0 || len(byWorker[1]) == 0 {
		t.Error("Expected both workers to process tasks")
	}

	if len(byWorker[1]) < 3 {
		t.Errorf("Expected worker1 to process at least 3 tasks, got %d", len(byWorker[1]))
	}

	// Check for dead tasks
	deadTasks := pool.DeadTasks()
	if len(deadTasks) > 0 {
		t.Errorf("Got unexpected dead tasks: %+v", deadTasks)
	}
}

// TestTaskQueueDistribution verifies that tasks are distributed evenly across workers
func TestTaskQueueDistribution(t *testing.T) {
	ctx := context.Background()
	numWorkers := 3
	numTasks := 30

	// Create workers that track how many tasks they process
	workers := make([]Worker[int], numWorkers)
	counters := make([]*CountingWorker, numWorkers)

	for i := 0; i < numWorkers; i++ {
		worker := &CountingWorker{}
		workers[i] = worker
		counters[i] = worker
	}

	pool := New(ctx, workers)

	// Submit tasks
	for i := 0; i < numTasks; i++ {
		err := pool.Submit(i)
		if err != nil {
			t.Fatalf("Failed to submit task %d: %v", i, err)
		}
	}

	// Wait for tasks to complete
	err := pool.WaitWithCallback(ctx, func(q, p, d int) bool {
		return q > 0 || p > 0
	}, 10*time.Millisecond)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	pool.Shutdown()

	// Calculate distribution metrics
	total := 0
	minCount := numTasks
	maxCount := 0

	for _, counter := range counters {
		count := counter.GetCount()
		total += count
		if count < minCount {
			minCount = count
		}
		if count > maxCount {
			maxCount = count
		}
	}

	// Check that all tasks were processed
	if total != numTasks {
		t.Errorf("Expected %d total tasks processed, got %d", numTasks, total)
	}

	// Check distribution (allow for some variance)
	expectedPerWorker := numTasks / numWorkers
	maxVariance := expectedPerWorker / 2

	if maxCount-minCount > maxVariance {
		t.Errorf("Task distribution too uneven. Min: %d, Max: %d, Expected: %d ±%d",
			minCount, maxCount, expectedPerWorker, maxVariance)
	}
}

// TestConcurrentSubmission tests that concurrent task submission works correctly
func TestConcurrentSubmission(t *testing.T) {
	ctx := context.Background()
	worker := &CountingWorker{}
	pool := New(ctx, []Worker[int]{worker})

	numTasks := 100
	var wg sync.WaitGroup
	wg.Add(numTasks)

	// Submit tasks concurrently
	for i := 0; i < numTasks; i++ {
		go func(taskID int) {
			defer wg.Done()
			err := pool.Submit(taskID)
			if err != nil {
				t.Errorf("Failed to submit task %d: %v", taskID, err)
			}
		}(i)
	}

	wg.Wait()

	// Wait for processing to complete
	err := pool.WaitWithCallback(ctx, func(q, p, d int) bool {
		return q > 0 || p > 0
	}, 10*time.Millisecond)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	pool.Shutdown()

	if worker.GetCount() != numTasks {
		t.Errorf("Expected %d tasks processed, got %d", numTasks, worker.GetCount())
	}
}

// TestMaxDurationPerAttempt verifies that tasks respect their max duration per attempt
func TestMaxDurationPerAttempt(t *testing.T) {
	ctx := context.Background()
	maxDuration := 100 * time.Millisecond

	worker := &SlowWorker{}
	pool := New(ctx, []Worker[int]{worker},
		WithLogLevel[int](logs.LevelDebug),
		WithAttempts[int](1), // Only try once
		WithRetryIf[int](func(err error) bool {
			// Don't retry on timeout
			return err != nil && !errors.Is(err, context.DeadlineExceeded)
		}),
	)

	// Submit task with max duration
	err := pool.Submit(1, WithMaxContextDuration[int](maxDuration))
	if err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}

	// Wait sufficiently long for the task to timeout
	time.Sleep(maxDuration + 150*time.Millisecond)

	// Check if the task ended up in dead tasks due to timeout
	deadTasks := pool.DeadTasks()
	if len(deadTasks) == 0 {
		t.Error("Expected task to be in dead tasks due to timeout")
	}

	if len(deadTasks) > 0 {
		if !errors.Is(deadTasks[0].Errors[0], context.DeadlineExceeded) {
			t.Errorf("Expected deadline exceeded error, got: %v", deadTasks[0].Errors[0])
		}
	}

	fmt.Println(deadTasks)

	pool.Shutdown()
}

// TestMetricsTracking verifies that pool metrics are tracked correctly
func TestMetricsTracking(t *testing.T) {
	ctx := context.Background()
	worker := &FlakyWorker{failuresLeft: 2}

	pool := New(ctx, []Worker[int]{worker},
		WithAttempts[int](3),
		WithRetryIf[int](func(err error) bool { return true }),
	)

	// Submit a task that will fail twice then succeed
	err := pool.Submit(1)
	if err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}

	// Wait for processing to complete
	err = pool.WaitWithCallback(ctx, func(q, p, d int) bool {
		return q > 0 || p > 0
	}, 10*time.Millisecond)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	pool.Shutdown()

	metrics := pool.Metrics()

	expectedMetrics := Metrics{
		TasksSubmitted: 1,
		TasksProcessed: 3, // Initial attempt + 2 retries
		TasksSucceeded: 1,
		TasksFailed:    2,
		DeadTasks:      0,
	}

	if metrics != expectedMetrics {
		t.Errorf("Metrics mismatch.\nExpected: %+v\nGot: %+v", expectedMetrics, metrics)
	}
}

// TestTaskTimingMetrics verifies that task timing information is tracked correctly
func TestTaskTimingMetrics(t *testing.T) {
	ctx := context.Background()
	worker := &SlowIncrementWorker{}

	pool := New(ctx, []Worker[int]{worker})

	// Submit a task
	err := pool.Submit(1)
	if err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}

	var taskWrapper *TaskWrapper[int]

	// Wait briefly to let the task get queued and start processing
	time.Sleep(50 * time.Millisecond)

	// Get the task and check its timing info
	found := false
	pool.RangeTasks(func(data TaskWrapper[int], workerID int, status TaskStatus) bool {
		taskWrapper = &data
		found = true
		return false
	})

	if !found {
		t.Fatal("Task not found in pool")
	}

	if len(taskWrapper.QueuedAt()) == 0 {
		t.Error("QueuedAt timestamps not recorded")
	}

	if len(taskWrapper.ProcessedAt()) == 0 {
		t.Error("ProcessedAt timestamps not recorded")
	}

	// Verify timestamps are in chronological order
	queuedTime := taskWrapper.QueuedAt()[0]
	if len(taskWrapper.ProcessedAt()) > 0 {
		processedTime := taskWrapper.ProcessedAt()[0]
		if !queuedTime.Before(processedTime) {
			t.Error("Task processed time is before queued time")
		}
	}

	pool.Shutdown()
}

func TestWorkerContextValues(t *testing.T) {
	type contextKey string
	const testKey = contextKey("test-key")
	const testValue = "test-value"

	// Create context with value
	parentCtx := context.WithValue(context.Background(), testKey, testValue)

	var valueReceived string
	var valueMu sync.Mutex

	// Create a worker that checks context value
	worker := &SimpleWorker{
		RunFunc: func(ctx context.Context, data int) error {
			// Get value from context
			if val, ok := ctx.Value(testKey).(string); ok {
				valueMu.Lock()
				valueReceived = val
				valueMu.Unlock()
			} else {
				t.Error("Expected context value not found or wrong type")
			}
			return nil
		},
	}

	// Create pool with context
	pool := New(parentCtx, []Worker[int]{worker})

	// Submit a task
	err := pool.Submit(1)
	if err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}

	// Wait for task completion
	err = pool.WaitWithCallback(parentCtx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	pool.Shutdown()

	// Verify context value was received
	valueMu.Lock()
	if valueReceived != testValue {
		t.Errorf("Expected context value %q, got %q", testValue, valueReceived)
	}
	valueMu.Unlock()
}

// Helper worker type for testing
type SimpleWorker struct {
	RunFunc func(context.Context, int) error
}

func (w *SimpleWorker) Run(ctx context.Context, data int) error {
	if w.RunFunc != nil {
		return w.RunFunc(ctx, data)
	}
	return nil
}

func TestWorkerContextFeatures(t *testing.T) {
	t.Run("Cancellation propagation", func(t *testing.T) {
		parentCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		var cancelReceived bool
		var cancelMu sync.Mutex

		worker := &SimpleWorker{
			RunFunc: func(ctx context.Context, data int) error {
				// Signal that we're running
				defer wg.Done()

				select {
				case <-ctx.Done():
					cancelMu.Lock()
					cancelReceived = true
					cancelMu.Unlock()
					return ctx.Err()
				case <-time.After(5 * time.Second): // Timeout just in case
					t.Error("Timeout waiting for cancellation")
					return nil
				}
			},
		}

		pool := New(parentCtx, []Worker[int]{worker})

		// Submit task and wait for worker to start
		err := pool.Submit(1)
		if err != nil {
			t.Fatalf("Failed to submit task: %v", err)
		}

		// Give time for worker to start
		time.Sleep(100 * time.Millisecond)

		// Cancel parent context
		cancel()

		// Wait for worker to complete
		wg.Wait()

		// Verify worker received cancellation
		cancelMu.Lock()
		if !cancelReceived {
			t.Error("Worker did not receive context cancellation")
		}
		cancelMu.Unlock()

		pool.Shutdown()
	})

	t.Run("Deadline propagation", func(t *testing.T) {
		deadline := time.Now().Add(100 * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()

		var deadlineReceived time.Time
		var deadlineMu sync.Mutex

		worker := &SimpleWorker{
			RunFunc: func(ctx context.Context, data int) error {
				if d, ok := ctx.Deadline(); ok {
					deadlineMu.Lock()
					deadlineReceived = d
					deadlineMu.Unlock()
				}
				return nil
			},
		}

		pool := New(ctx, []Worker[int]{worker})
		pool.Submit(1)

		pool.WaitWithCallback(ctx, func(q, p, d int) bool {
			return q > 0 || p > 0
		}, 10*time.Millisecond)

		deadlineMu.Lock()
		if !deadlineReceived.Equal(deadline) {
			t.Errorf("Expected deadline %v, got %v", deadline, deadlineReceived)
		}
		deadlineMu.Unlock()
	})

	t.Run("Multiple context values", func(t *testing.T) {
		type key1 string
		type key2 int

		ctx := context.Background()
		ctx = context.WithValue(ctx, key1("string"), "value1")
		ctx = context.WithValue(ctx, key2(42), "value2")

		var values []string
		var valuesMu sync.Mutex

		worker := &SimpleWorker{
			RunFunc: func(ctx context.Context, data int) error {
				valuesMu.Lock()
				values = append(values,
					ctx.Value(key1("string")).(string),
					ctx.Value(key2(42)).(string))
				valuesMu.Unlock()
				return nil
			},
		}

		pool := New(ctx, []Worker[int]{worker})
		pool.Submit(1)
		pool.WaitWithCallback(ctx, func(q, p, d int) bool {
			return q > 0 || p > 0
		}, 10*time.Millisecond)

		valuesMu.Lock()
		if len(values) != 2 || values[0] != "value1" || values[1] != "value2" {
			t.Errorf("Expected values [value1 value2], got %v", values)
		}
		valuesMu.Unlock()
	})
}
