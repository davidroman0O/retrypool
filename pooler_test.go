package retrypool

import (
	"context"
	"errors"
	"log"
	"sync"
	"testing"
	"time"
)

// Define a simple worker that increments a counter
type IncrementWorker struct {
	mu      sync.Mutex
	counter int
}

func (w *IncrementWorker) Run(ctx context.Context, data int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.counter += data
	return nil
}

// Define a worker that fails a certain number of times before succeeding
type FlakyWorker struct {
	failuresLeft int
	mu           sync.Mutex
	count        int
}

func (w *FlakyWorker) Inc() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.count++
	return w.count
}

func (w *FlakyWorker) Run(ctx context.Context, data int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	time.Sleep(100 * time.Millisecond) // Simulate processing time
	if w.failuresLeft > 0 {
		w.failuresLeft--
		return errors.New("temporary error")
	}
	return nil
}

// Define a worker that always returns an unrecoverable error
type UnrecoverableWorker struct{}

func (w *UnrecoverableWorker) Run(ctx context.Context, data int) error {
	return Unrecoverable(errors.New("unrecoverable error"))
}

// Test basic task dispatch and processing
func TestBasicDispatch(t *testing.T) {
	ctx := context.Background()
	worker := &IncrementWorker{}
	pool := New(ctx, []Worker[int]{worker})

	err := pool.Dispatch(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	// Wait for the task to be processed
	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 10*time.Millisecond)

	pool.Close()
	if worker.counter != 1 {
		t.Errorf("Expected counter to be 1, got %d", worker.counter)
	}
}

// Test task retries with a fixed delay
func TestRetriesWithFixedDelay(t *testing.T) {
	ctx := context.Background()
	worker := &FlakyWorker{failuresLeft: 2}
	pool := New(ctx, []Worker[int]{worker},
		WithAttempts[int](2),
		WithDelay[int](50*time.Millisecond),
		WithDelayType[int](FixedDelay[int]),
	)

	err := pool.Dispatch(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	// Wait for the task to be processed
	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 10*time.Millisecond)

	pool.Close()
	if worker.failuresLeft != 0 {
		t.Errorf("Expected failuresLeft to be 0, got %d", worker.failuresLeft)
	}
}

// Test unlimited retries with backoff delay
func TestUnlimitedRetriesWithBackoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	worker := &FlakyWorker{failuresLeft: 5}
	pool := New(ctx, []Worker[int]{worker},
		WithAttempts[int](UnlimitedAttempts),
		WithDelay[int](100*time.Millisecond),
		WithDelayType[int](BackOffDelay[int]),
	)

	err := pool.Dispatch(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 100*time.Millisecond)

	if worker.failuresLeft != 0 {
		t.Errorf("Expected failuresLeft to be 0, got %d", worker.failuresLeft)
	}
}

// Test task time limit
func TestTaskTimeLimit(t *testing.T) {
	ctx := context.Background()
	worker := &FlakyWorker{failuresLeft: 5}
	pool := New(ctx, []Worker[int]{worker},
		WithAttempts[int](UnlimitedAttempts),
		WithDelay[int](100*time.Millisecond),
		WithDelayType[int](FixedDelay[int]),
	)

	err := pool.Dispatch(1, WithTimeLimit[int](500*time.Millisecond))
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 10*time.Millisecond)

	pool.Close()

	deadTasks := pool.DeadTasks()
	if len(deadTasks) != 1 {
		t.Errorf("Expected 1 dead task, got %d", len(deadTasks))
	}

	if worker.failuresLeft > 3 {
		t.Errorf("Expected failuresLeft to be less than or equal to 3, got %d", worker.failuresLeft)
	}
}

// Test custom RetryIf function
func TestCustomRetryIf(t *testing.T) {
	ctx := context.Background()
	worker := &FlakyWorker{failuresLeft: 2}
	pool := New(ctx, []Worker[int]{worker},
		WithAttempts[int](2),
		WithRetryIf[int](func(err error) bool {
			return err.Error() == "temporary error"
		}),
	)

	err := pool.Dispatch(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	pool.Close()

	if worker.failuresLeft != 0 {
		t.Errorf("Expected failuresLeft to be 0, got %d", worker.failuresLeft)
	}
}

// Test OnRetry callback
func TestOnRetryCallback(t *testing.T) {
	ctx := context.Background()
	worker := &FlakyWorker{failuresLeft: 2}
	retryAttempts := 0
	pool := New(ctx, []Worker[int]{worker},
		WithAttempts[int](2),
		WithOnRetry[int](func(attempt int, err error, task *TaskWrapper[int]) {
			retryAttempts++
			log.Printf("Retry attempt %d for task %d due to error: %v", attempt, task.data, err)
		}),
	)

	err := pool.Dispatch(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	pool.Close()

	if retryAttempts != 2 {
		t.Errorf("Expected 2 retry attempts, got %d", retryAttempts)
	}

	if worker.failuresLeft != 0 {
		t.Errorf("Expected failuresLeft to be 0, got %d", worker.failuresLeft)
	}
}

// Test handling unrecoverable errors
func TestUnrecoverableError(t *testing.T) {
	ctx := context.Background()
	worker := &UnrecoverableWorker{}
	pool := New(ctx, []Worker[int]{worker},
		WithAttempts[int](2),
	)

	err := pool.Dispatch(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 10*time.Millisecond)

	pool.Close()

	deadTasks := pool.DeadTasks()
	if len(deadTasks) != 1 {
		t.Errorf("Expected 1 dead task, got %d", len(deadTasks))
	}

	if len(deadTasks[0].Errors) != 1 {
		t.Errorf("Expected 1 error, got %d", len(deadTasks[0].Errors))
	}

	if deadTasks[0].Errors[0].Error() != "unrecoverable error" {
		t.Errorf("Expected unrecoverable error, got %v", deadTasks[0].Errors[0])
	}
}

// Test handling of multiple workers
func TestMultipleWorkers(t *testing.T) {
	ctx := context.Background()
	worker1 := &IncrementWorker{}
	worker2 := &IncrementWorker{}
	pool := New(ctx, []Worker[int]{worker1, worker2})

	// Dispatch multiple tasks
	for i := 0; i < 10; i++ {
		err := pool.Dispatch(1)
		if err != nil {
			t.Fatalf("Failed to dispatch task: %v", err)
		}
	}

	pool.Close()

	totalCounter := worker1.counter + worker2.counter
	if totalCounter != 10 {
		t.Errorf("Expected total counter to be 10, got %d", totalCounter)
	}
}

// Test processing count and queue size
func TestProcessingCountAndQueueSize(t *testing.T) {
	ctx := context.Background()
	worker := &IncrementWorker{}
	pool := New(ctx, []Worker[int]{worker})

	// Dispatch multiple tasks
	for i := 0; i < 5; i++ {
		err := pool.Dispatch(1)
		if err != nil {
			t.Fatalf("Failed to dispatch task: %v", err)
		}
	}

	queueSize := pool.QueueSize()
	if queueSize != 5 {
		t.Errorf("Expected queue size to be 5, got %d", queueSize)
	}

	processingCount := pool.ProcessingCount()
	if processingCount != 0 {
		t.Errorf("Expected processing count to be 0, got %d", processingCount)
	}

	pool.Close()

	if worker.counter != 5 {
		t.Errorf("Expected counter to be 5, got %d", worker.counter)
	}
}

// Test ForceClose
func TestForceClose(t *testing.T) {
	ctx := context.Background()
	worker := &IncrementWorker{}
	pool := New(ctx, []Worker[int]{worker})

	// Dispatch multiple tasks
	for i := 0; i < 10; i++ {
		err := pool.Dispatch(1)
		if err != nil {
			t.Fatalf("Failed to dispatch task: %v", err)
		}
	}

	pool.ForceClose()

	queueSize := pool.QueueSize()
	if queueSize != 0 {
		t.Errorf("Expected queue size to be 0 after ForceClose, got %d", queueSize)
	}

	processingCount := pool.ProcessingCount()
	if processingCount != 0 {
		t.Errorf("Expected processing count to be 0 after ForceClose, got %d", processingCount)
	}
}

// Test WaitWithCallback
func TestWaitWithCallback(t *testing.T) {
	ctx := context.Background()
	worker := &IncrementWorker{}
	pool := New(ctx, []Worker[int]{worker})

	// Dispatch multiple tasks
	for i := 0; i < 5; i++ {
		err := pool.Dispatch(1)
		if err != nil {
			t.Fatalf("Failed to dispatch task: %v", err)
		}
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := pool.WaitWithCallback(waitCtx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	if worker.counter != 5 {
		t.Errorf("Expected counter to be 5, got %d", worker.counter)
	}
}

// TestMaxDelay ensures that the delay doesn't exceed the maximum specified delay
func TestMaxDelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	worker := &FlakyWorker{failuresLeft: 10}

	pool := New(ctx, []Worker[int]{worker},
		WithAttempts[int](5), // Limit attempts to ensure the task fails
		WithDelay[int](100*time.Millisecond),
		WithMaxDelay[int](300*time.Millisecond),
		WithDelayType[int](BackOffDelay[int]),
	)

	err := pool.Dispatch(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	err = pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 10*time.Millisecond)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	pool.Close()

	deadTasks := pool.DeadTasks()
	if len(deadTasks) != 1 {
		t.Fatalf("Expected 1 dead task, got %d", len(deadTasks))
	}

	deadTask := deadTasks[0]
	if deadTask.TotalDuration > 5*(300*time.Millisecond+100*time.Millisecond) {
		t.Errorf("Total duration exceeded expected maximum: %v", deadTask.TotalDuration)
	}
}

// Test MaxJitter with RandomDelay
func TestMaxJitterWithRandomDelay(t *testing.T) {
	ctx := context.Background()
	worker := &FlakyWorker{failuresLeft: 1}
	pool := New(ctx, []Worker[int]{worker},
		WithAttempts[int](2),
		WithMaxJitter[int](100*time.Millisecond),
		WithDelayType[int](RandomDelay[int]),
	)

	err := pool.Dispatch(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	startTime := time.Now()
	pool.Close()
	elapsedTime := time.Since(startTime)

	// Expected maximum time is processing time + max jitter + processing time
	expectedMaxTime := 100*time.Millisecond + // Initial processing time
		100*time.Millisecond + // Max jitter delay
		100*time.Millisecond // Second processing time

	if elapsedTime > expectedMaxTime+50*time.Millisecond { // Adding buffer for overhead
		t.Errorf("Expected total retry time to be less than %v, got %v", expectedMaxTime+50*time.Millisecond, elapsedTime)
	}
}

// TestContextCancellation ensures that the pool stops processing when the context is canceled
func TestContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	worker := &FlakyWorker{failuresLeft: 10}

	pool := New(ctx, []Worker[int]{worker},
		WithAttempts[int](UnlimitedAttempts),
		WithDelay[int](100*time.Millisecond),
	)

	err := pool.Dispatch(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	// Cancel the context after a short delay
	time.AfterFunc(300*time.Millisecond, cancel)

	err = pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 10*time.Millisecond)
	if err != context.Canceled {
		t.Fatalf("Expected context.Canceled error, got: %v", err)
	}

	pool.Close()

	if worker.failuresLeft >= 10 {
		t.Errorf("Expected some attempts to be made before cancellation")
	}
}

// TestTimeLimitWithUnlimitedRetries tests a task with time limit and unlimited retries
func TestTimeLimitWithUnlimitedRetries(t *testing.T) {
	ctx := context.Background()
	worker := &FlakyWorker{failuresLeft: 10}
	pool := New(ctx, []Worker[int]{worker},
		WithAttempts[int](UnlimitedAttempts),
		WithDelay[int](50*time.Millisecond),
		WithDelayType[int](FixedDelay[int]),
	)

	err := pool.Dispatch(1, WithTimeLimit[int](300*time.Millisecond))
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	pool.Close()

	// The exact number of failures may vary due to timing, so we check for a range
	if worker.failuresLeft > 7 || worker.failuresLeft < 5 {
		t.Errorf("Expected failuresLeft to be between 5 and 7 due to time limit, got %d", worker.failuresLeft)
	}

	deadTasks := pool.DeadTasks()
	if len(deadTasks) != 1 {
		t.Errorf("Expected 1 dead task, got %d", len(deadTasks))
	}
}

// Test multiple tasks processed by multiple workers
func TestMultipleTasksMultipleWorkers(t *testing.T) {
	ctx := context.Background()
	worker1 := &IncrementWorker{}
	worker2 := &IncrementWorker{}
	pool := New(ctx, []Worker[int]{worker1, worker2})

	// Dispatch multiple tasks
	for i := 0; i < 20; i++ {
		err := pool.Dispatch(1)
		if err != nil {
			t.Fatalf("Failed to dispatch task: %v", err)
		}
	}

	pool.Close()

	totalCounter := worker1.counter + worker2.counter
	if totalCounter != 20 {
		t.Errorf("Expected total counter to be 20, got %d", totalCounter)
	}
}

// TestMaxAttempts tests that the pool stops retrying after max attempts
func TestMaxAttempts(t *testing.T) {
	ctx := context.Background()
	worker := &FlakyWorker{failuresLeft: 10}
	maxAttempts := 3
	pool := New(ctx, []Worker[int]{worker},
		WithAttempts[int](maxAttempts),
		WithDelay[int](50*time.Millisecond),
		WithDelayType[int](FixedDelay[int]),
	)

	err := pool.Dispatch(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	pool.Close()

	expectedFailuresLeft := 10 - maxAttempts
	if worker.failuresLeft != expectedFailuresLeft {
		t.Errorf("Expected failuresLeft to be %d after max attempts, got %d", expectedFailuresLeft, worker.failuresLeft)
	}

	deadTasks := pool.DeadTasks()
	if len(deadTasks) != 1 {
		t.Errorf("Expected 1 dead task, got %d", len(deadTasks))
	}
}

// Test that unrecoverable errors are not retried even if RetryIf allows it
func TestUnrecoverableErrorWithCustomRetryIf(t *testing.T) {
	ctx := context.Background()
	worker := &UnrecoverableWorker{}
	pool := New(ctx, []Worker[int]{worker},
		WithAttempts[int](2),
		WithRetryIf[int](func(err error) bool {
			return true // Retry on all errors
		}),
	)

	err := pool.Dispatch(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 10*time.Millisecond)

	pool.Close()

	deadTasks := pool.DeadTasks()
	if len(deadTasks) != 1 {
		t.Errorf("Expected 1 dead task, got %d", len(deadTasks))
	}

	if len(deadTasks[0].Errors) != 1 {
		t.Errorf("Expected 1 error, got %d", len(deadTasks[0].Errors))
	}

	if deadTasks[0].Errors[0].Error() != "unrecoverable error" {
		t.Errorf("Expected unrecoverable error, got %v", deadTasks[0].Errors[0])
	}
}

// CustomTimer implements the Timer interface for testing purposes
type CustomTimer struct {
	durations []time.Duration
	mu        sync.Mutex
	called    bool // Add this field to track if After was called
}

func (t *CustomTimer) After(d time.Duration) <-chan time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.durations = append(t.durations, d)
	t.called = true // Set this to true when After is called
	return time.After(d)
}

// TestCustomTimer tests the custom timer implementation
func TestCustomTimer(t *testing.T) {
	ctx := context.Background()
	worker := &FlakyWorker{failuresLeft: 2}
	customTimer := &CustomTimer{}
	pool := New(ctx, []Worker[int]{worker},
		WithAttempts[int](3),
		WithDelay[int](100*time.Millisecond),
		WithDelayType[int](FixedDelay[int]),
		WithTimer[int](customTimer),
	)

	// Verify that the custom timer was set correctly
	if pool.timer != customTimer {
		t.Fatalf("Custom timer was not set correctly in the pool")
	}

	err := pool.Dispatch(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	// Wait for the task to be processed
	time.Sleep(500 * time.Millisecond)

	pool.Close()

	// Add debug output
	t.Logf("Timer called: %v", customTimer.called)
	t.Logf("Number of durations: %d", len(customTimer.durations))
	t.Logf("Durations: %v", customTimer.durations)

	if !customTimer.called {
		t.Error("Expected custom timer to be called, but it wasn't")
	}

	expectedCalls := 2 // We expect 2 calls because the task fails twice before succeeding
	if len(customTimer.durations) != expectedCalls {
		t.Errorf("Expected %d delay durations recorded, got %d", expectedCalls, len(customTimer.durations))
	}

	expectedDuration := 100 * time.Millisecond
	allowedDeviation := 1 * time.Millisecond

	for _, d := range customTimer.durations {
		if d < expectedDuration-allowedDeviation || d > expectedDuration+allowedDeviation {
			t.Errorf("Expected delay of %v (±%v), got %v", expectedDuration, allowedDeviation, d)
		}
	}
}

// TestDynamicWorkerManagement tests adding and removing workers dynamically
func TestDynamicWorkerManagement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	worker1 := &IncrementWorker{}
	pool := New(ctx, []Worker[int]{worker1})

	// Dispatch initial tasks
	for i := 0; i < 5; i++ {
		err := pool.Dispatch(1)
		if err != nil {
			t.Fatalf("Failed to dispatch task: %v", err)
		}
	}

	t.Log("first dispatch ", pool.QueueSize())
	// Wait for initial tasks to be processed
	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 10*time.Millisecond)

	pool.RemoveWorker(0)

	// Add a new worker dynamically
	worker2 := &IncrementWorker{}
	pool.AddWorker(worker2)

	// Dispatch more tasks after adding worker2
	for i := 0; i < 5; i++ {
		err := pool.Dispatch(1)
		if err != nil {
			t.Fatalf("Failed to dispatch task: %v", err)
		}
	}

	t.Log("second dispatch ", pool.QueueSize())
	// Wait for all tasks to be processed
	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 10*time.Millisecond)

	pool.Close()

	totalCounter := worker1.counter + worker2.counter
	if totalCounter != 10 {
		t.Errorf("Expected total counter to be 10, got %d", totalCounter)
	}

	if worker1.counter == 0 || worker2.counter == 0 {
		t.Errorf("Expected both workers to process tasks, got worker1: %d, worker2: %d", worker1.counter, worker2.counter)
	}

	t.Logf("Worker1 counter: %d, Worker2 counter: %d", worker1.counter, worker2.counter)
}

// TestRemoveWorker tests removing a worker from the pool
func TestRemoveWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	worker1 := &IncrementWorker{}
	worker2 := &IncrementWorker{}
	pool := New(ctx, []Worker[int]{worker1, worker2})

	// Dispatch multiple tasks
	for i := 0; i < 20; i++ {
		err := pool.Dispatch(1)
		if err != nil {
			t.Fatalf("Failed to dispatch task: %v", err)
		}
	}

	// Wait for some tasks to be processed
	time.Sleep(100 * time.Millisecond)

	// Remove worker1 (assuming workerID is 0)
	err := pool.RemoveWorker(0)
	if err != nil {
		t.Fatalf("Failed to remove worker: %v", err)
	}

	// Wait for all tasks to be processed
	err = pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 10*time.Millisecond)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	pool.Close()

	totalCounter := worker1.counter + worker2.counter
	if totalCounter != 20 {
		t.Errorf("Expected total counter to be 20, got %d", totalCounter)
	}

	// Check that worker2 processed tasks after worker1 was removed
	if worker2.counter == 0 {
		t.Errorf("Expected worker2 to process tasks after worker1 removal")
	}

	t.Logf("Worker1 counter: %d, Worker2 counter: %d", worker1.counter, worker2.counter)
}

// Define a SlowWorker for testing InterruptWorker
type SlowWorker struct {
	processing bool
	mu         sync.Mutex
	started    chan struct{}
}

func (w *SlowWorker) Run(ctx context.Context, data int) error {
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

// TestInterruptWorker tests interrupting a worker
func TestInterruptWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	worker := &SlowWorker{
		started: make(chan struct{}),
	}
	pool := New(ctx, []Worker[int]{worker},
		WithRetryIf[int](func(err error) bool {
			return err.Error() == "worker interrupted"
		}),
	)

	// Dispatch a task
	for i := 0; i < 10; i++ {
		err := pool.Dispatch(i)
		if err != nil {
			t.Fatalf("Failed to dispatch task: %v", err)
		}
	}

	// Wait for the worker to start processing
	select {
	case <-worker.started:
	case <-ctx.Done():
		t.Fatal("Timed out waiting for worker to start")
	}

	// Interrupt the worker (assuming workerID is 0)
	err := pool.RemoveWorker(0)
	if err != nil {
		t.Fatalf("Failed to interrupt worker: %v", err)
	}

	// Wait for a bit to allow requeueing
	time.Sleep(100 * time.Millisecond)

	// Check that the task is either requeued or being processed
	queueSize := pool.QueueSize()
	processingCount := pool.ProcessingCount()
	if queueSize == 0 && processingCount == 0 {
		t.Errorf("Expected task to be requeued or processing, got queue size: %d, processing count: %d", queueSize, processingCount)
	}

	// Wait for the task to be processed
	err = pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 10*time.Millisecond)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	pool.Close()

	t.Logf("Final queue size: %d, processing count: %d", pool.QueueSize(), pool.ProcessingCount())
}

// TestOnTaskSuccessAndFailureCallbacks tests the OnTaskSuccess and OnTaskFailure callbacks
func TestOnTaskSuccessAndFailureCallbacks(t *testing.T) {
	ctx := context.Background()
	worker := &FlakyWorker{failuresLeft: 1}
	var successCalled, failureCalled bool
	var mu sync.Mutex

	var counter int

	pool := New(ctx, []Worker[int]{worker},
		WithOnTaskSuccess[int](func(controller WorkerController[int], workerID int, worker Worker[int], task *TaskWrapper[int]) {
			mu.Lock()
			successCalled = true
			mu.Unlock()
		}),
		WithOnTaskFailure[int](func(controller WorkerController[int], workerID int, worker Worker[int], task *TaskWrapper[int], err error) DeadTaskAction {
			mu.Lock()
			failureCalled = true
			if f, ok := worker.(*FlakyWorker); ok {
				counter = f.Inc()
			}
			mu.Unlock()
			return DeadTaskActionRetry
		}),
	)

	err := pool.Dispatch(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	err = pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	pool.Close()

	if counter != 1 {
		t.Errorf("Expected counter to be 1, got %d", counter)
	}

	mu.Lock()
	defer mu.Unlock()
	if !successCalled {
		t.Errorf("Expected OnTaskSuccess callback to be called")
	}
	if !failureCalled {
		t.Errorf("Expected OnTaskFailure callback to be called")
	}
}

// Define FailingWorker that implements Worker[int]
type FailingWorker struct {
	id        int
	processed []int
	mu        sync.Mutex
}

func (worker *FailingWorker) Run(ctx context.Context, data int) error {
	worker.mu.Lock()
	worker.processed = append(worker.processed, data)
	worker.mu.Unlock()
	return errors.New("intentional error")
}

// TestTriedWorkersHandling tests that tasks are not retried by the same worker unless all workers have tried
func TestTriedWorkersHandling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	numWorkers := 3
	workers := make([]Worker[int], numWorkers)
	failingWorkers := make([]*FailingWorker, numWorkers)

	for i := 0; i < numWorkers; i++ {
		worker := &FailingWorker{id: i}
		failingWorkers[i] = worker
		workers[i] = worker
	}

	pool := New(ctx, workers,
		WithAttempts[int](numWorkers),
		WithRetryIf[int](func(err error) bool {
			return true
		}),
	)

	err := pool.Dispatch(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	err = pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	pool.Close()

	// Now, check that each worker has processed the task once
	for i, worker := range failingWorkers {
		if len(worker.processed) != 1 {
			t.Errorf("Expected worker %d to process the task once, got %d", i, len(worker.processed))
		}
	}
}

// TestDeadTaskErrorsAndDurations tests that DeadTask contains correct total duration
func TestDeadTaskErrorsAndDurations(t *testing.T) {
	ctx := context.Background()
	worker := &FlakyWorker{failuresLeft: 3}

	pool := New(ctx, []Worker[int]{worker},
		WithAttempts[int](2),
		WithDelay[int](200*time.Millisecond),
		WithDelayType[int](FixedDelay[int]),
	)

	err := pool.Dispatch(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	err = pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	pool.Close()

	deadTasks := pool.DeadTasks()
	if len(deadTasks) != 1 {
		t.Errorf("Expected 1 dead task, got %d", len(deadTasks))
	}

	deadTask := deadTasks[0]

	// Check totalDuration
	expectedMinDuration := 100 * time.Millisecond // At least one processing time
	if deadTask.TotalDuration < expectedMinDuration {
		t.Errorf("Expected total duration at least %v, got %v", expectedMinDuration, deadTask.TotalDuration)
	}

	// Check number of errors
	if len(deadTask.Errors) != 2 {
		t.Errorf("Expected 2 error, got %d", len(deadTask.Errors))
	}

	// Check error messages
	if deadTask.Errors[0].Error() != "temporary error" {
		t.Errorf("Expected 'temporary error', got '%v'", deadTask.Errors[0])
	}

	t.Logf("Dead task details: Retries: %d, TotalDuration: %v, Errors: %v", deadTask.Retries, deadTask.TotalDuration, deadTask.Errors)
}

// TestCombineDelayAndMaxBackOffN tests CombineDelay and maxBackOffN logic
func TestCombineDelayAndMaxBackOffN(t *testing.T) {
	ctx := context.Background()
	worker := &FlakyWorker{failuresLeft: 5}
	delays := make([]time.Duration, 0)
	var mu sync.Mutex

	// Custom DelayTypeFunc that records the delays
	recordingDelay := func(n int, err error, config *Config[int]) time.Duration {
		delay := config.delay << n
		mu.Lock()
		delays = append(delays, delay)
		mu.Unlock()
		return delay
	}

	pool := New(ctx, []Worker[int]{worker},
		WithAttempts[int](3),
		WithDelay[int](100*time.Millisecond),
		WithDelayType[int](CombineDelay[int](BackOffDelay[int], recordingDelay)),
	)

	err := pool.Dispatch(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	err = pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	pool.Close()

	// Now, check that delays are as expected
	expectedDelays := []time.Duration{
		200 * time.Millisecond, // 100ms << 1
		400 * time.Millisecond, // 100ms << 2
		800 * time.Millisecond, // 100ms << 3
	}

	mu.Lock()
	defer mu.Unlock()
	for i, delay := range delays {
		if i >= len(expectedDelays) {
			break
		}
		if delay != expectedDelays[i] {
			t.Errorf("Expected delay %v at attempt %d, got %v", expectedDelays[i], i+1, delay)
		}
	}
}

/////

// TestWorker is a worker implementation specifically for this test
type TestWorker struct {
	mu           sync.Mutex
	processCount int
	processChan  chan struct{}
}

func NewTestWorker() *TestWorker {
	return &TestWorker{
		processChan: make(chan struct{}, 1),
	}
}

func (w *TestWorker) Run(ctx context.Context, data int) error {
	w.mu.Lock()
	w.processCount++
	w.mu.Unlock()

	select {
	case w.processChan <- struct{}{}:
	default:
	}

	// Simulate some work
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(50 * time.Millisecond):
		return nil
	}
}

func (w *TestWorker) GetProcessCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.processCount
}

func TestWorkerInterruptAndRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testWorker := NewTestWorker()
	pool := New(ctx, []Worker[int]{testWorker})

	// Dispatch a task
	err := pool.Dispatch(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	// Wait for the worker to start processing
	select {
	case <-testWorker.processChan:
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for worker to start processing")
	}

	// Interrupt the worker
	err = pool.InterruptWorker(0)
	if err != nil {
		t.Fatalf("Failed to interrupt worker: %v", err)
	}

	// Wait for the worker to stop processing
	time.Sleep(100 * time.Millisecond)

	// Restart the worker
	err = pool.RestartWorker(0)
	if err != nil {
		t.Fatalf("Failed to restart worker: %v", err)
	}

	// Dispatch another task
	err = pool.Dispatch(2)
	if err != nil {
		t.Fatalf("Failed to dispatch task after restart: %v", err)
	}

	// Wait for the worker to process the new task
	select {
	case <-testWorker.processChan:
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for worker to process task after restart")
	}

	// Test interrupting and immediately restarting
	err = pool.InterruptWorker(0, WithRestart())
	if err != nil {
		t.Fatalf("Failed to interrupt and restart worker: %v", err)
	}

	// Dispatch one more task
	err = pool.Dispatch(3)
	if err != nil {
		t.Fatalf("Failed to dispatch task after interrupt and restart: %v", err)
	}

	// Wait for the worker to process the new task
	select {
	case <-testWorker.processChan:
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for worker to process task after interrupt and restart")
	}

	// Verify that the worker processed all tasks
	processCount := testWorker.GetProcessCount()
	if processCount != 3 {
		t.Errorf("Expected worker to process 3 tasks in total, but processed %d", processCount)
	}

	// Clean up
	cancel()
	pool.Close()
}
