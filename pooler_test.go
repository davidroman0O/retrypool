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
	pool := NewPool(ctx, []Worker[int]{worker})

	err := pool.Dispatch(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	pool.Close()
	if worker.counter != 1 {
		t.Errorf("Expected counter to be 1, got %d", worker.counter)
	}
}

// Test task retries with a fixed delay
func TestRetriesWithFixedDelay(t *testing.T) {
	ctx := context.Background()
	worker := &FlakyWorker{failuresLeft: 2}
	pool := NewPool(ctx, []Worker[int]{worker},
		WithAttempts[int](2),
		WithDelay[int](100*time.Millisecond),
		WithDelayType[int](FixedDelay[int]),
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

// Test unlimited retries with backoff delay
func TestUnlimitedRetriesWithBackoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	worker := &FlakyWorker{failuresLeft: 5}
	pool := NewPool(ctx, []Worker[int]{worker},
		WithAttempts[int](UnlimitedAttempts),
		WithDelay[int](100*time.Millisecond),
		WithDelayType[int](BackOffDelay[int]),
	)

	err := pool.Dispatch(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	pool.WaitWithCallback(ctx, func(queueSize, processingCount int) bool {
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
	pool := NewPool(ctx, []Worker[int]{worker},
		WithAttempts[int](UnlimitedAttempts),
		WithDelay[int](100*time.Millisecond),
		WithDelayType[int](FixedDelay[int]),
	)

	err := pool.Dispatch(1, WithTimeLimit[int](500*time.Millisecond))
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

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
	pool := NewPool(ctx, []Worker[int]{worker},
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
	pool := NewPool(ctx, []Worker[int]{worker},
		WithAttempts[int](2),
		WithOnRetry[int](func(attempt int, err error, task int) {
			retryAttempts++
			log.Printf("Retry attempt %d for task %d due to error: %v", attempt, task, err)
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
	pool := NewPool(ctx, []Worker[int]{worker},
		WithAttempts[int](2),
	)

	err := pool.Dispatch(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	pool.Close()

	deadTasks := pool.DeadTasks()
	if len(deadTasks) != 1 {
		t.Errorf("Expected 1 dead task, got %d", len(deadTasks))
	}

	if deadTasks[0].Error.Error() != "unrecoverable error" {
		t.Errorf("Expected unrecoverable error, got %v", deadTasks[0].Error)
	}
}

// Test handling of multiple workers
func TestMultipleWorkers(t *testing.T) {
	ctx := context.Background()
	worker1 := &IncrementWorker{}
	worker2 := &IncrementWorker{}
	pool := NewPool(ctx, []Worker[int]{worker1, worker2})

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
	pool := NewPool(ctx, []Worker[int]{worker})

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
	pool := NewPool(ctx, []Worker[int]{worker})

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
	pool := NewPool(ctx, []Worker[int]{worker})

	// Dispatch multiple tasks
	for i := 0; i < 5; i++ {
		err := pool.Dispatch(1)
		if err != nil {
			t.Fatalf("Failed to dispatch task: %v", err)
		}
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := pool.WaitWithCallback(waitCtx, func(queueSize, processingCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	if worker.counter != 5 {
		t.Errorf("Expected counter to be 5, got %d", worker.counter)
	}
}

// Test MaxDelay
func TestMaxDelay(t *testing.T) {
	ctx := context.Background()
	worker := &FlakyWorker{failuresLeft: 2}
	pool := NewPool(ctx, []Worker[int]{worker},
		WithAttempts[int](2),
		WithDelay[int](100*time.Millisecond),
		WithMaxDelay[int](150*time.Millisecond),
		WithDelayType[int](BackOffDelay[int]),
	)

	err := pool.Dispatch(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	startTime := time.Now()
	pool.Close()
	elapsedTime := time.Since(startTime)

	// Adjusted expected total time considering processing time and delays
	expectedMaxTime := 100*time.Millisecond + // Initial run time
		100*time.Millisecond + // Processing time for first retry
		150*time.Millisecond + // Max delay applied on second retry
		100*time.Millisecond // Processing time for second retry

	if elapsedTime > expectedMaxTime+50*time.Millisecond { // Adding buffer for overhead
		t.Errorf("Expected total retry time to be less than %v, got %v", expectedMaxTime+50*time.Millisecond, elapsedTime)
	}
}

// Test MaxJitter with RandomDelay
func TestMaxJitterWithRandomDelay(t *testing.T) {
	ctx := context.Background()
	worker := &FlakyWorker{failuresLeft: 1}
	pool := NewPool(ctx, []Worker[int]{worker},
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

// Test context cancellation
func TestContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	worker := &FlakyWorker{failuresLeft: 5}
	pool := NewPool(ctx, []Worker[int]{worker},
		WithAttempts[int](UnlimitedAttempts),
	)

	err := pool.Dispatch(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	// Cancel the context after some time
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	pool.WaitWithCallback(ctx, func(queueSize, processingCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 50*time.Millisecond)

	if worker.failuresLeft >= 5 {
		t.Errorf("Expected failuresLeft to be less than 5 due to context cancellation, got %d", worker.failuresLeft)
	}
}

// TestTimeLimitWithUnlimitedRetries tests a task with time limit and unlimited retries
func TestTimeLimitWithUnlimitedRetries(t *testing.T) {
	ctx := context.Background()
	worker := &FlakyWorker{failuresLeft: 10}
	pool := NewPool(ctx, []Worker[int]{worker},
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
	pool := NewPool(ctx, []Worker[int]{worker1, worker2})

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
	pool := NewPool(ctx, []Worker[int]{worker},
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
	pool := NewPool(ctx, []Worker[int]{worker},
		WithAttempts[int](2),
		WithRetryIf[int](func(err error) bool {
			return true // Retry on all errors
		}),
	)

	err := pool.Dispatch(1)
	if err != nil {
		t.Fatalf("Failed to dispatch task: %v", err)
	}

	pool.Close()

	deadTasks := pool.DeadTasks()
	if len(deadTasks) != 1 {
		t.Errorf("Expected 1 dead task, got %d", len(deadTasks))
	}

	if deadTasks[0].Error.Error() != "unrecoverable error" {
		t.Errorf("Expected unrecoverable error, got %v", deadTasks[0].Error)
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
	pool := NewPool(ctx, []Worker[int]{worker},
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
