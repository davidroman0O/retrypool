package tests

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/davidroman0O/retrypool"
)

// MockWorker is a mock implementation of the Worker interface for testing
type MockWorker struct {
	ID             int
	processDelay   time.Duration
	processedTasks []*TestTaskData
	mu             sync.Mutex

	startCalled  bool
	stopCalled   bool
	pauseCalled  bool
	resumeCalled bool
	removeCalled bool
}

type TestTaskData struct {
	Value int
}

func (w *MockWorker) Run(ctx context.Context, data TestTaskData) error {
	// Simulate processing delay
	select {
	case <-time.After(w.processDelay):
		w.mu.Lock()
		w.processedTasks = append(w.processedTasks, &data)
		w.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *MockWorker) OnStart(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.startCalled = true
}

func (w *MockWorker) OnStop(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stopCalled = true
}

func (w *MockWorker) OnPause(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pauseCalled = true
}

func (w *MockWorker) OnResume(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.resumeCalled = true
}

func (w *MockWorker) OnRemove(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.removeCalled = true
}

// TestPoolLifecycleAsync tests the lifecycle capabilities in asynchronous mode
func TestPoolLifecycleAsync(t *testing.T) {
	ctx := context.Background()

	worker1 := &MockWorker{processDelay: 10 * time.Millisecond}
	worker2 := &MockWorker{processDelay: 10 * time.Millisecond}
	workers := []retrypool.Worker[TestTaskData]{worker1, worker2}

	var pool *retrypool.Pool[TestTaskData]
	pool = retrypool.New[TestTaskData](ctx, workers,
		retrypool.WithAttempts[TestTaskData](3),
		retrypool.WithOnTaskSuccess[TestTaskData](func(data TestTaskData) {
			fmt.Printf("Task succeeded: %v\n", data)
		}),
		retrypool.WithOnTaskFailure[TestTaskData](func(data TestTaskData, err error) retrypool.TaskAction {
			fmt.Printf("Task failed: %v, error: %v\n", data, err)
			return retrypool.TaskActionRetry
		}),
	)

	// Submit tasks
	taskCount := 10
	fmt.Println("Submitting tasks")
	for i := 0; i < taskCount; i++ {
		err := pool.Submit(TestTaskData{Value: i})
		if err != nil {
			t.Errorf("Failed to submit task: %v", err)
		}
	}

	fmt.Println("Pausing worker1")
	// Pause worker1
	err := pool.Pause(worker1.ID)
	if err != nil {
		t.Errorf("Failed to pause worker1: %v", err)
	}

	// Submit more tasks
	fmt.Println("Submitting more tasks")
	for i := taskCount; i < taskCount*2; i++ {
		err := pool.Submit(TestTaskData{Value: i})
		if err != nil {
			t.Errorf("Failed to submit task: %v", err)
		}
	}

	fmt.Println("Resuming worker1")
	// Resume worker1
	err = pool.Resume(worker1.ID)
	if err != nil {
		t.Errorf("Failed to resume worker1: %v", err)
	}
	fmt.Println("Worker1 resumed")

	fmt.Println("Removing worker2")
	// Remove worker2
	err = pool.Remove(worker2.ID)
	if err != nil {
		t.Errorf("Failed to remove worker2: %v", err)
	}
	fmt.Println("Worker2 removed")

	// Wait for all tasks to be processed
	err = pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, time.Millisecond*50)
	if err != nil {
		t.Errorf("Error while waiting for pool: %v", err)
	}

	// Check that all tasks were processed
	totalProcessed := len(worker1.processedTasks) + len(worker2.processedTasks)
	if totalProcessed != taskCount*2 {
		t.Errorf("Not all tasks were processed, expected %d, got %d", taskCount*2, totalProcessed)
	}

	// Check worker callbacks
	if !worker1.startCalled {
		t.Errorf("Worker1 OnStart was not called")
	}
	if !worker1.pauseCalled {
		t.Errorf("Worker1 OnPause was not called")
	}
	if !worker1.resumeCalled {
		t.Errorf("Worker1 OnResume was not called")
	}

	if !worker2.startCalled {
		t.Errorf("Worker2 OnStart was not called")
	}
	if worker2.pauseCalled {
		t.Errorf("Worker2 OnPause should not have been called")
	}
	if worker2.resumeCalled {
		t.Errorf("Worker2 OnResume should not have been called")
	}
	if !worker2.stopCalled {
		t.Errorf("Worker2 OnStop was not called")
	}
	if !worker2.removeCalled {
		t.Errorf("Worker2 OnRemove was not called")
	}

	count := 0
	pool.RangeDeadTasks(func(dt *retrypool.DeadTask[TestTaskData]) bool {
		fmt.Printf("Dead task: %v - %v\n", dt.Data, dt.Reason)
		count++
		return true
	})

	if count != 0 {
		t.Errorf("Expected 0 dead tasks, got %d", count)
	}

	// Close the pool
	err = pool.Close()
	if err != nil {
		t.Errorf("Failed to close pool: %v", err)
	}
}

// TestPoolLifecycleSync tests the lifecycle capabilities in synchronous mode
func TestPoolLifecycleSync(t *testing.T) {
	ctx := context.Background()

	worker1 := &MockWorker{processDelay: 10 * time.Millisecond}
	worker2 := &MockWorker{processDelay: 10 * time.Millisecond}
	workers := []retrypool.Worker[TestTaskData]{worker1, worker2}

	pool := retrypool.New[TestTaskData](ctx, workers,
		retrypool.WithAttempts[TestTaskData](3),
		retrypool.WithOnTaskSuccess[TestTaskData](func(data TestTaskData) {
			fmt.Printf("Task succeeded: %v\n", data)
		}),
		retrypool.WithOnTaskFailure[TestTaskData](func(data TestTaskData, err error) retrypool.TaskAction {
			fmt.Printf("Task failed: %v, error: %v\n", data, err)
			return retrypool.TaskActionRetry
		}),
		retrypool.WithSynchronousMode[TestTaskData](),
		retrypool.WithOnDeadTask[TestTaskData](func(deadTaskIndex int) {
			fmt.Printf("Task %d is dead\n", deadTaskIndex)
		}),
	)

	// Submit tasks
	taskCount := 10
	fmt.Println("Submitting tasks")
	for i := 0; i < taskCount; i++ {
		err := pool.Submit(TestTaskData{Value: i})
		if err != nil {
			t.Errorf("Failed to submit task: %v", err)
		}
	}

	// Pause worker1
	fmt.Println("Pausing worker1")
	err := pool.Pause(worker1.ID)
	if err != nil {
		t.Errorf("Failed to pause worker1: %v", err)
	}

	// Submit more tasks
	fmt.Println("Submitting more tasks")
	for i := taskCount; i < taskCount*2; i++ {
		err := pool.Submit(TestTaskData{Value: i})
		if err != nil {
			t.Errorf("Failed to submit task: %v", err)
		}
	}

	fmt.Println("Resuming worker1")
	// Resume worker1
	err = pool.Resume(worker1.ID)
	if err != nil {
		t.Errorf("Failed to resume worker1: %v", err)
	}
	fmt.Println("Worker1 resumed")

	fmt.Println("Removing worker2")
	// Remove worker2
	err = pool.Remove(worker2.ID)
	if err != nil {
		t.Errorf("Failed to remove worker2: %v", err)
	}
	fmt.Println("Worker2 removed")

	// Wait for all tasks to be processed
	err = pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		fmt.Println("Queue size:", queueSize, "Processing count:", processingCount, "Dead task count:", deadTaskCount)
		return queueSize > 0 || processingCount > 0
	}, time.Millisecond*50)
	if err != nil {
		t.Errorf("Error while waiting for pool: %v", err)
	}

	// Check that all tasks were processed
	totalProcessed := len(worker1.processedTasks) + len(worker2.processedTasks)
	if totalProcessed != taskCount*2 {
		t.Errorf("Not all tasks were processed, expected %d, got %d", taskCount*2, totalProcessed)
	}

	// Check worker callbacks
	if !worker1.startCalled {
		t.Errorf("Worker1 OnStart was not called")
	}
	if !worker1.pauseCalled {
		t.Errorf("Worker1 OnPause was not called")
	}
	if !worker1.resumeCalled {
		t.Errorf("Worker1 OnResume was not called")
	}

	if !worker2.startCalled {
		t.Errorf("Worker2 OnStart was not called")
	}
	if worker2.pauseCalled {
		t.Errorf("Worker2 OnPause should not have been called")
	}
	if worker2.resumeCalled {
		t.Errorf("Worker2 OnResume should not have been called")
	}
	if !worker2.stopCalled {
		t.Errorf("Worker2 OnStop was not called")
	}
	if !worker2.removeCalled {
		t.Errorf("Worker2 OnRemove was not called")
	}

	// Close the pool
	err = pool.Close()
	if err != nil {
		t.Errorf("Failed to close pool: %v", err)
	}
}

// TestPoolPauseAllWorkers tests pausing all workers and ensuring tasks go to dead tasks
func TestPoolPauseAllWorkers(t *testing.T) {
	ctx := context.Background()

	worker1 := &MockWorker{processDelay: 10 * time.Millisecond}
	workers := []retrypool.Worker[TestTaskData]{worker1}

	pool := retrypool.New[TestTaskData](ctx, workers,
		retrypool.WithAttempts[TestTaskData](1),
		retrypool.WithDeadTasksLimit[TestTaskData](100),
		retrypool.WithNoWorkerPolicy[TestTaskData](retrypool.NoWorkerPolicyAddToDeadTasks),
	)

	// Pause the only worker
	err := pool.Pause(worker1.ID)
	if err != nil {
		t.Errorf("Failed to pause worker1: %v", err)
	}

	// Submit tasks
	taskCount := 5
	for i := 0; i < taskCount; i++ {
		err := pool.Submit(TestTaskData{Value: i})
		if err != nil {
			if !errors.Is(err, retrypool.ErrNoWorkersAvailable) {
				t.Errorf("Unexpected error when submitting task: %v", err)
			}
		}
	}

	// Wait briefly to allow any processing (there should be none)
	time.Sleep(time.Millisecond * 100)

	// Check that tasks went to dead tasks
	deadTaskCount := int(pool.DeadTaskCount())
	if deadTaskCount != taskCount {
		t.Errorf("Expected %d dead tasks, got %d", taskCount, deadTaskCount)
	}

	// Resume worker
	err = pool.Resume(worker1.ID)
	if err != nil {
		t.Errorf("Failed to resume worker1: %v", err)
	}

	// Check that no additional tasks were processed
	totalProcessed := len(worker1.processedTasks)
	if totalProcessed != 0 {
		t.Errorf("No tasks should have been processed, but got %d", totalProcessed)
	}

	// Close the pool
	err = pool.Close()
	if err != nil {
		t.Errorf("Failed to close pool: %v", err)
	}
}

// TestPoolRemoveAllWorkers tests removing all workers and ensuring tasks go to dead tasks
func TestPoolRemoveAllWorkers(t *testing.T) {
	ctx := context.Background()

	worker1 := &MockWorker{processDelay: 10 * time.Millisecond}
	workers := []retrypool.Worker[TestTaskData]{worker1}

	pool := retrypool.New[TestTaskData](ctx, workers,
		retrypool.WithAttempts[TestTaskData](1),
		retrypool.WithDeadTasksLimit[TestTaskData](100),
		retrypool.WithNoWorkerPolicy[TestTaskData](retrypool.NoWorkerPolicyAddToDeadTasks),
	)

	// Remove the only worker
	err := pool.Remove(worker1.ID)
	if err != nil {
		t.Errorf("Failed to remove worker1: %v", err)
	}

	// Submit tasks
	taskCount := 5
	for i := 0; i < taskCount; i++ {
		err := pool.Submit(TestTaskData{Value: i})
		if err != nil {
			if !errors.Is(err, retrypool.ErrNoWorkersAvailable) {
				t.Errorf("Unexpected error when submitting task: %v", err)
			}
		}
	}

	// Wait briefly to allow any processing (there should be none)
	time.Sleep(time.Millisecond * 100)

	// Check that tasks went to dead tasks
	deadTaskCount := int(pool.DeadTaskCount())
	if deadTaskCount != taskCount {
		t.Errorf("Expected %d dead tasks, got %d", taskCount, deadTaskCount)
	}

	// Close the pool
	err = pool.Close()
	if err != nil {
		t.Errorf("Failed to close pool: %v", err)
	}
}

type contextKey string

const (
	testKey1 = contextKey("test-key-1")
	testKey2 = contextKey("test-key-2")
)

// contextTestWorker records context values and states
type contextTestWorker struct {
	mu sync.Mutex

	// Context value tracking
	receivedValues map[contextKey]interface{}

	// Deadline tracking
	receivedDeadline time.Time
	hasDeadline      bool

	// Cancellation tracking
	wasCancelled bool

	// Processing state
	started chan struct{}
}

func newContextTestWorker() *contextTestWorker {
	return &contextTestWorker{
		receivedValues: make(map[contextKey]interface{}),
		started:        make(chan struct{}),
	}
}

func (w *contextTestWorker) Run(ctx context.Context, data int) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Store context values
	if val := ctx.Value(testKey1); val != nil {
		w.receivedValues[testKey1] = val
	}
	if val := ctx.Value(testKey2); val != nil {
		w.receivedValues[testKey2] = val
	}

	// Check deadline
	if deadline, ok := ctx.Deadline(); ok {
		w.receivedDeadline = deadline
		w.hasDeadline = true
	}

	close(w.started)

	// Wait to allow cancellation test
	select {
	case <-ctx.Done():
		w.wasCancelled = true
		return ctx.Err()
	case <-time.After(100 * time.Millisecond):
		return nil
	}
}

// TODO: Add test logs
// TODO: add sync vs async
func TestContextPropagation(t *testing.T) {
	tests := []struct {
		name           string
		setupContext   func(context.Context) (context.Context, context.CancelFunc)
		expectedValues map[contextKey]interface{}
		checkDeadline  bool
		checkCancel    bool
	}{
		{
			name: "Basic Value Propagation",
			setupContext: func(ctx context.Context) (context.Context, context.CancelFunc) {
				ctx = context.WithValue(ctx, testKey1, "value1")
				ctx = context.WithValue(ctx, testKey2, "value2")
				return ctx, func() {}
			},
			expectedValues: map[contextKey]interface{}{
				testKey1: "value1",
				testKey2: "value2",
			},
		},
		{
			name: "Deadline Propagation",
			setupContext: func(ctx context.Context) (context.Context, context.CancelFunc) {
				return context.WithDeadline(ctx, time.Now().Add(200*time.Millisecond))
			},
			checkDeadline: true,
		},
		{
			name: "Cancellation Propagation",
			setupContext: func(ctx context.Context) (context.Context, context.CancelFunc) {
				return context.WithCancel(ctx)
			},
			checkCancel: true,
		},
		{
			name: "Combined Context Features",
			setupContext: func(ctx context.Context) (context.Context, context.CancelFunc) {
				ctx = context.WithValue(ctx, testKey1, "value1")
				return context.WithDeadline(ctx, time.Now().Add(200*time.Millisecond))
			},
			expectedValues: map[contextKey]interface{}{
				testKey1: "value1",
			},
			checkDeadline: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseCtx := context.Background()
			ctx, cancel := tt.setupContext(baseCtx)
			defer cancel()

			worker := newContextTestWorker()
			pool := retrypool.New(ctx, []retrypool.Worker[int]{worker})

			// Submit task
			err := pool.Submit(1)
			if err != nil {
				t.Fatalf("Failed to submit task: %v", err)
			}

			// Wait for worker to start
			select {
			case <-worker.started:
			case <-time.After(time.Second):
				t.Fatal("Worker did not start in time")
			}

			// Test cancellation if needed
			if tt.checkCancel {
				cancel()
				time.Sleep(50 * time.Millisecond)

				worker.mu.Lock()
				if !worker.wasCancelled {
					t.Error("Expected worker to be cancelled")
				}
				worker.mu.Unlock()
			}

			// Verify context values
			worker.mu.Lock()
			for key, expectedValue := range tt.expectedValues {
				if gotValue := worker.receivedValues[key]; gotValue != expectedValue {
					t.Errorf("Expected value %v for key %v, got %v", expectedValue, key, gotValue)
				}
			}

			// Verify deadline
			if tt.checkDeadline {
				if !worker.hasDeadline {
					t.Error("Expected deadline to be set")
				}
				if !worker.receivedDeadline.After(time.Now()) {
					t.Error("Expected deadline to be in the future when received")
				}
			}
			worker.mu.Unlock()

			if err := pool.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
