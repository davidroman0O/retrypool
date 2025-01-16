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

// workerWithRetries is a test worker that fails a specified number of times before succeeding
type workerWithRetries struct {
	ID            int
	mu            sync.Mutex
	failuresLeft  map[int]int         // Maps task data to number of remaining failures
	attempts      map[int]int         // Tracks total attempts per task
	failureCount  map[int]int         // Tracks total failures per task
	successCount  map[int]int         // Tracks successful completions per task
	successAfter  map[int]int         // Defines how many times each task should fail before succeeding
	failureTiming map[int][]time.Time // Records when each failure occurred
}

func newWorkerWithRetries() *workerWithRetries {
	return &workerWithRetries{
		failuresLeft:  make(map[int]int),
		attempts:      make(map[int]int),
		failureCount:  make(map[int]int),
		successCount:  make(map[int]int),
		successAfter:  make(map[int]int),
		failureTiming: make(map[int][]time.Time),
	}
}

func (w *workerWithRetries) Run(_ context.Context, data int) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.attempts[data]++

	// Initialize failuresLeft if not set
	if _, exists := w.failuresLeft[data]; !exists {
		w.failuresLeft[data] = w.successAfter[data]
	}

	if w.failuresLeft[data] > 0 {
		w.failureCount[data]++
		w.failuresLeft[data]--
		w.failureTiming[data] = append(w.failureTiming[data], time.Now())
		return errors.New("task failed")
	}

	w.successCount[data]++
	return nil
}

type testCase struct {
	name            string
	maxAttempts     int
	taskFailures    map[int]int // Maps task ID to number of times it should fail
	expectedDeads   int
	expectedSuccess int
}

func TestRetryWithAttempts(t *testing.T) {
	testCases := []testCase{
		{
			name:        "Success after single retry",
			maxAttempts: 2,
			taskFailures: map[int]int{
				1: 1, // Task 1 fails once then succeeds
				2: 1, // Task 2 fails once then succeeds
			},
			expectedDeads:   0,
			expectedSuccess: 2,
		},
		{
			name:        "Some tasks exceed max attempts",
			maxAttempts: 3,
			taskFailures: map[int]int{
				1: 2, // Task 1 fails twice then succeeds
				2: 4, // Task 2 fails 4 times (should go to dead tasks)
				3: 1, // Task 3 fails once then succeeds
			},
			expectedDeads:   1,
			expectedSuccess: 2,
		},
		{
			name:        "All tasks succeed within attempts",
			maxAttempts: 5,
			taskFailures: map[int]int{
				1: 3, // Task 1 fails 3 times then succeeds
				2: 4, // Task 2 fails 4 times then succeeds
			},
			expectedDeads:   0,
			expectedSuccess: 2,
		},
		{
			name:        "All tasks exceed max attempts",
			maxAttempts: 2,
			taskFailures: map[int]int{
				1: 3, // Task 1 fails 3 times
				2: 3, // Task 2 fails 3 times
			},
			expectedDeads:   2,
			expectedSuccess: 0,
		},
	}

	modes := []struct {
		name string
		sync bool
	}{
		{"Async Mode", false},
		{"Sync Mode", true},
	}

	for _, mode := range modes {
		for _, tt := range testCases {
			testName := tt.name + " (" + mode.name + ")"
			t.Run(testName, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				worker := newWorkerWithRetries()

				// Configure worker with expected failures
				for taskID, failures := range tt.taskFailures {
					worker.successAfter[taskID] = failures
				}

				options := []retrypool.Option[int]{
					retrypool.WithAttempts[int](tt.maxAttempts),
					retrypool.WithDelay[int](50 * time.Millisecond),
					retrypool.WithOnTaskFailure[int](func(data int, metadata retrypool.Metadata, err error) retrypool.TaskAction {
						worker.mu.Lock()
						attempts := worker.attempts[data]
						worker.mu.Unlock()

						if attempts >= tt.maxAttempts {
							return retrypool.TaskActionAddToDeadTasks
						}
						return retrypool.TaskActionRetry
					}),
				}

				if mode.sync {
					options = append(options, retrypool.WithSynchronousMode[int]())
				}

				pool := retrypool.New(ctx, []retrypool.Worker[int]{worker}, options...)

				// Submit all tasks
				for taskID := range tt.taskFailures {
					if err := pool.Submit(taskID); err != nil {
						t.Fatalf("Failed to submit task %d: %v", taskID, err)
					}
				}

				// Improved wait condition
				err := pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
					worker.mu.Lock()
					totalAttempts := 0
					for _, attempts := range worker.attempts {
						totalAttempts += attempts
					}
					successCount := len(worker.successCount)
					worker.mu.Unlock()

					return queueSize > 0 || processingCount > 0 ||
						(successCount < tt.expectedSuccess && deadTaskCount < tt.expectedDeads)
				}, 100*time.Millisecond)

				if err != nil {
					t.Fatal(err)
				}

				// Allow time for final processing
				time.Sleep(200 * time.Millisecond)

				// Verify results
				if got := len(worker.successCount); got != tt.expectedSuccess {
					t.Errorf("Expected %d successful tasks, got %d", tt.expectedSuccess, got)
				}

				deadCount := pool.DeadTaskCount()
				if int(deadCount) != tt.expectedDeads {
					t.Errorf("Expected %d dead tasks, got %d", tt.expectedDeads, deadCount)
				}

				// Verify retry timing
				worker.mu.Lock()
				for taskID, timings := range worker.failureTiming {
					if len(timings) > 1 {
						for i := 1; i < len(timings); i++ {
							delay := timings[i].Sub(timings[i-1])
							if delay < 45*time.Millisecond { // Allow for some timing variation
								t.Errorf("Task %d: Retry delay too short: %v", taskID, delay)
							}
						}
					}
				}

				// Verify attempt counts
				for taskID, expectedFailures := range tt.taskFailures {
					expectedAttempts := expectedFailures + 1 // Add 1 for the success attempt
					if expectedAttempts > tt.maxAttempts {
						expectedAttempts = tt.maxAttempts
					}

					actualAttempts := worker.attempts[taskID]
					if actualAttempts != expectedAttempts {
						t.Errorf("Task %d: Expected %d attempts, got %d", taskID, expectedAttempts, actualAttempts)
					}
				}
				worker.mu.Unlock()

				if err := pool.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

// retryDelayWorker is a worker that fails a specified number of times
type retryDelayWorker struct {
	mu            sync.Mutex
	attemptTimes  []time.Time
	failuresLeft  int
	retriesNeeded int
}

func newRetryDelayWorker(failures int) *retryDelayWorker {
	return &retryDelayWorker{
		failuresLeft:  failures,
		retriesNeeded: failures,
		attemptTimes:  make([]time.Time, 0),
	}
}

func (w *retryDelayWorker) Run(ctx context.Context, data int) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.attemptTimes = append(w.attemptTimes, time.Now())

	if w.failuresLeft > 0 {
		fmt.Println("Intentional failure", data)
		w.failuresLeft--
		return errors.New("intentional failure")
	}
	fmt.Println("Success", data)
	return nil
}

func TestRetryWithCustomDelay(t *testing.T) {
	tests := []struct {
		name          string
		failures      int
		delayOptions  []retrypool.Option[int]
		expectedDelay time.Duration
		allowedJitter time.Duration
		skipIntervals bool
	}{
		{
			name:     "Fixed Delay",
			failures: 3,
			delayOptions: []retrypool.Option[int]{
				retrypool.WithAttempts[int](4),
				retrypool.WithDelay[int](100 * time.Millisecond),
			},
			expectedDelay: 100 * time.Millisecond,
			allowedJitter: 20 * time.Millisecond,
		},
		{
			name:     "Exponential Backoff",
			failures: 3,
			delayOptions: []retrypool.Option[int]{
				retrypool.WithAttempts[int](4),
				retrypool.WithDelayFunc[int](func(retries int, err error, config *retrypool.Config[int]) time.Duration {
					return time.Duration(50*(1<<retries)) * time.Millisecond
				}),
			},
			skipIntervals: true, // Check intervals individually
		},
		{
			name:     "With Max Delay",
			failures: 4,
			delayOptions: []retrypool.Option[int]{
				retrypool.WithAttempts[int](5),
				retrypool.WithDelay[int](50 * time.Millisecond),
				retrypool.WithMaxDelay[int](150 * time.Millisecond),
				retrypool.WithDelayFunc[int](func(retries int, err error, config *retrypool.Config[int]) time.Duration {
					return time.Duration(50*(1<<retries)) * time.Millisecond
				}),
			},
			skipIntervals: true,
		},
		{
			name:     "With Max Jitter",
			failures: 3,
			delayOptions: []retrypool.Option[int]{
				retrypool.WithAttempts[int](4),
				retrypool.WithDelay[int](100 * time.Millisecond),
				retrypool.WithMaxJitter[int](50 * time.Millisecond),
			},
			expectedDelay: 100 * time.Millisecond,
			allowedJitter: 50 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			worker := newRetryDelayWorker(tt.failures)

			pool := retrypool.New(ctx, []retrypool.Worker[int]{worker}, tt.delayOptions...)

			// Submit task that will fail and retry
			err := pool.Submit(1)
			if err != nil {
				t.Fatalf("Failed to submit task: %v", err)
			}

			// Wait for completion
			err = pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
				// fmt.Println("Queue size:", queueSize, "Processing count:", processingCount, "Dead task count:", deadTaskCount)
				return queueSize > 0 || processingCount > 0
			}, 10*time.Millisecond)

			if err != nil {
				t.Fatalf("Error waiting for completion: %v", err)
			}

			if err := pool.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
