package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/davidroman0O/retrypool"
)

type timeLimitWorker struct {
	mu             sync.Mutex
	processTime    time.Duration
	attemptsCount  map[int]int
	processingTime map[int][]time.Duration
	lastError      error
}

func newTimeLimitWorker(processTime time.Duration) *timeLimitWorker {
	return &timeLimitWorker{
		processTime:    processTime,
		attemptsCount:  make(map[int]int),
		processingTime: make(map[int][]time.Duration),
	}
}

func (w *timeLimitWorker) Run(ctx context.Context, data int) error {
	w.mu.Lock()
	w.attemptsCount[data]++
	w.mu.Unlock()

	startTime := time.Now()

	select {
	case <-ctx.Done():
		duration := time.Since(startTime)
		w.mu.Lock()
		w.processingTime[data] = append(w.processingTime[data], duration)
		w.lastError = ctx.Err()
		w.mu.Unlock()
		return ctx.Err()
	case <-time.After(w.processTime):
		duration := time.Since(startTime)
		w.mu.Lock()
		w.processingTime[data] = append(w.processingTime[data], duration)
		w.mu.Unlock()
		return nil
	}
}

func TestTaskProcessingTimeLimits(t *testing.T) {
	tests := []struct {
		name                  string
		processTime           time.Duration
		taskTimeLimit         time.Duration
		attemptTimeout        time.Duration
		attempts              int
		expectedAttempts      int
		expectError           bool
		expectTaskInDeadTasks bool
	}{
		{
			name:             "Task completes within time limit",
			processTime:      50 * time.Millisecond,
			taskTimeLimit:    100 * time.Millisecond,
			attemptTimeout:   0,
			attempts:         1,
			expectedAttempts: 1,
			expectError:      false,
		},
		{
			name:                  "Task exceeds time limit and times out",
			processTime:           150 * time.Millisecond,
			taskTimeLimit:         100 * time.Millisecond,
			attemptTimeout:        0,
			attempts:              1,
			expectedAttempts:      1,
			expectError:           true,
			expectTaskInDeadTasks: true,
		},
		{
			name:                  "Task retries on failure until max attempts",
			processTime:           150 * time.Millisecond,
			taskTimeLimit:         0, // No task time limit
			attemptTimeout:        100 * time.Millisecond,
			attempts:              3,
			expectedAttempts:      3,
			expectError:           true,
			expectTaskInDeadTasks: true,
		},
		{
			name:             "Task succeeds on retry before max attempts",
			processTime:      150 * time.Millisecond,
			taskTimeLimit:    0,
			attemptTimeout:   200 * time.Millisecond,
			attempts:         3,
			expectedAttempts: 1,
			expectError:      false,
		},
		{
			name:                  "Combined task time limit and attempt timeout",
			processTime:           75 * time.Millisecond,
			taskTimeLimit:         200 * time.Millisecond,
			attemptTimeout:        50 * time.Millisecond,
			attempts:              5,
			expectedAttempts:      4, // Should attempt 4 times before total time limit is exceeded
			expectError:           true,
			expectTaskInDeadTasks: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			worker := newTimeLimitWorker(tt.processTime)

			options := []retrypool.Option[int]{
				retrypool.WithAttempts[int](tt.attempts),
				retrypool.WithDelay[int](10 * time.Millisecond),
			}

			pool := retrypool.New(ctx, []retrypool.Worker[int]{worker}, options...)

			taskOpts := []retrypool.TaskOption[int]{}
			if tt.taskTimeLimit > 0 {
				taskOpts = append(taskOpts, retrypool.WithTaskTimeout[int](tt.taskTimeLimit))
			}
			if tt.attemptTimeout > 0 {
				taskOpts = append(taskOpts, retrypool.WithTaskDuration[int](tt.attemptTimeout))
			}

			err := pool.Submit(1, taskOpts...)
			if err != nil {
				t.Fatalf("Failed to submit task: %v", err)
			}

			// Wait for processing to complete
			err = pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
				// Return false when we're done
				return !(queueSize == 0 && processingCount == 0)
			}, 50*time.Millisecond)

			if err != nil {
				t.Fatalf("Error waiting for completion: %v", err)
			}

			// Verify attempts count
			worker.mu.Lock()
			attempts := worker.attemptsCount[1]
			if attempts != tt.expectedAttempts {
				t.Errorf("Expected %d attempts, got %d",
					tt.expectedAttempts, attempts)
			}

			// Verify processing times and timeouts
			times := worker.processingTime[1]
			for i, duration := range times {
				t.Logf("Attempt %d duration: %v", i+1, duration)

				if tt.attemptTimeout > 0 && duration > tt.attemptTimeout+(20*time.Millisecond) {
					t.Errorf("Processing time %v exceeded attempt timeout %v on attempt %d",
						duration, tt.attemptTimeout, i+1)
				}
			}
			worker.mu.Unlock()

			// Verify dead task status
			deadTasks := pool.DeadTaskCount()
			if tt.expectTaskInDeadTasks {
				if deadTasks == 0 {
					t.Error("Expected task to be in dead tasks")
				}
			} else {
				if deadTasks > 0 {
					t.Errorf("Unexpected dead tasks: %v", deadTasks)
				}
			}

			if err := pool.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
