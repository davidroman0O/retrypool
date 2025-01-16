package tests

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/davidroman0O/retrypool"
)

// TaskAttempt records information about each attempt to process a task
type TaskAttempt struct {
	taskID    int
	attempt   int
	timestamp time.Time
}

// Worker that tracks execution timing of tasks
type timedRetryWorker struct {
	ID         int
	mu         sync.Mutex
	attempts   []TaskAttempt
	shouldFail map[int]bool // tracks which tasks should fail on first attempt
}

func newTimedRetryWorker() *timedRetryWorker {
	return &timedRetryWorker{
		attempts:   make([]TaskAttempt, 0),
		shouldFail: make(map[int]bool),
	}
}

func (w *timedRetryWorker) Run(_ context.Context, data int) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	currentAttempt := 0
	for i := range w.attempts {
		if w.attempts[i].taskID == data {
			currentAttempt++
		}
	}

	w.attempts = append(w.attempts, TaskAttempt{
		taskID:    data,
		attempt:   currentAttempt,
		timestamp: time.Now(),
	})

	// Fail on first attempt if this task is marked for failure
	if currentAttempt == 0 && w.shouldFail[data] {
		return errors.New("first attempt failure")
	}
	return nil
}

func TestImmediateRetry(t *testing.T) {
	tests := []struct {
		name     string
		syncMode bool
	}{
		{"Asynchronous Mode", false},
		{"Synchronous Mode", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use a longer timeout for safety
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			worker := newTimedRetryWorker()

			// Mark tasks that should fail on first attempt
			worker.shouldFail[1] = true
			worker.shouldFail[3] = true
			worker.shouldFail[5] = true

			options := []retrypool.Option[int]{
				retrypool.WithOnTaskFailure[int](func(data int, metadata retrypool.Metadata, err error) retrypool.TaskAction {
					return retrypool.TaskActionRetry
				}),
			}

			if tt.syncMode {
				options = append(options, retrypool.WithSynchronousMode[int]())
			}

			pool := retrypool.New(ctx, []retrypool.Worker[int]{worker}, options...)

			// Submit tasks
			numTasks := 6
			wg := sync.WaitGroup{}
			wg.Add(numTasks)

			for i := 0; i < numTasks; i++ {
				i := i // Capture loop variable
				go func() {
					defer wg.Done()
					var err error
					if worker.shouldFail[i] {
						err = pool.Submit(i, retrypool.WithImmediateRetry[int]())
					} else {
						err = pool.Submit(i)
					}
					if err != nil {
						t.Errorf("Failed to submit task %d: %v", i, err)
					}
				}()
			}

			// Wait for all submissions
			wg.Wait()

			// Wait for completion with progress callback
			err := pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
				return queueSize > 0 || processingCount > 0
			}, 100*time.Millisecond)

			if err != nil {
				t.Fatalf("WaitWithCallback failed: %v", err)
			}

			// Verify retry outcomes
			worker.mu.Lock()
			attempts := make([]TaskAttempt, len(worker.attempts))
			copy(attempts, worker.attempts)
			worker.mu.Unlock()

			// Group attempts by taskID
			retryAttempts := make(map[int][]TaskAttempt)
			for _, attempt := range attempts {
				retryAttempts[attempt.taskID] = append(retryAttempts[attempt.taskID], attempt)
			}

			// Verify each task's attempts
			for taskID := range retryAttempts {
				attempts := retryAttempts[taskID]
				if worker.shouldFail[taskID] {
					if len(attempts) < 2 {
						t.Errorf("Task %d should have at least 2 attempts, got %d", taskID, len(attempts))
					}
				} else {
					if len(attempts) != 1 {
						t.Errorf("Task %d should have exactly 1 attempt, got %d", taskID, len(attempts))
					}
				}
			}

			if err := pool.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
