package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/davidroman0O/retrypool"
)

type pauseTestWorker struct {
	ID             int
	mu             sync.Mutex
	processedTasks map[int]time.Time
}

func newPauseTestWorker() *pauseTestWorker {
	return &pauseTestWorker{
		processedTasks: make(map[int]time.Time),
	}
}

func (w *pauseTestWorker) Run(_ context.Context, data int) error {
	time.Sleep(10 * time.Millisecond)
	w.mu.Lock()
	w.processedTasks[data] = time.Now()
	w.mu.Unlock()
	return nil
}

func TestPauseResume(t *testing.T) {
	tests := []struct {
		name     string
		syncMode bool
	}{
		{"Asynchronous Mode", false},
		{"Synchronous Mode", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			worker := newPauseTestWorker()
			var pool *retrypool.Pool[int]
			if tt.syncMode {
				pool = retrypool.New(
					ctx,
					[]retrypool.Worker[int]{worker},
					retrypool.WithSynchronousMode[int](),
				)
			} else {
				pool = retrypool.New(
					ctx,
					[]retrypool.Worker[int]{worker},
				)
			}

			// Submit initial batch of tasks
			for i := 0; i < 20; i++ {
				if err := pool.Submit(i); err != nil {
					t.Fatalf("Failed to submit task %d: %v", i, err)
				}
			}

			// Wait for some tasks to process
			time.Sleep(100 * time.Millisecond)

			// Record tasks processed before pause
			worker.mu.Lock()
			beforePause := len(worker.processedTasks)
			worker.mu.Unlock()

			// Pause the worker
			if err := pool.Pause(0); err != nil {
				t.Fatalf("Failed to pause worker: %v", err)
			}

			// Try to submit tasks during pause - these should fail
			failedCount := 0
			for i := 20; i < 30; i++ {
				if err := pool.Submit(i); err != nil {
					failedCount++
				}
			}

			if failedCount == 0 {
				t.Error("Expected task submissions to fail during pause")
			}

			// Wait during pause
			time.Sleep(200 * time.Millisecond)

			worker.mu.Lock()
			duringPause := len(worker.processedTasks) - beforePause
			worker.mu.Unlock()

			// Resume the worker
			if err := pool.Resume(0); err != nil {
				t.Fatalf("Failed to resume worker: %v", err)
			}

			// Submit more tasks after resume
			for i := 30; i < 40; i++ {
				if err := pool.Submit(i); err != nil {
					t.Fatalf("Failed to submit task %d after resume: %v", i, err)
				}
			}

			// Wait for completion
			time.Sleep(300 * time.Millisecond)

			// Final count
			worker.mu.Lock()
			afterResume := len(worker.processedTasks)
			worker.mu.Unlock()

			t.Logf("Tasks processed: Before pause=%d, During pause=%d, Total=%d",
				beforePause, duringPause, afterResume)

			// Verify minimal or no processing during pause
			if duringPause > 2 { // Allow for a couple in-flight tasks
				t.Errorf("Too many tasks processed during pause: %d", duringPause)
			}

			// Close the pool
			if err := pool.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
