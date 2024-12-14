package tests

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/davidroman0O/retrypool"
)

// concurrentWorker tracks task processing with atomic counters
type concurrentWorker struct {
	processedCount atomic.Int64
	activeCount    atomic.Int64
	maxConcurrent  atomic.Int64
	mu             sync.Mutex
	processed      []int // track order of processed tasks
}

func (w *concurrentWorker) Run(ctx context.Context, data int) error {
	// Track concurrent processing
	current := w.activeCount.Add(1)
	defer w.activeCount.Add(-1)

	// Update max concurrent seen
	for {
		max := w.maxConcurrent.Load()
		if current <= max || w.maxConcurrent.CompareAndSwap(max, current) {
			break
		}
	}

	// Simulate work with varying duration
	delay := time.Duration(50+data%50) * time.Millisecond
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(delay):
	}

	// Track processed tasks
	w.mu.Lock()
	w.processed = append(w.processed, data)
	w.mu.Unlock()

	w.processedCount.Add(1)
	return nil
}

func TestConcurrentSubmission(t *testing.T) {
	tests := []struct {
		name        string
		numWorkers  int
		numTasks    int
		batchSize   int
		submitDelay time.Duration
	}{
		{
			name:       "Many Workers Many Tasks",
			numWorkers: 4,
			numTasks:   100,
			batchSize:  10,
		},
		{
			name:       "Fast Sequential Tasks",
			numWorkers: 2,
			numTasks:   50,
			batchSize:  1,
		},
		{
			name:        "Delayed Batches",
			numWorkers:  3,
			numTasks:    30,
			batchSize:   5,
			submitDelay: 10 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// Create workers
			workers := make([]concurrentWorker, tt.numWorkers)

			// Create pool with workers
			workerList := make([]retrypool.Worker[int], tt.numWorkers)
			for i := range workers {
				workerList[i] = &workers[i]
			}

			pool := retrypool.New(ctx, workerList)

			// Submit tasks in batches
			var wg sync.WaitGroup
			for start := 0; start < tt.numTasks; start += tt.batchSize {
				wg.Add(1)
				go func(startIdx int) {
					defer wg.Done()
					end := startIdx + tt.batchSize
					if end > tt.numTasks {
						end = tt.numTasks
					}

					for i := startIdx; i < end; i++ {
						if tt.submitDelay > 0 {
							time.Sleep(tt.submitDelay)
						}

						if err := pool.Submit(i); err != nil {
							t.Errorf("Failed to submit task %d: %v", i, err)
						}
					}
				}(start)
			}

			// Wait for submissions to complete
			wg.Wait()

			// Wait for processing
			err := pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
				return queueSize > 0 || processingCount > 0
			}, 50*time.Millisecond)
			if err != nil {
				t.Fatalf("Error waiting for completion: %v", err)
			}

			// Verify results
			var totalProcessed int64
			var maxConcurrent int64

			for i, w := range workers {
				processed := w.processedCount.Load()
				concurrent := w.maxConcurrent.Load()
				totalProcessed += processed
				if concurrent > maxConcurrent {
					maxConcurrent = concurrent
				}

				t.Logf("Worker %d: processed %d tasks, max concurrent: %d",
					i, processed, concurrent)

			}

			if totalProcessed != int64(tt.numTasks) {
				t.Errorf("Expected %d tasks processed, got %d", tt.numTasks, totalProcessed)
			}

			// Check worker states
			workerIDs, err := pool.Workers()
			if err != nil {
				t.Fatal(err)
			}
			if len(workerIDs) != tt.numWorkers {
				t.Errorf("Expected %d workers, got %d", tt.numWorkers, len(workerIDs))
			}

			// Check task states
			if pool.DeadTaskCount() > 0 {
				t.Errorf("Expected no dead tasks, got %d", pool.DeadTaskCount())
			}

			if err := pool.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
