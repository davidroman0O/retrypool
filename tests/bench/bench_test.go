package retrypool_test

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	_ "go.uber.org/automaxprocs"

	"github.com/davidroman0O/retrypool"
)

// SimpleTask represents the data structure for tasks
type SimpleTask struct {
	// Add any fields if necessary
}

// SimpleWorker is a worker that processes SimpleTask
type SimpleWorker struct {
	ID          int
	TaskCounter *int64 // Pointer to shared counter
}

func (w *SimpleWorker) Run(ctx context.Context, data SimpleTask) error {
	// Simulate processing time if needed (e.g., time.Sleep)
	// For maximum throughput, we can remove any sleep
	atomic.AddInt64(w.TaskCounter, 1)
	return nil
}

func TestThroughput(t *testing.T) {
	// Shared task counter
	var totalProcessed int64

	// Number of workers
	numWorkers := runtime.NumCPU() * 2

	// Create workers
	workers := make([]retrypool.Worker[SimpleTask], numWorkers)
	for i := 0; i < numWorkers; i++ {
		workers[i] = &SimpleWorker{
			ID:          i,
			TaskCounter: &totalProcessed,
		}
	}

	// Create context
	ctx, cancel := context.WithCancel(context.Background())

	// Create the pool
	pool := retrypool.New(ctx, workers)

	// Start time
	startTime := time.Now()

	// Duration of the benchmark
	benchmarkDuration := 10 * time.Second

	// End time
	endTime := startTime.Add(benchmarkDuration)

	// Submit tasks continuously until benchmark duration elapses
	go func() {
		for {
			// Check if the benchmark duration has elapsed
			if time.Now().After(endTime) {
				// Stop accepting new tasks
				cancel()
				return
			}

			err := pool.Submit(SimpleTask{})
			if err != nil {
				t.Errorf("Failed to submit task: %v", err)
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				metrics := pool.GetMetricsSnapshot() // on the old code it's `metrics := pool.Metrics()`
				t.Logf("Tasks processed: %d, Tasks submitted: %d, Tasks succeeded: %d, Dead tasks: %d, Queues: %v", metrics.TasksProcessed, metrics.TasksSubmitted, metrics.TasksSucceeded, metrics.DeadTasks, metrics.Queues)
			}
		}
	}()

	// Wait for the benchmark duration to complete
	<-ctx.Done()

	// Optionally, wait for all tasks to be processed
	pool.WaitWithCallback(context.Background(), func(queueSize, processingCount, deadTaskCount int) bool {
		fmt.Println("Queue size:", queueSize, "Processing count:", processingCount, "Dead task count:", deadTaskCount)
		return queueSize > 0 && processingCount > 0
	}, time.Millisecond*100)

	// Calculate the actual duration
	actualDuration := time.Since(startTime)

	// Get the total tasks processed
	totalTasks := atomic.LoadInt64(&totalProcessed)

	// Calculate throughput
	throughput := float64(totalTasks) / actualDuration.Seconds()

	t.Logf("Total tasks processed: %d", totalTasks)
	t.Logf("Average throughput: %.2f tasks/second", throughput)
}
