package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/davidroman0O/retrypool"
	_ "go.uber.org/automaxprocs"
)

// Task represents a simple work unit
type Task struct {
	ID        int
	Timestamp time.Time
}

// BenchmarkWorker processes tasks with minimal simulated work
type BenchmarkWorker struct {
	processed int64
}

func (w *BenchmarkWorker) Run(ctx context.Context, task Task) error {
	// Minimal simulated work to optimize throughput, adjust if necessary for more real workloads
	atomic.AddInt64(&w.processed, 1)
	return nil
}

func main() {
	// Creating a context with a cancel function for clean shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Increase the number of workers based on available CPU cores to improve parallelism
	numWorkers := 8
	workers := make([]retrypool.Worker[Task], numWorkers)
	workerStats := make([]*BenchmarkWorker, numWorkers)

	for i := 0; i < numWorkers; i++ {
		w := &BenchmarkWorker{}
		workers[i] = w
		workerStats[i] = w
	}

	// Create and initialize the optimized pool
	pool := retrypool.New(ctx, workers)

	submissionDone := make(chan struct{})

	// Improved task submission with context cancellation check to avoid overhead on pool shutdown
	go func() {
		taskID := 0
		for ctx.Err() == nil {
			task := Task{
				ID:        taskID,
				Timestamp: time.Now(),
			}
			if err := pool.Submit(task); err != nil {
				if err == retrypool.ErrPoolClosed {
					break
				}
				log.Printf("Failed to submit task: %v", err)
			}
			taskID++
		}
		close(submissionDone)
	}()

	// Adjusted ticker to 500ms to capture finer-grained progress and enhance responsiveness
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	startTime := time.Now()
	var lastTotal int64

	for {
		select {
		case <-ctx.Done():
			goto Done
		case <-ticker.C:
			var totalProcessed int64
			for _, w := range workerStats {
				totalProcessed += atomic.LoadInt64(&w.processed)
			}
			elapsed := time.Since(startTime)
			throughput := totalProcessed - lastTotal
			fmt.Printf("[%v] Tasks processed: %d (Total: %d, Avg: %.2f/sec)\n",
				elapsed.Round(time.Second),
				throughput,
				totalProcessed,
				float64(totalProcessed)/elapsed.Seconds())
			lastTotal = totalProcessed
		}
	}

Done:
	<-submissionDone
	pool.Shutdown()

	var finalTotal int64
	for _, w := range workerStats {
		finalTotal += atomic.LoadInt64(&w.processed)
	}

	totalDuration := time.Since(startTime)
	fmt.Printf("\nFinal Results:\n")
	fmt.Printf("Total tasks processed: %d\n", finalTotal)
	fmt.Printf("Total time: %v\n", totalDuration.Round(time.Millisecond))
	fmt.Printf("Average throughput: %.2f tasks/second\n", float64(finalTotal)/totalDuration.Seconds())

	// Pool metrics output
	metrics := pool.Metrics()
	fmt.Printf("\nPool Metrics:\n")
	fmt.Printf("Tasks Submitted: %d\n", metrics.TasksSubmitted)
	fmt.Printf("Tasks Processed: %d\n", metrics.TasksProcessed)
	fmt.Printf("Tasks Succeeded: %d\n", metrics.TasksSucceeded)
	fmt.Printf("Tasks Failed: %d\n", metrics.TasksFailed)
	fmt.Printf("Dead Tasks: %d\n", metrics.DeadTasks)
}
