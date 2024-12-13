package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/davidroman0O/retrypool"
)

//// There is a bug here

// Define the data type for tasks
type TaskData struct {
	ID int
}

// Implement the Worker interface
type ConcurrentWorker struct{}

func (w *ConcurrentWorker) Run(ctx context.Context, data TaskData) error {
	// Simulate some work
	time.Sleep(time.Millisecond * 10)
	return nil
}

func main() {
	ctx := context.Background()

	// Number of workers and tasks
	numWorkers := 10
	numTasks := 1000

	// Create workers
	workers := make([]retrypool.Worker[TaskData], numWorkers)
	for i := 0; i < numWorkers; i++ {
		workers[i] = &ConcurrentWorker{}
	}

	// Create the pool with the workers
	pool := retrypool.New[TaskData](ctx, workers)

	// WaitGroup to wait for all submissions to complete
	var wg sync.WaitGroup

	// Submit tasks concurrently
	for i := 0; i < numTasks; i++ {
		wg.Add(1)
		go func(taskID int) {
			defer wg.Done()
			err := pool.Submit(TaskData{ID: taskID})
			if err != nil {
				fmt.Printf("Error submitting task ID %d: %v\n", taskID, err)
			}
		}(i)
	}

	// Wait for all submissions to complete
	wg.Wait()

	// Wait for all tasks to be processed
	for {
		if pool.QueueSize() == 0 && pool.ProcessingCount() == 0 {
			break
		}
		fmt.Printf("Queue size: %d, Processing count: %d\n", pool.QueueSize(), pool.ProcessingCount())
		time.Sleep(time.Millisecond * 100)
	}

	// Close the pool
	pool.Close()

	metrics := pool.GetMetricsSnapshot()
	fmt.Println("Metrics:", metrics.TasksSucceeded, metrics.TasksProcessed, metrics.TasksFailed, metrics.DeadTasks)

	fmt.Println("All tasks submitted and processed successfully.")
}
