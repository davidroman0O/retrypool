package main

import (
	"context"
	"fmt"
	"time"

	"github.com/davidroman0O/retrypool"
)

// MyTask represents the data structure for the task.
type MyTask struct {
	ID int
}

// MyWorker implements the retrypool.Worker interface.
type MyWorker struct{}

// Run simulates task processing with a sleep.
func (w *MyWorker) Run(ctx context.Context, data MyTask) error {
	time.Sleep(500 * time.Millisecond)
	fmt.Printf("Task %d completed successfully.\n", data.ID)
	return nil
}

func main() {
	ctx := context.Background()

	// Initialize the retrypool with one worker.
	pool := retrypool.New[MyTask](ctx, []retrypool.Worker[MyTask]{&MyWorker{}})

	// Dispatch multiple tasks.
	for i := 1; i <= 5; i++ {
		err := pool.Dispatch(MyTask{ID: i})
		if err != nil {
			fmt.Printf("Failed to dispatch task %d: %v\n", i, err)
		}
	}

	// Use WaitWithCallback to monitor the pool status.
	err := pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		fmt.Printf("QueueSize: %d, ProcessingCount: %d, DeadTaskCount: %d\n", queueSize, processingCount, deadTaskCount)
		// Continue waiting until all tasks are processed.
		return queueSize > 0 || processingCount > 0
	}, 1*time.Second)
	if err != nil {
		fmt.Printf("Error while waiting: %v\n", err)
	}

	// Close the pool after all tasks are done.
	pool.Close()
}
