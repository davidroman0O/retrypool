package main

import (
	"context"
	"fmt"
	"log"
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
	select {
	case <-time.After(3 * time.Second): // Simulate long processing time
		fmt.Printf("Task %d completed successfully.\n", data.ID)
		return nil
	case <-ctx.Done():
		fmt.Printf("Task %d canceled due to timeout.\n", data.ID)
		return ctx.Err()
	}
}

func main() {
	ctx := context.Background()

	// Initialize the retrypool with one worker.
	pool := retrypool.New[MyTask](ctx, []retrypool.Worker[MyTask]{&MyWorker{}})

	// Dispatch a task with a time limit of 2 seconds.
	err := pool.Submit(MyTask{ID: 1}, retrypool.WithTimeLimit[MyTask](2*time.Second))
	if err != nil {
		fmt.Printf("Failed to dispatch task: %v\n", err)
	}

	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		log.Printf("Queue size: %d, Processing count: %d", queueSize, processingCount)
		return queueSize > 0 || processingCount > 0
	}, 1*time.Second)

	// Wait for all tasks to complete.
	pool.Shutdown()
}
