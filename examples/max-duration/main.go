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
	select {
	case <-time.After(3 * time.Second): // Simulate long processing time
		fmt.Printf("Task %d completed successfully.\n", data.ID)
		return nil
	case <-ctx.Done():
		fmt.Printf("Task %d attempt canceled due to max duration.\n", data.ID)
		return ctx.Err()
	}
}

func main() {
	ctx := context.Background()

	// Initialize the retrypool with one worker.
	pool := retrypool.New[MyTask](ctx, []retrypool.Worker[MyTask]{&MyWorker{}})

	// Dispatch a task with a max duration of 1 second per attempt.
	err := pool.Submit(MyTask{ID: 2}, retrypool.WithMaxContextDuration[MyTask](1*time.Second))
	if err != nil {
		fmt.Printf("Failed to dispatch task: %v\n", err)
	}

	// Wait for all tasks to complete.
	pool.Shutdown()
}
