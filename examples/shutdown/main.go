package main

import (
	"context"
	"fmt"
	"time"

	"github.com/davidroman0O/retrypool"
)

// Define the data type for tasks
type TaskData struct {
	ID int
}

// Implement the Worker interface
type ShutdownWorker struct{}

func (w *ShutdownWorker) Run(ctx context.Context, data TaskData) error {
	// Simulate some work
	select {
	case <-ctx.Done():
		// Handle cancellation
		fmt.Printf("Task ID %d was cancelled.\n", data.ID)
		return ctx.Err()
	case <-time.After(time.Millisecond * 100):
		// Simulate successful task completion
		fmt.Printf("Task ID %d completed.\n", data.ID)
		return nil
	}
}

func main() {
	ctx := context.Background()

	// Create the pool with workers
	pool := retrypool.New[TaskData](
		ctx,
		[]retrypool.Worker[TaskData]{&ShutdownWorker{}},
	)

	// Submit tasks
	numTasks := 10
	for i := 1; i <= numTasks; i++ {
		taskID := i
		err := pool.Submit(TaskData{ID: taskID})
		if err != nil {
			fmt.Printf("Error submitting task ID %d: %v\n", taskID, err)
		}
	}

	// Wait for tasks to complete or context cancellation
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Second*2)
	defer cancel()

	err := pool.WaitWithCallback(ctxWithTimeout, func(queueSize, processingCount, deadTaskCount int) bool {
		fmt.Printf("Queue Size: %d, Processing Count: %d, Dead Task Count: %d\n", queueSize, processingCount, deadTaskCount)
		// Continue waiting if there are tasks in the queue or being processed
		return queueSize > 0 || processingCount > 0
	}, time.Millisecond*500)

	if err != nil {
		fmt.Printf("Waiting interrupted: %v\n", err)
	}

	// Close the pool gracefully
	pool.Close()

	fmt.Println("Pool closed gracefully.")
}
