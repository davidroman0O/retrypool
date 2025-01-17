package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/davidroman0O/retrypool"
)

// Define a custom worker type
type MyWorker struct {
	ID int
}

// Implement the Worker interface
func (w *MyWorker) Run(ctx context.Context, data int) error {
	fmt.Printf("Worker %d starting task with data: %d\n", w.ID, data)

	// Simulate processing time
	select {
	case <-time.After(time.Duration(data) * time.Second):
		fmt.Printf("Worker %d completed task with data: %d\n", w.ID, data)
		return nil
	case <-ctx.Done():
		fmt.Printf("Worker %d task with data %d canceled: %v\n", w.ID, data, ctx.Err())
		return ctx.Err()
	}
}

// Every task that takes more than x seconds will be canceled, if you implement your worker right you will return the ctx.Err(), which will put the task in the dead task list
func main() {
	ctx := context.Background()

	// Create a pool with one worker
	pool := retrypool.New[int](ctx, []retrypool.Worker[int]{&MyWorker{}},
		retrypool.WithAttempts[int](2),        // Each task can retry once
		retrypool.WithDelay[int](time.Second), // Delay between retries
		retrypool.WithIfRetry[int](func(err error) bool {
			return errors.Is(err, context.DeadlineExceeded)
		}),
	)

	// Submit tasks with per-attempt timeout (WithDuration)
	fmt.Println("Submitting tasks with per-attempt timeout:")
	for i := 1; i <= 3; i++ {
		err := pool.Submit(i,
			retrypool.WithTaskDuration[int](2*time.Second), // Each attempt has 2 seconds
		)
		if err != nil {
			fmt.Println("Error submitting task:", err)
		}
	}

	// Submit tasks with total timeout (WithTimeout)
	fmt.Println("\nSubmitting tasks with total timeout:")
	for i := 4; i <= 6; i++ {
		err := pool.Submit(i,
			retrypool.WithTaskTimeout[int](3*time.Second), // Entire task must finish in 3 seconds
		)
		if err != nil {
			fmt.Println("Error submitting task:", err)
		}
	}

	// Wait for all tasks to complete
	err := pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize+processingCount > 0
	}, time.Second)

	if err != nil {
		fmt.Println("Error waiting for tasks:", err)
	}

	// Print dead tasks
	fmt.Println("\nDead tasks:")
	pool.RangeDeadTasks(func(dt *retrypool.DeadTask[int]) bool {
		fmt.Printf("Dead task with data: %d, reason: %s, errors: %v\n", dt.Data, dt.Reason, dt.Errors)
		return true
	})

	// Close the pool
	pool.Close()
}
