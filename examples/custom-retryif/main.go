package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/davidroman0O/retrypool"
)

// Define custom error types
var (
	ErrTemporary    = errors.New("temporary error")
	ErrPermanent    = errors.New("permanent error")
	ErrNotFound     = errors.New("resource not found")
	ErrUnauthorized = errors.New("unauthorized")
)

// Define a custom worker type
type MyWorker struct {
	ID int
}

// Implement the Worker interface
func (w *MyWorker) Run(ctx context.Context, data string) error {
	fmt.Printf("Worker %d processing task with data: %s\n", w.ID, data)

	// Simulate different errors based on input data
	// They will all fails but what's important is the amount of times they are allowed to retry
	switch data {
	case "temp":
		fmt.Println("Encountered temporary error")
		return errors.Join(ErrTemporary, fmt.Errorf("network issue"))
	case "perm":
		fmt.Println("Encountered permanent error")
		return errors.Join(ErrPermanent, fmt.Errorf("invalid input"))
	case "notfound":
		fmt.Println("Encountered not found error")
		return errors.Join(ErrNotFound, fmt.Errorf("item does not exist"))
	case "unauth":
		fmt.Println("Encountered unauthorized error")
		return errors.Join(ErrUnauthorized, fmt.Errorf("invalid credentials"))
	default:
		fmt.Printf("Worker %d completed task with data: %s\n", w.ID, data)
		return nil
	}
}

func main() {
	ctx := context.Background()

	// Define custom retry logic
	retryIfFunc := func(err error) bool {
		// Retry only on temporary errors
		return errors.Is(err, ErrTemporary)
	}

	// Create a pool with custom retry logic
	pool := retrypool.New[string](ctx, []retrypool.Worker[string]{&MyWorker{}},
		retrypool.WithAttempts[string](3), // Try up to 3 times
		retrypool.WithIfRetry[string](retryIfFunc),
	)

	// Submit tasks with different error scenarios
	tasks := []string{"temp", "perm", "notfound", "unauth", "success"}
	for _, data := range tasks {
		err := pool.Submit(data)
		if err != nil {
			fmt.Println("Error submitting task:", err)
		}
	}

	// Wait for tasks to complete
	err := pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize+processingCount > 0
	}, time.Second)

	if err != nil {
		fmt.Println("Error waiting for tasks:", err)
	}

	// Print dead tasks
	fmt.Println("\nDead tasks:")
	pool.RangeDeadTasks(func(dt *retrypool.DeadTask[string]) bool {
		// On that output, observe that only the `temp` task was retried multiple times
		fmt.Printf("Dead task with data: %s, reason: %s, errors: %v\n", dt.Data, dt.Reason, dt.Errors)
		return true
	})

	// Close the pool
	pool.Close()
}
