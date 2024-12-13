package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/davidroman0O/retrypool"
)

// MyTask represents the data we'll process.
type MyTask struct {
	Value int
}

// MyWorker implements the retrypool.Worker interface.
type MyWorker struct {
	ID int
}

// Run processes a task.
// It simulates failure for odd numbers by returning an error.
func (w *MyWorker) Run(ctx context.Context, task MyTask) error {
	if task.Value%2 != 0 {
		// Simulate a failure for odd numbers.
		fmt.Printf("Worker %d failed to process task with value %d\n", w.ID, task.Value)
		return errors.New("simulated failure")
	}
	fmt.Printf("Worker %d successfully processed task with value %d\n", w.ID, task.Value)
	// Simulate some work.
	time.Sleep(200 * time.Millisecond)
	return nil
}

func main() {
	ctx := context.Background()

	// Create workers.
	workers := []retrypool.Worker[MyTask]{}
	for i := 0; i < 3; i++ {
		workers = append(workers, &MyWorker{})
	}

	// Configure the pool with an exponential backoff retry policy.
	pool := retrypool.New(ctx, workers,
		// Limit the number of retry attempts to 2.
		retrypool.WithAttempts[MyTask](2),
		// Set an exponential backoff policy with a base delay and maximum delay.
		retrypool.WithRetryPolicy[MyTask](retrypool.ExponentialBackoffRetryPolicy[MyTask]{
			BaseDelay: 500 * time.Millisecond, // Initial delay.
			MaxDelay:  5 * time.Second,        // Maximum delay between retries.
			MaxJitter: 500 * time.Millisecond, // Maximum jitter to add randomness.
		}),
	)

	// Submit tasks.
	for i := 1; i <= 6; i++ {
		task := MyTask{Value: i}
		err := pool.Submit(task)
		if err != nil {
			fmt.Printf("Error submitting task: %v\n", err)
		}
	}

	// Wait for all tasks to be processed.
	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, time.Second/4)

	// Close the pool.
	pool.Close()

	// Optionally, print dead tasks.
	pool.RangeDeadTasks(func(dt *retrypool.DeadTask[MyTask]) bool {
		fmt.Printf("Task with value %d failed after %d retries. Errors: %v\n",
			dt.Data.Value, dt.Retries, dt.Errors)
		return true
	})
}
