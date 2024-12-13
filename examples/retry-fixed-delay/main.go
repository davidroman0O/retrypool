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
// It simulates failure for even numbers by returning an error.
func (w *MyWorker) Run(ctx context.Context, task MyTask) error {
	if task.Value%2 == 0 {
		// Simulate a failure for even numbers.
		fmt.Printf("Worker %d failed to process task with value %d\n", w.ID, task.Value)
		return errors.New("simulated failure")
	}
	fmt.Printf("Worker %d successfully processed task with value %d\n", w.ID, task.Value)
	// Simulate some work.
	time.Sleep(300 * time.Millisecond)
	return nil
}

func main() {
	ctx := context.Background()

	// Create workers.
	workers := []retrypool.Worker[MyTask]{}
	for i := 0; i < 4; i++ {
		workers = append(workers, &MyWorker{})
	}

	// Configure the pool with a fixed delay retry policy.
	pool := retrypool.New(ctx, workers,
		// Limit the number of retry attempts to 3.
		retrypool.WithAttempts[MyTask](3),
		// Set a fixed delay of 1 second between retries.
		retrypool.WithRetryPolicy[MyTask](retrypool.FixedDelayRetryPolicy[MyTask]{
			Delay:     1 * time.Second,
			MaxJitter: 0,
		}),
	)
	defer pool.Close()

	// Submit tasks.
	for i := 0; i < 10; i++ {
		task := MyTask{Value: i}
		err := pool.Submit(task)
		if err != nil {
			fmt.Printf("Error submitting task: %v\n", err)
		}
	}

	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, time.Second/4)

	pool.Close()
}
