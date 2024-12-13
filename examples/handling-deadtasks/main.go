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
// It fails for numbers greater than 5.
func (w *MyWorker) Run(ctx context.Context, task MyTask) error {
	if task.Value > 5 {
		// Simulate a failure.
		fmt.Printf("Worker %d failed to process task with value %d\n", w.ID, task.Value)
		return errors.New("simulated failure")
	}
	fmt.Printf("Worker %d successfully processed task with value %d\n", w.ID, task.Value)
	// Simulate some work.
	time.Sleep(100 * time.Millisecond)
	return nil
}

func main() {
	ctx := context.Background()

	// Create workers.
	workers := []retrypool.Worker[MyTask]{&MyWorker{}, &MyWorker{}}

	// Define the onDeadTask callback function.
	onDeadTask := func(deadTaskIndex int) {
		fmt.Printf("Task added to dead tasks list at index %d\n", deadTaskIndex)
	}

	// Configure the pool with maximum retry attempts and onDeadTask callback.
	pool := retrypool.New(ctx, workers,
		retrypool.WithAttempts[MyTask](3), // Maximum of 3 attempts.
		retrypool.WithOnDeadTask[MyTask](onDeadTask),
	)
	defer pool.Close()

	// Submit tasks.
	for i := 1; i <= 8; i++ {
		task := MyTask{Value: i}
		err := pool.Submit(task)
		if err != nil {
			fmt.Printf("Error submitting task %d: %v\n", task.Value, err)
		} else {
			fmt.Printf("Task %d submitted successfully\n", task.Value)
		}
	}

	// Wait for all tasks to be processed.
	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, time.Second/4)

	// Close the pool.
	pool.Close()

	// Inspect dead tasks.
	pool.RangeDeadTasks(func(dt *retrypool.DeadTask[MyTask]) bool {
		fmt.Printf("Dead Task - Value: %d, Retries: %d, Errors: %v\n",
			dt.Data.Value, dt.Retries, dt.Errors)
		return true
	})
}
