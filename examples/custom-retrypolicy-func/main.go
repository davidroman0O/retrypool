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
// It simulates failure for numbers not divisible by 3.
func (w *MyWorker) Run(ctx context.Context, task MyTask) error {
	if task.Value%2 != 0 {
		// Simulate a failure.
		fmt.Printf("Worker %d failed to process task with value %d\n", w.ID, task.Value)
		return errors.New("simulated failure")
	}
	fmt.Printf("Worker %d successfully processed task with value %d\n", w.ID, task.Value)
	// Simulate some work.
	time.Sleep(150 * time.Millisecond)
	return nil
}

// CustomDelayFunc is a custom function for computing retry delays.
func CustomDelayFunc[T any](retries int, err error, config *retrypool.Config[T]) time.Duration {
	// Implement a custom delay logic.
	// For example, use a Fibonacci sequence for delays.
	fibDelay := fibonacci(retries) * 100 * time.Millisecond
	// Set a maximum delay.
	if fibDelay > 5*time.Second {
		fibDelay = 5 * time.Second
	}
	fmt.Printf("CustomDelayFunc: retries=%d, delay=%v\n", retries, fibDelay)
	return fibDelay
}

// fibonacci computes the nth Fibonacci number.
func fibonacci(n int) time.Duration {
	if n <= 1 {
		return time.Duration(n)
	}
	var a, b time.Duration = 0, 1
	for i := 2; i <= n; i++ {
		a, b = b, a+b
	}
	return b
}

func main() {
	ctx := context.Background()

	// Create workers.
	workers := []retrypool.Worker[MyTask]{}
	for i := 0; i < 4; i++ {
		workers = append(workers, &MyWorker{})
	}

	// Configure the pool with a custom delay function.
	pool := retrypool.New(ctx, workers,
		// Limit the number of retry attempts to 7.
		retrypool.WithAttempts[MyTask](7),
		// Use the custom delay function.
		retrypool.WithDelayFunc[MyTask](CustomDelayFunc[MyTask]),
	)

	// Submit tasks.
	for i := 1; i <= 9; i++ {
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
