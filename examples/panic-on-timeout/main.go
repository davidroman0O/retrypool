package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/davidroman0O/retrypool"
)

// SlowWorker is a worker that deliberately takes longer than the specified timeout
type SlowWorker struct{}

func (w *SlowWorker) Run(ctx context.Context, data interface{}) error {
	fmt.Println("Worker started, sleeping for 5 seconds...")
	select {
	case <-time.After(5 * time.Second):
		fmt.Println("Worker completed")
		return nil
	case <-ctx.Done():
		fmt.Println("Worker interrupted")
		return ctx.Err()
	}
}

func main() {
	// Create a new pool with a single worker
	pool := retrypool.New(context.Background(), []retrypool.Worker[interface{}]{&SlowWorker{}}, retrypool.WithAttempts[interface{}](1))
	defer pool.Close()

	// Dispatch a task with a 2-second timeout and panic on timeout enabled
	err := pool.Dispatch(
		"example task",
		retrypool.WithMaxDuration[interface{}](2*time.Second),
		retrypool.WithPanicOnTimeout[interface{}](),
	)
	if err != nil {
		log.Fatalf("Failed to dispatch task: %v", err)
	}

	// Use a recovery function to catch the panic
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Caught panic:", r)
		}
	}()

	// Wait for the task to complete or panic
	err = pool.WaitWithCallback(context.Background(), func(queueSize, processingCount, deadTaskCount int) bool {
		fmt.Printf("Queue size: %d, Processing count: %d\n", queueSize, processingCount)
		return queueSize > 0 || processingCount > 0
	}, 500*time.Millisecond)

	if err != nil {
		log.Fatalf("Error waiting for tasks to complete: %v", err)
	}

	fmt.Println("All tasks completed")
}
