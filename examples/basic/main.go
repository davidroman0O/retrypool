package main

import (
	"context"
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
	ID int // this is automatically set by the pool
}

// Run processes a task.
func (w *MyWorker) Run(ctx context.Context, task MyTask) error {
	fmt.Printf("Worker %d processing task with value %d\n", w.ID, task.Value)
	return nil
}

func main() {
	ctx := context.Background()

	// Create workers.
	workers := []retrypool.Worker[MyTask]{}
	for i := 0; i < 5; i++ {
		workers = append(workers, &MyWorker{})
	}

	// Create the pool.
	pool := retrypool.New(ctx, workers)
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
