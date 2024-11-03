package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/davidroman0O/retrypool"
)

// MyTask represents the data structure for the task.
type MyTask struct {
	ID int
}

// MyWorker implements the retrypool.Worker interface.
type MyWorker struct{}

// Run simulates a panic during task processing.
func (w *MyWorker) Run(ctx context.Context, data MyTask) error {
	panic(fmt.Sprintf("panic in task %d", data.ID))
}

func main() {
	ctx := context.Background()

	// Define a custom panic handler.
	panicHandler := func(task MyTask, v interface{}, stackTrace string) {
		fmt.Printf("Recovered from panic in task %d: %v\n", task.ID, v)
		fmt.Printf("Stack trace:\n%s\n", stackTrace)
	}

	// Initialize the retrypool with one worker and custom panic handler.
	pool := retrypool.New[MyTask](ctx, []retrypool.Worker[MyTask]{&MyWorker{}},
		retrypool.WithPanicHandler[MyTask](panicHandler),
		retrypool.WithAttempts[MyTask](3),
	)

	// Dispatch a task.
	err := pool.Submit(MyTask{ID: 6})
	if err != nil {
		fmt.Printf("Failed to dispatch task: %v\n", err)
	}

	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		log.Printf("Queue size: %d, Processing count: %d", queueSize, processingCount)
		return queueSize > 0 || processingCount > 0
	}, 1*time.Second)

	// Wait for all tasks to complete.
	pool.Shutdown()
}
