package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/davidroman0O/retrypool"
)

// MyTask represents the data structure for the task.
type MyTask struct {
	ID int
}

// MyWorker implements the retrypool.Worker interface.
type MyWorker struct {
	attempts int
}

// Run simulates task processing that may return an unrecoverable error.
func (w *MyWorker) Run(ctx context.Context, data MyTask) error {
	w.attempts++
	if w.attempts == 3 {
		fmt.Printf("Worker processing task %d, attempt %d: unrecoverable error.\n", data.ID, w.attempts)
		return retrypool.Unrecoverable(errors.New("unrecoverable error"))
	}
	fmt.Printf("Worker processing task %d, attempt %d: recoverable error.\n", data.ID, w.attempts)
	return errors.New("recoverable error")
}

func main() {
	ctx := context.Background()

	// Initialize the retrypool with one worker.
	pool := retrypool.New[MyTask](ctx, []retrypool.Worker[MyTask]{&MyWorker{}})

	// Dispatch a task.
	err := pool.Dispatch(MyTask{ID: 4})
	if err != nil {
		fmt.Printf("Failed to dispatch task: %v\n", err)
	}

	// Wait for all tasks to complete.
	pool.Close()

	// Retrieve dead tasks.
	deadTasks := pool.DeadTasks()
	for _, dt := range deadTasks {
		fmt.Printf("Dead task %d after %d retries: %v\n", dt.Data.ID, dt.Retries, dt.Errors)
	}
}
