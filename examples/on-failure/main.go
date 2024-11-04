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

// Run simulates task processing that fails.
func (w *MyWorker) Run(ctx context.Context, data MyTask) error {
	w.attempts++
	fmt.Printf("Worker processing task %d, attempt %d\n", data.ID, w.attempts)
	return errors.New("simulated error")
}

func main() {
	ctx := context.Background()

	// Define custom OnTaskFailure to control retry behavior.
	onTaskFailure := func(controller retrypool.WorkerController[MyTask], workerID int, worker retrypool.Worker[MyTask], task *retrypool.TaskWrapper[MyTask], err error) retrypool.DeadTaskAction {
		if task.Retries() >= 3 {
			fmt.Printf("Task %d failed after %d retries. Not retrying further.\n", task.Data().ID, task.Retries())
			return retrypool.DeadTaskActionAddToDeadTasks
		}
		fmt.Printf("Retrying task %d (retry %d)\n", task.Data().ID, task.Retries())
		return retrypool.DeadTaskActionRetry
	}

	// Initialize the retrypool with custom OnTaskFailure.
	pool := retrypool.New[MyTask](ctx, []retrypool.Worker[MyTask]{&MyWorker{}},
		retrypool.WithOnTaskFailure[MyTask](onTaskFailure),
	)

	// Dispatch a task.
	err := pool.Submit(MyTask{ID: 8})
	if err != nil {
		fmt.Printf("Failed to dispatch task: %v\n", err)
	}

	// Wait for all tasks to complete.
	pool.Shutdown()

	// Retrieve dead tasks.
	deadTasks := pool.DeadTasks()
	for _, dt := range deadTasks {
		fmt.Printf("Dead task %d after %d retries.\n", dt.Data.ID, dt.Retries)
	}
}
