package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/davidroman0O/retrypool"
)

// SimulatedTask represents a task with an ID and a duration
type SimulatedTask struct {
	ID       int
	Duration time.Duration
}

// SimulatedWorker simulates work by sleeping for the task's duration
type SimulatedWorker struct{}

func (w *SimulatedWorker) Run(ctx context.Context, task SimulatedTask) error {
	select {
	case <-time.After(task.Duration):
		fmt.Printf("Task %d completed after %v\n", task.ID, task.Duration)
		return nil
	case <-ctx.Done():
		fmt.Printf("Task %d was cancelled after %v\n", task.ID, task.Duration)
		return ctx.Err()
	}
}

func main() {
	ctx := context.Background()
	workers := []retrypool.Worker[SimulatedTask]{&SimulatedWorker{}, &SimulatedWorker{}}

	pool := retrypool.NewPool(ctx, workers,
		retrypool.WithAttempts[SimulatedTask](1), // No retries for this example
		retrypool.WithOnTaskSuccess[SimulatedTask](func(_ retrypool.WorkerController, _ int, _ retrypool.Worker[SimulatedTask], task *retrypool.TaskWrapper[SimulatedTask]) {
			fmt.Printf("Task %d succeeded\n", task.Data().ID)
		}),
		retrypool.WithOnTaskFailure[SimulatedTask](func(_ retrypool.WorkerController, _ int, _ retrypool.Worker[SimulatedTask], task *retrypool.TaskWrapper[SimulatedTask], err error) {
			fmt.Printf("Task %d failed: %v\n", task.Data().ID, err)
		}),
	)

	// Dispatch tasks with different durations and time limits
	for i := 1; i <= 10; i++ {
		taskDuration := time.Duration(rand.Intn(5)+1) * time.Second
		timeLimit := time.Duration(rand.Intn(5)+1) * time.Second

		task := SimulatedTask{ID: i, Duration: taskDuration}
		err := pool.Dispatch(task, retrypool.WithTimeLimit[SimulatedTask](timeLimit))
		if err != nil {
			log.Printf("Failed to dispatch task %d: %v", i, err)
		} else {
			fmt.Printf("Dispatched task %d with duration %v and time limit %v\n", i, taskDuration, timeLimit)
		}
	}

	// Wait for all tasks to complete
	err := pool.WaitWithCallback(ctx, func(queueSize, processingCount int) bool {
		fmt.Printf("Queue size: %d, Processing: %d\n", queueSize, processingCount)
		return queueSize > 0 || processingCount > 0
	}, 500*time.Millisecond)

	if err != nil {
		log.Printf("Error while waiting for tasks to complete: %v", err)
	}

	pool.Close()

	// Print dead tasks
	deadTasks := pool.DeadTasks()
	fmt.Printf("\nDead tasks: %d\n", len(deadTasks))
	for _, task := range deadTasks {
		fmt.Printf("Task %d failed after %v. Errors: %v\n", task.Data.ID, task.TotalDuration, task.Errors)
	}
}
