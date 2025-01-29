package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/davidroman0O/retrypool"
)

// Define a custom worker type
type MyWorker struct {
	ID int
}

// Implement the Worker interface
func (w *MyWorker) Run(ctx context.Context, data int) error {
	fmt.Printf("Worker %d processing task with data: %d\n", w.ID, data)

	// Simulate 75% failure
	if rand.Intn(4) < 3 {
		err := fmt.Errorf("random failure on worker %d", w.ID)
		fmt.Println(err)
		return err
	}

	fmt.Printf("Worker %d completed task with data: %d\n", w.ID, data)
	return nil
}

func main() {
	rand.Seed(time.Now().UnixNano())

	ctx := context.Background()

	// Create a pool with two workers
	pool := retrypool.New[int](ctx, []retrypool.Worker[int]{&MyWorker{}, &MyWorker{}},
		retrypool.WithAttempts[int](3), // Set max attempts to 3
		retrypool.WithOnTaskAttempt(func(task *retrypool.Task[int], workerID int) {
			fmt.Println(workerID, task.GetMetadata(), task.GetAttemptedWorkers()) // Showing how to bounce from worker to worker
		}),
	)

	// Submit tasks with immediate retry
	fmt.Println("Submitting tasks with immediate retry:")
	for i := 1; i <= 5; i++ {
		m := retrypool.NewMetadata()
		m.Set("task", i)
		// All those tasks will stay fixed on their worker but they will try to retry by taking the first position in the taskqueue of their worker (taskqueue != current task)
		err := pool.Submit(i, retrypool.WithTaskImmediateRetry[int](), retrypool.WithTaskMetadata[int](m))
		if err != nil {
			fmt.Println("Error submitting task:", err)
		}
	}

	// Submit tasks with bounce retry
	fmt.Println("\nSubmitting tasks with bounce retry:")
	for i := 6; i <= 10; i++ {
		// All those tasks will bounce from worker to worker every time they fail, so you will see an array of IDs from the callback up there
		err := pool.Submit(i, retrypool.WithTaskBounceRetry[int]())
		if err != nil {
			fmt.Println("Error submitting task:", err)
		}
	}

	// Wait for all tasks to complete
	err := pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize+processingCount > 0
	}, time.Second)

	if err != nil {
		fmt.Println("Error waiting for tasks:", err)
	}

	// Print dead tasks
	fmt.Println("\nDead tasks:")
	pool.RangeDeadTasks(func(dt *retrypool.DeadTask[int]) bool {
		fmt.Printf("Dead task with data: %d, errors: %v\n", dt.Data, dt.Errors)
		return true
	})

	// Close the pool
	pool.Close()
}
