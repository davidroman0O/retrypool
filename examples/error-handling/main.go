package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/davidroman0O/retrypool"
)

// Define the data type for tasks
type TaskData struct {
	ID int
}

// Implement the Worker interface
type ErrorWorker struct{}

func (w *ErrorWorker) Run(ctx context.Context, data TaskData) error {
	// Simulate some work
	time.Sleep(time.Millisecond * 100)
	fmt.Printf("Processed task ID: %d\n", data.ID)
	return nil
}

func main() {
	ctx := context.Background()

	// Create the pool with a maximum queue size of 5
	pool := retrypool.New[TaskData](
		ctx,
		[]retrypool.Worker[TaskData]{&ErrorWorker{}},
		retrypool.WithMaxQueueSize[TaskData](5),
	)

	// Submit tasks exceeding the maximum queue size
	for i := 1; i <= 10; i++ {
		taskID := i
		err := pool.Submit(TaskData{ID: taskID})
		if err != nil {
			if errors.Is(err, retrypool.ErrMaxQueueSizeExceeded) {
				fmt.Printf("Error submitting task ID %d: Queue size exceeded.\n", taskID)
			} else {
				fmt.Printf("Error submitting task ID %d: %v\n", taskID, err)
			}
		} else {
			fmt.Printf("Submitted task ID: %d\n", taskID)
		}
	}

	// Close the pool
	pool.Close()

	// Attempt to submit a task after closing the pool
	err := pool.Submit(TaskData{ID: 11})
	if err != nil {
		if errors.Is(err, retrypool.ErrPoolClosed) {
			fmt.Printf("Error submitting task ID 11: Pool is closed.\n")
		} else {
			fmt.Printf("Error submitting task ID 11: %v\n", err)
		}
	}

	// Wait for remaining tasks to complete
	time.Sleep(time.Second * 2)
}
