package main

import (
	"context"
	"fmt"
	"time"

	"github.com/davidroman0O/retrypool"
)

// Define the data type for tasks
type TaskData struct {
	ID int
}

// Implement the Worker interface
type SimpleWorker struct{}

func (w *SimpleWorker) Run(ctx context.Context, data TaskData) error {
	fmt.Printf("Processing task ID: %d\n", data.ID)
	// Simulate work
	time.Sleep(time.Millisecond * 100)
	fmt.Printf("Completed task ID: %d\n", data.ID)
	return nil
}

func main() {
	ctx := context.Background()

	// Create a pool with one worker
	pool := retrypool.New[TaskData](ctx, []retrypool.Worker[TaskData]{&SimpleWorker{}})

	// Number of tasks to submit
	numTasks := 5

	for i := 1; i <= numTasks; i++ {
		taskID := i

		// Create notifications
		queuedNotif := retrypool.NewQueuedNotification()
		processedNotif := retrypool.NewProcessedNotification()

		// Submit task with notifications
		err := pool.Submit(TaskData{ID: taskID},
			retrypool.WithTaskQueuedNotification[TaskData](queuedNotif),
			retrypool.WithTaskProcessedNotification[TaskData](processedNotif),
		)
		if err != nil {
			fmt.Printf("Error submitting task ID %d: %v\n", taskID, err)
			continue
		}

		// Wait for the task to be queued
		go func(id int) {
			queuedNotif.Wait()
			fmt.Printf("Task ID %d has been queued.\n", id)
		}(taskID)

		// Wait for the task to be processed
		go func(id int) {
			processedNotif.Wait()
			fmt.Printf("Task ID %d has been processed.\n", id)
		}(taskID)
	}

	// Wait for all tasks to complete
	time.Sleep(time.Second * 2)

	// Close the pool
	pool.Close()
}
