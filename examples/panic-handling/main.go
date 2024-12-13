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
type PanicWorker struct{}

func (w *PanicWorker) Run(ctx context.Context, data TaskData) error {
	if data.ID == 3 {
		// Intentionally cause a panic
		panic(fmt.Sprintf("Intentional panic in task ID: %d", data.ID))
	}
	fmt.Printf("Processing task ID: %d\n", data.ID)
	time.Sleep(time.Millisecond * 100)
	fmt.Printf("Completed task ID: %d\n", data.ID)
	return nil
}

func main() {
	ctx := context.Background()

	// Define panic handlers
	onPanic := func(recovery interface{}, stackTrace string) {
		fmt.Printf("Panic recovered: %v\nStack Trace:\n%s\n", recovery, stackTrace)
	}

	onWorkerPanic := func(workerID int, recovery interface{}, stackTrace string) {
		fmt.Printf("Panic in worker %d: %v\nStack Trace:\n%s\n", workerID, recovery, stackTrace)
	}

	// Create a pool with one worker and panic handlers
	pool := retrypool.New[TaskData](
		ctx,
		[]retrypool.Worker[TaskData]{&PanicWorker{}},
		retrypool.WithAttempts[TaskData](2), // otherwise we will retry FOREVER
		retrypool.WithOnPanic[TaskData](onPanic),
		retrypool.WithOnWorkerPanic[TaskData](onWorkerPanic),
	)

	// Number of tasks to submit
	numTasks := 5

	for i := 1; i <= numTasks; i++ {
		taskID := i
		err := pool.Submit(TaskData{ID: taskID})
		if err != nil {
			fmt.Printf("Error submitting task ID %d: %v\n", taskID, err)
			continue
		}
	}

	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, time.Second/4)

	pool.RangeDeadTasks(func(dt *retrypool.DeadTask[TaskData]) bool {
		fmt.Printf("Dead task ID %d: %v\n", dt.Data.ID, dt.Errors)
		return true
	})

	// Close the pool
	pool.Close()
}
