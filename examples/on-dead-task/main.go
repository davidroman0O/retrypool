package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/davidroman0O/retrypool"
)

type MyTask struct {
	ID   int
	Data string
}

type MyWorker struct{}

func (w *MyWorker) Run(ctx context.Context, task *MyTask) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Millisecond * time.Duration(rand.Intn(50))):
		// Increase failure rate to 50%
		if rand.Float32() < 0.9 {
			return fmt.Errorf("task %d failed", task.ID)
		}
		fmt.Printf("Task %d completed successfully\n", task.ID)
		return nil
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workers := []retrypool.Worker[*MyTask]{
		&MyWorker{},
		&MyWorker{},
		&MyWorker{},
	}

	pool := retrypool.New(ctx, workers,
		retrypool.WithAttempts[*MyTask](5),
		retrypool.WithOnTaskFailure[*MyTask](func(controller retrypool.WorkerController[*MyTask], workerID int, worker retrypool.Worker[*MyTask], task *retrypool.TaskWrapper[*MyTask], err error) retrypool.DeadTaskAction {
			myTask := task.Data()
			fmt.Printf("Task %d failed (attempt %d): %v\n", myTask.ID, task.Retries()+1, err)

			if task.Retries() >= 2 {
				fmt.Printf("Task %d has failed too many times, adding to dead tasks\n", myTask.ID)
				return retrypool.DeadTaskActionAddToDeadTasks
			}
			return retrypool.DeadTaskActionRetry
		}),
		retrypool.WithOnNewDeadTask[*MyTask](func(deadTask *retrypool.DeadTask[*MyTask]) {
			fmt.Printf("New dead task: ID %d, Retries %d, Total Duration %v\n",
				deadTask.Data.ID, deadTask.Retries, deadTask.TotalDuration)
		}),
	)

	// Increase number of tasks to 20
	for i := 0; i < 20; i++ {
		task := &MyTask{ID: i, Data: fmt.Sprintf("Task data %d", i)}
		err := pool.Submit(task)
		if err != nil {
			log.Printf("Error dispatching task: %v", err)
		}
	}

	// Wait for tasks to complete
	err := pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		fmt.Printf("Tasks in queue: %d, Tasks processing: %d, Dead tasks: %d\n", queueSize, processingCount, deadTaskCount)
		return queueSize > 0 || processingCount > 0
	}, time.Second)

	if err != nil {
		log.Printf("Error waiting for tasks: %v", err)
	}

	fmt.Println("All tasks processed.")

	// Process dead tasks
	deadTaskCount := 0
	for {
		deadTask, err := pool.PullDeadTask(0)
		if err != nil {
			fmt.Printf("No more dead tasks available or error: %v\n", err)
			break
		}
		deadTaskCount++
		fmt.Printf("Retrieved dead task: ID %d, Retries %d, Errors: %v\n",
			deadTask.Data.ID, deadTask.Retries, deadTask.Errors)
		// Here you could attempt to process the dead task again or log it
	}

	fmt.Printf("Total dead tasks processed: %d\n", deadTaskCount)

	pool.Shutdown()
}
