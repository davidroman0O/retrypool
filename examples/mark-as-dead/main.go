package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/davidroman0O/retrypool"
)

// MyTask represents the data structure for tasks
type MyTask struct {
	ID       int
	Name     string
	Attempts int
}

func (t MyTask) Hashcode() interface{} {
	return fmt.Sprintf("%d", t.ID)
}

// MyWorker implements the Worker interface for MyTask
type MyWorker struct {
	workerID int
	mu       sync.Mutex
	taskRuns map[int]int // Keep track of how many times a task has been run
}

func (w *MyWorker) Run(ctx context.Context, data MyTask) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Initialize taskRuns map if nil
	if w.taskRuns == nil {
		w.taskRuns = make(map[int]int)
	}

	// Increment the run count for the task
	w.taskRuns[data.ID]++

	// Simulate task processing
	fmt.Printf("Worker %d processing task: %v (Attempt %d)\n", w.workerID, data, w.taskRuns[data.ID])
	// Simulate processing time
	// time.Sleep(500 * time.Millisecond)

	// Simulate an error for a specific task on the first attempt
	if data.ID == 2 && w.taskRuns[data.ID] <= 1 {
		return fmt.Errorf("simulated error on task %d", data.ID)
	}

	return nil
}

func main() {
	// Create a context for the pool
	ctx := context.Background()

	// Initialize workers
	workers := []retrypool.Worker[MyTask]{}
	for i := 1; i <= 3; i++ {
		workers = append(workers, &MyWorker{workerID: i})
	}

	// Create the pool with the workers and limit retries to 1 attempt
	pool := retrypool.New(ctx, workers, retrypool.WithAttempts[MyTask](1))

	// Dispatch tasks to the pool
	for i := 1; i <= 5; i++ {
		task := MyTask{
			ID:   i,
			Name: fmt.Sprintf("Task-%d", i),
		}
		err := pool.Dispatch(task)
		if err != nil {
			log.Printf("Failed to dispatch task %d: %v", i, err)
		}
	}

	// Wait briefly to let tasks start processing
	time.Sleep(500 * time.Millisecond)

	if pool.GetTaskStatus(MyTask{ID: 2}) != retrypool.TaskStatusDead {
		// Mark task ID 2 as dead
		err := pool.PutTaskAsDead(MyTask{ID: 2})
		if err != nil {
			fmt.Printf("Failed to mark task as dead: %v\n", err)
		} else {
			fmt.Printf("Task with ID %d has been marked as dead.\n", 2)
		}
	}

	// Retrieve dead tasks
	deadTasks := pool.DeadTasks()
	fmt.Printf("Dead tasks: %v\n", deadTasks)

	// Requeue a dead task
	if len(deadTasks) > 0 {
		taskToRequeue := deadTasks[0].Data
		err := pool.DispatchDeadTask(taskToRequeue)
		if err != nil {
			log.Printf("Failed to requeue dead task: %v", err)
		} else {
			fmt.Printf("Requeued dead task: %v\n", taskToRequeue)
		}
	}

	// Wait for tasks to complete
	pool.Close()
	fmt.Println("All tasks completed.")
}
