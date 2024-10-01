package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/davidroman0O/retrypool"
)

// ExampleWorker implements the Worker interface
type ExampleWorker struct {
	ID int
}

// Randomly simulate a task that can either succeed or take too long
func (w *ExampleWorker) Run(ctx context.Context, data TaskStr) error {
	log.Printf("Worker %d processing task: %s", w.ID, data)

	// Random task duration between 1 to 4 seconds
	taskDuration := time.Duration(rand.Intn(4)+1) * time.Second

	select {
	case <-time.After(taskDuration): // Simulate task work
		// Randomly decide if task should succeed or fail
		if rand.Float32() < 0.5 { // 50% chance of success
			log.Printf("Worker %d successfully completed task: %s", w.ID, data)
			return nil
		} else {
			err := errors.New("random task failure")
			log.Printf("Worker %d task failed: %s, error: %v", w.ID, data, err)
			return err
		}
	case <-ctx.Done(): // If maxDuration or timeLimit is exceeded, cancel the task
		log.Printf("Worker %d task canceled: %s due to exceeded duration", w.ID, data)
		return ctx.Err()
	}
}

type TaskStr struct {
	Data string
}

func (t TaskStr) Hashcode() interface{} {
	return fmt.Sprintf("%d", t.Data)
}

func main() {
	rand.Seed(time.Now().UnixNano()) // Seed the random generator

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create workers
	workers := []retrypool.Worker[TaskStr]{
		&ExampleWorker{ID: 1},
		&ExampleWorker{ID: 2},
	}

	// Initialize pool with workers
	pool := retrypool.New(ctx, workers)

	// Dispatch multiple tasks with some randomness
	for i := 1; i <= 5; i++ {
		taskData := fmt.Sprintf("Task-%d", i)
		err := pool.Dispatch(
			TaskStr{taskData},
			retrypool.WithMaxDuration[TaskStr](2*time.Second), // Max duration per attempt
			retrypool.WithTimeLimit[TaskStr](5*time.Second),   // Overall time limit across retries
		)
		if err != nil {
			log.Fatalf("Failed to dispatch task: %v", err)
		}
	}

	// Wait for the pool to finish processing tasks with a callback
	err := pool.WaitWithCallback(ctx, func(queueSize, processingCount int) bool {
		log.Printf("Queue size: %d, Processing count: %d", queueSize, processingCount)
		return queueSize > 0 || processingCount > 0
	}, 1*time.Second)

	if err != nil {
		log.Fatalf("Error during pool wait: %v", err)
	}

	// List dead tasks (if any)
	deadTasks := pool.DeadTasks()
	if len(deadTasks) > 0 {
		log.Println("Dead tasks:")
		for _, task := range deadTasks {
			log.Printf("Task data: %v, Retries: %d, Errors: %v", task.Data, task.Retries, task.Errors)
		}
	} else {
		log.Println("No dead tasks.")
	}

	log.Println("All tasks processed.")
}
