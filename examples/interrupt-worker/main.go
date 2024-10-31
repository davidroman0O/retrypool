package main

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/davidroman0O/retrypool"
)

// Define a simple Worker implementation.
type MyWorker struct {
	ID int
}

func (w *MyWorker) Run(ctx context.Context, data int) error {
	log.Printf("Worker %d started processing task: %d\n", w.ID, data)
	// Simulate work
	select {
	case <-time.After(time.Duration(rand.Intn(500)+500) * time.Millisecond):
		log.Printf("Worker %d completed task: %d\n", w.ID, data)
		return nil
	case <-ctx.Done():
		log.Printf("Worker %d task %d was canceled\n", w.ID, data)
		return ctx.Err()
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Create a context for the pool
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create workers
	workers := []retrypool.Worker[int]{
		&MyWorker{ID: 0},
		&MyWorker{ID: 1},
		&MyWorker{ID: 2},
	}

	// Create the pool
	pool := retrypool.New[int](ctx, workers)

	// Dispatch tasks
	for i := 1; i <= 10; i++ {
		pool.Dispatch(i)
	}

	// Give some time for workers to start processing
	time.Sleep(1 * time.Second)

	// List current tasks
	pool.RangeTasks(func(data retrypool.TaskWrapper[int], workerID int, status retrypool.TaskStatus) bool {
		switch status {
		case retrypool.TaskStatusProcessing:
			log.Printf("Worker %d is processing task: %d\n", workerID, data)
		case retrypool.TaskStatusQueued:
			log.Printf("Task %d is queued and waiting for processing\n", data)
		}
		return true
	})

	// Interrupt worker 1, force panic, remove it, and reassign its task
	log.Printf("Interrupting worker 1\n")
	err := pool.InterruptWorker(1, retrypool.WithForcePanic(), retrypool.WithReassignTask())
	if err != nil {
		log.Println("Error interrupting worker:", err)
	}
	log.Printf("Worker 1 interrupted\n")

	<-time.After(2 * time.Second)

	// List current tasks again
	pool.RangeTasks(func(data retrypool.TaskWrapper[int], workerID int, status retrypool.TaskStatus) bool {
		switch status {
		case retrypool.TaskStatusProcessing:
			log.Printf("Worker %d is processing task: %d\n", workerID, data)
		case retrypool.TaskStatusQueued:
			log.Printf("Task %d is queued and waiting for processing\n", data)
		}
		return true
	})

	err = pool.RestartWorker(1)
	if err != nil {
		log.Println("Error restarting worker:", err)
	}
	log.Printf("Worker 1 restarted\n")

	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		log.Printf("Queue size: %d, Processing count: %d", queueSize, processingCount)
		return queueSize > 0 || processingCount > 0
	}, 1*time.Second)
}
