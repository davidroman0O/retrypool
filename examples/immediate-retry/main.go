package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/davidroman0O/retrypool"
)

type Task struct {
	ID             int
	AttemptCount   int
	ImmediateRetry bool
}

type Worker struct {
	ID             int
	SuccessCount   int32
	FailureCount   int32
	ProcessedTasks map[int]bool
}

func (w *Worker) Run(ctx context.Context, task Task) error {
	w.ProcessedTasks[task.ID] = true
	task.AttemptCount++

	// Simulate work
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)

	if rand.Float32() < 0.3 { // 30% chance of failure
		atomic.AddInt32(&w.FailureCount, 1)
		return fmt.Errorf("task %d failed on worker %d, attempt %d", task.ID, w.ID, task.AttemptCount)
	}

	atomic.AddInt32(&w.SuccessCount, 1)
	return nil
}

func main() {
	rand.Seed(time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	workers := make([]retrypool.Worker[Task], 3)
	for i := range workers {
		workers[i] = &Worker{ID: i, ProcessedTasks: make(map[int]bool)}
	}

	pool := retrypool.NewPool(ctx, workers,
		retrypool.WithAttempts[Task](5),
		retrypool.WithDelay[Task](100*time.Millisecond),
		retrypool.WithMaxJitter[Task](50*time.Millisecond),
		retrypool.WithOnRetry[Task](func(attempt int, err error, task Task) {
			log.Printf("Retrying task %d, attempt %d: %v", task.ID, attempt, err)
		}),
	)

	// Dispatch tasks
	for i := 0; i < 20; i++ {
		task := Task{ID: i, ImmediateRetry: i%2 == 0} // Even-numbered tasks use immediate retry
		var err error
		if task.ImmediateRetry {
			err = pool.Dispatch(task, retrypool.WithImmediateRetry[Task]())
		} else {
			err = pool.Dispatch(task)
		}
		if err != nil {
			log.Printf("Failed to dispatch task %d: %v", i, err)
		}
	}

	// Wait for all tasks to complete
	err := pool.WaitWithCallback(ctx, func(queueSize, processingCount int) bool {
		log.Printf("Queue size: %d, Processing: %d", queueSize, processingCount)
		return queueSize > 0 || processingCount > 0
	}, 500*time.Millisecond)

	if err != nil {
		log.Printf("Pool wait error: %v", err)
	}

	pool.Close()

	// Print results
	for i, w := range workers {
		worker := w.(*Worker)
		log.Printf("Worker %d - Successes: %d, Failures: %d, Unique tasks: %d",
			i, worker.SuccessCount, worker.FailureCount, len(worker.ProcessedTasks))
	}

	deadTasks := pool.DeadTasks()
	log.Printf("Dead tasks: %d", len(deadTasks))
	for _, dt := range deadTasks {
		log.Printf("Dead task %d - Retries: %d, Total Duration: %v", dt.Data.ID, dt.Retries, dt.TotalDuration)
	}
}
