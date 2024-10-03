package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/davidroman0O/retrypool"
)

type Task struct {
	ID             int
	ImmediateRetry bool
}

type DemoWorker struct {
	ID        int
	mu        sync.Mutex
	processed map[int]int
}

func (w *DemoWorker) Run(ctx context.Context, task Task) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.processed[task.ID]++
	count := w.processed[task.ID]

	if rand.Float32() < 0.95 { // 70% chance of failure
		log.Printf("Worker %d: Task %d failed (running attempt %d on worker %d)", w.ID, task.ID, count, w.ID)
		return fmt.Errorf("simulated failure")
	}

	log.Printf("Worker %d: Task %d succeeded (running attempt %d on worker %d)", w.ID, task.ID, count, w.ID)
	return nil
}

func main() {
	rand.Seed(time.Now().UnixNano())
	ctx := context.Background()

	numWorkers := 3
	workers := make([]retrypool.Worker[Task], numWorkers)
	for i := 0; i < numWorkers; i++ {
		workers[i] = &DemoWorker{
			ID:        i,
			processed: make(map[int]int),
		}
	}

	pool := retrypool.New(ctx, workers,
		retrypool.WithAttempts[Task](retrypool.UnlimitedAttempts),
		retrypool.WithDelay[Task](50*time.Millisecond),
		retrypool.WithDelayType[Task](retrypool.CombineDelay[Task](
			retrypool.FixedDelay[Task],
		)),
		retrypool.WithOnRetry[Task](func(attempt int, err error, task *retrypool.TaskWrapper[Task]) {
			workers := task.TriedWorkers()
			tried := []int{}
			for k, v := range workers {
				if v {
					tried = append(tried, k)
				}
			}
			log.Printf("Retrying task %d (attempt %d - %v): %v", task.Data().ID, attempt, tried, err)
		}),
	)

	numTasks := 5
	for i := 0; i < numTasks; i++ {
		task := Task{
			ID:             i,
			ImmediateRetry: true, // Even-numbered tasks use immediate retry
		}
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
	err := pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		log.Printf("Queue size: %d, Processing: %d", queueSize, processingCount)
		return queueSize > 0 || processingCount > 0
	}, 500*time.Millisecond)

	if err != nil {
		log.Printf("Error while waiting for tasks to complete: %v", err)
	}

	pool.Close()

	// Print results
	for i, w := range workers {
		demoWorker := w.(*DemoWorker)
		log.Printf("Worker %d processed:", i)
		for taskID, count := range demoWorker.processed {
			log.Printf("  Task %d: %d times", taskID, count)
		}
	}

	deadTasks := pool.DeadTasks()
	log.Printf("Dead tasks: %d", len(deadTasks))
	for _, dt := range deadTasks {
		log.Printf("Dead task %d - Retries: %d, Total Duration: %v", dt.Data.ID, dt.Retries, dt.TotalDuration)
	}
}
