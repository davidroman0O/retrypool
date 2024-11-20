package main

import (
	"context"
	"log"
	"time"

	"github.com/davidroman0O/retrypool"
)

type MyWorker struct{}

func (w *MyWorker) Run(ctx context.Context, data int) error {
	// Simulate work
	select {
	case <-time.After(100 * time.Millisecond):
		// Work done
		return nil
	case <-ctx.Done():
		// Context canceled
		return ctx.Err()
	}
}

func main() {
	ctx := context.Background()

	workers := []retrypool.Worker[int]{&MyWorker{}, &MyWorker{}, &MyWorker{}}
	var pool *retrypool.Pool[int]

	pool = retrypool.New(ctx, workers, retrypool.WithOnTaskSuccess(func(controller retrypool.WorkerController[int], workerID int, worker retrypool.Worker[int], data int, retries int, totalDuration time.Duration, timeLimit time.Duration, maxDuration time.Duration, scheduledTime time.Time, triedWorkers map[int]bool, errors []error, durations []time.Duration, queuedAt []time.Time, processedAt []time.Time) {
		log.Printf("Worker %d processed data %d\n", workerID, data)
		if workerID == 1 {
			log.Println("Remove worker 1")
			controller.RemovalWorker(workerID)
			<-time.After(1 * time.Second)
			log.Println("Add worker 3")
			pool.AddWorker(&MyWorker{})
		}
	}))

	// Dispatch initial tasks
	for i := 0; i < 10; i++ {
		pool.Submit(i)
	}

	// Pause worker 1
	<-time.After(1 * time.Second)

	// Dispatch more tasks
	for i := 10; i < 20; i++ {
		pool.Submit(i)
	}

	// Wait for a while
	<-time.After(1 * time.Second)

	// Dispatch more tasks
	for i := 20; i < 30; i++ {
		pool.Submit(i)
	}

	// Wait for tasks to complete
	pool.WaitWithCallback(context.Background(), func(queueSize, processingCount, deadTaskCount int) bool {
		log.Printf("Queue size: %d, Processing count: %d\n", queueSize, processingCount)
		return queueSize == 0 && processingCount == 0
	}, 1*time.Second)
	pool.Shutdown()
}
