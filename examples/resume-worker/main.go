package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/davidroman0O/retrypool"
)

type TaskInt struct {
	Data int
}

func (t TaskInt) Hashcode() interface{} {
	return fmt.Sprintf("%d", t.Data)
}

type MyWorker struct{}

func (w *MyWorker) Run(ctx context.Context, data TaskInt) error {
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

	workers := []retrypool.Worker[TaskInt]{&MyWorker{}, &MyWorker{}, &MyWorker{}}
	var pool *retrypool.Pool[TaskInt]

	pool = retrypool.New(ctx, workers, retrypool.WithOnTaskSuccess(func(controller retrypool.WorkerController[TaskInt], workerID int, worker retrypool.Worker[TaskInt], task *retrypool.TaskWrapper[TaskInt]) {
		log.Printf("Worker %d processed data %d\n", workerID, task.Data())
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
		pool.Dispatch(TaskInt{i})
	}

	// Pause worker 1
	<-time.After(1 * time.Second)

	// Dispatch more tasks
	for i := 10; i < 20; i++ {
		pool.Dispatch(TaskInt{i})
	}

	// Wait for a while
	<-time.After(1 * time.Second)

	// Dispatch more tasks
	for i := 20; i < 30; i++ {
		pool.Dispatch(TaskInt{i})
	}

	// Wait for tasks to complete
	pool.WaitWithCallback(context.Background(), func(queueSize, processingCount int) bool {
		log.Printf("Queue size: %d, Processing count: %d\n", queueSize, processingCount)
		return queueSize == 0 && processingCount == 0
	}, 1*time.Second)
	pool.Close()
}
