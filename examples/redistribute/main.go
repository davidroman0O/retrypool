package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/davidroman0O/retrypool"
)

type SimpleWorker struct{}

func (w *SimpleWorker) Run(ctx context.Context, data int) error {
	time.Sleep(time.Duration(data) * time.Millisecond)
	fmt.Printf("Processed: %d\n", data)
	return nil
}

func main() {
	ctx := context.Background()
	workers := []retrypool.Worker[int]{&SimpleWorker{}}
	pool := retrypool.New(ctx, workers)

	err := pool.Submit(100)
	if err != nil {
		log.Printf("Dispatch error: %v", err)
	}

	err = pool.Submit(200)
	if err != nil {
		log.Printf("Dispatch error: %v", err)
	}

	err = pool.Submit(300)
	if err != nil {
		log.Printf("Dispatch error: %v", err)
	}

	err = pool.Submit(400)
	if err != nil {
		log.Printf("Dispatch error: %v", err)
	}

	pool.RangeTasks(func(data *retrypool.TaskWrapper[int], workerID int, status retrypool.TaskStatus) bool {
		fmt.Println("\tTask data", data.Data(), "Worker ID", workerID, "Status", status.String())
		return true
	})

	fmt.Println("Available workers", pool.AvailableWorkers())
	fmt.Println("Worker counts", pool.GetWorkerCount())

	pool.AddWorker(&SimpleWorker{})

	pool.RangeTasks(func(data *retrypool.TaskWrapper[int], workerID int, status retrypool.TaskStatus) bool {
		fmt.Println("\tTask data", data.Data(), "Worker ID", workerID, "Status", status.String())
		return true
	})

	fmt.Println("Available workers", pool.AvailableWorkers())
	fmt.Println("Worker counts", pool.GetWorkerCount())

	fmt.Println("redistribute tasks")
	pool.RedistributeTasks()

	pool.RangeTasks(func(data *retrypool.TaskWrapper[int], workerID int, status retrypool.TaskStatus) bool {
		fmt.Println("\tTask data", data.Data(), "Worker ID", workerID, "Status", status.String())
		return true
	})

	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		fmt.Printf("Queue size: %d, Processing count: %d, Dead task count: %d\n", queueSize, processingCount, deadTaskCount)
		return queueSize > 0 || processingCount > 0
	}, time.Second)

	pool.Shutdown()
	fmt.Println("All tasks completed")
}
