package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/davidroman0O/retrypool"
)

type SlowWorker struct{}

func (w *SlowWorker) Run(ctx context.Context, data int) error {
	select {
	case <-time.After(time.Duration(data) * time.Second):
		fmt.Printf("Processed: %d\n", data)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	workers := []retrypool.Worker[int]{&SlowWorker{}, &SlowWorker{}}
	pool := retrypool.New(ctx, workers)

	for i := 1; i <= 10; i++ {
		err := pool.Submit(i)
		if err != nil {
			log.Printf("Dispatch error: %v", err)
		}
	}

	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		fmt.Printf("Queue size: %d, Processing: %d\n", queueSize, processingCount)
		return queueSize > 0 || processingCount > 0
	}, 500*time.Millisecond)

	fmt.Println("Context cancelled or all tasks completed")

	deadTasks := pool.DeadTasks()
	for _, task := range deadTasks {
		fmt.Printf("Dead task: %+v\n", task)
	}
}
