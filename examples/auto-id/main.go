package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/davidroman0O/retrypool"
)

type SimpleWorker struct {
	ID int
}

func (w *SimpleWorker) Run(ctx context.Context, data int) error {
	fmt.Println("Worker ID", w.ID)
	fmt.Println("Worker Context ID", ctx.Value(retrypool.WorkerIDKey))
	return nil
}

func main() {
	ctx := context.Background()
	workers := []retrypool.Worker[int]{&SimpleWorker{}, &SimpleWorker{}}
	pool := retrypool.New(ctx, workers)

	for i := 0; i < 10; i++ {
		err := pool.Submit(i)
		if err != nil {
			log.Printf("Dispatch error: %v", err)
		}
	}

	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		fmt.Println("Queue size", queueSize, "Processing count", processingCount, "Dead task count", deadTaskCount)
		return queueSize > 0 || processingCount > 0
	}, time.Second)

	pool.Shutdown()
	fmt.Println("All tasks completed")
}
