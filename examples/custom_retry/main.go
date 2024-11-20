package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/davidroman0O/retrypool"
)

type FlakyWorker struct{}

func (w *FlakyWorker) Run(ctx context.Context, data int) error {
	if data%3 == 0 {
		return fmt.Errorf("simulated error for data: %d", data)
	}
	fmt.Printf("Processed: %d\n", data)
	return nil
}

func main() {
	ctx := context.Background()
	workers := []retrypool.Worker[int]{&FlakyWorker{}, &FlakyWorker{}}
	pool := retrypool.New(ctx, workers,
		retrypool.WithAttempts[int](3),
		retrypool.WithDelay[int](time.Second),
		retrypool.WithOnRetry[int](func(err error, data int, retries int, totalDuration time.Duration, timeLimit time.Duration, maxDuration time.Duration, scheduledTime time.Time, triedWorkers map[int]bool, errors []error, durations []time.Duration, queuedAt []time.Time, processedAt []time.Time) {
			log.Printf("Retrying task %d, attempt %d: %v", data, retries, err)
		}),
	)

	for i := 1; i <= 10; i++ {
		err := pool.Submit(i)
		if err != nil {
			log.Printf("Dispatch error: %v", err)
		}
	}

	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		fmt.Printf("Queue size: %d, processing count: %d, dead task count: %d\n", queueSize, processingCount, deadTaskCount)
		return queueSize > 0
	}, time.Second)

	pool.Shutdown()
	fmt.Println("All tasks completed")

	deadTasks := pool.DeadTasks()
	for _, task := range deadTasks {
		fmt.Printf("Dead task: %+v\n", task)
	}
}
