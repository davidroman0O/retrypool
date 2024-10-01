package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/davidroman0O/retrypool"
)

type RetryWorker struct{}

func (w *RetryWorker) Run(ctx context.Context, data TaskInt) error {
	if rand.Float32() < 0.5 {
		return fmt.Errorf("simulated error for data: %d", data)
	}
	fmt.Printf("Processed: %d\n", data)
	return nil
}

type TaskInt struct {
	Data int
}

func (t TaskInt) Hashcode() interface{} {
	return fmt.Sprintf("%d", t.Data)
}

func customDelayType[T retrypool.Hashable](n int, _ error, config *retrypool.Config[T]) time.Duration {
	// Use getters to access the config properties
	baseDelay := config.Delay()
	maxDelay := config.MaxDelay()
	maxJitter := config.MaxJitter()

	// Calculate a quadratic backoff
	delay := baseDelay * time.Duration(n*n)

	// Apply maxDelay if set
	if maxDelay > 0 && delay > maxDelay {
		delay = maxDelay
	}

	// Add jitter if maxJitter is set
	if maxJitter > 0 {
		jitter := time.Duration(rand.Int63n(int64(maxJitter)))
		delay += jitter
	}

	return delay
}

func main() {
	ctx := context.Background()
	workers := []retrypool.Worker[TaskInt]{&RetryWorker{}, &RetryWorker{}}
	pool := retrypool.New(ctx, workers,
		retrypool.WithAttempts[TaskInt](5),
		retrypool.WithDelay[TaskInt](100*time.Millisecond),
		retrypool.WithMaxDelay[TaskInt](2*time.Second),
		retrypool.WithMaxJitter[TaskInt](500*time.Millisecond),
		retrypool.WithDelayType[TaskInt](customDelayType[TaskInt]),
	)

	for i := 1; i <= 10; i++ {
		err := pool.Dispatch(TaskInt{i})
		if err != nil {
			log.Printf("Dispatch error: %v", err)
		}
	}

	pool.Close()
	fmt.Println("All tasks completed")

	deadTasks := pool.DeadTasks()
	for _, task := range deadTasks {
		fmt.Printf("Dead task: %+v\n", task)
	}
}
