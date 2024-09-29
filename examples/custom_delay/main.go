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

func (w *RetryWorker) Run(ctx context.Context, data int) error {
	if rand.Float32() < 0.5 {
		return fmt.Errorf("simulated error for data: %d", data)
	}
	fmt.Printf("Processed: %d\n", data)
	return nil
}

func customDelayType[T any](n int, _ error, config *retrypool.Config[T]) time.Duration {
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
	workers := []retrypool.Worker[int]{&RetryWorker{}, &RetryWorker{}}
	pool := retrypool.New(ctx, workers,
		retrypool.WithAttempts[int](5),
		retrypool.WithDelay[int](100*time.Millisecond),
		retrypool.WithMaxDelay[int](2*time.Second),
		retrypool.WithMaxJitter[int](50*time.Millisecond),
		retrypool.WithDelayType[int](customDelayType[int]),
	)

	for i := 1; i <= 10; i++ {
		err := pool.Dispatch(i)
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
