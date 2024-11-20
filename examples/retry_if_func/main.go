package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/davidroman0O/retrypool"
)

// Custom error types
var (
	ErrTemporary = errors.New("temporary error")
	ErrCritical  = errors.New("critical error")
)

type CustomWorker struct{}

func (w *CustomWorker) Run(ctx context.Context, data int) error {
	rand.Seed(time.Now().UnixNano())
	switch rand.Intn(3) {
	case 0:
		return nil // Success
	case 1:
		return fmt.Errorf("temporary error occurred: %w", ErrTemporary)
	default:
		return fmt.Errorf("critical error occurred: %w", ErrCritical)
	}
}

// CustomRetryIfFunc demonstrates how to use RetryIfFunc
func CustomRetryIfFunc(err error) bool {
	// Retry on temporary errors, but not on critical errors
	return errors.Is(err, ErrTemporary)
}

func main() {
	ctx := context.Background()
	workers := []retrypool.Worker[int]{&CustomWorker{}, &CustomWorker{}}

	pool := retrypool.New(ctx, workers,
		retrypool.WithAttempts[int](5),
		retrypool.WithDelay[int](100*time.Millisecond),
		retrypool.WithRetryIf[int](CustomRetryIfFunc),
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

	pool.Shutdown()
	fmt.Println("All tasks completed")

	deadTasks := pool.DeadTasks()
	for _, task := range deadTasks {
		fmt.Printf("Dead task: %+v\n", task)
	}
}
