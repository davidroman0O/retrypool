package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/davidroman0O/retrypool"
	"github.com/davidroman0O/retrypool/logs"

	"math/rand"
)

// MyTask represents the data structure for the task.
type MyTask struct {
	ID int
}

// MyWorker implements the retrypool.Worker interface.
type MyWorker struct {
	attempts int
}

// Run simulates task processing that always fails.
func (w *MyWorker) Run(ctx context.Context, data MyTask) error {
	w.attempts++
	fmt.Printf("Worker processing task %d, attempt %d\n", data.ID, w.attempts)
	return errors.New("simulated task failure")
}

func main() {
	ctx := context.Background()

	// Configure exponential backoff with jitter.
	delayType := func(n int, _ error, config *retrypool.Config[MyTask]) time.Duration {
		baseDelay := config.Delay()
		maxDelay := config.MaxDelay()

		// Exponential backoff
		delay := baseDelay * (1 << n)
		if delay > maxDelay {
			delay = maxDelay
		}

		// Add jitter
		jitter := time.Duration(rand.Int63n(int64(config.MaxJitter())))
		return delay + jitter
	}

	// Initialize the retrypool with one worker and custom delayType.
	pool := retrypool.New[MyTask](ctx, []retrypool.Worker[MyTask]{&MyWorker{}},
		retrypool.WithAttempts[MyTask](2),
		retrypool.WithDelay[MyTask](500*time.Millisecond),
		retrypool.WithMaxDelay[MyTask](5*time.Second),
		retrypool.WithMaxJitter[MyTask](500*time.Millisecond),
		retrypool.WithDelayType[MyTask](delayType),
		retrypool.WithLogLevel[MyTask](logs.LevelDebug),
	)

	// Dispatch a task.
	err := pool.Submit(MyTask{ID: 3})
	if err != nil {
		fmt.Printf("Failed to dispatch task: %v\n", err)
	}

	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		fmt.Printf("Queue size: %d, processing count: %d, dead task count: %d\n", queueSize, processingCount, deadTaskCount)
		return queueSize > 0
	}, time.Second)

	// Wait for all tasks to complete.
	pool.Shutdown()
}
