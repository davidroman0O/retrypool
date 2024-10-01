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

type TaskInt struct {
	Data int
}

func (t TaskInt) Hashcode() interface{} {
	return fmt.Sprintf("%d", t.Data)
}

// Custom error types
var (
	ErrTemporary = errors.New("temporary error")
	ErrCritical  = errors.New("critical error")
)

type CustomWorker struct{}

func (w *CustomWorker) Run(ctx context.Context, data TaskInt) error {
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
	workers := []retrypool.Worker[TaskInt]{&CustomWorker{}, &CustomWorker{}}

	pool := retrypool.New(ctx, workers,
		retrypool.WithAttempts[TaskInt](5),
		retrypool.WithDelay[TaskInt](100*time.Millisecond),
		retrypool.WithRetryIf[TaskInt](CustomRetryIfFunc),
		retrypool.WithOnRetry[TaskInt](func(attempt int, err error, task *retrypool.TaskWrapper[TaskInt]) {
			log.Printf("Retrying task %d, attempt %d: %v", task, attempt, err)
		}),
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
