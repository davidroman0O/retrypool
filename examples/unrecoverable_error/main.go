package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"

	"github.com/davidroman0O/retrypool"
)

type UnrecoverableWorker struct{}

func (w *UnrecoverableWorker) Run(ctx context.Context, data int) error {
	if rand.Float32() < 0.3 {
		return retrypool.Unrecoverable(fmt.Errorf("unrecoverable error for data: %d", data))
	}
	if rand.Float32() < 0.5 {
		return fmt.Errorf("recoverable error for data: %d", data)
	}
	fmt.Printf("Processed: %d\n", data)
	return nil
}

func main() {
	ctx := context.Background()
	workers := []retrypool.Worker[int]{&UnrecoverableWorker{}, &UnrecoverableWorker{}}
	pool := retrypool.NewPool(ctx, workers,
		retrypool.WithAttempts[int](3),
		retrypool.WithOnRetry[int](func(attempt int, err error, task int) {
			log.Printf("Retrying task %d, attempt %d: %v", task, attempt, err)
		}),
	)

	for i := 1; i <= 20; i++ {
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
