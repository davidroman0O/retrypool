package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/davidroman0O/retrypool"
)

type FlakyWorker struct{}

func (w *FlakyWorker) Run(ctx context.Context, data TaskInt) error {
	if data.Data%3 == 0 {
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

func main() {
	ctx := context.Background()
	workers := []retrypool.Worker[TaskInt]{&FlakyWorker{}, &FlakyWorker{}}
	pool := retrypool.New(ctx, workers,
		retrypool.WithAttempts[TaskInt](3),
		retrypool.WithDelay[TaskInt](time.Second),
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
