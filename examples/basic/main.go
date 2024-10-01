package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/davidroman0O/retrypool"
)

type SimpleWorker struct{}

func (w *SimpleWorker) Run(ctx context.Context, data TaskInt) error {
	time.Sleep(time.Duration(data.Data) * time.Millisecond)
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
	workers := []retrypool.Worker[TaskInt]{&SimpleWorker{}, &SimpleWorker{}}
	pool := retrypool.New(ctx, workers)

	for i := 1; i <= 10; i++ {
		err := pool.Dispatch(TaskInt{i * 100})
		if err != nil {
			log.Printf("Dispatch error: %v", err)
		}
	}

	pool.Close()
	fmt.Println("All tasks completed")
}
