package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/davidroman0O/retrypool"
)

type SimpleWorker struct{}

func (w *SimpleWorker) Run(ctx context.Context, data int) error {
	time.Sleep(time.Duration(data) * time.Millisecond)
	fmt.Printf("Processed: %d\n", data)
	return nil
}

func main() {
	ctx := context.Background()
	workers := []retrypool.Worker[int]{&SimpleWorker{}, &SimpleWorker{}}
	pool := retrypool.New(ctx, workers)

	for i := 1; i <= 10; i++ {
		whenProcessed := make(chan struct{})
		err := pool.Dispatch(i*100, retrypool.WithBeingProcessed[int](whenProcessed))
		if err != nil {
			log.Printf("Dispatch error: %v", err)
		}
		<-whenProcessed
	}

	pool.Close()
	fmt.Println("All tasks completed")
}
