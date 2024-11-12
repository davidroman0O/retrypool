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

	whenQueued := retrypool.NewQueuedNotification()
	err := pool.Submit(1, retrypool.WithQueued[int](whenQueued))
	if err != nil {
		log.Printf("Dispatch error: %v", err)
	}
	<-whenQueued

	whenProcessed := retrypool.NewProcessedNotification()
	err = pool.Submit(2, retrypool.WithBeingProcessed[int](whenProcessed))
	if err != nil {
		log.Printf("Dispatch error: %v", err)
	}
	<-whenProcessed

	whenProcessed2 := retrypool.NewProcessedNotification()
	whenQueued2 := retrypool.NewQueuedNotification()
	err = pool.Submit(3, retrypool.WithQueued[int](whenQueued2), retrypool.WithBeingProcessed[int](whenProcessed2))
	if err != nil {
		log.Printf("Dispatch error: %v", err)
	}
	<-whenQueued2
	<-whenProcessed2

	err = pool.Submit(4)
	if err != nil {
		log.Printf("Dispatch error: %v", err)
	}

	pool.Shutdown()
	fmt.Println("All tasks completed")
}
