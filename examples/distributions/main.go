package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/davidroman0O/retrypool"
)

// SimpleWorker implements the Worker interface
type SimpleWorker struct {
	PoolID         int
	ID             int
	processedTasks []int
	mu             sync.Mutex
}

func (w *SimpleWorker) Run(ctx context.Context, data int) error {
	// Record the processed task
	w.mu.Lock()
	w.processedTasks = append(w.processedTasks, data)
	w.mu.Unlock()

	return nil
}

// If you execute this example many times, on big amount of tasks you will start to see a difference without the round robin
func main() {
	ctx := context.Background()
	var wg sync.WaitGroup

	// Create the first pool with workers IDs 1 and 2
	workersPool1 := []retrypool.Worker[int]{
		&SimpleWorker{PoolID: 1, ID: 1},
		&SimpleWorker{PoolID: 1, ID: 2},
	}
	pool1 := retrypool.New[int](ctx, workersPool1)

	// Create the second pool with the same worker IDs 1 and 2
	workersPool2 := []retrypool.Worker[int]{
		&SimpleWorker{PoolID: 2, ID: 1},
		&SimpleWorker{PoolID: 2, ID: 2},
	}
	pool2 := retrypool.New[int](ctx, workersPool2, retrypool.WithRoundRobinAssignment[int]())

	// Submit tasks 1 to 10 to the first pool
	for i := 1; i <= 100; i++ {
		err := pool1.Submit(i)
		if err != nil {
			fmt.Printf("Error submitting task %d to pool1: %v\n", i, err)
		}
	}

	// Submit tasks 1 to 10 to the second pool
	for i := 1; i <= 100; i++ {
		err := pool2.Submit(i)
		if err != nil {
			fmt.Printf("Error submitting task %d to pool2: %v\n", i, err)
		}
	}

	// Wait for the pools to process all tasks
	wg.Add(2)
	go func() {
		defer wg.Done()
		pool1.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
			return queueSize > 0 || processingCount > 0
		}, time.Second)
	}()

	go func() {
		defer wg.Done()
		pool2.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
			return queueSize > 0 || processingCount > 0
		}, time.Second)
	}()

	wg.Wait()

	// Shutdown the pools
	pool1.Shutdown()
	pool2.Shutdown()

	// Display task distribution for pool1
	fmt.Println("\nTask distribution in Pool 1 without RoundRobin (smallest queue based):")
	for _, worker := range workersPool1 {
		if sw, ok := worker.(*SimpleWorker); ok {
			sw.mu.Lock()
			fmt.Printf("Pool %d - Worker %d processed tasks: %v\n", sw.PoolID, sw.ID, len(sw.processedTasks))
			sw.mu.Unlock()
		}
	}

	// Display task distribution for pool2
	fmt.Println("\nTask distribution in Pool 2 with RoundRobin (worker based):")
	for _, worker := range workersPool2 {
		if sw, ok := worker.(*SimpleWorker); ok {
			sw.mu.Lock()
			fmt.Printf("Pool %d - Worker %d processed tasks: %v\n", sw.PoolID, sw.ID, len(sw.processedTasks))
			sw.mu.Unlock()
		}
	}
}
