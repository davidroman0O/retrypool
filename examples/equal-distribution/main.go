package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/davidroman0O/retrypool"
)

// SimpleWorker implements the Worker interface
type SimpleWorker struct {
	ID int
}

func (w *SimpleWorker) Run(ctx context.Context, data int) error {
	log.Printf("Worker %d processing task: %d", w.ID, data)
	time.Sleep(time.Second) // Simulate work
	return nil
}

func main() {
	ctx := context.Background()

	// Create 5 workers
	workers := make([]retrypool.Worker[int], 5)
	for i := 0; i < 5; i++ {
		workers[i] = &SimpleWorker{ID: i}
	}

	// Create the pool
	pool := retrypool.New[int](ctx, workers)

	// Function to dispatch tasks and print queue sizes
	dispatchAndPrint := func(useEqualDistribution bool) {
		fmt.Printf("\nDispatching with equal distribution: %v\n", useEqualDistribution)
		for i := 0; i < 20; i++ { // Increased to 20 tasks for better demonstration
			var err error
			// if useEqualDistribution {
			err = pool.Submit(i)
			// } else {
			// 	err = pool.Submit(i)
			// }
			if err != nil {
				log.Printf("Error dispatching task %d: %v", i, err)
			}
		}

		// Print queue sizes and tasks
		workerQueues := make(map[int][]int)
		pool.RangeTasks(func(data retrypool.TaskWrapper[int], workerID int, status retrypool.TaskStatus) bool {
			if status == retrypool.TaskStatusQueued {
				workerQueues[workerID] = append(workerQueues[workerID], data.Data())
			}
			return true
		})

		for workerID, tasks := range workerQueues {
			fmt.Printf("Worker %d queue size: %d, Tasks: %v\n", workerID, len(tasks), tasks)
		}
	}

	// Dispatch without equal distribution
	dispatchAndPrint(false)

	// Wait for tasks to start processing
	time.Sleep(100 * time.Millisecond)

	// Dispatch with equal distribution
	dispatchAndPrint(true)

	// Wait for all tasks to complete
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
			return queueSize > 0 || processingCount > 0
		}, time.Second)
	}()

	wg.Wait()
	pool.Shutdown()
}
