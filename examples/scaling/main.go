package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/davidroman0O/retrypool"
	"github.com/davidroman0O/retrypool/logs"
)

type SimpleWorker struct {
	ID        int
	processed int32
}

func (w *SimpleWorker) Run(ctx context.Context, data int) error {
	atomic.AddInt32(&w.processed, 1)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Duration(data) * time.Millisecond):
		fmt.Printf("Worker %d processed task: %d (total: %d)\n",
			w.ID, data, atomic.LoadInt32(&w.processed))
		return nil
	}
}

func printMetrics(pool *retrypool.Pool[int]) {
	metrics := pool.Metrics()
	fmt.Printf("\nCurrent Metrics:\n")
	fmt.Printf("Queue Size: %d\n", pool.QueueSize())
	fmt.Printf("Processing: %d\n", pool.ProcessingCount())
	fmt.Printf("Dead Tasks: %d\n", pool.DeadTaskCount())
	fmt.Printf("Tasks Submitted: %d\n", metrics.TasksSubmitted)
	fmt.Printf("Tasks Processed: %d\n", metrics.TasksProcessed)
	fmt.Printf("Tasks Succeeded: %d\n", metrics.TasksSucceeded)
	fmt.Printf("Tasks Failed: %d\n", metrics.TasksFailed)

	// Print worker list
	workers := pool.ListWorkers()
	fmt.Printf("Active Workers (%d):\n", len(workers))
	for _, w := range workers {
		if sw, ok := w.Worker.(*SimpleWorker); ok {
			fmt.Printf("  Worker %d: Processed %d tasks\n",
				sw.ID, atomic.LoadInt32(&sw.processed))
		}
	}
	fmt.Println(strings.Repeat("-", 40))
}

func main() {
	ctx := context.Background()

	// Start with 2 workers
	workers := []retrypool.Worker[int]{
		&SimpleWorker{},
		&SimpleWorker{},
	}

	var pool *retrypool.Pool[int]
	pool = retrypool.New(
		ctx,
		workers,
		retrypool.WithLogLevel[int](logs.LevelDebug),
		retrypool.WithOnNewDeadTask(func(task *retrypool.DeadTask[int], idx int) {
			log.Printf("Dead task: %v", task)
			// Which mean we want to re-submit it
			pool.Submit(task.Data)
		}),
	)

	// Helper function to submit a batch of tasks
	submitBatch := func(count int, duration int) {
		fmt.Printf("\nSubmitting %d tasks (%dms each)...\n", count, duration)
		for i := 0; i < count; i++ {
			if err := pool.Submit(duration); err != nil {
				log.Printf("Failed to submit task: %v", err)
			}
		}
	}

	// Command loop
	for {
		fmt.Println("\nCommands:")
		fmt.Println("1. Submit tasks")
		fmt.Println("2. Add worker")
		fmt.Println("3. Remove worker")
		fmt.Println("4. Show metrics")
		fmt.Println("5. Exit")
		fmt.Print("Enter command: ")

		var cmd int
		fmt.Scan(&cmd)

		switch cmd {
		case 1:
			var count, duration int
			fmt.Print("Enter number of tasks: ")
			fmt.Scan(&count)
			fmt.Print("Enter task duration (ms): ")
			fmt.Scan(&duration)
			submitBatch(count, duration)

		case 2:
			worker := &SimpleWorker{}
			workerID := pool.AddWorker(worker)
			fmt.Printf("Added worker(internal ID: %d)\n", workerID)

		case 3:
			workers := pool.ListWorkers()
			if len(workers) == 0 {
				fmt.Println("No workers to remove")
				continue
			}

			fmt.Printf("Current workers: %d\n", len(workers))
			fmt.Print("Enter worker ID to remove: ")
			var workerID int
			fmt.Scan(&workerID)

			err := pool.RemoveWorker(workerID)
			if err != nil {
				fmt.Printf("Error removing worker: %v\n", err)
			} else {
				fmt.Printf("Removed worker %d\n", workerID)
			}

		case 4:
			printMetrics(pool)

		case 5:
			fmt.Println("Shutting down...")
			pool.Shutdown()
			return

		default:
			fmt.Println("Invalid command")
		}

		// Print metrics after each action
		if cmd != 4 { // Don't print twice for command 4
			time.Sleep(100 * time.Millisecond) // Brief pause to let metrics update
			printMetrics(pool)
		}
	}
}
