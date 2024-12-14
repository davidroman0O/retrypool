package main

import (
	"context"
	"fmt"
	"time"

	"github.com/davidroman0O/retrypool"
)

// MyTask represents the data we'll process.
type MyTask struct {
	Value int
}

// MyWorker implements the retrypool.Worker interface.
type MyWorker struct {
	ID int
}

// Run processes a task.
func (w *MyWorker) Run(ctx context.Context, task MyTask) error {
	fmt.Printf("Worker %d processing task with value %d\n", w.ID, task.Value)
	// Simulate some work.
	time.Sleep(300 * time.Millisecond)
	return nil
}

// OnStart is called when the worker starts.
func (w *MyWorker) OnStart(ctx context.Context) {
	fmt.Printf("Worker %d started\n", w.ID)
}

// OnPause is called when the worker is paused.
func (w *MyWorker) OnPause(ctx context.Context) {
	fmt.Printf("Worker %d paused\n", w.ID)
}

// OnResume is called when the worker is resumed.
func (w *MyWorker) OnResume(ctx context.Context) {
	fmt.Printf("Worker %d resumed\n", w.ID)
}

// OnStop is called when the worker stops.
func (w *MyWorker) OnStop(ctx context.Context) {
	fmt.Printf("Worker %d stopped\n", w.ID)
}

func main() {
	ctx := context.Background()

	// Create an initial worker.
	worker := &MyWorker{}
	workers := []retrypool.Worker[MyTask]{worker}

	// Create the pool.
	pool := retrypool.New(ctx, workers)
	defer pool.Close()

	// Submit initial tasks.
	for i := 1; i <= 5; i++ {
		task := MyTask{Value: i}
		err := pool.Submit(task)
		if err != nil {
			fmt.Printf("Error submitting task %d: %v\n", task.Value, err)
		}
	}

	// Wait a moment.
	time.Sleep(1 * time.Second)

	// Add a new worker.
	newWorker := &MyWorker{}
	err := pool.Add(newWorker, nil)
	if err != nil {
		fmt.Printf("Error adding new worker: %v\n", err)
	}

	// Submit more tasks.
	for i := 6; i <= 10; i++ {
		task := MyTask{Value: i}
		err := pool.Submit(task)
		if err != nil {
			fmt.Printf("Error submitting task %d: %v\n", task.Value, err)
		}
	}

	// Pause the first worker.
	err = pool.Pause(worker.ID)
	if err != nil {
		fmt.Printf("Error pausing worker %d: %v\n", worker.ID, err)
	}

	// Wait a moment.
	time.Sleep(1 * time.Second)

	// Resume the first worker.
	err = pool.Resume(worker.ID)
	if err != nil {
		fmt.Printf("Error resuming worker %d: %v\n", worker.ID, err)
	}

	// Wait a moment.
	time.Sleep(1 * time.Second)

	// Remove the second worker.
	err = pool.Remove(newWorker.ID)
	if err != nil {
		fmt.Printf("Error removing worker %d: %v\n", newWorker.ID, err)
	}

	// Submit final tasks.
	for i := 11; i <= 15; i++ {
		task := MyTask{Value: i}
		err := pool.Submit(task)
		if err != nil {
			fmt.Printf("Error submitting task %d: %v\n", task.Value, err)
		}
	}

	// Wait for all tasks to be processed.
	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, time.Second/4)

	// Close the pool.
	pool.Close()
}
