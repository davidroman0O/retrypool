package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/davidroman0O/retrypool"
)

// Simple Independent example first
type IndependentTask struct {
	ID           int
	Group        string
	Dependencies []int
	Description  string
}

func (t IndependentTask) GetDependencies() []int { return t.Dependencies }
func (t IndependentTask) GetGroupID() string     { return t.Group }
func (t IndependentTask) GetTaskID() int         { return t.ID }

type IndependentWorker struct{}

func (w *IndependentWorker) Run(ctx context.Context, task IndependentTask) error {
	log.Printf("Starting task %d (%s)", task.ID, task.Description)
	time.Sleep(500 * time.Millisecond)
	log.Printf("Completed task %d (%s)", task.ID, task.Description)
	return nil
}

func ExampleIndependent() {
	ctx := context.Background()

	dp, err := retrypool.NewIndependentPool[IndependentTask, string, int](
		ctx,
		retrypool.WithWorkerFactory(func() retrypool.Worker[IndependentTask] { return &IndependentWorker{} }),
	)
	if err != nil {
		log.Fatalf("Failed to create pool: %v", err)
	}

	// All tasks are known upfront and run independently
	tasks := []IndependentTask{
		{ID: 2, Group: "groupA", Dependencies: []int{1}, Description: "Second task"},
		{ID: 1, Group: "groupA", Dependencies: []int{}, Description: "First task"},
		{ID: 3, Group: "groupA", Dependencies: []int{2}, Description: "Third task"},
	}

	for _, task := range tasks {
		if err := dp.Submit(task); err != nil {
			log.Printf("Failed to submit task %d: %v", task.ID, err)
		}
	}

	// Wait for all tasks using WaitWithCallback
	err = dp.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 100*time.Millisecond)

	if err != nil {
		log.Printf("Error waiting for tasks: %v", err)
	}
}

// Now the proper Blocking example
type BlockingTask struct {
	ID           int
	Group        string
	Dependencies []int
	Description  string
	Pool         *retrypool.BlockingPool[BlockingTask, string, int] // Reference to pool for submitting from inside
	done         chan struct{}
}

func (t BlockingTask) GetDependencies() []int { return t.Dependencies }
func (t BlockingTask) GetGroupID() string     { return t.Group }
func (t BlockingTask) GetTaskID() int         { return t.ID }

type BlockingWorker struct {
	ID int
}

func (w *BlockingWorker) Run(ctx context.Context, data BlockingTask) error {

	fmt.Println("\t\t Running Task", data.Group, data.ID, "with deps", data.Dependencies)

	<-time.After(500 * time.Millisecond)

	// For tasks 1 and 2, submit next task FIRST before doing anything else
	if data.ID < 3 {
		nextID := data.ID + 1
		nextDone := make(chan struct{})

		nextTask := BlockingTask{
			ID:           nextID,
			Group:        data.Group,
			Dependencies: []int{data.ID},
			Pool:         data.Pool,
			// Description:  fmt.Sprintf("Task %d that creates Task %d", nextID, nextID+1),
			done: nextDone,
		}

		fmt.Println("\t\t\t Creating Next Task", w.ID, data.Group, nextID, "with deps", data.ID, "...", nextTask.Dependencies)

		// Submit next task immediately
		if err := data.Pool.Submit(nextTask); err != nil {
			return err
		}

		<-nextDone
	}

	close(data.done)

	return nil
}

func ExampleBlocking() {
	ctx := context.Background()

	dp, err := retrypool.NewBlockingPool[BlockingTask, string, int](
		ctx,
		retrypool.WithBlockingWorkerFactory(func() retrypool.Worker[BlockingTask] { return &BlockingWorker{} }),
	)
	if err != nil {
		log.Fatalf("Failed to create pool: %v", err)
	}

	done := make(chan struct{})
	// Only submit the initial task
	task1 := BlockingTask{
		ID:          1,
		Group:       "groupA",
		Description: "Initial task that creates Task 2",
		Pool:        dp, // Pass pool reference to allow internal task creation
		done:        done,
	}

	if err := dp.Submit(task1); err != nil {
		log.Fatalf("Failed to submit initial task: %v", err)
	}

	<-done

	// Wait for all tasks using WaitWithCallback
	err = dp.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 100*time.Millisecond)

	if err != nil {
		log.Printf("Error waiting for tasks: %v", err)
	}
}

func main() {
	fmt.Println("\nIncdependent Pool Example (tasks run in sequence):")
	ExampleIndependent()
	time.Sleep(time.Second) // Give time for logs to flush

	fmt.Println("\nBlocking Pool Example (tasks wait for others internally):")
	ExampleBlocking()
	time.Sleep(time.Second) // Give time for logs to flush
}
