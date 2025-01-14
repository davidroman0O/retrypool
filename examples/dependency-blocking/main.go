package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/davidroman0O/retrypool"
	"github.com/k0kubun/pp/v3"
)

// Now the proper Blocking example
type task struct {
	ID           int
	Group        string
	Dependencies []int
	Description  string
	Pool         *retrypool.BlockingPool[task, string, int] // Reference to pool for submitting from inside
	done         chan struct{}
}

func (t task) GetDependencies() []int { return t.Dependencies }
func (t task) GetGroupID() string     { return t.Group }
func (t task) GetTaskID() int         { return t.ID }

type worker struct {
	ID int
}

func (w *worker) Run(ctx context.Context, data task) error {

	fmt.Println("Running Task", data.Group, data.ID, "with deps", data.Dependencies)

	// Do some work
	time.Sleep(time.Second)

	// For tasks 1 and 2, create next task
	if data.ID < 3 {
		nextID := data.ID + 1
		nextDone := make(chan struct{})

		nextTask := task{
			ID:           nextID,
			Group:        data.Group,
			Dependencies: []int{data.ID},
			Pool:         data.Pool,
			done:         nextDone,
		}

		fmt.Printf("\tCreating Next Task %d with deps %v\n", nextID, nextTask.Dependencies)

		// Submit next task first
		if err := data.Pool.Submit(nextTask); err != nil {
			return err
		}

		fmt.Printf("\tWaiting for Next Task %d\n", nextID)
		// Now wait for next task
		<-nextDone

		// Signal our completion before waiting for next task
		// If you put that before... well it won't be ordered
		close(data.done)
	} else {
		// For the last task, just do work and complete
		close(data.done)
	}

	fmt.Printf("Task %d Done\n", data.ID)
	return nil
}

func main() {
	ctx := context.Background()
	var dp *retrypool.BlockingPool[task, string, int]
	var err error

	dp, err = retrypool.NewBlockingPool[task, string, int](
		ctx,
		retrypool.WithBlockingWorkerFactory(func() retrypool.Worker[task] { return &worker{} }),
		retrypool.WithBlockingOnGroupRemoved[task](func(groupID any, tasks []task) {
			fmt.Println("Group", groupID, "is done")
		}),
		retrypool.WithBlockingSnapshotHandler[task](func() {
			pp.Println(dp.GetSnapshot())
		}),
	)
	if err != nil {
		log.Fatalf("Failed to create pool: %v", err)
	}

	done := make(chan struct{})
	// Only submit the initial task
	task1 := task{
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
