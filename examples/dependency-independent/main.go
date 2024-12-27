package main

import (
	"context"
	"log"

	"github.com/davidroman0O/retrypool"
)

type worker struct{}

func (w *worker) Run(ctx context.Context, input task) error {
	log.Println("Running task", input.GetTaskID())
	return nil
}

type task struct {
	dependencies []int
	groupID      string
	taskID       int
}

func (t task) GetDependencies() []int {
	return t.dependencies
}

func (t task) GetGroupID() string {
	return t.groupID
}

func (t task) GetTaskID() int {
	return t.taskID
}

func main() {
	ctx := context.Background()
	pool, err := retrypool.NewIndependentPool[task, string, int](
		ctx,
		retrypool.WithIndependentWorkerFactory[task](func() retrypool.Worker[task] {
			return &worker{}
		}),
		retrypool.WithIndependentDependencyMode[task](retrypool.ReverseMode),
		retrypool.WithIndependentOnGroupRemoved[task](func(groupID any, tasks []task) {
			log.Println("Group removed:", groupID)
		}),
	)
	if err != nil {
		panic(err)
	}

	if err := pool.Submit(
		[]task{
			{dependencies: []int{}, groupID: "group1", taskID: 1},
			{dependencies: []int{1}, groupID: "group1", taskID: 2},
			{dependencies: []int{1}, groupID: "group1", taskID: 3},
			{dependencies: []int{2, 3}, groupID: "group1", taskID: 4},
		},
	); err != nil {
		panic(err)
	}

	if err := pool.WaitForGroup(ctx, "group1"); err != nil {
		panic(err)
	}

	pool.Close()
}
