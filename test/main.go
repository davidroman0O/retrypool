package main

import (
	"context"
	"fmt"
	"time"

	"github.com/davidroman0O/retrypool"
)

type task struct {
	id    retrypool.TaskID
	group retrypool.GroupID
	deps  []retrypool.TaskID
}

func (t task) GetDependencies() []retrypool.TaskID {
	return t.deps
}

func (t task) GetGroupID() retrypool.GroupID {
	return t.group
}

func (t task) GetTaskID() retrypool.TaskID {
	return t.id
}

// We implement a unique hash based on the group and task ID.
// Here we assume IDs are integers. Adjust hashing as needed for your ID types.
func (t task) HashID() uint64 {
	// Assuming taskID and groupID can be cast to int, or if not, implement a proper hashing method.
	// For demonstration, we assume they're integers.
	g := t.group.(int)
	i := t.id.(int)
	// Simple hash: combine group and task IDs.
	return uint64((g << 32) | (i & 0xffffffff))
}

type worker struct {
}

func (w *worker) Run(ctx context.Context, data task) error {
	fmt.Println("Running task", data.id)
	return nil
}

func main() {

	pool := retrypool.New(context.Background(), []retrypool.Worker[task]{&worker{}}, retrypool.WithRoundRobinDistribution[task]())

	dp, _ := retrypool.NewDependencyPool[task](
		pool,
		func() retrypool.Worker[task] {
			return &worker{}
		},
		retrypool.DependencyConfig[task]{
			MaximumGroupSize: 1,
		},
	)

	go dp.Start()

	if err := dp.AddTask(task{
		id:    2,
		group: 1,
		deps:  []retrypool.TaskID{1},
	}); err != nil {
		panic(err)
	}

	if err := dp.AddTask(task{
		id:    1,
		group: 1,
		deps:  []retrypool.TaskID{},
	}); err != nil {
		panic(err)
	}

	if err := dp.AddTask(task{
		id:    4,
		group: 1,
		deps:  []retrypool.TaskID{3},
	}); err != nil {
		panic(err)
	}

	if err := dp.AddTask(task{
		id:    3,
		group: 1,
		deps:  []retrypool.TaskID{2},
	}); err != nil {
		panic(err)
	}

	if err := dp.AddTask(task{
		id:    2,
		group: 2,
		deps:  []retrypool.TaskID{1},
	}); err != nil {
		panic(err)
	}

	if err := dp.AddTask(task{
		id:    3,
		group: 2,
		deps:  []retrypool.TaskID{1},
	}); err != nil {
		panic(err)
	}

	if err := dp.AddTask(task{
		id:    4,
		group: 2,
		deps:  []retrypool.TaskID{3},
	}); err != nil {
		panic(err)
	}

	<-time.After(5 * time.Second)

}
