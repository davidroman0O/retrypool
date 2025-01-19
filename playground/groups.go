package main

import (
	"context"
	"sync/atomic"

	"github.com/davidroman0O/retrypool"
	"github.com/k0kubun/pp/v3"
)

var globalTicker atomic.Int32

type worker struct {
	ID int
}

func (w *worker) Run(ctx context.Context, data task) error {
	pp.Println("Worker", w.ID, "Processing task", data.ID, "from group", data.GID)
	retrypool.SetTaskMetadata(ctx, map[string]any{
		"taskdata": data.ID,
	})
	globalTicker.Add(1)
	return nil
}

type task struct {
	ID  int
	GID string
}

func (t task) GetGroupID() string {
	return t.GID
}

func main() {
	globalTicker.Store(0)
	ctx := context.Background()
	var pool *retrypool.GroupPool[task, string]
	var err error

	pool, err = retrypool.NewGroupPool[task](
		ctx,
		retrypool.WithGroupPoolWorkerFactory[task, string](
			func() retrypool.Worker[task] {
				return &worker{}
			}),
		retrypool.WithGroupPoolMaxActivePools[task, string](2),
		retrypool.WithGroupPoolMaxWorkersPerPool[task, string](4),
		retrypool.WithGroupPoolUseFreeWorkerOnly[task, string](),
		retrypool.WithGroupPoolOnTaskSuccess[task, string](func(gid string, pool uint, data task, metadata map[string]any) {
			pp.Println("Task", data.ID, "from group", data.GID, "completed", metadata, "with", pool, "workers")
		}),
		retrypool.WithGroupPoolOnSnapshot[task, string](func(snapshot retrypool.GroupMetricsSnapshot[task, string]) {
			pp.Println("group pool snapshot::", snapshot)
		}),
	)
	if err != nil {
		panic(err)
	}

	groups := []string{"group1", "group2", "group3", "group4", "group5"}

	for _, g := range groups {
		go func(groupID string) {
			pool.StartGroup(groupID)
			for i := range 1000 {
				if err := pool.SubmitToGroup(groupID, task{GID: groupID, ID: i}); err != nil {
					panic(err)
				}
			}
			pool.EndGroup(groupID)
		}(g)
	}

	for _, v := range groups {
		pool.WaitGroup(ctx, v)
	}

	pool.Close()

	pp.Println("final:: ", pool.GetSnapshot())

	pp.Println("globalTicker::", globalTicker.Load())
}
