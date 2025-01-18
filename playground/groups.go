package main

import (
	"context"
	"time"

	"github.com/davidroman0O/retrypool"
	"github.com/k0kubun/pp/v3"
)

type worker struct {
	ID int
}

func (w *worker) Run(ctx context.Context, data task) error {
	pp.Println("Worker", w.ID, "Processing task", data.ID, "from group", data.GID)
	retrypool.SetTaskMetadata(ctx, map[string]any{
		"taskdata": data.ID,
	})
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
		retrypool.WithGroupPoolUseFreeWorkerOnly[task, string](),
		retrypool.WithGroupPoolOnTaskSuccess[task, string](func(gid string, pool uint, data task, metadata map[string]any) {
			pp.Println("Task", data.ID, "from group", data.GID, "completed", metadata, "with", pool, "workers")
		}),
		retrypool.WithGroupPoolOnSnapshot[task, string](func(shot retrypool.MetricsSnapshot[task]) {
			pp.Println(shot)
		}),
	)
	if err != nil {
		panic(err)
	}

	groups := []string{"group1", "group2", "group3", "group4", "group5"}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				pp.Println(pool.GetSnapshot())
			}
		}
	}()

	for _, g := range groups {
		go func() {
			for i := range 1000 {
				if err := pool.Submit(task{GID: g, ID: i}); err != nil {
					panic(err)
				}
			}
		}()
	}

	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0 || deadTaskCount > 0
	}, time.Second)

	pool.Close()
}
