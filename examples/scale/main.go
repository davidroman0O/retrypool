package main

import (
	"context"
	"fmt"
	"log"
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
		retrypool.WithLogLevel[int](logs.LevelInfo),
		// Since we will scale down, that mean workers will dispatch their tasks into the dead tasks
		retrypool.WithOnNewDeadTask(func(task *retrypool.DeadTask[int], idx int) {
			log.Printf("Dead task: %v", task)
			// Which mean we want to re-submit it
			pool.Submit(task.Data)
		}),
	)

	submitBatch := func(count int, duration int) {
		fmt.Printf("\nSubmitting %d tasks (%dms each)...\n", count, duration)
		list := []<-chan struct{}{}
		for i := 0; i < count; i++ {
			processed := retrypool.NewProcessedNotification()
			if err := pool.Submit(duration, retrypool.WithBeingProcessed[int](processed)); err != nil {
				log.Printf("Failed to submit task: %v", err)
			}
			list = append(list, processed.Done())
		}
		for i := 0; i < len(list); i++ {
			<-list[i]
		}
	}

	fmt.Println(" == Listing workers == ")
	pool.RangeWorkers(func(workerID int, worker retrypool.Worker[int]) bool {
		if sw, ok := worker.(*SimpleWorker); ok {
			fmt.Printf("Worker %d processed %d tasks\n",
				sw.ID, atomic.LoadInt32(&sw.processed))
		}
		return true
	})

	fmt.Println(" == Submitting tasks == ")
	submitBatch(5, 1)
	fmt.Println(" == Listing workers == ")
	pool.RangeWorkers(func(workerID int, worker retrypool.Worker[int]) bool {
		if sw, ok := worker.(*SimpleWorker); ok {
			fmt.Printf("Worker %d processed %d tasks\n",
				sw.ID, atomic.LoadInt32(&sw.processed))
		}
		return true
	})

	fmt.Println(" == Adding more workers == ")
	pool.AddWorker(&SimpleWorker{})
	fmt.Println(" == Listing workers == ")
	pool.RangeWorkers(func(workerID int, worker retrypool.Worker[int]) bool {
		if sw, ok := worker.(*SimpleWorker); ok {
			fmt.Printf("Worker %d processed %d tasks\n",
				sw.ID, atomic.LoadInt32(&sw.processed))
		}
		return true
	})

	fmt.Println(" == Submitting tasks == ")
	submitBatch(5, 1)

	fmt.Println(" == Listing workers == ")
	pool.RangeWorkers(func(workerID int, worker retrypool.Worker[int]) bool {
		if sw, ok := worker.(*SimpleWorker); ok {
			fmt.Printf("Worker %d processed %d tasks\n",
				sw.ID, atomic.LoadInt32(&sw.processed))
		}
		return true
	})

	fmt.Println(" == Removing worker == ")
	pool.RemoveWorker(1)

	fmt.Println(" == Listing workers == ")
	pool.RangeWorkers(func(workerID int, worker retrypool.Worker[int]) bool {
		if sw, ok := worker.(*SimpleWorker); ok {
			fmt.Printf("Worker %d processed %d tasks\n",
				sw.ID, atomic.LoadInt32(&sw.processed))
		}
		return true
	})

	fmt.Println(" == Submitting tasks == ")
	submitBatch(5, 1)

	fmt.Println(" == Listing workers == ")
	pool.RangeWorkers(func(workerID int, worker retrypool.Worker[int]) bool {
		if sw, ok := worker.(*SimpleWorker); ok {
			fmt.Printf("Worker %d processed %d tasks\n",
				sw.ID, atomic.LoadInt32(&sw.processed))
		}
		return true
	})

}
