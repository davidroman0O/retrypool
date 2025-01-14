package main

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/davidroman0O/retrypool"
	"github.com/k0kubun/pp/v3"
)

type worker struct{}

func (w *worker) Run(ctx context.Context, data *retrypool.RequestResponse[int, struct{}]) error {

	<-time.After(time.Millisecond * time.Duration(50+rand.Intn(10000)))

	return nil
}

func detectUnevenDistribution(taskQueues map[int]int) bool {
	// Calculate total sum of values
	totalSum := 0
	for _, value := range taskQueues {
		totalSum += value
	}

	// Calculate the expected average distribution
	numKeys := len(taskQueues)
	expectedValue := float64(totalSum) / float64(numKeys)

	// Check each key for significant deviation
	for key, value := range taskQueues {
		if math.Abs(float64(value)-expectedValue) > 1 {
			fmt.Printf("Key %d has an uneven distribution with value %d (expected ~%.2f)\n", key, value, expectedValue)
			return true
		}
	}
	return false
}

func main() {
	ctx := context.Background()
	var pool *retrypool.Pool[*retrypool.RequestResponse[int, struct{}]]

	pool = retrypool.New[*retrypool.RequestResponse[int, struct{}]](
		ctx,
		[]retrypool.Worker[*retrypool.RequestResponse[int, struct{}]]{
			&worker{},
			&worker{},
			&worker{},
			&worker{},
			&worker{},
		},
		retrypool.WithAttempts[*retrypool.RequestResponse[int, struct{}]](3),
		retrypool.WithRoundRobinDistribution[*retrypool.RequestResponse[int, struct{}]](),
		retrypool.WithSnapshots[*retrypool.RequestResponse[int, struct{}]](),
		retrypool.WithSnapshotInterval[*retrypool.RequestResponse[int, struct{}]](time.Second),
		retrypool.WithSnapshotCallback[*retrypool.RequestResponse[int, struct{}]](func(ms retrypool.MetricsSnapshot[*retrypool.RequestResponse[int, struct{}]]) {
			pp.Println("player snapshot", ms)
			// we have a bug of tasks queued being ONE worker instead of distributed properly
			if detectUnevenDistribution(ms.TaskQueues) {
				pool.RedistributeAllTasks()
			}
		}),
		retrypool.WithOnPanic[*retrypool.RequestResponse[int, struct{}]](func(recovery interface{}, stackTrace string) {
			fmt.Println("detected panic", recovery, stackTrace)
		}),
		retrypool.WithOnTaskFailure[*retrypool.RequestResponse[int, struct{}]](func(data *retrypool.RequestResponse[int, struct{}], err error) retrypool.TaskAction {
			fmt.Println("task failed", data, err)
			return retrypool.TaskActionRetry
		}),
	)

	go func() {
		for i := range 100 {
			<-time.After(time.Millisecond * time.Duration(50+rand.Intn(500)))
			pool.Submit(retrypool.NewRequestResponse[int, struct{}](i), retrypool.WithMetadata[*retrypool.RequestResponse[int, struct{}]](map[string]any{"idx": i}))
		}
	}()

	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		fmt.Println("queue size", queueSize, "processing count", processingCount, "dead task count", deadTaskCount)
		return queueSize > 0 || processingCount > 0 || deadTaskCount > 0
	}, time.Second)

	pool.Close()
}
