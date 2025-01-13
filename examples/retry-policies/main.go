package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/davidroman0O/retrypool"
)

// Define the data type for tasks
type TaskData struct {
	ID int
}

// Implement the Worker interface
type DynamicWorker struct{}

func (w *DynamicWorker) Run(ctx context.Context, data TaskData) error {
	if data.ID%2 == 0 {
		// Simulate an error on even IDs
		return fmt.Errorf("failed to process task ID: %d", data.ID)
	}
	fmt.Printf("Processed task ID: %d successfully.\n", data.ID)
	return nil
}

// It will loop forever but it's to demonstrate the dynamic retry policy
func main() {
	ctx := context.Background()

	// Shared variable to hold the retry policy
	var retryPolicy retrypool.RetryPolicy[TaskData]
	retryPolicy = retrypool.FixedDelayRetryPolicy[TaskData]{Delay: time.Millisecond * 500}

	// Mutex to protect access to retryPolicy
	var policyMu sync.RWMutex

	// Override the DelayFunc to use the dynamic retryPolicy
	delayFunc := func(retries int, err error, config *retrypool.Config[TaskData]) time.Duration {
		policyMu.RLock()
		defer policyMu.RUnlock()
		return retryPolicy.ComputeDelay(retries, err, config)
	}

	// Create the pool with the initial retry policy
	pool := retrypool.New[TaskData](
		ctx,
		[]retrypool.Worker[TaskData]{&DynamicWorker{}},
		retrypool.WithDelayFunc(delayFunc),
	)

	// Function to adjust the retry policy based on metrics
	go func() {
		for {
			time.Sleep(time.Second * 2)
			failedTasks := pool.GetSnapshot().TasksFailed
			if failedTasks > 3 {
				policyMu.Lock()
				fmt.Println("Adjusting retry policy to use exponential backoff.")
				retryPolicy = retrypool.ExponentialBackoffRetryPolicy[TaskData]{
					BaseDelay: time.Second,
					MaxDelay:  time.Second * 10,
					MaxJitter: time.Second,
				}
				policyMu.Unlock()
			}
		}
	}()

	// Submit tasks
	for i := 1; i <= 10; i++ {
		taskID := i
		err := pool.Submit(TaskData{ID: taskID})
		if err != nil {
			fmt.Printf("Error submitting task ID %d: %v\n", taskID, err)
		}
	}

	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, time.Second/2)

	// Close the pool
	pool.Close()
}
