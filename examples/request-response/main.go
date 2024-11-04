package main

import (
	"context"
	"fmt"
	"time"

	"github.com/davidroman0O/retrypool"
)

// MyTask represents the request data.
type MyTask struct {
	ID int
}

// MyResponse represents the response data.
type MyResponse struct {
	Result string
}

// MyWorker implements the retrypool.Worker interface.
type MyWorker struct{}

// Run processes the task and uses RequestResponse to return the result.
func (w *MyWorker) Run(ctx context.Context, data *retrypool.RequestResponse[MyTask, MyResponse]) error {
	time.Sleep(1 * time.Second) // Simulate processing time
	response := MyResponse{Result: fmt.Sprintf("Processed task %d", data.Request.ID)}
	data.Complete(response)
	return nil
}

func main() {
	ctx := context.Background()

	// Initialize the retrypool with one worker.
	pool := retrypool.New[*retrypool.RequestResponse[MyTask, MyResponse]](ctx, []retrypool.Worker[*retrypool.RequestResponse[MyTask, MyResponse]]{&MyWorker{}})

	// Create a new RequestResponse instance.
	requestResponse := retrypool.NewRequestResponse[MyTask, MyResponse](MyTask{ID: 10})

	// Dispatch the task.
	err := pool.Submit(requestResponse)
	if err != nil {
		fmt.Printf("Failed to dispatch task: %v\n", err)
	}

	// Wait for the task to complete and retrieve the response.
	response, err := requestResponse.Wait(ctx)
	if err != nil {
		fmt.Printf("Task failed: %v\n", err)
	} else {
		fmt.Printf("Received response: %s\n", response.Result)
	}

	// Close the pool.
	pool.Shutdown()
}
