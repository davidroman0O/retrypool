package main

import (
	"context"
	"fmt"
	"time"

	"github.com/davidroman0O/retrypool"
)

// Define the input and output types
type RequestData struct {
	ID int
}

type ResponseData struct {
	Result string
}

// Implement the Worker interface
type ResponseWorker struct{}

func (w *ResponseWorker) Run(ctx context.Context, data *retrypool.RequestResponse[RequestData, ResponseData]) error {
	// Simulate processing time
	time.Sleep(time.Millisecond * 200)

	var id int
	data.ConsultRequest(func(rd RequestData) error {
		id = rd.ID
		return nil
	})

	// Simulate an error for a specific ID
	if id == 3 {
		err := fmt.Errorf("failed to process request ID: %d", id)
		data.CompleteWithError(err)
		return nil // Return nil to avoid retrying
	}

	// Create a response
	response := ResponseData{
		Result: fmt.Sprintf("Processed request ID: %d", id),
	}

	// Complete the request with the response
	data.Complete(response)
	return nil
}

func main() {
	ctx := context.Background()

	// Create a pool with the ResponseWorker
	pool := retrypool.New[*retrypool.RequestResponse[RequestData, ResponseData]](
		ctx,
		[]retrypool.Worker[*retrypool.RequestResponse[RequestData, ResponseData]]{&ResponseWorker{}},
		retrypool.WithAttempts[*retrypool.RequestResponse[RequestData, ResponseData]](3),
	)

	// Number of requests to submit
	numRequests := 5

	// Slice to hold references to the requests
	requests := make([]*retrypool.RequestResponse[RequestData, ResponseData], numRequests)

	for i := 1; i <= numRequests; i++ {
		// Create a new RequestResponse instance
		req := retrypool.NewRequestResponse[RequestData, ResponseData](RequestData{ID: i})
		requests[i-1] = req

		// Submit the request to the pool
		err := pool.Submit(req)
		if err != nil {
			req.ConsultRequest(func(rd RequestData) error {
				fmt.Printf("Error submitting request ID %d: %v\n", rd.ID, err)
				return nil
			})
			continue
		}
	}

	// Wait for all requests to complete and collect responses
	for _, req := range requests {
		go func(r *retrypool.RequestResponse[RequestData, ResponseData]) {
			// Wait with a timeout
			ctx, cancel := context.WithTimeout(ctx, time.Second*2)
			defer cancel()

			resp, err := r.Wait(ctx)
			if err != nil {
				r.ConsultRequest(func(rd RequestData) error {
					fmt.Printf("Request ID %d failed: %v\n", rd.ID, err)
					return nil
				})
			} else {
				r.ConsultRequest(func(rd RequestData) error {
					fmt.Printf("Request ID %d succeeded: %s\n", rd.ID, resp.Result)
					return nil
				})
			}
		}(req)
	}

	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, time.Second)

	// Close the pool
	pool.Close()
}
