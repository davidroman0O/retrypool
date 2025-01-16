package tests

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/davidroman0O/retrypool"
)

// reqRespWorker handles request-response tasks
type reqRespWorker struct {
	ID           int
	processDelay time.Duration
	shouldFail   bool
}

type Request struct {
	ID      int
	Payload string
	rr      *retrypool.RequestResponse[Request, Response]
}

type Response struct {
	RequestID int
	Result    string
	Timestamp time.Time
}

func (w *reqRespWorker) Run(ctx context.Context, req Request) error {
	select {
	case <-time.After(w.processDelay):
		if w.shouldFail {
			return errors.New("worker failed to process request")
		}
		// Complete the request-response
		if req.rr != nil {
			req.rr.Complete(Response{
				RequestID: req.ID,
				Result:    "processed",
				Timestamp: time.Now(),
			})
		}
		return nil
	case <-ctx.Done():
		if req.rr != nil {
			req.rr.CompleteWithError(ctx.Err())
		}
		return ctx.Err()
	}
}

func TestRequestResponse(t *testing.T) {
	tests := []struct {
		name        string
		timeout     time.Duration
		delay       time.Duration
		shouldFail  bool
		concurrent  int
		syncMode    bool
		expectError bool
	}{
		{
			name:        "Basic Success",
			timeout:     time.Second,
			delay:       50 * time.Millisecond,
			shouldFail:  false,
			concurrent:  1,
			syncMode:    false,
			expectError: false,
		},
		{
			name:        "Timeout Failure",
			timeout:     50 * time.Millisecond,
			delay:       100 * time.Millisecond,
			shouldFail:  false,
			concurrent:  1,
			syncMode:    false,
			expectError: true,
		},
		{
			name:        "Worker Error",
			timeout:     time.Second,
			delay:       50 * time.Millisecond,
			shouldFail:  true,
			concurrent:  1,
			syncMode:    false,
			expectError: true,
		},
		{
			name:        "Concurrent Requests",
			timeout:     time.Second,
			delay:       50 * time.Millisecond,
			shouldFail:  false,
			concurrent:  10,
			syncMode:    false,
			expectError: false,
		},
		{
			name:        "Sync Mode Success",
			timeout:     time.Second,
			delay:       50 * time.Millisecond,
			shouldFail:  false,
			concurrent:  1,
			syncMode:    true,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			worker := &reqRespWorker{
				processDelay: tt.delay,
				shouldFail:   tt.shouldFail,
			}

			options := []retrypool.Option[Request]{
				retrypool.WithOnTaskFailure[Request](func(data Request, metadata retrypool.Metadata, err error) retrypool.TaskAction {
					if data.rr != nil {
						data.rr.CompleteWithError(err)
					}
					return retrypool.TaskActionRemove
				}),
			}
			if tt.syncMode {
				options = append(options, retrypool.WithSynchronousMode[Request]())
			}

			pool := retrypool.New(ctx, []retrypool.Worker[Request]{worker}, options...)

			var wg sync.WaitGroup
			results := make(chan error, tt.concurrent)

			// Launch concurrent requests
			for i := 0; i < tt.concurrent; i++ {
				wg.Add(1)
				go func(reqID int) {
					defer wg.Done()

					reqCtx, cancel := context.WithTimeout(ctx, tt.timeout)
					defer cancel()

					// Create request-response
					rr := retrypool.NewRequestResponse[Request, Response](Request{})

					// Create request with reference to its request-response
					req := Request{
						ID:      reqID,
						Payload: "test payload",
						rr:      rr,
					}

					// Submit the request
					err := pool.Submit(req)
					if err != nil {
						results <- err
						return
					}

					// Wait for response
					_, err = rr.Wait(reqCtx)
					results <- err
				}(i)
			}

			// Wait for all requests to complete
			wg.Wait()
			close(results)

			// Check results
			var errors []error
			for err := range results {
				if err != nil {
					errors = append(errors, err)
				}
			}

			if tt.expectError && len(errors) == 0 {
				t.Error("Expected errors but got none")
			} else if !tt.expectError && len(errors) > 0 {
				t.Errorf("Expected no errors but got: %v", errors)
			}

			if err := pool.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRequestResponseCancellation(t *testing.T) {
	ctx := context.Background()
	worker := &reqRespWorker{
		processDelay: 200 * time.Millisecond,
	}

	pool := retrypool.New(ctx, []retrypool.Worker[Request]{worker})

	// Create request-response
	rr := retrypool.NewRequestResponse[Request, Response](Request{})

	// Create request with cancellable context
	reqCtx, cancel := context.WithCancel(ctx)
	req := Request{
		ID:      1,
		Payload: "test",
		rr:      rr,
	}

	// Submit the request
	err := pool.Submit(req)
	if err != nil {
		t.Fatal(err)
	}

	// Cancel the context after a short delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	// Wait should return with context cancellation error
	_, err = rr.Wait(reqCtx)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context cancellation error, got: %v", err)
	}

	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRequestResponseMultipleResponses(t *testing.T) {
	ctx := context.Background()
	worker := &reqRespWorker{
		processDelay: 50 * time.Millisecond,
	}

	pool := retrypool.New(ctx, []retrypool.Worker[Request]{worker})

	// Create request-response
	rr := retrypool.NewRequestResponse[Request, Response](Request{})

	// Create and submit request
	req := Request{
		ID:      1,
		Payload: "test",
		rr:      rr,
	}

	err := pool.Submit(req)
	if err != nil {
		t.Fatal(err)
	}

	// Try to complete multiple times (only first should take effect)
	response1 := Response{RequestID: 1, Result: "first", Timestamp: time.Now()}
	response2 := Response{RequestID: 1, Result: "second", Timestamp: time.Now()}

	rr.Complete(response1)
	rr.Complete(response2) // Should be ignored

	// Wait should return the first response
	resp, err := rr.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if resp.Result != "first" {
		t.Errorf("Expected first response, got: %v", resp.Result)
	}

	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
}
