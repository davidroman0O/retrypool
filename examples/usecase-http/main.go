package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/davidroman0O/retrypool"
)

// Server-side code remains mostly the same
type Server struct {
	tokenFailureRates map[string]float32
	mu                sync.Mutex
}

func NewServer() *Server {
	return &Server{
		tokenFailureRates: map[string]float32{
			"token1": 0.3,
			"token2": 0.5,
			"token3": 0.7,
		},
	}
}

func (s *Server) handleRequest(w http.ResponseWriter, r *http.Request) {
	token := r.Header.Get("Authorization")
	if token == "" {
		http.Error(w, "Missing Authorization header", http.StatusUnauthorized)
		return
	}

	s.mu.Lock()
	failureRate := s.tokenFailureRates[token]
	s.mu.Unlock()

	if rand.Float32() < failureRate {
		http.Error(w, "Random failure occurred", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Request processed successfully")
}

// Client-side code with adaptations
type APIWorker struct {
	ID          int
	BearerToken string
}

type Data struct {
	URL     string
	Payload interface{}
}

func (w *APIWorker) Run(ctx context.Context, rr *retrypool.RequestResponse[Data, error]) error {
	client := &http.Client{}
	var p interface{}
	var url string
	rr.ConsultRequest(func(d Data) {
		p = d.Payload
		url = d.URL
	})

	payload, err := json.Marshal(p)
	if err != nil {
		rr.CompleteWithError(fmt.Errorf("error marshaling payload: %w", err))
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(payload))
	if err != nil {
		rr.CompleteWithError(fmt.Errorf("error creating request: %w", err))
		return err
	}

	req.Header.Set("Authorization", w.BearerToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		rr.CompleteWithError(fmt.Errorf("error making request: %w", err))
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		err := fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
		rr.CompleteWithError(err)
		return err
	}

	fmt.Printf("Worker %d: Successfully processed task\n", w.ID)
	rr.Complete(nil) // Success case
	return nil
}

func main() {
	// Start the server
	server := NewServer()
	http.HandleFunc("/api", server.handleRequest)
	go func() {
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	// Wait for the server to start
	time.Sleep(time.Second)

	// Client code
	ctx := context.Background()

	workers := []retrypool.Worker[*retrypool.RequestResponse[Data, error]]{
		&APIWorker{BearerToken: "token1"},
		&APIWorker{BearerToken: "token2"},
		&APIWorker{BearerToken: "token3"},
	}

	pool := retrypool.New(ctx, workers,
		retrypool.WithAttempts[*retrypool.RequestResponse[Data, error]](3),
		retrypool.WithDelay[*retrypool.RequestResponse[Data, error]](time.Second),
		retrypool.WithMaxDelay[*retrypool.RequestResponse[Data, error]](5*time.Second),
		retrypool.WithMaxJitter[*retrypool.RequestResponse[Data, error]](500*time.Millisecond),
		retrypool.WithOnTaskFailure[*retrypool.RequestResponse[Data, error]](func(data *retrypool.RequestResponse[Data, error], err error) retrypool.TaskAction {
			data.ConsultRequest(func(d Data) {
				log.Printf("Task failed (URL: %s): %v", d.URL, err)
			})
			return retrypool.TaskActionRetry
		}),
	)

	for i := 0; i < 20; i++ {
		task := retrypool.NewRequestResponse[Data, error](Data{
			URL:     "http://localhost:8080/api",
			Payload: map[string]interface{}{"key": fmt.Sprintf("value%d", i)},
		})

		err := pool.Submit(task, retrypool.WithBounceRetry[*retrypool.RequestResponse[Data, error]]())
		if err != nil {
			log.Printf("Failed to dispatch task: %v", err)
			continue
		}
	}

	err := pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		fmt.Printf("Queue size: %d, Processing: %d, Dead tasks: %d\n",
			queueSize, processingCount, deadTaskCount)
		return queueSize > 0 || processingCount > 0
	}, 500*time.Millisecond)

	if err != nil {
		log.Printf("Error waiting for tasks: %v", err)
	}

	// View dead tasks
	pool.RangeDeadTasks(func(dt *retrypool.DeadTask[*retrypool.RequestResponse[Data, error]]) bool {
		fmt.Printf("Dead task (after %d retries): %v\n", dt.Retries, dt.Reason)
		return true
	})

	// Get metrics snapshot
	metrics := pool.GetMetricsSnapshot()
	fmt.Printf("Final metrics - Submitted: %d, Processed: %d, Succeeded: %d, Failed: %d, Dead: %d\n",
		metrics.TasksSubmitted, metrics.TasksProcessed, metrics.TasksSucceeded,
		metrics.TasksFailed, metrics.DeadTasks)

	if err := pool.Close(); err != nil {
		log.Printf("Error closing pool: %v", err)
	}
}
