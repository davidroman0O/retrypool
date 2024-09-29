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

/// this was the main usecase why I created this library

// Server-side code
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

// Client-side code
type APIWorker struct {
	ID          int
	BearerToken string
}

type TaskData struct {
	URL     string
	Payload interface{}
}

func (w *APIWorker) Run(ctx context.Context, data TaskData) error {
	client := &http.Client{}
	payload, err := json.Marshal(data.Payload)
	if err != nil {
		return fmt.Errorf("error marshaling payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", data.URL, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	req.Header.Set("Authorization", w.BearerToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	fmt.Printf("Worker %d: Successfully processed task\n", w.ID)
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

	workers := []retrypool.Worker[TaskData]{
		&APIWorker{ID: 1, BearerToken: "token1"},
		&APIWorker{ID: 2, BearerToken: "token2"},
		&APIWorker{ID: 3, BearerToken: "token3"},
	}

	pool := retrypool.New(ctx, workers,
		retrypool.WithAttempts[TaskData](3),
		retrypool.WithDelay[TaskData](time.Second),
		retrypool.WithMaxDelay[TaskData](5*time.Second),
		retrypool.WithMaxJitter[TaskData](500*time.Millisecond),
		retrypool.WithOnRetry[TaskData](func(attempt int, err error, task *retrypool.TaskWrapper[TaskData]) {
			log.Printf("Retrying task (URL: %s) after attempt %d: %v", task.Data().URL, attempt, err)
		}),
	)

	for i := 0; i < 20; i++ {
		task := TaskData{
			URL:     "http://localhost:8080/api",
			Payload: map[string]interface{}{"key": fmt.Sprintf("value%d", i)},
		}

		err := pool.Dispatch(task)
		if err != nil {
			log.Printf("Failed to dispatch task: %v", err)
		}
	}

	pool.WaitWithCallback(ctx, func(queueSize, processingCount int) bool {
		fmt.Printf("Queue size: %d, Processing: %d\n", queueSize, processingCount)
		return queueSize > 0 || processingCount > 0
	}, 500*time.Millisecond)

	deadTasks := pool.DeadTasks()
	fmt.Printf("Number of dead tasks: %d\n", len(deadTasks))
	for _, task := range deadTasks {
		fmt.Printf("Dead task: %+v\n", task)
	}

	fmt.Println("All tasks completed")
}
