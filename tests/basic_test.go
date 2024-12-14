package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/davidroman0O/retrypool"
)

type testWorkerInt struct {
	ID int
}

func (w *testWorkerInt) Run(ctx context.Context, data int) error {
	time.Sleep(time.Duration(data) * time.Millisecond)
	fmt.Printf("worker %d processed: %d\n", w.ID, data)
	return nil
}

func TestSubmit(t *testing.T) {
	tests := []struct {
		name     string
		syncMode bool
	}{
		{"Asynchronous Mode", false},
		{"Synchronous Mode", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			var pool *retrypool.Pool[int]
			if tt.syncMode {
				pool = retrypool.New(
					ctx,
					[]retrypool.Worker[int]{&testWorkerInt{}},
					retrypool.WithSynchronousMode[int](),
				)
			} else {
				pool = retrypool.New(
					ctx,
					[]retrypool.Worker[int]{&testWorkerInt{}},
				)
			}

			if err := pool.Submit(1); err != nil {
				t.Fatal(err)
			}

			if err := pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
				return queueSize > 0 && processingCount > 0 && deadTaskCount > 0
			}, time.Second); err != nil {
				t.Fatal(err)
			}

			if err := pool.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// ProcessedTask holds the task data and the worker ID that processed it
type ProcessedTask struct {
	TaskData int
	WorkerID int
}

// MockWorker is a mock implementation of the Worker interface.
// It records the tasks it processes and the order in which they are processed.
type robinMockWorker struct {
	ID int // Exported field so it can be set via reflection
}

func (w *robinMockWorker) Run(ctx context.Context, data int) error {
	// Record the task data and worker ID
	processedTasksMu.Lock()
	processedTasks = append(processedTasks, ProcessedTask{
		TaskData: data,
		WorkerID: w.ID,
	})
	processedTasksMu.Unlock()
	return nil
}

// Shared variables to collect processed tasks
var (
	processedTasks   []ProcessedTask
	processedTasksMu sync.Mutex
)

func TestRoundRobinDistribution(t *testing.T) {
	testCases := []struct {
		name  string
		async bool
	}{
		{
			name:  "Synchronous Mode",
			async: false,
		},
		{
			name:  "Asynchronous Mode",
			async: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset processedTasks before each subtest
			processedTasksMu.Lock()
			processedTasks = nil
			processedTasksMu.Unlock()

			ctx := context.Background()

			// Number of workers and tasks
			numWorkers := 3
			numTasks := 100

			// Create mock workers
			workers := make([]retrypool.Worker[int], numWorkers)
			for i := 0; i < numWorkers; i++ {
				mockWorker := &robinMockWorker{}
				workers[i] = mockWorker
			}

			// Configure pool options
			options := []retrypool.Option[int]{
				retrypool.WithRoundRobinDistribution[int](),
			}
			if !tc.async {
				options = append(options, retrypool.WithSynchronousMode[int]())
			}

			// Create the pool with the specified options
			pool := retrypool.New[int](
				ctx,
				workers,
				options...,
			)

			defer pool.Close()

			// Submit tasks
			for i := 0; i < numTasks; i++ {
				err := pool.Submit(i)
				if err != nil {
					t.Fatalf("Failed to submit task %d: %v", i, err)
				}
			}

			// Wait for all tasks to be processed
			err := pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
				return queueSize > 0 || processingCount > 0
			}, 10*time.Millisecond)
			if err != nil {
				t.Fatalf("Error while waiting for tasks to complete: %v", err)
			}

			// Check that we have processed all tasks
			processedTasksMu.Lock()
			if len(processedTasks) != numTasks {
				t.Fatalf("Expected %d processed tasks, got %d", numTasks, len(processedTasks))
			}

			// Build the assignments
			taskAssignments := make([]int, numTasks)
			for _, pt := range processedTasks {
				taskAssignments[pt.TaskData] = pt.WorkerID
			}
			processedTasksMu.Unlock()

			// Verify that tasks were assigned in round-robin fashion
			for i := 0; i < numTasks; i++ {
				expectedWorkerID := i % numWorkers
				actualWorkerID := taskAssignments[i]
				fmt.Printf("Task %d: expected worker %d, actual worker %d\n", i, expectedWorkerID, actualWorkerID)
				if actualWorkerID != expectedWorkerID {
					t.Errorf("Task %d was processed by worker %d, expected worker %d", i, actualWorkerID, expectedWorkerID)
				}
			}
		})
	}
}

// AggressiveWorker is a worker implementation for testing.
type AggressiveWorker struct {
	Index int // Unique identifier assigned in the test
}

func (w *AggressiveWorker) Run(ctx context.Context, data int) error {
	// Simulate variable processing time
	time.Sleep(5 * time.Millisecond)

	// Record the task data and worker index
	processedTasksMu.Lock()
	processedTasks = append(processedTasks, ProcessedTask{
		TaskData: data,
		WorkerID: w.Index,
	})
	processedTasksMu.Unlock()
	return nil
}

func TestAggressiveRoundRobinDistribution(t *testing.T) {
	testCases := []struct {
		name       string
		async      bool
		numWorkers int
		numTasks   int
	}{
		{
			name:       "Synchronous Mode - High Load",
			async:      false,
			numWorkers: 10,
			numTasks:   100,
		},
		{
			name:       "Asynchronous Mode - High Load",
			async:      true,
			numWorkers: 10,
			numTasks:   100,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset processedTasks before each subtest
			processedTasksMu.Lock()
			processedTasks = nil
			processedTasksMu.Unlock()

			ctx := context.Background()

			// Create workers with assigned indices
			workers := make([]retrypool.Worker[int], tc.numWorkers)
			for i := 0; i < tc.numWorkers; i++ {
				worker := &AggressiveWorker{Index: i}
				workers[i] = worker
			}

			// Configure pool options
			options := []retrypool.Option[int]{
				retrypool.WithRoundRobinDistribution[int](),
			}
			if !tc.async {
				options = append(options, retrypool.WithSynchronousMode[int]())
			}

			// Create the pool with the specified options
			pool := retrypool.New[int](
				ctx,
				workers,
				options...,
			)
			defer pool.Close()

			// Submit tasks sequentially
			for i := 0; i < tc.numTasks; i++ {
				err := pool.Submit(i)
				if err != nil {
					t.Fatalf("Failed to submit task %d: %v", i, err)
				}
			}

			// Wait for all tasks to be processed
			err := pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
				return queueSize > 0 || processingCount > 0
			}, 10*time.Millisecond)
			if err != nil {
				t.Fatalf("Error while waiting for tasks to complete: %v", err)
			}

			// Check that we have processed all tasks
			processedTasksMu.Lock()
			if len(processedTasks) != tc.numTasks {
				t.Fatalf("Expected %d processed tasks, got %d", tc.numTasks, len(processedTasks))
			}

			// Build the assignments
			taskAssignments := make(map[int]int) // map[taskData]workerIndex
			for _, pt := range processedTasks {
				taskAssignments[pt.TaskData] = pt.WorkerID
			}
			processedTasksMu.Unlock()

			// Verify that tasks were assigned in round-robin fashion
			for i := 0; i < tc.numTasks; i++ {
				expectedWorkerIndex := i % tc.numWorkers
				actualWorkerIndex, exists := taskAssignments[i]
				fmt.Printf("Task %d: expected worker %d, actual worker %d\n", i, expectedWorkerIndex, actualWorkerIndex)
				if !exists {
					t.Errorf("Task %d was not processed", i)
					continue
				}
				if actualWorkerIndex != expectedWorkerIndex {
					t.Errorf("Task %d was processed by worker %d, expected worker %d", i, actualWorkerIndex, expectedWorkerIndex)
				}
			}
		})
	}
}
