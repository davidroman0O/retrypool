package tests

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/davidroman0O/retrypool"
)

// panicWorker is a test worker that can be configured to panic
type panicWorker struct {
	ID               int
	mu               sync.Mutex
	attempts         map[int]int      // Tracks attempts per task
	panicOnAttempts  map[int][]int    // Maps task ID to which attempts should panic
	panics           map[int]int      // Tracks number of panics per task
	stackTraces      map[int][]string // Stores stack traces per task
	successfulTasks  map[int]bool     // Tracks which tasks completed successfully
	recoveryMessages map[int][]string // Stores panic messages per task
}

func newPanicWorker() *panicWorker {
	return &panicWorker{
		attempts:         make(map[int]int),
		panicOnAttempts:  make(map[int][]int),
		panics:           make(map[int]int),
		stackTraces:      make(map[int][]string),
		successfulTasks:  make(map[int]bool),
		recoveryMessages: make(map[int][]string),
	}
}

func (w *panicWorker) Run(_ context.Context, data int) error {
	w.mu.Lock()
	w.attempts[data]++
	currentAttempt := w.attempts[data]
	shouldPanic := false

	// Check if this attempt should panic
	if panicAttempts, exists := w.panicOnAttempts[data]; exists {
		for _, pa := range panicAttempts {
			if pa == currentAttempt {
				shouldPanic = true
				break
			}
		}
	}
	w.mu.Unlock()

	if shouldPanic {
		panic(fmt.Sprintf("planned panic for task %d on attempt %d", data, currentAttempt))
	}

	w.mu.Lock()
	w.successfulTasks[data] = true
	w.mu.Unlock()
	return nil
}

func TestWorkerPanic(t *testing.T) {
	tests := []struct {
		name            string
		maxAttempts     int
		taskPanics      map[int][]int // Maps task ID to attempts that should panic
		syncMode        bool
		expectedDeads   int
		expectedSuccess int
	}{
		{
			name:        "Single panic with retry success",
			maxAttempts: 2,
			taskPanics: map[int][]int{
				1: {1}, // Task 1 panics on first attempt, succeeds on second
			},
			syncMode:        false,
			expectedDeads:   0,
			expectedSuccess: 1,
		},
		{
			name:        "Multiple panics exceed max attempts",
			maxAttempts: 3,
			taskPanics: map[int][]int{
				1: {1, 2, 3}, // Task 1 panics on all attempts
			},
			syncMode:        false,
			expectedDeads:   1,
			expectedSuccess: 0,
		},
		{
			name:        "Mixed panic behavior sync mode",
			maxAttempts: 3,
			taskPanics: map[int][]int{
				1: {1, 2},    // Task 1 panics twice, succeeds on third attempt
				2: {1, 2, 3}, // Task 2 panics on all attempts
			},
			syncMode:        true,
			expectedDeads:   1,
			expectedSuccess: 1,
		},
		{
			name:        "One shot panic to deadtask",
			maxAttempts: 1,
			taskPanics: map[int][]int{
				1: {1}, // Task 1 panics on first and only attempt
			},
			syncMode:        false,
			expectedDeads:   1,
			expectedSuccess: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			worker := newPanicWorker()

			// Configure worker panic behavior
			for taskID, panicAttempts := range tt.taskPanics {
				worker.panicOnAttempts[taskID] = panicAttempts
			}

			panicHandler := func(workerID int, recovery interface{}, stackTrace string) {
				worker.mu.Lock()
				defer worker.mu.Unlock()

				// Extract task ID from panic message
				msg := fmt.Sprintf("%v", recovery)
				var taskID int
				fmt.Sscanf(msg, "planned panic for task %d", &taskID)

				worker.panics[taskID]++
				worker.stackTraces[taskID] = append(worker.stackTraces[taskID], stackTrace)
				worker.recoveryMessages[taskID] = append(worker.recoveryMessages[taskID], msg)
			}

			options := []retrypool.Option[int]{
				retrypool.WithAttempts[int](tt.maxAttempts),
				retrypool.WithDelay[int](50 * time.Millisecond),
				retrypool.WithOnWorkerPanic[int](panicHandler),
			}

			if tt.syncMode {
				options = append(options, retrypool.WithSynchronousMode[int]())
			}

			pool := retrypool.New(ctx, []retrypool.Worker[int]{worker}, options...)

			// Submit all tasks
			for taskID := range tt.taskPanics {
				if err := pool.Submit(taskID); err != nil {
					t.Fatalf("Failed to submit task %d: %v", taskID, err)
				}
			}

			// Wait for completion
			err := pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
				worker.mu.Lock()
				successCount := len(worker.successfulTasks)
				worker.mu.Unlock()

				return queueSize > 0 || processingCount > 0 ||
					(successCount < tt.expectedSuccess && deadTaskCount < tt.expectedDeads)
			}, 100*time.Millisecond)

			if err != nil {
				t.Fatal(err)
			}

			// Allow time for final processing
			time.Sleep(200 * time.Millisecond)

			// Verify results
			worker.mu.Lock()
			defer worker.mu.Unlock()

			// Verify successful tasks
			if got := len(worker.successfulTasks); got != tt.expectedSuccess {
				t.Errorf("Expected %d successful tasks, got %d", tt.expectedSuccess, got)
			}

			// Verify dead tasks
			deadCount := pool.DeadTaskCount()
			if int(deadCount) != tt.expectedDeads {
				t.Errorf("Expected %d dead tasks, got %d", tt.expectedDeads, deadCount)
			}

			// Verify panic occurrences
			for taskID, expectedPanics := range tt.taskPanics {
				actualPanics := len(worker.recoveryMessages[taskID])
				expectedPanicCount := len(expectedPanics)
				if expectedPanicCount > tt.maxAttempts {
					expectedPanicCount = tt.maxAttempts
				}

				if actualPanics != expectedPanicCount {
					t.Errorf("Task %d: Expected %d panics, got %d",
						taskID, expectedPanicCount, actualPanics)
				}

				// Verify stack traces were captured
				if len(worker.stackTraces[taskID]) != actualPanics {
					t.Errorf("Task %d: Missing stack traces for some panics", taskID)
				}

				// Verify stack traces contain useful information
				for _, trace := range worker.stackTraces[taskID] {
					if !strings.Contains(trace, "runtime/panic.go") {
						t.Errorf("Task %d: Stack trace doesn't contain expected panic info: %s",
							taskID, trace)
					}
				}

				// Verify attempts count
				expectedAttempts := expectedPanicCount
				if worker.successfulTasks[taskID] {
					expectedAttempts++ // Add one for the successful attempt
				}
				if worker.attempts[taskID] != expectedAttempts {
					t.Errorf("Task %d: Expected %d attempts, got %d",
						taskID, expectedAttempts, worker.attempts[taskID])
				}
			}

			if err := pool.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
