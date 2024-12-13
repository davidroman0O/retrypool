package tests

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/davidroman0O/retrypool"
)

type rateTestWorker struct {
	mu              sync.Mutex
	processingTimes []time.Time
}

func (w *rateTestWorker) Run(_ context.Context, data int) error {
	now := time.Now()
	w.mu.Lock()
	w.processingTimes = append(w.processingTimes, now)
	w.mu.Unlock()
	fmt.Printf("Worker processing task: %d at %v\n", data, now)
	return nil
}

// GetProcessingTimes returns a copy of the processing times
func (w *rateTestWorker) GetProcessingTimes() []time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	timesCopy := make([]time.Time, len(w.processingTimes))
	copy(timesCopy, w.processingTimes)
	return timesCopy
}

// GetIntervals calculates the intervals between processing times
func (w *rateTestWorker) GetIntervals() []time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	intervals := []time.Duration{}
	for i := 1; i < len(w.processingTimes); i++ {
		interval := w.processingTimes[i].Sub(w.processingTimes[i-1])
		intervals = append(intervals, interval)
	}
	return intervals
}

func TestRateLimit(t *testing.T) {
	tests := []struct {
		name             string
		rateLimit        float64 // requests per second
		numTasks         int
		expectedInterval time.Duration
		acceptableJitter time.Duration
		syncMode         bool
		description      string
	}{
		{
			name:             "Basic Rate Limit Async",
			rateLimit:        10,
			numTasks:         10,
			expectedInterval: 100 * time.Millisecond,
			acceptableJitter: 20 * time.Millisecond,
			syncMode:         false,
			description:      "Standard case with moderate rate",
		},
		{
			name:             "Basic Rate Limit Sync",
			rateLimit:        10,
			numTasks:         10,
			expectedInterval: 100 * time.Millisecond,
			acceptableJitter: 20 * time.Millisecond,
			syncMode:         true,
			description:      "Standard case in sync mode",
		},
		{
			name:             "High Rate Async",
			rateLimit:        100,
			numTasks:         50,
			expectedInterval: 10 * time.Millisecond,
			acceptableJitter: 8 * time.Millisecond, // the more tasks, the more jitter - acceptable
			syncMode:         false,
			description:      "High throughput in async mode",
		},
		{
			name:             "High Rate Sync",
			rateLimit:        100,
			numTasks:         50,
			expectedInterval: 10 * time.Millisecond,
			acceptableJitter: 8 * time.Millisecond, // the more tasks, the more jitter - acceptable
			syncMode:         true,
			description:      "High throughput in sync mode",
		},
		{
			name:             "Very Low Rate Async",
			rateLimit:        2,
			numTasks:         6,
			expectedInterval: 500 * time.Millisecond,
			acceptableJitter: 50 * time.Millisecond,
			syncMode:         false,
			description:      "Very conservative rate limiting async",
		},
		{
			name:             "Very Low Rate Sync",
			rateLimit:        2,
			numTasks:         6,
			expectedInterval: 500 * time.Millisecond,
			acceptableJitter: 50 * time.Millisecond,
			syncMode:         true,
			description:      "Very conservative rate limiting sync",
		},
		{
			name:             "Burst Test Async",
			rateLimit:        5,
			numTasks:         20,
			expectedInterval: 200 * time.Millisecond,
			acceptableJitter: 40 * time.Millisecond,
			syncMode:         false,
			description:      "Test burst behavior in async mode",
		},
		{
			name:             "Burst Test Sync",
			rateLimit:        5,
			numTasks:         20,
			expectedInterval: 200 * time.Millisecond,
			acceptableJitter: 40 * time.Millisecond,
			syncMode:         true,
			description:      "Test burst behavior in sync mode",
		},
		{
			name:             "Single Task Per Second Async",
			rateLimit:        1,
			numTasks:         5,
			expectedInterval: 1 * time.Second,
			acceptableJitter: 100 * time.Millisecond,
			syncMode:         false,
			description:      "Extreme rate limiting async",
		},
		{
			name:             "Single Task Per Second Sync",
			rateLimit:        1,
			numTasks:         5,
			expectedInterval: 1 * time.Second,
			acceptableJitter: 100 * time.Millisecond,
			syncMode:         true,
			description:      "Extreme rate limiting sync",
		},
		{
			name:             "Mixed Rate Test Async",
			rateLimit:        8,
			numTasks:         15,
			expectedInterval: 125 * time.Millisecond,
			acceptableJitter: 25 * time.Millisecond,
			syncMode:         false,
			description:      "Moderate rate with odd number of tasks async",
		},
		{
			name:             "Mixed Rate Test Sync",
			rateLimit:        8,
			numTasks:         15,
			expectedInterval: 125 * time.Millisecond,
			acceptableJitter: 25 * time.Millisecond,
			syncMode:         true,
			description:      "Moderate rate with odd number of tasks sync",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
			defer cancel()

			worker := &rateTestWorker{}
			options := []retrypool.Option[int]{
				retrypool.WithRateLimit[int](tt.rateLimit),
			}

			if tt.syncMode {
				options = append(options, retrypool.WithSynchronousMode[int]())
			}

			pool := retrypool.New(ctx, []retrypool.Worker[int]{worker}, options...)

			// Submit tasks
			for i := 0; i < tt.numTasks; i++ {
				err := pool.Submit(i)
				if err != nil {
					t.Fatalf("Failed to submit task %d: %v", i, err)
				}
			}

			// Wait for all tasks to be processed
			err := pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
				return len(worker.GetProcessingTimes()) < tt.numTasks
			}, 10*time.Millisecond)

			if err != nil {
				t.Fatalf("Error waiting for completion: %v", err)
			}

			// Analyze intervals
			times := worker.GetProcessingTimes()
			if len(times) != tt.numTasks {
				t.Errorf("Expected %d tasks, got %d", tt.numTasks, len(times))
			}

			intervals := worker.GetIntervals()
			if len(intervals) == 0 {
				t.Errorf("No intervals recorded")
				return
			}

			expectedInterval := tt.expectedInterval
			acceptableJitter := tt.acceptableJitter

			// Skip initial intervals to avoid startup anomalies
			startIndex := 0
			skipIntervals := 2
			if !tt.syncMode && len(intervals) > skipIntervals {
				startIndex = skipIntervals
			} else if tt.syncMode && len(intervals) > 1 {
				startIndex = 1
			}

			var outlierCount int
			var totalInterval time.Duration
			var validIntervals int

			for i := startIndex; i < len(intervals); i++ {
				interval := intervals[i]
				deviation := math.Abs(float64(interval - expectedInterval))
				if deviation > float64(acceptableJitter) {
					t.Logf("Interval %d (%v) is outside acceptable range (%v ± %v)", i+1, interval, expectedInterval, acceptableJitter)
					outlierCount++
				} else {
					t.Logf("Interval %d is acceptable: %v", i+1, interval)
				}
				totalInterval += interval
				validIntervals++
			}

			totalIntervals := len(intervals) - startIndex
			if totalIntervals > 0 {
				outlierPercentage := float64(outlierCount) / float64(totalIntervals) * 100
				t.Logf("Outlier intervals: %d/%d (%.2f%%)", outlierCount, totalIntervals, outlierPercentage)
				if outlierPercentage > 20 { // Increased acceptable outlier percentage to 20%
					t.Errorf("Too many intervals outside acceptable range")
				}
			}

			// Calculate average interval
			if validIntervals > 0 {
				averageInterval := totalInterval / time.Duration(validIntervals)
				t.Logf("Average interval: %v (expected ~%v)", averageInterval, expectedInterval)
			}

			if err := pool.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
