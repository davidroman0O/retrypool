package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/davidroman0O/retrypool"
	"github.com/davidroman0O/retrypool/logs"
)

// ScalingEvent represents a single scaling operation
type ScalingEvent struct {
	Timestamp     time.Time
	EventType     string // "up" or "down"
	WorkersBefore int
	WorkersAfter  int
	QueueSize     int
	Processing    int
	LoadRatio     float64
}

// ScalingHistory tracks all scaling events
type ScalingHistory struct {
	events []ScalingEvent
	mu     sync.Mutex
}

func (sh *ScalingHistory) Add(event ScalingEvent) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.events = append(sh.events, event)
}

func (sh *ScalingHistory) Print() {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	fmt.Println("\n=== Scaling History ===")
	if len(sh.events) == 0 {
		fmt.Println("No scaling events occurred")
		return
	}

	for i, event := range sh.events {
		fmt.Printf("\nEvent %d at %s:\n", i+1, event.Timestamp.Format("15:04:05"))
		fmt.Printf("  Type: %s\n", event.EventType)
		fmt.Printf("  Workers: %d → %d (%+d)\n",
			event.WorkersBefore,
			event.WorkersAfter,
			event.WorkersAfter-event.WorkersBefore)
		fmt.Printf("  Queue Size: %d\n", event.QueueSize)
		fmt.Printf("  Processing: %d\n", event.Processing)
		fmt.Printf("  Load Ratio: %.2f\n", event.LoadRatio)
	}

	// Print summary
	upEvents := 0
	downEvents := 0
	maxWorkers := 0
	for _, event := range sh.events {
		if event.EventType == "up" {
			upEvents++
		} else {
			downEvents++
		}
		if event.WorkersAfter > maxWorkers {
			maxWorkers = event.WorkersAfter
		}
	}

	fmt.Printf("\nSummary:\n")
	fmt.Printf("Total scaling events: %d\n", len(sh.events))
	fmt.Printf("Scale up events: %d\n", upEvents)
	fmt.Printf("Scale down events: %d\n", downEvents)
	fmt.Printf("Maximum workers reached: %d\n", maxWorkers)
}

type SimpleWorker struct {
	processed int32
}

func (w *SimpleWorker) Run(ctx context.Context, data int) error {
	atomic.AddInt32(&w.processed, 1)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Duration(data) * time.Millisecond):
		return nil
	}
}

func main() {
	ctx := context.Background()
	workers := []retrypool.Worker[int]{&SimpleWorker{}, &SimpleWorker{}}

	// Create scaling history tracker
	history := &ScalingHistory{}

	pool := retrypool.New(
		ctx,
		workers,
		retrypool.WithLogLevel[int](logs.LevelDebug),
		retrypool.WithAutoscaler[int](
			retrypool.AutoscalerConfig{
				MinWorkers:         2,
				MaxWorkers:         10,
				ScaleUpThreshold:   1.5,
				ScaleDownThreshold: 0.5,
				CooldownPeriod:     3 * time.Second,
				ScaleUpStep:        2,
				ScaleDownStep:      1,
				TargetLatency:      200 * time.Millisecond,
			},
		),
		// Add callbacks to track scaling events
		retrypool.WithOnScaleUp[int](func(before, after int, metrics retrypool.AutoscalerMetrics) {
			history.Add(ScalingEvent{
				Timestamp:     time.Now(),
				EventType:     "up",
				WorkersBefore: before,
				WorkersAfter:  after,
				QueueSize:     metrics.QueueSize,
				Processing:    metrics.ProcessingCount,
				LoadRatio:     float64(metrics.QueueSize+metrics.ProcessingCount) / float64(before),
			})
		}),
		retrypool.WithOnScaleDown[int](func(before, after int, metrics retrypool.AutoscalerMetrics) {
			history.Add(ScalingEvent{
				Timestamp:     time.Now(),
				EventType:     "down",
				WorkersBefore: before,
				WorkersAfter:  after,
				QueueSize:     metrics.QueueSize,
				Processing:    metrics.ProcessingCount,
				LoadRatio:     float64(metrics.QueueSize+metrics.ProcessingCount) / float64(before),
			})
		}),
	)

	waves := []struct {
		taskCount int
		duration  int
		wait      time.Duration
	}{
		{10, 100, 5 * time.Second},  // Light load
		{30, 200, 8 * time.Second},  // Medium load - should trigger scale up
		{50, 300, 10 * time.Second}, // Heavy load - should trigger more scaling
		{5, 50, 15 * time.Second},   // Light load - should trigger scale down
	}

	for i, wave := range waves {
		fmt.Printf("\n=== Wave %d: Submitting %d tasks (%dms each) ===\n",
			i+1, wave.taskCount, wave.duration)

		for j := 0; j < wave.taskCount; j++ {
			if err := pool.Submit(wave.duration); err != nil {
				log.Printf("Failed to submit task: %v", err)
			}
		}

		deadline := time.Now().Add(wave.wait)
		ticker := time.NewTicker(time.Second)
		for time.Now().Before(deadline) {
			qSize := pool.QueueSize()
			pCount := pool.ProcessingCount()
			dCount := pool.DeadTaskCount()
			fmt.Printf("Status: Queue=%d, Processing=%d, Dead=%d\n",
				qSize, pCount, dCount)
			<-ticker.C
			if qSize == 0 && pCount == 0 {
				break
			}
		}
		ticker.Stop()
	}

	pool.Shutdown()
	fmt.Printf("\nFinal metrics: %+v\n", pool.Metrics())

	// Print scaling history
	history.Print()
}
