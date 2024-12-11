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

// ScalingEvent represents a single scaling operation with enhanced metrics
type ScalingEvent struct {
	Timestamp      time.Time
	EventType      string // "up" or "down"
	WorkersBefore  int
	WorkersAfter   int
	QueueSize      int
	Processing     int
	LoadRatio      float64
	AvgLatency     time.Duration
	TriggerReason  string    // "load" or "latency"
	LastScaleTime  time.Time // To track time between scales
	CompletionRate float64   // Tasks completed per second at time of scaling
}

// ScalingHistory tracks all scaling events with enhanced analytics
type ScalingHistory struct {
	events         []ScalingEvent
	startTime      time.Time
	completedTasks int64
	mu             sync.Mutex
}

func NewScalingHistory() *ScalingHistory {
	return &ScalingHistory{
		startTime: time.Now(),
	}
}

func (sh *ScalingHistory) Add(event ScalingEvent) {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	// Calculate time since last scale
	if len(sh.events) > 0 {
		event.LastScaleTime = sh.events[len(sh.events)-1].Timestamp
	}

	sh.events = append(sh.events, event)
}

func (sh *ScalingHistory) IncrementCompleted() {
	atomic.AddInt64(&sh.completedTasks, 1)
}

func (sh *ScalingHistory) Print() {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	fmt.Println("\n=== Scaling History ===")
	if len(sh.events) == 0 {
		fmt.Println("No scaling events occurred")
		return
	}

	var totalLatency time.Duration
	upEvents := 0
	downEvents := 0
	maxWorkers := 0
	loadTriggered := 0
	latencyTriggered := 0

	for i, event := range sh.events {
		fmt.Printf("\nEvent %d at %s:\n", i+1, event.Timestamp.Format("15:04:05"))
		fmt.Printf("  Type: %s (Triggered by: %s)\n", event.EventType, event.TriggerReason)
		fmt.Printf("  Workers: %d → %d (%+d)\n",
			event.WorkersBefore,
			event.WorkersAfter,
			event.WorkersAfter-event.WorkersBefore)
		fmt.Printf("  Queue Size: %d, Processing: %d\n", event.QueueSize, event.Processing)
		fmt.Printf("  Load Ratio: %.2f\n", event.LoadRatio)
		fmt.Printf("  Avg Latency: %v\n", event.AvgLatency)
		fmt.Printf("  Completion Rate: %.1f tasks/sec\n", event.CompletionRate)

		if event.LastScaleTime != (time.Time{}) {
			timeSinceLastScale := event.Timestamp.Sub(event.LastScaleTime)
			fmt.Printf("  Time Since Last Scale: %v\n", timeSinceLastScale)
		}

		if event.EventType == "up" {
			upEvents++
		} else {
			downEvents++
		}
		if event.WorkersAfter > maxWorkers {
			maxWorkers = event.WorkersAfter
		}
		if event.TriggerReason == "load" {
			loadTriggered++
		} else {
			latencyTriggered++
		}
		totalLatency += event.AvgLatency
	}

	avgLatency := totalLatency / time.Duration(len(sh.events))
	totalDuration := time.Since(sh.startTime)
	overallRate := float64(atomic.LoadInt64(&sh.completedTasks)) / totalDuration.Seconds()

	fmt.Printf("\nPerformance Summary:\n")
	fmt.Printf("Total runtime: %v\n", totalDuration.Round(time.Second))
	fmt.Printf("Average processing latency: %v\n", avgLatency)
	fmt.Printf("Overall completion rate: %.1f tasks/sec\n", overallRate)

	fmt.Printf("\nScaling Summary:\n")
	fmt.Printf("Total scaling events: %d\n", len(sh.events))
	fmt.Printf("Scale up events: %d\n", upEvents)
	fmt.Printf("Scale down events: %d\n", downEvents)
	fmt.Printf("Load triggered: %d\n", loadTriggered)
	fmt.Printf("Latency triggered: %d\n", latencyTriggered)
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

	history := NewScalingHistory()

	pool := retrypool.New(
		ctx,
		workers,
		retrypool.WithLogLevel[int](logs.LevelDebug),
		retrypool.WithAutoscaler[int](
			retrypool.AutoscalerConfig{
				MinWorkers:         2,
				MaxWorkers:         10,
				ScaleUpThreshold:   1.2,             // More aggressive up scaling
				ScaleDownThreshold: 0.6,             // More aggressive down scaling
				CooldownPeriod:     1 * time.Second, // Shorter cooldown
				ScaleUpStep:        1,               // Smaller steps for more granular scaling
				ScaleDownStep:      1,
				TargetLatency:      150 * time.Millisecond,
			},
		),
		retrypool.WithOnScaleUp[int](func(before, after int, metrics retrypool.AutoscalerMetrics) {
			loadRatio := float64(metrics.QueueSize+metrics.ProcessingCount) / float64(before)
			triggerReason := "load"
			if metrics.AverageLatency > 150*time.Millisecond {
				triggerReason = "latency"
			}

			completionRate := float64(atomic.LoadInt64(&history.completedTasks)) / time.Since(history.startTime).Seconds()

			history.Add(ScalingEvent{
				Timestamp:      time.Now(),
				EventType:      "up",
				WorkersBefore:  before,
				WorkersAfter:   after,
				QueueSize:      metrics.QueueSize,
				Processing:     metrics.ProcessingCount,
				LoadRatio:      loadRatio,
				AvgLatency:     metrics.AverageLatency,
				TriggerReason:  triggerReason,
				CompletionRate: completionRate,
			})
		}),
		retrypool.WithOnScaleDown[int](func(before, after int, metrics retrypool.AutoscalerMetrics) {
			loadRatio := float64(metrics.QueueSize+metrics.ProcessingCount) / float64(before)
			completionRate := float64(atomic.LoadInt64(&history.completedTasks)) / time.Since(history.startTime).Seconds()

			history.Add(ScalingEvent{
				Timestamp:      time.Now(),
				EventType:      "down",
				WorkersBefore:  before,
				WorkersAfter:   after,
				QueueSize:      metrics.QueueSize,
				Processing:     metrics.ProcessingCount,
				LoadRatio:      loadRatio,
				AvgLatency:     metrics.AverageLatency,
				TriggerReason:  "load",
				CompletionRate: completionRate,
			})
		}),
	)

	// More dynamic load pattern
	waves := []struct {
		taskCount int
		duration  int
		wait      time.Duration
	}{
		{10, 100, 3 * time.Second}, // Light initial load
		{30, 200, 5 * time.Second}, // Medium load - should trigger scale up
		{50, 150, 5 * time.Second}, // Heavy load - more scale up
		{70, 300, 8 * time.Second}, // Very heavy load with slower tasks
		{20, 100, 5 * time.Second}, // Reduced load - should start scaling down
		{5, 50, 10 * time.Second},  // Light load - more scale down
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

	metrics := pool.Metrics()
	fmt.Printf("\nFinal metrics: %+v\n", metrics)

	// Print detailed scaling history
	history.Print()
}
