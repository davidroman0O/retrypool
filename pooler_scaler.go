package retrypool

import (
	"context"
	"reflect"
	"sort"
	"time"

	"github.com/davidroman0O/retrypool/logs"
)

type OnScaleUpFunc[T any] func(beforeWorkers, afterWorkers int, metrics AutoscalerMetrics)
type OnScaleDownFunc[T any] func(beforeWorkers, afterWorkers int, metrics AutoscalerMetrics)

func WithOnScaleUp[T any](fn OnScaleUpFunc[T]) Option[T] {
	return func(p *Pool[T]) {
		p.onScaleUp = fn
	}
}

func WithOnScaleDown[T any](fn OnScaleDownFunc[T]) Option[T] {
	return func(p *Pool[T]) {
		p.onScaleDown = fn
	}
}

// AutoscalerConfig holds configuration for the pool's auto-scaling behavior
type AutoscalerConfig struct {
	MinWorkers         int           // Minimum number of workers to maintain
	MaxWorkers         int           // Maximum number of workers allowed
	ScaleUpThreshold   float64       // Queue size / worker ratio that triggers scale up
	ScaleDownThreshold float64       // Queue size / worker ratio that triggers scale down
	CooldownPeriod     time.Duration // Minimum time between scaling operations
	ScaleUpStep        int           // Number of workers to add when scaling up
	ScaleDownStep      int           // Number of workers to remove when scaling down
	TargetLatency      time.Duration // Target processing latency per task
}

// DefaultAutoscalerConfig returns a default configuration for the autoscaler
func DefaultAutoscalerConfig() AutoscalerConfig {
	return AutoscalerConfig{
		MinWorkers:         1,
		MaxWorkers:         10,
		ScaleUpThreshold:   2.0, // Scale up when tasks/worker > 2
		ScaleDownThreshold: 0.5, // Scale down when tasks/worker < 0.5
		CooldownPeriod:     30 * time.Second,
		ScaleUpStep:        1,
		ScaleDownStep:      1,
		TargetLatency:      1 * time.Second,
	}
}

// AutoscalerMetrics holds metrics used for scaling decisions
type AutoscalerMetrics struct {
	QueueSize       int
	ProcessingCount int
	WorkerCount     int
	AverageLatency  time.Duration
	LastScaleTime   time.Time
}

// autoscaler holds the autoscaling state and configuration
type autoscaler[T any] struct {
	config     AutoscalerConfig
	cancelFunc context.CancelFunc
	pool       *Pool[T]
}

// WithAutoscaler adds autoscaling capability to the pool
func WithAutoscaler[T any](config AutoscalerConfig) Option[T] {
	return func(p *Pool[T]) {
		// Check if autoscaler already exists
		if p.autoscaler != nil {
			logs.Warn(context.Background(), "Autoscaler already configured, ignoring duplicate configuration")
			return
		}

		ctx, cancel := context.WithCancel(p.ctx)
		p.autoscaler = &autoscaler[T]{
			config:     config,
			cancelFunc: cancel,
			pool:       p,
		}
		go p.autoscaler.run(ctx)
	}
}

// run continuously monitors the pool and adjusts the number of workers
func (a *autoscaler[T]) run(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics := a.collectMetrics()
			if metrics.WorkerCount > 0 { // Only adjust if we have workers
				a.adjustWorkerCount(metrics)
			}
		}
	}
}

// collectMetrics gathers current pool metrics for scaling decisions
func (a *autoscaler[T]) collectMetrics() AutoscalerMetrics {
	p := a.pool
	p.mu.Lock()

	// Calculate queue size directly
	queueSize := 0
	for _, queue := range p.taskQueues {
		queueSize += queue.Length()
	}

	// Collect all metrics in one critical section
	metrics := AutoscalerMetrics{
		QueueSize:       queueSize,
		ProcessingCount: p.processing,
		WorkerCount:     len(p.workers),
	}

	// Collect tasks for latency calculation
	tasks := make([]*TaskWrapper[T], 0)
	for _, queue := range p.taskQueues {
		tasks = append(tasks, queue.Tasks()...)
	}
	for _, state := range p.workers {
		if state.currentTask != nil {
			tasks = append(tasks, state.currentTask)
		}
	}

	p.mu.Unlock()

	// Calculate average latency outside the critical section
	var totalLatency time.Duration
	var taskCount int

	for _, task := range tasks {
		task.mu.Lock()
		for _, d := range task.durations {
			totalLatency += d
			taskCount++
		}
		task.mu.Unlock()
	}

	if taskCount > 0 {
		metrics.AverageLatency = totalLatency / time.Duration(taskCount)
	}

	return metrics
}

// adjustWorkerCount makes scaling decisions based on current metrics
func (a *autoscaler[T]) adjustWorkerCount(metrics AutoscalerMetrics) {
	p := a.pool
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check cooldown period
	if time.Since(metrics.LastScaleTime) < a.config.CooldownPeriod {
		logs.Debug(context.Background(), "Scaling skipped - in cooldown",
			"remainingCooldown", a.config.CooldownPeriod-time.Since(metrics.LastScaleTime))
		return
	}

	totalLoad := float64(metrics.QueueSize + metrics.ProcessingCount)
	loadRatio := totalLoad / float64(metrics.WorkerCount)

	logs.Debug(context.Background(), "Evaluating scaling conditions",
		"queueSize", metrics.QueueSize,
		"processingCount", metrics.ProcessingCount,
		"workerCount", metrics.WorkerCount,
		"loadRatio", loadRatio,
		"avgLatency", metrics.AverageLatency)

	switch {
	case a.shouldScaleUp(metrics, loadRatio):
		a.scaleUpLocked(metrics)
	case a.shouldScaleDown(metrics, loadRatio):
		a.scaleDownLocked(metrics)
	default:
		logs.Debug(context.Background(), "No scaling needed",
			"loadRatio", loadRatio,
			"workerCount", metrics.WorkerCount)
	}
}

// shouldScaleUp determines if the pool should add more workers
func (a *autoscaler[T]) shouldScaleUp(metrics AutoscalerMetrics, loadRatio float64) bool {
	if metrics.WorkerCount >= a.config.MaxWorkers {
		return false
	}

	latencyTooHigh := metrics.AverageLatency > a.config.TargetLatency
	shouldScale := loadRatio > a.config.ScaleUpThreshold || latencyTooHigh

	if shouldScale {
		logs.Debug(context.Background(), "Scale up condition met",
			"loadRatio", loadRatio,
			"threshold", a.config.ScaleUpThreshold,
			"latency", metrics.AverageLatency,
			"targetLatency", a.config.TargetLatency)
	}

	return shouldScale
}

// shouldScaleDown determines if the pool should remove workers
func (a *autoscaler[T]) shouldScaleDown(metrics AutoscalerMetrics, loadRatio float64) bool {
	if metrics.WorkerCount <= a.config.MinWorkers {
		return false
	}

	latencyGood := metrics.AverageLatency < a.config.TargetLatency/2
	shouldScale := loadRatio < a.config.ScaleDownThreshold && latencyGood

	if shouldScale {
		logs.Debug(context.Background(), "Scale down condition met",
			"loadRatio", loadRatio,
			"threshold", a.config.ScaleDownThreshold,
			"latency", metrics.AverageLatency,
			"targetLatency", a.config.TargetLatency)
	}

	return shouldScale
}

// scaleUpLocked adds workers to the pool - must be called with pool mutex held
func (a *autoscaler[T]) scaleUpLocked(metrics AutoscalerMetrics) {
	p := a.pool
	currentWorkers := len(p.workers)
	toAdd := min(a.config.ScaleUpStep, a.config.MaxWorkers-currentWorkers)

	if toAdd <= 0 {
		logs.Debug(context.Background(), "Scale up skipped - at max workers",
			"currentWorkers", currentWorkers,
			"maxWorkers", a.config.MaxWorkers)
		return
	}

	// Get an existing worker to use as a template
	var templateWorker Worker[T]
	for _, state := range p.workers {
		templateWorker = state.worker
		break
	}

	if templateWorker == nil {
		logs.Error(context.Background(), "No template worker available for scaling")
		return
	}

	beforeCount := len(p.workers)
	addedWorkers := 0

	// Create and add new workers
	for i := 0; i < toAdd; i++ {
		// Create new worker instance using reflection
		worker := reflect.New(reflect.TypeOf(templateWorker).Elem()).Interface().(Worker[T])

		// Directly add worker without acquiring lock (since we already hold it)
		workerCtx, workerCancel := context.WithCancel(p.ctx)
		state := &workerState[T]{
			worker:   worker,
			stopChan: make(chan struct{}),
			doneChan: make(chan struct{}),
			cancel:   workerCancel,
			ctx:      workerCtx,
		}

		// Set worker ID
		workerID := p.nextWorkerID
		p.nextWorkerID++
		p.workers[workerID] = state

		// Start worker goroutine
		p.errGroup.Go(func() error {
			return p.workerLoop(workerID)
		})

		addedWorkers++
	}

	afterCount := len(p.workers)

	// Trigger the scale up callback if defined
	if addedWorkers > 0 && p.onScaleUp != nil {
		p.onScaleUp(beforeCount, afterCount, metrics)
	}

	metrics.LastScaleTime = time.Now()
	logs.Info(context.Background(), "Scaled up workers",
		"before", beforeCount,
		"added", addedWorkers,
		"after", afterCount,
		"loadRatio", float64(metrics.QueueSize+metrics.ProcessingCount)/float64(metrics.WorkerCount))

	// Signal all waiting workers
	p.cond.Broadcast()
}

// scaleDownLocked removes workers from the pool - must be called with pool mutex held
func (a *autoscaler[T]) scaleDownLocked(metrics AutoscalerMetrics) {
	p := a.pool
	currentWorkers := len(p.workers)
	toRemove := min(a.config.ScaleDownStep, currentWorkers-a.config.MinWorkers)

	if toRemove <= 0 {
		logs.Debug(context.Background(), "Scale down skipped - at min workers",
			"currentWorkers", currentWorkers,
			"minWorkers", a.config.MinWorkers)
		return
	}

	// Find least busy workers
	type workerLoad struct {
		id    int
		tasks int
	}
	loads := make([]workerLoad, 0, len(p.workers))

	for id, queue := range p.taskQueues {
		loads = append(loads, workerLoad{
			id:    id,
			tasks: queue.Length(),
		})
	}

	// Sort by load ascending
	sort.Slice(loads, func(i, j int) bool {
		return loads[i].tasks < loads[j].tasks
	})

	beforeCount := len(p.workers)
	removedCount := 0

	// Remove workers with lowest load
	for i := 0; i < toRemove && i < len(loads); i++ {
		workerID := loads[i].id
		if state, exists := p.workers[workerID]; exists && !state.removed {
			state.removed = true
			state.cancel()
			close(state.stopChan)
			delete(p.workers, workerID)
			removedCount++

			// Redistribute any remaining tasks
			if queue := p.taskQueues[workerID]; queue != nil {
				p.redistributeTasksLocked(workerID, queue)
			}
			delete(p.taskQueues, workerID)
		}
	}

	afterCount := len(p.workers)

	// Trigger the scale down callback if defined
	if removedCount > 0 && p.onScaleDown != nil {
		p.onScaleDown(beforeCount, afterCount, metrics)
	}

	metrics.LastScaleTime = time.Now()
	logs.Info(context.Background(), "Scaled down workers",
		"before", beforeCount,
		"removed", removedCount,
		"after", afterCount,
		"loadRatio", float64(metrics.QueueSize+metrics.ProcessingCount)/float64(metrics.WorkerCount))

	// Signal all waiting workers
	p.cond.Broadcast()
}

// UpdateConfig allows updating the autoscaler configuration
func (p *Pool[T]) UpdateScalingConfig(config AutoscalerConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.autoscaler != nil {
		p.autoscaler.config = config
	}
}

// StopAutoscaler stops the autoscaler if it's running
func (p *Pool[T]) StopAutoscaler() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.autoscaler != nil {
		p.autoscaler.cancelFunc()
		p.autoscaler = nil
	}
}

// First, move redistributeTasksLocked to be a method of Pool
func (p *Pool[T]) redistributeTasksLocked(workerID int, queue TaskQueue[T]) {
	tasks := queue.Tasks()
	availableWorkers := make([]int, 0)

	for wID, wState := range p.workers {
		if wID != workerID && !wState.removed {
			availableWorkers = append(availableWorkers, wID)
		}
	}

	if len(availableWorkers) > 0 {
		for i, task := range tasks {
			targetWorkerID := availableWorkers[i%len(availableWorkers)]
			task.mu.Lock()
			task.triedWorkers = make(map[int]bool)
			task.queuedAt = append(task.queuedAt, time.Now())
			task.mu.Unlock()

			targetQueue := p.taskQueues[targetWorkerID]
			if targetQueue == nil {
				targetQueue = &taskQueue[T]{}
				p.taskQueues[targetWorkerID] = targetQueue
			}
			targetQueue.Enqueue(task)

			logs.Debug(context.Background(), "Redistributed task from removed worker",
				"fromWorkerID", workerID,
				"toWorkerID", targetWorkerID)
		}
	}
}
