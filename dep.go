package retrypool

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sasha-s/go-deadlock"
)

// TaskMode defines whether tasks are blocking (wait for child tasks) or independent
type TaskMode int

const (
	TaskModeBlocking    TaskMode = iota // Tasks can create and must wait for child tasks
	TaskModeIndependent                 // Tasks run independently without waiting
)

// ExecutionOrder defines the sequence in which tasks are processed
type ExecutionOrder int

const (
	ExecutionOrderForward ExecutionOrder = iota // Process tasks in order (1->2->3)
	ExecutionOrderReverse                       // Process tasks in reverse order (3->2->1)
)

// DependencyConfig holds configuration for the dependency pool
type DependencyConfig[T any, GID comparable, TID comparable] struct {
	TaskMode       TaskMode
	ExecutionOrder ExecutionOrder
	MinWorkers     int
	MaxWorkers     int
	ScaleUpRatio   float64
}

// DependentTask interface must be implemented by task data to provide dependency information
type DependentTask[GID comparable, TID comparable] interface {
	GetDependencies() []TID
	GetGroupID() GID
	GetTaskID() TID
}

// taskState holds the state and metadata for a task
type taskState[T any, GID comparable, TID comparable] struct {
	task         T
	taskID       TID
	groupID      GID
	dependencies []TID
	children     []TID
	submitted    bool
	completed    bool
	mu           deadlock.RWMutex
	completionCh chan struct{}
}

// taskGroup manages tasks within the same group
type taskGroup[T any, GID comparable, TID comparable] struct {
	id        GID
	tasks     map[TID]*taskState[T, GID, TID]
	completed map[TID]bool
	order     []TID
	mu        deadlock.RWMutex
}

// DependencyPool manages task dependencies and execution
type DependencyPool[T any, GID comparable, TID comparable] struct {
	pooler        Pooler[T]
	workerFactory WorkerFactory[T]
	config        DependencyConfig[T, GID, TID]

	mu          deadlock.RWMutex
	taskGroups  map[GID]*taskGroup[T, GID, TID]
	workerCount int
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewDependencyPool creates a new dependency pool with the given configuration
func NewDependencyPool[T any, GID comparable, TID comparable](
	pooler Pooler[T],
	workerFactory WorkerFactory[T],
	config DependencyConfig[T, GID, TID],
) (*DependencyPool[T, GID, TID], error) {
	if config.MinWorkers <= 0 {
		config.MinWorkers = 1
	}
	if config.MaxWorkers <= 0 {
		config.MaxWorkers = 100
	}
	if config.ScaleUpRatio <= 0 {
		config.ScaleUpRatio = 2.0
	}

	ctx, cancel := context.WithCancel(context.Background())
	dp := &DependencyPool[T, GID, TID]{
		pooler:        pooler,
		workerFactory: workerFactory,
		config:        config,
		taskGroups:    make(map[GID]*taskGroup[T, GID, TID]),
		ctx:           ctx,
		cancel:        cancel,
	}

	// Initialize minimum number of workers
	for i := 0; i < config.MinWorkers; i++ {
		worker := workerFactory()
		if err := pooler.Add(worker, nil); err != nil {
			return nil, fmt.Errorf("failed to add initial worker: %w", err)
		}
		dp.workerCount++
	}

	// Set up task completion handler
	pooler.SetOnTaskSuccess(dp.handleTaskCompletion)

	return dp, nil
}

// Submit submits a new task to the pool
func (dp *DependencyPool[T, GID, TID]) Submit(data T) error {
	dtask, ok := any(data).(DependentTask[GID, TID])
	if !ok {
		return errors.New("data does not implement DependentTask interface")
	}

	dp.mu.Lock()
	defer dp.mu.Unlock()

	groupID := dtask.GetGroupID()
	taskID := dtask.GetTaskID()
	deps := dtask.GetDependencies()

	// Scale workers if needed
	if err := dp.scaleWorkersIfNeeded(); err != nil {
		return fmt.Errorf("worker scaling failed: %w", err)
	}

	// Get or create task group
	group, exists := dp.taskGroups[groupID]
	if !exists {
		group = &taskGroup[T, GID, TID]{
			id:        groupID,
			tasks:     make(map[TID]*taskState[T, GID, TID]),
			completed: make(map[TID]bool),
			order:     make([]TID, 0),
		}
		dp.taskGroups[groupID] = group
	}

	// Create task state
	task := &taskState[T, GID, TID]{
		task:         data,
		taskID:       taskID,
		groupID:      groupID,
		dependencies: deps,
		completionCh: make(chan struct{}),
	}

	group.mu.Lock()
	group.tasks[taskID] = task
	group.order = append(group.order, taskID)
	group.mu.Unlock()

	// Handle based on task mode and execution order
	if dp.config.TaskMode == TaskModeBlocking {
		// In blocking mode, tasks can start immediately but may need to wait for children
		return dp.submitTask(task)
	}

	// In independent mode, check if we can start based on execution order
	if dp.canStartIndependentTask(group, task) {
		return dp.submitTask(task)
	}

	return nil // Task stored for later execution
}

// canStartIndependentTask checks if a task can start based on execution order
func (dp *DependencyPool[T, GID, TID]) canStartIndependentTask(group *taskGroup[T, GID, TID], task *taskState[T, GID, TID]) bool {
	group.mu.RLock()
	defer group.mu.RUnlock()

	if dp.config.ExecutionOrder == ExecutionOrderForward {
		// Check if all dependencies are complete
		for _, depID := range task.dependencies {
			if !group.completed[depID] {
				return false
			}
		}
		return true
	}

	// In reverse order, only start if no other tasks depend on this one
	for _, otherTask := range group.tasks {
		for _, depID := range otherTask.dependencies {
			if depID == task.taskID && !otherTask.completed {
				return false
			}
		}
	}
	return true
}

// submitTask submits a task to the worker pool
func (dp *DependencyPool[T, GID, TID]) submitTask(task *taskState[T, GID, TID]) error {
	task.mu.Lock()
	if task.submitted {
		task.mu.Unlock()
		return nil
	}
	task.submitted = true
	task.mu.Unlock()

	// Use bounce retry to ensure parent and child tasks run on different workers
	opts := []TaskOption[T]{}
	if dp.config.TaskMode == TaskModeBlocking {
		opts = append(opts, WithBounceRetry[T]())
	}

	return dp.pooler.SubmitToFreeWorker(task.task, opts...)
}

// handleTaskCompletion processes task completion
func (dp *DependencyPool[T, GID, TID]) handleTaskCompletion(data T) {
	dtask := any(data).(DependentTask[GID, TID])

	dp.mu.Lock()
	group, exists := dp.taskGroups[dtask.GetGroupID()]
	if !exists {
		dp.mu.Unlock()
		return
	}
	dp.mu.Unlock()

	// Handle completion first
	group.mu.Lock()
	task := group.tasks[dtask.GetTaskID()]
	if task != nil {
		task.completed = true
		close(task.completionCh)
		group.completed[dtask.GetTaskID()] = true
	}
	group.mu.Unlock()

	// If we're in independent mode, check for next tasks
	if dp.config.TaskMode == TaskModeIndependent {
		var tasksToCheck []TID

		group.mu.RLock()
		if dp.config.ExecutionOrder == ExecutionOrderForward {
			tasksToCheck = append([]TID{}, group.order...)
		} else {
			// Create reversed copy of order
			tasksToCheck = make([]TID, len(group.order))
			for i := len(group.order) - 1; i >= 0; i-- {
				tasksToCheck[len(group.order)-1-i] = group.order[i]
			}
		}
		group.mu.RUnlock()

		// Check each task without holding the main lock
		for _, taskID := range tasksToCheck {
			group.mu.RLock()
			task := group.tasks[taskID]
			group.mu.RUnlock()

			if task != nil && !task.submitted {
				if dp.canStartIndependentTask(group, task) {
					dp.submitTask(task)
				}
			}
		}
	}
}

// processNextIndependentTasks starts any tasks that are now eligible to run
func (dp *DependencyPool[T, GID, TID]) processNextIndependentTasks(group *taskGroup[T, GID, TID]) {
	if dp.config.ExecutionOrder == ExecutionOrderForward {
		for _, taskID := range group.order {
			task := group.tasks[taskID]
			if !task.submitted && dp.canStartIndependentTask(group, task) {
				dp.submitTask(task)
			}
		}
	} else {
		// Process in reverse order
		for i := len(group.order) - 1; i >= 0; i-- {
			task := group.tasks[group.order[i]]
			if !task.submitted && dp.canStartIndependentTask(group, task) {
				dp.submitTask(task)
			}
		}
	}
}

// WaitForTask waits for a specific task to complete
func (dp *DependencyPool[T, GID, TID]) WaitForTask(groupID GID, taskID TID) error {
	dp.mu.RLock()
	group, exists := dp.taskGroups[groupID]
	if !exists {
		dp.mu.RUnlock()
		return fmt.Errorf("group %v not found", groupID)
	}

	group.mu.RLock()
	task, exists := group.tasks[taskID]
	if !exists {
		group.mu.RUnlock()
		dp.mu.RUnlock()
		return fmt.Errorf("task %v not found", taskID)
	}
	completionCh := task.completionCh
	group.mu.RUnlock()
	dp.mu.RUnlock()

	select {
	case <-completionCh:
		return nil
	case <-dp.ctx.Done():
		return dp.ctx.Err()
	}
}

// WaitWithCallback waits for tasks to complete with a callback for monitoring
func (dp *DependencyPool[T, GID, TID]) WaitWithCallback(
	ctx context.Context,
	callback func(queueSize, processingCount, deadTaskCount int) bool,
	interval time.Duration,
) error {
	return dp.pooler.WaitWithCallback(ctx, callback, interval)
}

// scaleWorkersIfNeeded scales the worker pool if needed
func (dp *DependencyPool[T, GID, TID]) scaleWorkersIfNeeded() error {
	currentWorkers := dp.pooler.GetFreeWorkers()

	totalTasks := int64(0)
	dp.pooler.RangeWorkerQueues(func(workerID int, queueSize int64) bool {
		totalTasks += queueSize
		return true
	})
	totalTasks += dp.pooler.ProcessingCount()

	desiredWorkers := int(float64(totalTasks) / dp.config.ScaleUpRatio)
	if desiredWorkers < dp.config.MinWorkers {
		desiredWorkers = dp.config.MinWorkers
	}
	if desiredWorkers > dp.config.MaxWorkers {
		desiredWorkers = dp.config.MaxWorkers
	}

	for i := len(currentWorkers); i < desiredWorkers; i++ {
		worker := dp.workerFactory()
		if err := dp.pooler.Add(worker, nil); err != nil {
			return err
		}
		dp.workerCount++
	}

	return nil
}

// Close gracefully shuts down the pool
func (dp *DependencyPool[T, GID, TID]) Close() error {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	dp.cancel()
	dp.taskGroups = make(map[GID]*taskGroup[T, GID, TID])
	return dp.pooler.Close()
}

// GetWorkerCount returns the current number of workers
func (dp *DependencyPool[T, GID, TID]) GetWorkerCount() int {
	dp.mu.RLock()
	defer dp.mu.RUnlock()
	return dp.workerCount
}

// ScaleTo scales the worker pool to a specific size
func (dp *DependencyPool[T, GID, TID]) ScaleTo(count int) error {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	if count < dp.config.MinWorkers {
		count = dp.config.MinWorkers
	}
	if count > dp.config.MaxWorkers {
		count = dp.config.MaxWorkers
	}

	currentWorkers, err := dp.pooler.Workers()
	if err != nil {
		return err
	}

	// Remove excess workers
	for i := len(currentWorkers); i > count; i-- {
		if err := dp.pooler.Remove(currentWorkers[i-1]); err != nil {
			return err
		}
		dp.workerCount--
	}

	// Add needed workers
	for i := len(currentWorkers); i < count; i++ {
		worker := dp.workerFactory()
		if err := dp.pooler.Add(worker, nil); err != nil {
			return err
		}
		dp.workerCount++
	}

	return nil
}
