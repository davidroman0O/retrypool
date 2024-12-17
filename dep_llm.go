package retrypool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Errors
var (
	ErrTaskAlreadyExists = errors.New("task already exists in group")
	ErrCyclicDependency  = errors.New("cyclic dependency detected")
	ErrMissingDependency = errors.New("missing dependency")
)

// DependencyPool manages tasks with dependencies
type DependencyPool[T any, GID comparable, TID comparable] struct {
	pooler        Pooler[T]
	workerFactory WorkerFactory[T]
	config        DependencyConfig[T, GID, TID]

	mu     sync.RWMutex
	groups map[GID]*groupState[T, GID, TID]
}

// DependencyConfig holds configuration for the dependency pool
type DependencyConfig[T any, GID comparable, TID comparable] struct {
	// Add any configuration options you need
	MaxConcurrentTasks int
}

// groupState tracks the state of tasks within a group
type groupState[T any, GID comparable, TID comparable] struct {
	id            GID
	mu            sync.RWMutex
	tasks         map[TID]*taskState[T, GID, TID]
	processing    map[TID]struct{}
	completed     map[TID]struct{}
	pendingCount  int
	currentActive int // tracks currently active tasks for MaxConcurrentTasks
}

type dependencyTaskConfig[T any, GID comparable, TID comparable] struct {
	waitForCompletion bool // will have to wait for dependent task to complete before processing (non-blocking processing) ELSE will be processed in parallel (blocking processing)
}

type dependencyTaskOption[T any, GID comparable, TID comparable] func(*dependencyTaskConfig[T, GID, TID])

func WithWaitForCompletion[T any, GID comparable, TID comparable]() dependencyTaskOption[T, GID, TID] {
	return func(cfg *dependencyTaskConfig[T, GID, TID]) {
		cfg.waitForCompletion = true
	}
}

type DependentTask[GID comparable, TID comparable] interface {
	GetDependencies() []TID
	GetGroupID() GID
	GetTaskID() TID
}

// taskState tracks individual task state
type taskState[T any, GID comparable, TID comparable] struct {
	data              T
	id                TID
	groupID           GID
	dependencies      []TID
	waitForCompletion bool
	isProcessing      bool
	isCompleted       bool
}

func NewDependencyPool[T any, GID comparable, TID comparable](
	pooler Pooler[T],
	workerFactory WorkerFactory[T],
	config DependencyConfig[T, GID, TID],
) (*DependencyPool[T, GID, TID], error) {
	dp := &DependencyPool[T, GID, TID]{
		pooler:        pooler,
		workerFactory: workerFactory,
		config:        config,
		groups:        make(map[GID]*groupState[T, GID, TID]),
	}

	// Set up task completion handler
	pooler.SetOnTaskSuccess(func(data T) {
		if dtask, ok := any(data).(DependentTask[GID, TID]); ok {
			dp.markTaskCompleted(dtask.GetGroupID(), dtask.GetTaskID())
		}
	})

	return dp, nil
}

func (dp *DependencyPool[T, GID, TID]) Submit(data T, opt ...dependencyTaskOption[T, GID, TID]) error {
	dtask, ok := any(data).(DependentTask[GID, TID])
	if !ok {
		return errors.New("data does not implement DependentTask interface")
	}

	cfg := dependencyTaskConfig[T, GID, TID]{}
	for _, o := range opt {
		o(&cfg)
	}

	dp.mu.Lock()
	// Get or create group state
	group, exists := dp.groups[dtask.GetGroupID()]
	if !exists {
		group = &groupState[T, GID, TID]{
			id:         dtask.GetGroupID(),
			tasks:      make(map[TID]*taskState[T, GID, TID]),
			processing: make(map[TID]struct{}),
			completed:  make(map[TID]struct{}),
		}
		dp.groups[dtask.GetGroupID()] = group
	}
	dp.mu.Unlock()

	group.mu.Lock()
	// Check if task already exists in this group
	if _, exists := group.tasks[dtask.GetTaskID()]; exists {
		group.mu.Unlock()
		return fmt.Errorf("%w: task ID %v in group %v", ErrTaskAlreadyExists, dtask.GetTaskID(), dtask.GetGroupID())
	}

	// Create task state
	task := &taskState[T, GID, TID]{
		data:              data,
		id:                dtask.GetTaskID(),
		groupID:           dtask.GetGroupID(),
		dependencies:      dtask.GetDependencies(),
		waitForCompletion: cfg.waitForCompletion,
	}

	// Add task to group tracking
	group.tasks[task.id] = task
	group.pendingCount++
	group.mu.Unlock()

	// Check dependencies and submit if ready
	if err := dp.checkAndSubmitTasks(group); err != nil {
		return err
	}

	return nil
}

func (dp *DependencyPool[T, GID, TID]) checkAndSubmitTasks(group *groupState[T, GID, TID]) error {
	group.mu.Lock()
	defer group.mu.Unlock()

	readyTasks := dp.findReadyTasks(group)
	if len(readyTasks) == 0 {
		return nil
	}

	fmt.Printf("Found %d ready tasks\n", len(readyTasks))
	for _, task := range readyTasks {
		if err := dp.submitTask(task, group); err != nil {
			return fmt.Errorf("failed to submit task %v: %w", task.id, err)
		}
	}

	return nil
}

func (dp *DependencyPool[T, GID, TID]) findReadyTasks(group *groupState[T, GID, TID]) []*taskState[T, GID, TID] {
	var readyTasks []*taskState[T, GID, TID]

	// If waitForCompletion is true for any running task, don't process more tasks
	if len(group.processing) > 0 {
		for _, task := range group.tasks {
			if task.isProcessing && task.waitForCompletion {
				return nil
			}
		}
	}

	for _, task := range group.tasks {
		if task.isProcessing || task.isCompleted {
			continue
		}

		// Check if all dependencies are completed
		allDepsCompleted := true
		for _, depID := range task.dependencies {
			if _, completed := group.completed[depID]; !completed {
				allDepsCompleted = false
				break
			}
		}

		if allDepsCompleted {
			readyTasks = append(readyTasks, task)
		}
	}

	return readyTasks
}

func (dp *DependencyPool[T, GID, TID]) submitTask(task *taskState[T, GID, TID], group *groupState[T, GID, TID]) error {
	// Mark task as processing
	task.isProcessing = true
	group.processing[task.id] = struct{}{}

	// Create callbacks for task state tracking
	options := []TaskOption[T]{
		WithRunningCb[T](func() {
			group.mu.Lock()
			task.isProcessing = true
			group.mu.Unlock()
		}),
		WithProcessedCb[T](func() {
			group.mu.Lock()
			task.isCompleted = true
			delete(group.processing, task.id)
			group.completed[task.id] = struct{}{}
			group.pendingCount--
			group.mu.Unlock()

			// Check for next tasks
			if err := dp.checkAndSubmitTasks(group); err != nil {
				fmt.Printf("Error submitting next tasks: %v\n", err)
			}
		}),
	}

	fmt.Printf("Submitting task ID %v in group %v with deps %v\n", task.id, task.groupID, task.dependencies)
	return dp.pooler.Submit(task.data, options...)
}

func (dp *DependencyPool[T, GID, TID]) markTaskCompleted(groupID GID, taskID TID) {
	dp.mu.RLock()
	group, exists := dp.groups[groupID]
	dp.mu.RUnlock()

	if exists {
		group.mu.Lock()
		if task, exists := group.tasks[taskID]; exists {
			if !task.isCompleted { // Only process if not already completed
				fmt.Printf("Completing task ID %v in group %v\n", taskID, groupID)
				task.isCompleted = true
				delete(group.processing, taskID)
				group.completed[taskID] = struct{}{}
				group.pendingCount--
			}
		}
		group.mu.Unlock()

		// Check for next tasks
		if err := dp.checkAndSubmitTasks(group); err != nil {
			fmt.Printf("Error submitting next tasks after completion: %v\n", err)
		}
	}
}

func (dp *DependencyPool[T, GID, TID]) WaitWithCallback(ctx context.Context, callback func(queueSize, processingCount, deadTaskCount int) bool, interval time.Duration) error {
	return dp.pooler.WaitWithCallback(ctx, callback, interval)
}

func (dp *DependencyPool[T, GID, TID]) Close() error {
	// We can't hold the pool mutex while waiting for groups
	var wg sync.WaitGroup

	// First get all groups while holding the lock
	dp.mu.RLock()
	groups := make([]*groupState[T, GID, TID], 0, len(dp.groups))
	for _, group := range dp.groups {
		groups = append(groups, group)
	}
	dp.mu.RUnlock()

	// Now wait for each group to complete
	for _, group := range groups {
		wg.Add(1)
		go func(g *groupState[T, GID, TID]) {
			defer wg.Done()
			for {
				g.mu.Lock()
				processing := len(g.processing)
				g.mu.Unlock()

				if processing == 0 {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
		}(group)
	}

	wg.Wait()
	return dp.pooler.Close()
}
